import Foundation
import SQLite
import JQWrapper
import BilibiliAPI
import SwiftyJSON

let MAX_ATTEMPTS = 3

func parseMetadata(from str: String?) throws -> JSON? {
    guard let str = str else {
        return nil
    }
    return try JSON(data: str.data(using: .utf8)!)
}

func mergeMetadata(for type: TaskType, old: JSON?, new: JSON?) throws -> JSON? {
    guard let old = old else { return new }
    guard let new = new else { return old }
    let out = try old.merged(with: new)
    switch type {
    case .search: return out
    case .tag_detail: return out
    case .tag_top:
        // sample_videos
        return out
    case .video_relatedVideos:
        // referrers
        return out
    case .video_tags: return out
    case .user_submissions: return out
    case .user_favoriteFolderList: return out
    case .folder_videoItems: return out
    }
}

enum TaskStatus: Int {
    case pending    = 0
    case running    = 1
    case done       = -1
    case frozen     = -2
}
public enum TaskInitialStatus {
    case pending
    case done
    case frozen
    
    var statusInQueue: TaskStatus {
        switch self {
        case .pending: return .pending
        case .done: return .done
        case .frozen: return .frozen
        }
    }
}

public struct EnqueuedTask {
    var task: APITask
    var initialStatus: TaskInitialStatus
    var priority: Double
    var referrer: TaskQueueDB.Referrer
    public init(_ task: APITask, initialStatus: TaskInitialStatus = .pending, priority: Double = 0, referrer: TaskQueueDB.Referrer) {
        self.task = task
        self.initialStatus = initialStatus
        self.priority = priority
        self.referrer = referrer
    }
}

public enum TaskReport {
    case done
    case doneOnLastPage
    case shouldTurnPage
    case shouldFreezeCurrentProgress
    case shouldFreezeFollowUpProgress
    case failed
}

public class TaskQueueDB {
    public let db: Connection

    lazy var selectAffectedRowCountStatement = try! db.prepare(#"SELECT changes()"#)
    public init(path: String) throws {
        db = try Connection(path)

        db.busyHandler({ (_) in true })

        try! db.execute(#"""
            CREATE TABLE IF NOT EXISTS queue (
                task_id INTEGER NOT NULL,
                
                type INTEGER NOT NULL,
                query_id INTEGER NOT NULL,
                
                priority INTEGER NOT NULL DEFAULT 0,
                status INTEGER NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                
                progress INTEGER NULL,
                metadata TEXT NULL,
                referrers TEXT NOT NULL,
                
                PRIMARY KEY (task_id),
                UNIQUE (type, query_id)
            )
            """#)
        try! db.execute(#"""
            CREATE INDEX IF NOT EXISTS index_queue_task ON queue(
                status,
                priority DESC,
                attempts ASC,
                task_id ASC
            )
            """#)
        try! db.execute(#"""
            CREATE TABLE IF NOT EXISTS query_for_search (
                search_reference_id INTEGER NOT NULL,
                keyword TEXT NOT NULL,
                order_id INTEGER NOT NULL,
                duration_id INTEGER NOT NULL,
                subregion_id INTEGER NOT NULL,

                PRIMARY KEY (search_reference_id),
                UNIQUE (keyword, order_id, duration_id, subregion_id)
            )
            """#)
        try! db.execute(#"""
            CREATE TABLE IF NOT EXISTS query_for_folder (
                folder_reference_id INTEGER NOT NULL,
                owner_uid INTEGER NOT NULL,
                fid INTEGER NOT NULL,

                PRIMARY KEY (folder_reference_id),
                UNIQUE (owner_uid, fid)
            )
            """#)

//        // for test purpose
//        try! db.prepare(#"""
//            UPDATE queue SET status = ? WHERE status = ?
//            """#).bind(TaskStatus.pending.rawValue, TaskStatus.frozen.rawValue).run()
        
        try! db.prepare(#"""
            UPDATE queue SET status = ? WHERE status = ?
            """#).bind(TaskStatus.frozen.rawValue, TaskStatus.running.rawValue).run()
        let cleaned = Int(selectAffectedRowCountStatement.fetchFirstOnlyRow()[0] as! Int64)
        if cleaned != 0 {
            logger.log(.warning, msg: "\(cleaned) tasks have not finished on last run",
                       functionName: #function, lineNum: #line, fileName: #file)
        }
    }
    
    public enum Referrer {
        case taskID(Int64)
        case root
        case ignore // 如: 每个视频都会是其拥有 tag 的 referrer
    }
    
    lazy var selectTaskStatement = try! self.db.prepare(#"""
        SELECT task_id, type, query_id, priority, status, attempts, progress, metadata, referrers FROM queue WHERE type = ? AND query_id = ?
        """#)
    lazy var selectSearchQueryStatement = try! self.db.prepare(#"""
        SELECT search_reference_id
        FROM query_for_search
        WHERE keyword = ? AND order_id = ? AND duration_id = ? AND subregion_id = ?
        """#)
    lazy var selectFolderQueryStatement = try! self.db.prepare(#"""
        SELECT folder_reference_id
        FROM query_for_folder
        WHERE owner_uid = ? AND fid = ?
        """#)
    lazy var insertSearchQueryStatement = try! self.db.prepare(#"""
        INSERT INTO query_for_search (keyword, order_id, duration_id, subregion_id)
        VALUES (?, ?, ?, ?)
        """#)
    lazy var insertFolderQueryStatement = try! self.db.prepare(#"""
        INSERT INTO query_for_folder (owner_uid, fid)
        VALUES (?, ?)
        """#)
    lazy var selectLastRowIDStatement = try! self.db.prepare(#"""
        SELECT last_insert_rowid()
        """#)
    lazy var insertTaskStatement = try! self.db.prepare(#"""
        INSERT OR REPLACE INTO queue (task_id, type, query_id, priority, status, attempts, progress, metadata, referrers) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """#)
    lazy var updateTaskStatement = try! self.db.prepare(#"""
        UPDATE queue
        SET priority = ?, status = ?, attempts = 0, metadata = ?, referrers = ?
        WHERE task_id = ?
        """#)
    
    private func _enqueue(task: APITask, initialStatus: TaskInitialStatus = .pending, priority: Double = 0, referrer: Referrer) throws {
        
        let type = task.query.type
        
        let queryID: Int64?
        let query = TaskQueryInQueue(from: task.query)
        switch query {
        case .direct(let id):
            queryID = Int64(id)
        case .search(let keyword, let order_id, let duration_id, let subregion_id):
            let queryRecord = self.selectSearchQueryStatement.bind(keyword, order_id, duration_id, subregion_id).fatchNilOrFirstOnlyRow()
            if let queryRecord = queryRecord {
                queryID = (queryRecord[0] as! Int64)
            } else {
                queryID = nil
            }
        case .folder(let owner_uid, let fid):
            let queryRecord = self.selectFolderQueryStatement.bind(Int64(owner_uid), Int64(fid)).fatchNilOrFirstOnlyRow()
            if let queryRecord = queryRecord {
                queryID = (queryRecord[0] as! Int64)
            } else {
                queryID = nil
            }
        default:
            fatalError()
        }
        
        let oldSameTaskInQueue: TaskInQueue?
        if let queryID = queryID {
            let oldRecord = self.selectTaskStatement.bind(type.rawValue, queryID).fatchNilOrFirstOnlyRow()
            if let oldRecord = oldRecord {
                oldSameTaskInQueue = try TaskInQueue(fromRecord: oldRecord)
            } else {
                switch type {
                case .search, .folder_videoItems:
                    fatalError("Impossible")
                default:
                    oldSameTaskInQueue = nil
                }
            }
        } else {
            oldSameTaskInQueue = nil
        }
        
        let referrerInQueue: (taskID: Int64, timestamp: Int64)?
        switch referrer {
        case .ignore:
            referrerInQueue = nil
        case .taskID(let taskID):
            referrerInQueue = (taskID: taskID, timestamp: Int64(Date().timeIntervalSince1970))
        case .root:
            referrerInQueue = (taskID: 0, timestamp: Int64(Date().timeIntervalSince1970))
        }
        
        if let oldSameTaskInQueue = oldSameTaskInQueue {
            
            let newStatus: TaskStatus
            if oldSameTaskInQueue.status != .running
                && (initialStatus == .done
                    || oldSameTaskInQueue.status == .frozen) {
                newStatus = initialStatus.statusInQueue
            } else {
                newStatus = oldSameTaskInQueue.status
            }
            
            try self.updateTaskStatement.bind(
                // priority
                max(oldSameTaskInQueue.priority, priority),
                // status
                newStatus.rawValue,
                // metadata
                try mergeMetadata(for: type,
                              old: oldSameTaskInQueue.metadata,
                              new: task.metadata)?.rawString(),
                // referrers
                String(data: try JSONEncoder()
                    .encode((oldSameTaskInQueue.referrers + [referrerInQueue].compactMap { $0 })
                        .map { [$0.taskID, $0.timestamp] }), encoding: .utf8)!,
                oldSameTaskInQueue.taskID!
            ).run()
            return
        }
        
        var newTaskInQueue = TaskInQueue(
            fromNewTask: task,
            priority: priority,
            status: initialStatus,
            metadata: task.metadata,
            referrers: [referrerInQueue].compactMap { $0 }
        )
        switch type {
        case .search, .folder_videoItems:
            if let queryID = queryID {
                logger.log(.warning, msg: "Currently, this branch is impossible to reach!",
                           functionName: #function, lineNum: #line, fileName: #file)
                newTaskInQueue.setQuery(toOuterReferenceID: queryID)
            } else {
                try [
                    TaskType.search: insertSearchQueryStatement,
                    TaskType.folder_videoItems: insertFolderQueryStatement
                ][type]!.bind(newTaskInQueue.queryRow).run()
                let lastRowID = try self.selectLastRowIDStatement.run().fetchFirstOnlyRow()[0] as! Int64
                newTaskInQueue.setQuery(toOuterReferenceID: lastRowID)
            }
        default:
            break
        }
        // attempts 清零
        try insertTaskStatement.bind(newTaskInQueue.row).run()
    }
    public func enqueue(_ tasks: [EnqueuedTask]) throws {
        try self.db.transaction {
            for task in tasks {
                try self._enqueue(
                    task: task.task,
                    initialStatus: task.initialStatus,
                    priority: task.priority,
                    referrer: task.referrer)
            }
        }
    }
    public func enqueue(_ tasks: EnqueuedTask...) throws {
        try self.enqueue(tasks)
    }
    
    lazy var selectTopTaskStatement = try! self.db.prepare(#"""
        SELECT task_id, type, query_id, priority, status, attempts, progress, metadata, referrers
        FROM queue
        WHERE status = \#(TaskStatus.pending.rawValue) AND attempts < \#(MAX_ATTEMPTS)
        ORDER BY priority DESC, attempts ASC, task_id ASC
        LIMIT 1
        """#)
    lazy var selectSearchQueryWithReferrerIDStatement = try! self.db.prepare(#"""
        SELECT keyword, order_id, duration_id, subregion_id
        FROM query_for_search
        WHERE search_reference_id = ?
        """#)
    lazy var selectFolderQueryWithReferrerIDStatement = try! self.db.prepare(#"""
        SELECT owner_uid, fid
        FROM query_for_folder
        WHERE folder_reference_id = ?
        """#)
    lazy var updateTaskStatusToRunningStatement = try! self.db.prepare(#"""
        UPDATE queue SET status = \#(TaskStatus.running.rawValue) WHERE task_id = ?
        """#)
    public func dequeue() throws -> APITask? {
        var nextTaskInQueue: TaskInQueue!
        try self.db.transaction {
            guard let nextTaskRecord =
                self.selectTopTaskStatement.fatchNilOrFirstOnlyRow() else {
                    return // no pending tasks in queue
            }
            
            nextTaskInQueue = try TaskInQueue(fromRecord: nextTaskRecord)
            
            switch nextTaskInQueue.query {
            case .referring(let reference_id):
                nextTaskInQueue.convertQuery(fromOuterReferenceRecord: [
                    TaskType.search: self.selectSearchQueryWithReferrerIDStatement,
                    TaskType.folder_videoItems: self.selectFolderQueryWithReferrerIDStatement
                    ][nextTaskInQueue.type]!.bind(reference_id).fetchFirstOnlyRow()
                )
            case .search, .folder:
                fatalError("What?")
            case .direct:
                break
            }
            
            try updateTaskStatusToRunningStatement.bind(nextTaskInQueue.taskID!).run()
        }
       
        if nextTaskInQueue == nil {
            return nil
        }
        var nextTask = nextTaskInQueue.buildQuery().buildTask(withMetadata: nextTaskInQueue.metadata)
        nextTask.taskID = nextTaskInQueue.taskID
        return nextTask
    }
    
    lazy var updateTaskStatusStatement = try! self.db.prepare(#"""
        UPDATE queue
        SET
            status = ?,
            progress = progress + ?, -- NULL + 1 => NULL
            attempts = CASE ? WHEN 1 THEN 0 ELSE attempts + 1 END
            WHERE task_id = ?
        """#)
    public func report(query: APIQuery, taskID: Int64, _ report: TaskReport) throws {
//        print("reporting: query: \(query), taskID: \(taskID), report: \(report)")
        let nextStatus: TaskStatus
        let delatProgress: Int
        let shouldResetAttempts: Bool
        switch report {
        case .done:
            nextStatus = .done
            delatProgress = 1
            shouldResetAttempts = false
        case .doneOnLastPage:
            nextStatus = .done
            delatProgress = 0
            shouldResetAttempts = false
        case .shouldTurnPage:
            nextStatus = .pending
            delatProgress = 1
            shouldResetAttempts = true
        case .shouldFreezeCurrentProgress:
            nextStatus = .frozen
            delatProgress = 0
            shouldResetAttempts = false
        case .shouldFreezeFollowUpProgress:
            nextStatus = .frozen
            delatProgress = 1
            shouldResetAttempts = true
        case .failed:
            nextStatus = .pending
            delatProgress = 0
            shouldResetAttempts = false
        }
        var updatedCount: Int! = nil
        try db.transaction {
            try updateTaskStatusStatement.bind(
                nextStatus.rawValue,
                delatProgress,
                shouldResetAttempts ? 1 : 0,
                taskID).run()
            updatedCount = Int(selectAffectedRowCountStatement.fetchFirstOnlyRow()[0] as! Int64)
        }
        if updatedCount != 1 {
            fatalError("Bad report! updated rows: \(updatedCount!).")
        }
    }
    
    lazy var selectMetadataStatement = try! self.db.prepare(#"""
        SELECT metadata FROM queue WHERE task_id = ?
        """#)
    lazy var updateMetadataStatement = try! self.db.prepare(#"""
        UPDATE queue
        SET metadata = ?
        WHERE task_id = ?
        """#)
    public func updateMetadata(taskType: TaskType, taskID: Int64, metadata: JSON?) throws {
        try self.db.transaction {
            let oldMetadataText = self.selectMetadataStatement.bind(taskID)
                .fetchFirstOnlyRow()[0] as! String?
            let oldMetadata = try parseMetadata(from: oldMetadataText)
            
            let metadata = try mergeMetadata(for: taskType, old: oldMetadata, new: metadata)
            
            try updateMetadataStatement
                .bind(metadata?.rawString(), taskID)
                .run()
        }
    }
}
