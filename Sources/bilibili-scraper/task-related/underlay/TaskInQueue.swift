import BilibiliAPI
import SwiftyJSON
import SQLite
import Foundation

func numericItemToDouble(_ x: Binding?) -> Double? {
    guard let x = x else {
        return nil
    }
    do {
        if let x = x as? Double {
            return x
        }
        return Double(x as! Int64)
    }
}

public enum TaskQueryInQueue {
    case direct(id: UInt64)
    case search(keyword: String, order_id: Int?, duration_id: Int?, subregion_id: Int?)
    case folder(owner_uid: UInt64, fid: UInt64)
    case referring(reference_id: Int64)
    
    init(from query: APIQuery) {
        switch query {
        case let query as SearchQuery:
            self = .search(keyword: query.keyword, order_id: query.order?.int, duration_id: query.duration?.rawValue, subregion_id: query.regionID)
        case let query as VideoRelatedVideosQuery:
            self = .direct(id: query.aid)
        case let query as VideoTagsQuery:
            self = .direct(id: query.aid)
        case let query as UserSubmissionsQuery:
            self = .direct(id: query.uid)
        case let query as UserFavoriteFolderListQuery:
            self = .direct(id: query.uid)
        case let query as TagDetailQuery:
            self = .direct(id: query.tid)
        case let query as TagTopQuery:
            self = .direct(id: query.tid)
        case let query as FavoriteFolderVideosQuery:
            self = .folder(owner_uid: query.uid, fid: query.fid)
        default:
            fatalError("Unknown query!")
        }
    }
}

public struct TaskInQueue {
    let taskID: Int64?
    
    let type: TaskType
    public private(set) var query: TaskQueryInQueue
    
    let priority: Double
    let status: TaskStatus
    let attempts: Int
    
    let progress: Int?
    let metadata: JSON?
    let referrers: [(taskID: Int64, timestamp: Int64)]
    
    init(taskID: Int64? = nil, fromNewTask task: APITask, priority: Double = 0, shouldFreeze: Bool = false, metadata: JSON? = nil, referrers: [(taskID: Int64, timestamp: Int64)]) {
        self.taskID = taskID
        self.type = task.query.type
        self.query = TaskQueryInQueue(from: task.query)
        self.priority = priority
        if shouldFreeze {
            self.status = .frozen
        } else {
            self.status = .pending
        }
        self.attempts = 0
        if let query = task.query as? MultipageAPIQuery {
            self.progress = query.pageNumber ?? 1
        } else {
            self.progress = nil
        }
        self.metadata = metadata
        self.referrers = referrers
    }
    
    init(fromRecord record: [Binding?]) throws {
        self.taskID = (record[0] as! Int64)
        
        self.type = TaskType(rawValue: Int(record[1] as! Int64))!
        switch self.type {
        case .search, .folder_favoriteFolder:
            self.query = .referring(reference_id: record[2] as! Int64)
        case .video_relatedVideos, .video_tags, .user_submissions, .user_favoriteFolderList, .tag_detail, .tag_top:
            self.query = .direct(id: UInt64(record[2] as! Int64))
        }
        self.priority = numericItemToDouble(record[3])!
        self.status = TaskStatus(rawValue: Int(record[4] as! Int64))!
        self.attempts = Int(record[5] as! Int64)
        self.progress = record[6] == nil ? nil : Int(record[6] as! Int64)
        let metadata = record[7] == nil ? nil : (record[7] as! String)
        if let metadata = metadata {
            self.metadata = try JSON(data: metadata.data(using: .utf8)!)
        } else {
            self.metadata = nil
        }
        let referrers = try JSONDecoder().decode([[Int64]].self, from: (record[8] as! String).data(using: .utf8)!)
        self.referrers = referrers.map { (taskID: $0[0], timestamp: $0[1]) }
    }
    
    mutating func convertQuery(fromOuterReferenceRecord record: [Binding?]) {
        switch self.type {
        case .search:
            if record.count != 4 {
                fatalError("Query item count not match!")
            }
            let order_id = record[1] == nil ? nil : SearchOrder(from: Int(record[1] as! Int64))?.int
            let duration_id = record[2] == nil ? nil : SearchDuration(rawValue: Int(record[2] as! Int64))!.rawValue
            let subregion_id = record[3] == nil ? nil : VideoRegion(rawValue: Int(record[3] as! Int64))!.rawValue
            self.query = .search(keyword: record[0] as! String,
                                 order_id: order_id,
                                 duration_id: duration_id,
                                 subregion_id: subregion_id)
        case .folder_favoriteFolder:
            if record.count != 2 {
                fatalError("Query item count not match!")
            }
            self.query = .folder(owner_uid: UInt64(record[0] as! Int64),
                                 fid: UInt64(record[1] as! Int64))
        default:
            fatalError("Unexpected task type!")
        }
    }
    
    mutating func setQuery(toOuterReferenceID id: Int64) {
        self.query = .referring(reference_id: id)
    }
    
    var row: [Binding?] {
        let queryID: Int64
        switch self.query {
        case .direct(let id):
            queryID = Int64(id)
        case .referring(let reference_id):
            queryID = reference_id
        default:
            fatalError("queryID should be either direct or referring")
        }
        return [
            self.taskID, // auto-increasing if null
            self.type.rawValue,
            queryID,
            self.priority,
            self.status.rawValue,
            self.attempts,
            self.progress,
            self.metadata?.rawString(),
            String(data: try! JSONEncoder().encode(self.referrers.map { [$0.taskID, $0.timestamp] }), encoding: .utf8)!
        ]
    }
    
    var queryRow: [Binding?] {
        switch self.query {
        case .folder(let owner_uid, let fid):
            return [Int64(owner_uid), Int64(fid)]
        case .search(let keyword, let order_id, let duration_id, let subregion_id):
            return [keyword, order_id, duration_id, subregion_id]
        default:
            fatalError("Unexpected task type!")
        }
    }
    
    func buildQuery() -> APIQuery {
        switch self.query {
        case .direct(let id):
            switch self.type {
            case .video_relatedVideos:
                return VideoRelatedVideosQuery(aid: id)
            case .video_tags:
                return VideoTagsQuery(aid: id)
            case .user_submissions:
//                return UserSubmissionsQuery(uid: id, pageNumber: self.progress ?? 1)
                return UserSubmissionSearchQuery(uid: id, keyword: "",
                                                 order: .pubdate,
                                                 pageNumber: self.progress ?? 1,
                                                 pageSize: 100)
            case .user_favoriteFolderList:
                return UserFavoriteFolderListQuery(uid: id)
            case .tag_detail:
                return TagDetailQuery(tid: id, pageNumber: self.progress ?? 1)
            case .tag_top:
                return TagTopQuery(tid: id, pageNumber: self.progress ?? 1)
            default:
                fatalError("Unexpected task type!")
            }
        case .search(let keyword, let order_id, let duration_id, let subregion_id):
            return SearchQuery(keyword: keyword, order: SearchOrder.init(from: order_id ?? -1), duration: SearchDuration(rawValue: duration_id ?? -1), regionID: subregion_id ?? -1, pageNumber: self.progress ?? 1)
        case .folder(let owner_uid, let fid):
            return FavoriteFolderVideosQuery(uid: owner_uid, fid: fid, pageNumber: self.progress ?? 1)
        case .referring(_):
            fatalError(#"APIQuery can't be built from TaskInQueue whose TaskQueryInQueue's type is "referring"!"#)
        }
    }
}
