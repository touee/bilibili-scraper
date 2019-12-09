import Foundation
import BilibiliAPI
import BilibiliEntityDB

extension Array where Element == EnqueuedTask {
    enum TaskStatus {
        case frozen
        case pass
    }
    func getCount(of type: TaskType, with status: TaskStatus?) -> Int {
        self.filter {
            if let status = status {
                return $0.task.input.type == type &&
                $0.shouldFreeze == (status == .frozen)
            }
            return $0.task.input.type == type
        }.count
    }
}

class ReportCenter {
    internal class TaskStatus: NSCopying, CustomStringConvertible {
        internal var query: APIQuery
        internal var founds: EntityCollection! = nil
        internal struct NewFounds {
            internal let users: [UInt64]?
            internal let tags: [UInt64]?
            internal let videos: [UInt64]?
            internal let folders: [(uid: UInt64, fid: UInt64)]?
//            let subregions: [Int]
        }
        internal var newFounds: NewFounds! = nil
        internal var newTasks: [EnqueuedTask]! = nil
        
        internal init(query: APIQuery) {
            self.query = query
        }
        internal init(query: APIQuery, founds: EntityCollection?, newFounds: NewFounds?, newTasks: [EnqueuedTask]?) {
            self.query = query
            self.founds = founds
            self.newFounds = newFounds
            self.newTasks = newTasks
        }
        func copy(with zone: NSZone? = nil) -> Any {
            return TaskStatus(query: self.query,
                              founds: self.founds,
                              newFounds: self.newFounds,
                              newTasks: self.newTasks)
        }
        
        var description: String {
            var result = ""
            if let users = self.founds.users {
                result += """
                    [users]: \(users.count) found, \(self.newFounds.users!.count) newly found
                        new [Submissions] tasks: \(self.newTasks.getCount(of: .user_submissions, with: .pass)) pass, \(self.newTasks.getCount(of: .user_submissions, with: .frozen)) frozen
                        new [FolderList] tasks: \(self.newTasks.getCount(of: .folder_favoriteFolder, with: .pass)) pass, \(self.newTasks.getCount(of: .folder_favoriteFolder, with: .frozen)) frozen
                    """
            }
            if let tags = self.founds.tags {
                if !result.isEmpty { result += "\n" }
                result += """
                    [tags]: \(tags.count) found, \(self.newFounds.tags!.count) newly found
                        new [Detail] tasks: \(self.newTasks.getCount(of: .tag_detail, with: .pass)) pass, \(self.newTasks.getCount(of: .tag_detail, with: .frozen)) frozen
                        new [Top] tasks: \(self.newTasks.getCount(of: .tag_top, with: .pass)) pass, \(self.newTasks.getCount(of: .tag_top, with: .frozen)) frozen
                    """
            }
            if let videos = self.founds.videos {
                if !result.isEmpty { result += "\n" }
                result += """
                    [video]: \(videos.count) found, \(self.newFounds.videos!.count) newly found
                        new [RelatedVideos] tasks: \(self.newTasks.getCount(of: .video_relatedVideos, with: .pass)) pass, \(self.newTasks.getCount(of: .video_relatedVideos, with: .frozen)) frozen
                        new [Tags] tasks: \(self.newTasks.getCount(of: .video_tags, with: .pass)) pass, \(self.newTasks.getCount(of: .video_tags, with: .frozen)) frozen
                    """
            }
            if let folders = self.founds.folders {
                if !result.isEmpty { result += "\n" }
                result += """
                    [folders]: \(folders.count) found, \(self.newFounds.folders!.count) newly found
                        new [Videos] tasks: \(self.newTasks.getCount(of: .folder_favoriteFolder, with: .pass)) pass, frozen \(self.newTasks.getCount(of: .folder_favoriteFolder, with: .frozen))
                    """
            }
            return result
        }
    }
    var currentTasks = [Int64: TaskStatus]() // TaskID -> TaskStatus
    var lock = NSLock()
    
    func reportNewQuery(taskID: Int64, query: APIQuery) {
        lock.lock()
        defer { lock.unlock() }
        self.currentTasks[taskID] = .init(query: query)
    }
    func reportFound(taskID: Int64, found: EntityCollection) {
        lock.lock()
        defer { lock.unlock() }
        self.currentTasks[taskID]!.founds = found
    }
    func reportNewFound(taskID: Int64, newFounds: EntityCollection) {
        lock.lock()
        defer { lock.unlock() }
        self.currentTasks[taskID]!.newFounds = .init(
            users: newFounds.users?.map { $0.uid },
            tags: newFounds.tags?.map { $0.tid },
            videos: newFounds.videos?.map { $0.aid },
            folders: newFounds.folders?.map { (uid: $0.owner_uid, fid: $0.fid) })
    }
    func reportNewTasks(taskID: Int64, newTasks: [EnqueuedTask]) {
        lock.lock()
        defer { lock.unlock() }
        self.currentTasks[taskID]!.newTasks = newTasks
    }
    func snapshot(of taskID: Int64) -> TaskStatus {
        lock.lock()
        defer { lock.unlock() }
        return self.currentTasks[taskID]!.copy() as! TaskStatus
    }
    func reportFinish(taskID: Int64) {
        lock.lock()
        defer { lock.unlock() }
        self.currentTasks[taskID] = nil
    }
}
