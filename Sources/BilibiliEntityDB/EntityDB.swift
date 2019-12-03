import Foundation
import SQLite

final public class EntityDB {
    public let connection: Connection
    
    lazy var videoTable = VideoTable(db: self)
    lazy var userTable = UserTable(db: self)
    lazy var tagTable = TagTable(db: self)
    lazy var subregionTable = SubregionTable(db: self)
    lazy var folderTable = FolderTable(db: self)
//    lazy var subregionTable =
    
    public init(path: String) throws {
        self.connection = try Connection(path)
        self.connection.busyHandler({ (_) in true })
    }
    
    public func update(videos: inout [VideoEntity]?, users: inout [UserEntity]?,
                       tags: inout [TagEntity]?, folders: inout [FolderEntity]?,
                       folderItmes: inout (uid: UInt64, fid: UInt64, items: [FolderVideo])?,
                       videosTags: [(aid: UInt64, tags: [VideoTag])]?,
                       userCurrentVisibleVideoCount: (UInt64, Int)?) {
        try! connection.transaction {
            if var videos = videos {
                for i in 0..<videos.count {
                    self.videoTable.update(video: &videos[i])
                }
            }
            if var users = users {
                for i in 0..<users.count {
                    self.userTable.update(user: &users[i])
                }
            }
            if var tags = tags {
                for i in 0..<tags.count {
                    self.tagTable.update(tag: &tags[i])
                }
            }
            if var folders = folders {
                for i in 0..<folders.count {
                    self.folderTable.update(folder: &folders[i])
                    self.userTable.updateHidesFolders(uid: folders[i].owner_uid, value: false)
                }
            }
            if let folderItmes = folderItmes {
                self.folderTable.insertFolderVideoItems(
                    uid: folderItmes.uid, fid: folderItmes.fid,
                    items: folderItmes.items)
            }
            if let videosTags = videosTags {
                for videoTags in videosTags {
                    let isCompleteList = (videoTags.tags.count == 0
                        || videoTags.tags[0].info != nil)
                    videoTags.tags.forEach { if ($0.info != nil) != isCompleteList {
                        fatalError("tagsBelonged 传入的标签类型需一致!")
                        } }
                    for tag in videoTags.tags {
                        self.videoTable.insertVideoTag(for: videoTags.aid, tid: tag.tid, info: tag.info)
                    }
                    if isCompleteList {
                        self.videoTable.updateIsTagListCompleteToTrue(for: videoTags.aid)
                    }
                }
            }
            if let countInfo = userCurrentVisibleVideoCount {
                self.userTable.updateCurrentVisibleVideoCount(uid: countInfo.0,
                                                              count: countInfo.1)
            }
        }
    }
    
    public func updateSubregion(subregion: inout SubregionEntity) {
        try! self.connection.transaction {
            self.subregionTable.update(subregion: &subregion)
        }
    }
    
    public func updateUserHidesFolders(uid: UInt64, value: Bool) {
        try! self.connection.transaction {
            self.userTable.updateHidesFolders(uid: uid, value: value)
        }
    }
    
}
