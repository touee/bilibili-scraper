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
    
    public func update(users: inout [UserEntity]?, tags: inout [TagEntity]?,
                       subregions: inout [SubregionEntity]?,
                       videos: inout [VideoEntity]?, folders: inout [FolderEntity]?,
                       folderItmes: (uid: UInt64, fid: UInt64, items: [FolderVideo])?,
                       videosTags: [(aid: UInt64, tags: [VideoTag])]?,
                       userCurrentVisibleVideoCount: (UInt64, Int)?) {
        try! connection.transaction {
            if var users = users {
                for i in 0..<users.count {
                    self.userTable.update(user: &users[i])
                }
            }
            if let countInfo = userCurrentVisibleVideoCount {
                self.userTable.updateCurrentVisibleVideoCount(uid: countInfo.0,
                                                              count: countInfo.1)
            }
            if var tags = tags {
                for i in 0..<tags.count {
                    self.tagTable.update(tag: &tags[i])
                }
            }
            if var subregions = subregions {
                for i in 0..<subregions.count {
                    self.subregionTable.update(subregion: &subregions[i])
                }
            }
            if var videos = videos {
                for i in 0..<videos.count {
                    self.videoTable.update(video: &videos[i])
                }
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
        }
    }
    
    // update but don't care about updated result
    public func update(users: [UserEntity]?, tags: [TagEntity]?,
    subregions: [SubregionEntity]?,
    videos: [VideoEntity]?, folders: [FolderEntity]?,
    folderItmes: (uid: UInt64, fid: UInt64, items: [FolderVideo])?,
    videosTags: [(aid: UInt64, tags: [VideoTag])]?,
    userCurrentVisibleVideoCount: (UInt64, Int)?) {
        var users = users
        var tags = tags
        var subregions = subregions
        var videos = videos
        var folders = folders
        self.update(users: &users, tags: &tags, subregions: &subregions, videos: &videos, folders: &folders, folderItmes: folderItmes, videosTags: videosTags, userCurrentVisibleVideoCount: userCurrentVisibleVideoCount)
    }
    
    public func updateUserHidesFolders(uid: UInt64, value: Bool) {
        try! self.connection.transaction {
            self.userTable.updateHidesFolders(uid: uid, value: value)
        }
    }
}
