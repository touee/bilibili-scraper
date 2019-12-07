import Foundation
import SQLite

public class EntityCollection {
    public var users: [UserEntity]?
    public var tags: [TagEntity]?
    public var videos: [VideoEntity]?
    public var folders: [FolderEntity]?
    public var subregions: [SubregionEntity]?
    
    public init(users: [UserEntity]?, tags: [TagEntity]?, videos: [VideoEntity]?,folders: [FolderEntity]?, subregions: [SubregionEntity]?) {
        self.users = users
        self.tags = tags
        self.videos = videos
        self.folders = folders
        self.subregions = subregions
    }
}

public class EntityExtra {
    public var folderItmes: (uid: UInt64, fid: UInt64, items: [FolderVideo])?
    public var videosTags: [(aid: UInt64, tags: [VideoTag])]?
    public var userCurrentVisibleVideoCount: (UInt64, Int)?
    
    public init(folderItmes: (uid: UInt64, fid: UInt64, items: [FolderVideo])?, videosTags: [(aid: UInt64, tags: [VideoTag])]?, userCurrentVisibleVideoCount: (UInt64, Int)?) {
        self.folderItmes = folderItmes
        self.videosTags = videosTags
        self.userCurrentVisibleVideoCount = userCurrentVisibleVideoCount
    }
}

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
    
    public func update(_ c: EntityCollection, _ e: EntityExtra) {
        try! connection.transaction {
            if let users = c.users {
                for i in 0..<users.count {
                    self.userTable.update(user: &c.users![i])
                }
            }
            if let countInfo = e.userCurrentVisibleVideoCount {
                self.userTable.updateCurrentVisibleVideoCount(uid: countInfo.0,
                                                              count: countInfo.1)
            }
            if let tags = c.tags {
                for i in 0..<tags.count {
                    self.tagTable.update(tag: &c.tags![i])
                }
            }
            if let subregions = c.subregions {
                for i in 0..<subregions.count {
                    self.subregionTable.update(subregion: &c.subregions![i])
                }
            }
            if let videos = c.videos {
                for i in 0..<videos.count {
                    self.videoTable.update(video: &c.videos![i])
                }
            }
            if let videosTags = e.videosTags {
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
            if let folders = c.folders {
                for i in 0..<folders.count {
                    self.folderTable.update(folder: &c.folders![i])
                    self.userTable.updateHidesFolders(uid: folders[i].owner_uid, value: false)
                }
            }
            if let folderItmes = e.folderItmes {
                self.folderTable.insertFolderVideoItems(
                    uid: folderItmes.uid, fid: folderItmes.fid,
                    items: folderItmes.items)
            }
        }
    }
    
    public func updateUserHidesFolders(uid: UInt64, value: Bool) {
        try! self.connection.transaction {
            self.userTable.updateHidesFolders(uid: uid, value: value)
        }
    }
}
