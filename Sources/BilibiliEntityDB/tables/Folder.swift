import Foundation
import SQLite

public struct FolderVideo: Codable {
    public let aid: UInt64
    public let favorite_time: Int64
    
    public init(aid: UInt64, favorite_time: Int64) {
        self.aid = aid
        self.favorite_time = favorite_time
    }
    
    public func encode(to encoder: Encoder) {
        var c = encoder.unkeyedContainer()
        try! c.encode(self.aid)
        try! c.encode(self.favorite_time)
    }
    
    public init(from decoder: Decoder) {
        var c = try! decoder.unkeyedContainer()
        self.aid = try! c.decode(UInt64.self)
        self.favorite_time = try! c.decode(Int64.self)
    }
}

public struct FolderEntity {
    // 第一遍
    public var owner_uid: UInt64
    public var fid: UInt64
    public var name: String
    public var capacity: Int
    public var current_item_count: Int //
    public var create_time: Int64 //
    public var modify_time: Int64 //
    public var volatile: String?
    
    public init(owner_uid: UInt64, fid: UInt64, name: String, capacity: Int, current_item_count: Int, create_time: Int64, modify_time: Int64, volatile: String?) {
        self.owner_uid = owner_uid
        self.fid = fid
        self.name = name
        self.capacity = capacity
        self.current_item_count = current_item_count
        self.create_time = create_time
        self.modify_time = modify_time
        self.volatile = volatile
    }
}

class FolderTable: EntityDBTable {
    weak var db: EntityDB!
    static let name = "folder"
    
    static let columns = [
        Column(name: "folder_reference_id", type: .integer, isPrimaryKey: true),
        Column(name: "owner_uid",           type: .integer, references: (table: "user", column: "uid")),
        Column(name: "fid",                 type: .integer),
        Column(name: "name",                type: .text),
        Column(name: "capacity",            type: .integer),
        Column(name: "current_item_count",  type: .integer),
        Column(name: "create_time",         type: .integer),
        Column(name: "modify_time",         type: .integer),
        Column(name: "volatile",            type: .text,    isNullable: true),
    ]
    
    lazy var upsertStatement = try! buildUpsertStatement(
        db: self.connection, table: FolderTable.name,
        columns: FolderTable.columns
            .map { UpsertColumnRule(
                name: $0.name,
                upsertMode: $0.name == "video_items" ? .coalesceFromOld : .coalesceFromNew) },
        matchers: FolderTable.primaryKeyColumnNames)
//    lazy var selectStatement = try! self.buildSelectStatement(matchers: FolderTable.primaryKeyColumnNames)
    init(db: EntityDB) {
        self.db = db
        try! self.createTableIfNotExists()
        try! self.connection.execute(#"""
            CREATE TABLE IF NOT EXISTS folder_video_item (
                folder_reference_id INTEGER NOT NULL,
                item_aid            INTEGER NOT NULL,
                favorite_time         INTEGER NOT NULL,

                PRIMARY KEY(item_aid, folder_reference_id),
                FOREIGN KEY(item_aid) REFERENCES video(aid),
                FOREIGN KEY(folder_reference_id) REFERENCES folder(folder_reference_id)
            )
            """#)
    }
    
    func update(folder: inout FolderEntity) {
        let oldVolatile: String?
        if let old = try! self.connection
            .prepare(#"SELECT volatile FROM folder WHERE fid = ? AND owner_uid = ?"#)
            .bind(Int64(folder.fid), Int64(folder.owner_uid)).fatchNilOrFirstOnlyRow() {
            oldVolatile = old[0] as! String?
        } else {
            oldVolatile = nil
        }
        if let oldVolatile = oldVolatile {
            if let newVolatile = folder.volatile {
                folder.volatile = oldVolatile + "\n" + newVolatile
            } else {
                folder.volatile = oldVolatile
            }
        }
        
        try! self.upsertStatement.bind([
            ":folder_reference_id":  nil,
            ":owner_uid":            Int64(folder.owner_uid),
            ":fid":                  Int64(folder.fid),
            ":name":                 folder.name,
            ":capacity":             folder.capacity,
            ":current_item_count":   folder.current_item_count,
            ":create_time":          folder.create_time,
            ":modify_time":          folder.modify_time,
            ":volatile":             folder.volatile
            ]).run()
        
//        let updated = try! selectStatement.bind([Int64(folder.fid)]).run().getFirstOnlyRow()
    }
    
    lazy var selectFolderReferenceIDStatement = try! self.connection.prepare(#"""
        SELECT folder_reference_id FROM folder WHERE owner_uid = ? AND fid = ?
        """#)
    lazy var insertFolderVideoItemStatement = try! self.connection.prepare(#"""
        INSERT OR REPLACE INTO folder_video_item (folder_reference_id, item_aid, favorite_time) VALUES(?, ?, ?)
        """#)
    
    func insertFolderVideoItems(uid: UInt64, fid: UInt64, items: [FolderVideo]) {
        let folderReferenceID = UInt64(
            self.selectFolderReferenceIDStatement.bind(Int64(uid), Int64(fid)).fetchFirstOnlyRow()[0] as! Int64)
        for item in items {
            try! self.insertFolderVideoItemStatement.bind(Int64(folderReferenceID), Int64(item.aid), item.favorite_time).run()
        }
    }
}
