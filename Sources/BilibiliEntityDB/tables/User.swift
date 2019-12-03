import Foundation
import SQLite

public struct UserEntity {
    // 第一遍
    public var uid: UInt64
    public var name: String
    public var avatar_url: String?
    public var current_visible_video_count: Int?
    
    public init(uid: UInt64, name: String, avatar_url: String?) {
        self.uid = uid
        self.name = name
        self.avatar_url = avatar_url
    }
}

class UserTable: EntityDBTable {
    weak var db: EntityDB!
    static let name = "user"
    
    static let columns = [
        Column(name: "uid", type: .integer, isPrimaryKey: true),
        Column(name: "name", type: .text),
        Column(name: "avatar_url", type: .text, isNullable: true),
        Column(name: "hides_folders", type: .integer, isNullable: true),
        Column(name: "current_visible_video_count", type: .integer, isNullable: true),
    ]
    
    lazy var upsertStatement = try! buildUpsertStatement(
        db: self.connection, table: UserTable.name,
        columns: UserTable.columns
            .map { UpsertColumnRule(name: $0.name, upsertMode: .coalesceFromNew) },
        matchers: UserTable.primaryKeyColumnNames)
    lazy var selectStatement = try! self.buildSelectStatement(matchers: UserTable.primaryKeyColumnNames)
    init(db: EntityDB) {
        self.db = db
        try! self.createTableIfNotExists()
    }
    
    func update(user: inout UserEntity) {
        try! self.upsertStatement.bind([
            ":uid":                          Int64(user.uid),
            ":name":                         user.name,
            ":avatar_url":                   user.avatar_url,
            ":hides_folders":                nil,
            ":current_visible_video_count":  nil,
            ]).run()
        
        let updated = try! selectStatement.bind([
            ":uid": Int64(user.uid)
            ]).run().fetchFirstOnlyRow()
        
        user.name = updated[1] as! String
        user.avatar_url = updated[2] as! String?
    }
    
    lazy var updateHidesFoldersStatement
        = try! self.connection.prepare(
            #"UPDATE user SET hides_folders = ? WHERE uid = ?"#)
    func updateHidesFolders(uid: UInt64, value: Bool) {
        try! self.updateHidesFoldersStatement.bind(value, Int64(uid)).run()
    }
    
    lazy var updateCurrentVisibleVideoCountStatement
        = try! self.connection.prepare(
            #"UPDATE user SET current_visible_video_count = ? WHERE uid = ?"#)
    func updateCurrentVisibleVideoCount(uid: UInt64, count: Int) {
        try! self.updateCurrentVisibleVideoCountStatement.bind(count, Int64(uid)).run()
    }
}
