import Foundation
import SQLite

public struct TagEntity {
    // 第一遍
    public var tid: UInt64
    public var name: String
    public var type: Int?
    public var cover_url: String?
    public var head_cover_url: String?
    public var description: String?
    public var short_description: String?
    public var c_time: Int64?
    public var volatile: String?
    
    public init(tid: UInt64, name: String, type: Int?, cover_url: String?, head_cover_url: String?, description: String?, short_description: String?, c_time: Int64?, volatile: String?) {
        self.tid = tid
        self.name = name
        self.type = type
        self.cover_url = cover_url
        self.head_cover_url = head_cover_url
        self.description = description
        self.short_description = short_description
        self.c_time = c_time
        self.volatile = volatile?.trimmingCharacters(in: ["\n"])
    }
    
}

class TagTable: EntityDBTable {
    weak var db: EntityDB!
    static let name = "tag"
    
    static let columns = [
        Column(name: "tid",                 type: .integer, isPrimaryKey: true),
        Column(name: "name",                type: .text),
        Column(name: "type",                type: .integer, isNullable: true),
        Column(name: "cover_url",           type: .text,    isNullable: true),
        Column(name: "head_cover_url",      type: .text,    isNullable: true),
        Column(name: "description",         type: .text,    isNullable: true),
        Column(name: "short_description",   type: .text,    isNullable: true),
        Column(name: "c_time",              type: .integer, isNullable: true),
        Column(name: "volatile",            type: .text,    isNullable: true),
    ]
    
    lazy var upsertStatement = try! buildUpsertStatement(
        db: self.connection, table: TagTable.name,
        columns: TagTable.columns
            .map { UpsertColumnRule(
                name: $0.name,
                upsertMode: .coalesceFromNew
                /*$0.name == "volatile" ? .replace : .coalesceFromNew*/) },
        matchers: TagTable.primaryKeyColumnNames)
    lazy var selectStatement = try! self.buildSelectStatement(matchers: TagTable.primaryKeyColumnNames)
    
    init(db: EntityDB) {
        self.db = db
        try! self.createTableIfNotExists()
    }
    
    func update(tag: inout TagEntity) {
        let oldVolatile: String?
        if let old = try! self.connection.prepare(#"""
            SELECT volatile FROM tag WHERE tid = ?
            """#).bind(Int64(tag.tid)).fatchNilOrFirstOnlyRow() {
            oldVolatile = old[0] as! String?
        } else {
            oldVolatile = nil
        }
        if let oldVolatile = oldVolatile {
            if let newVolatile = tag.volatile {
                tag.volatile = oldVolatile + "\n" + newVolatile
            } else {
                tag.volatile = oldVolatile
            }
        }
        
        try! self.upsertStatement.bind([
            ":tid":                  Int64(tag.tid),
            ":name":                 tag.name,
            ":type":                 tag.type,
            ":cover_url":            tag.cover_url,
            ":head_cover_url":       tag.head_cover_url,
            ":description":          tag.description,
            ":short_description":    tag.short_description,
            ":c_time":               tag.c_time,
            ":volatile":             tag.volatile
            ]).run()
        
        let updated = try! selectStatement.bind([
            ":tid": Int64(tag.tid)
            ]).run().fetchFirstOnlyRow()
        
        tag.type = castBinding(updated[2])
        tag.cover_url = updated[3] as! String?
        tag.head_cover_url = updated[4] as! String?
        tag.description = updated[5] as! String?
        tag.short_description = updated[6] as! String?
        tag.c_time = castBinding(updated[7])
    }
}
