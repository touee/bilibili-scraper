import Foundation
import SQLite

public struct SubregionEntity {
    // 第一遍
    public var subregion_id: Int
    public var name: String?
        
    public init(subregion_id: Int, name: String?) {
        self.subregion_id = subregion_id
        self.name = name
    }
}

class SubregionTable: EntityDBTable {
    weak var db: EntityDB!
    static let name = "subregion"
    
    static let columns = [
        Column(name: "subregion_id",            type: .integer, isPrimaryKey: true),
        Column(name: "name",                    type: .text,    isNullable: true),
    ]
    
    lazy var upsertStatement =  try!  buildUpsertStatement(db: self.connection, table: SubregionTable.name, columns: [
        UpsertColumnRule(name: "subregion_id",  upsertMode: .decreaseIfOldIsNull),
        UpsertColumnRule(name: "name",          upsertMode: .replace),
        ], matchers: SubregionTable.primaryKeyColumnNames)
    lazy var selectStatement = try! self.buildSelectStatement(matchers: SubregionTable.primaryKeyColumnNames)
    init(db: EntityDB) {
        self.db = db
        try! self.createTableIfNotExists()
    }
    
    lazy var knownSet = { () -> Set<Int> in
        let result = try! self.db.connection.prepare(#"""
            SELECT json_group_array(subregion_id)
            FROM subregion
            """#).fetchFirstOnlyRow()[0] as! String
        let known = try! JSONDecoder().decode([Int].self, from: result.data(using: .utf8)!)
        return Set(known)
    }()

    public func update(subregion: inout SubregionEntity) {
        if knownSet.contains(subregion.subregion_id) {
            return
        }
        
        try! self.upsertStatement.bind([
            ":subregion_id":    subregion.subregion_id,
            ":name":            subregion.name,
            ]).run()
        
        let updated = try! selectStatement.bind([
            ":subregion_id": Int64(subregion.subregion_id)
            ]).run().fetchFirstOnlyRow()
        
        subregion.name = updated[1] as! String?
    }
}
