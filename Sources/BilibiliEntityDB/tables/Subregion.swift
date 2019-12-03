import Foundation
import SQLite

public struct SubregionEntity {
    // 第一遍
    public var subregion_id: Int?
    public var name: String
    
    public var subregion_reference_id: Int? = nil
    
    public init(subregion_id: Int?, name: String) {
        self.subregion_id = subregion_id
        self.name = name
    }
}

class SubregionTable: EntityDBTable {
    weak var db: EntityDB!
    static let name = "subregion"
    
    static let columns = [
        Column(name: "subregion_reference_id",  type: .integer, isPrimaryKey: true),
        Column(name: "subregion_id",            type: .integer, isNullable: true),
        Column(name: "name",                    type: .text,    isNullable: true),
    ]
    
    lazy var insertWithNameStatement = try! self.buildInsertStatement()
    lazy var upsertWithIDStatement =  try!  buildUpsertStatement(db: self.connection, table: SubregionTable.name, columns: [
        UpsertColumnRule(name: "subregion_reference_id",
                         upsertMode: .decreaseIfOldIsNull),
        UpsertColumnRule(name: "subregion_id",  upsertMode: .replace),
        UpsertColumnRule(name: "name",          upsertMode: .replace),
        ], matchers: ["subregion_id"])
    lazy var selectWithNameStatement = try! self.buildSelectStatement(matchers: ["name"])
    lazy var selectWithIDStatement = try! self.buildSelectStatement(matchers: ["subregion_id"])
    lazy var selectLastInsertRowID = try! self.connection.prepare(#"SELECT last_insert_rowid()"#)

    init(db: EntityDB) {
        self.db = db
        try! self.createTableIfNotExists()
    }
    
    var subregionNameToReferenceIDMap = [String: Int]()
    var subregionIDToReferenceIDMap = [Int: Int]()

    public func update(subregion: inout SubregionEntity) {
        if subregion.name == "" && subregion.subregion_id == nil {
            // 不足以获取信息
            return
        }
        
        do { // 先试着在本地 dict 中查找 subregion_reference_id
            if subregion.name != "",
                let refID = self.subregionNameToReferenceIDMap[subregion.name]  {
                
                subregion.subregion_reference_id = refID
                return
            } else if let id = subregion.subregion_id,
                let refID = self.subregionIDToReferenceIDMap[id] {
                
                subregion.subregion_reference_id = refID
                return
            }
        }
        
        // subregion_reference_id 不在本地 dict 中
        
        let refID: Int?
        if let id = subregion.subregion_id {
            try! self.upsertWithIDStatement.bind([
                ":subregion_reference_id": nil,
                ":subregion_id": id,
                ":name": subregion.name
                ]).run()
            let updated = self.selectWithIDStatement.bind([
                ":subregion_id": id
                ]).fetchFirstOnlyRow()
            refID = Int(updated[0] as! Int64)
        } else { // name ≠ ""
            let olds = self.selectWithNameStatement.bind([
                ":name": subregion.name
                ]).fatchAllRows()
            if olds.count == 1 {
                let old = olds[0]
                refID = Int(old[0] as! Int64)
                subregion.subregion_id = castBinding(old[1])
            } else if olds.count == 0 {
                // fixme: 新插入的 reference_id 不是负数而是非负数
                try! self.insertWithNameStatement.bind([
                    ":subregion_reference_id": nil,
                    ":subregion_id": nil,
                    ":name": subregion.name]).run()
                refID = Int(try! selectLastInsertRowID.run().fetchFirstOnlyRow()[0] as! Int64)
            } else {
                // subregion 重名, 无法判断
                 refID = nil
            }
        }
        
        do {
            if let refID = refID {
                if subregion.name != "" {
                    self.subregionNameToReferenceIDMap[subregion.name] = refID
                }
                if let id = subregion.subregion_id {
                    self.subregionIDToReferenceIDMap[id] = refID
                }
            }
        }
    }
}
