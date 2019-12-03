import SQLite

enum ColumnType: String {
    case text = "TEXT"
    case integer = "INTEGER"
    case blob = "BLOB"
}

struct Column {
    let name: String
    let type: ColumnType
    let isPrimaryKey: Bool
    let isNullable: Bool
    let references: (table: String, column: String)?
    init(name: String, type: ColumnType, isPrimaryKey: Bool = false, isNullable: Bool = false, references: (table: String, column: String)? = nil) {
        self.name = name
        self.type = type
        self.isPrimaryKey = isPrimaryKey
        self.isNullable = isNullable
        self.references = references
    }
}

protocol EntityDBTable {
    var db: EntityDB! { get }
    static var name: String { get }
    static var columns: [Column] { get }
}

extension EntityDBTable {
    var connection: Connection {
        return self.db.connection
    }
    static var table: Table { return Table(Self.name) }
    
    func createTableIfNotExists() throws {
        let template = #"""
            CREATE TABLE IF NOT EXISTS \#(Self.name) (
            %@
            )
            """#
        
        var columnTexts = [String]()
        
        for column in Self.columns {
            var text = #"\#(column.name) \#(column.type.rawValue)"#
//            if column.isPrimaryKey {
//                text += " PRIMARY KEY"
//            }
            if !column.isNullable {
                text += " NOT NULL"
            }
            if let references = column.references {
                text += " REFERENCES \(references.table) (\(references.column))"
            }
            columnTexts.append(text)
        }
        var body = columnTexts.joined(separator: ",\n\t")
        
        let pks = Self.columns.filter { $0.isPrimaryKey }.map { $0.name }
        if pks.count > 0 {
            body = body + ",\n\t" + #"PRIMARY KEY (\#(pks.joined(separator: ", ")))"#
        }
        
        let final = String(format: template, body)
        try self.connection.execute(final)
    }
    
    func buildSelectStatement(matchers: [String]) throws -> Statement {
        return try self.connection.prepare(#"""
            SELECT \#(Self.columns.map { $0.name }.joined(separator: ", "))
            FROM \#(Self.name)
            WHERE \#(matchers.map { "\($0) = :\($0)" }.joined(separator: ", "))
            """#)
    }
    
    func buildInsertStatement() throws -> Statement {
        return try self.connection.prepare(#"""
            INSERT INTO \#(Self.name) (\#(Self.columns.map { $0.name }.joined(separator: ", ")))
            VALUES (\#(Self.columns.map { ":" + $0.name }.joined(separator: ", ")))
            """#)
    }
    
    static var primaryKeyColumnNames: [String] {
        return Self.columns.filter { $0.isPrimaryKey }.map { $0.name }
    }
}
