import SQLite

struct UpsertColumnRule {
    let name: String
    let upsertMode: UpsertMode
}
enum UpsertMode {
    case replace     // nil, a -> a; a, nil -> nil
    case keepOld     // nil, a -> nil; a, nil -> a
    case coalesceFromOld    // nil, a -> a; a, nil -> a
    case coalesceFromNew    // nil, a -> a; a, nil -> a
    case min
    case max
//    case append(separator: Character) // a, b, separator: "|" -> a|b
    case decreaseIfOldIsNull
}

// https://stackoverflow.com/a/7511635
// don't care about injection
func buildUpsertStatement(db: Connection, table: String, columns: [UpsertColumnRule], matchers: [String]) throws -> Statement {
    let names = columns.map { $0.name }.joined(separator: ", ")
    let placeholders = columns.map { ":" + $0.name }.joined(separator: ", ")
    let selected = columns.map { column -> String in
        switch column.upsertMode {
        case .replace: return #"new.\#(column.name)"#
        case .keepOld: return #"old.\#(column.name)"#
        case .coalesceFromOld: return #"COALESCE(old.\#(column.name), new.\#(column.name))"#
        case .coalesceFromNew: return #"COALESCE(new.\#(column.name), old.\#(column.name))"#
        case .min: return #"min(new.\#(column.name), ifnull(old.\#(column.name), 0))"#
        case .max: return #"max(new.\#(column.name), ifnull(old.\#(column.name), 0))"#
//        case .append(let separator): return #"""
//        CASE WHEN new.\#(column.name) IS NULL
//        THEN
//            old.\#(column.name)
//        ELSE
//            CASE WHEN old.\#(column.name) IS NULL
//            THEN
//                new.\#(column.name)
//            ELSE
//                old.\#(column.name) || char(\#(separator.asciiValue!) || new.\#(column.name))
//            END
//        END
//        """#
        case .decreaseIfOldIsNull: return #"""
            COALESCE(
                old.\#(column.name),
                (SELECT COALESCE( min(\#(column.name)), 0 ) - 1 FROM \#(table))
            )
            """#
        }
        }.joined(separator: ", ")
    let match = matchers.map { #"new.\#($0) = old.\#($0)"# }.joined(separator: " AND ")
    if match.count == 0 { fatalError() }
    
    let stmtString = #"""
    WITH new (\#(names)) AS ( VALUES(\#(placeholders)) )
    INSERT OR REPLACE INTO \#(table) (\#(names))
    SELECT \#(selected)
    FROM new LEFT JOIN \#(table) AS old ON \#(match)
    """#
    return try db.prepare(stmtString)
}
