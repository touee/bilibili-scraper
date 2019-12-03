import SQLite

internal extension Statement {
    func fatchAllRows() -> [Statement.Element] {
        return self.map { $0 }
    }
    
    func fatchNilOrFirstOnlyRow() -> Statement.Element? {
        let rows = self.fatchAllRows()
        if rows.count > 1 {
            fatalError()
        } else if rows.count == 1 {
            return rows[0]
        }
        return nil
    }
    
    func fetchFirstOnlyRow() -> Statement.Element {
        return self.fatchNilOrFirstOnlyRow()!
    }
}
