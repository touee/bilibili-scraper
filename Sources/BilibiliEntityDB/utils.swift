import SQLite

func cast<T: BinaryInteger, U: BinaryInteger>(_ x: T?, to: U.Type) -> U? {
    guard let x = x else {
        return nil
    }
    return U(x)
}
func castBinding(_ x: Binding?) -> String? {
    guard let x = x else {
        return nil
    }
    return (x as! String)
}

func castBinding<T: BinaryInteger>(_ x: Binding?) -> T? {
    guard let x = x else {
        return nil
    }
    return T(x as! Int64)
}

extension Array {
    func removingDuplicated<T: Hashable>(basedOn fn: (Element) -> T) -> Array<Element> {
        var set = Set<T>()
        return self.filter {
            let key = fn($0)
            if set.contains(key) {
                return false
            }
            set.insert(key)
            return true
        }
    }
}
