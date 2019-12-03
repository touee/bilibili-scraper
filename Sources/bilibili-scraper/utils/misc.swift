import Foundation

internal func env(_ key: String) -> String? {
    return ProcessInfo.processInfo.environment[key]
}

internal func mkdirAll(_ path: String) throws {
    try FileManager.default.createDirectory(atPath: path, withIntermediateDirectories: true, attributes: [FileAttributeKey.posixPermissions: 0o644])
}
internal func createFile(_ path: String) -> Bool {
    return FileManager.default.createFile(atPath: path, contents: nil, attributes: [FileAttributeKey.posixPermissions: 0o644])
}

internal func roundDown<T: BinaryInteger>(_ num: T, _ x: T) -> T {
    return num / x * x
}

internal func toCSVRow(_ strings: [String]) -> String {
    return strings.map {
        let s = $0.replacingOccurrences(of: "\"", with: "\"\"")
        if s.contains(where: { $0 == "\"" || $0 == "," }) {
            return "\"" + s + "\""
        }
        return s
        }.joined(separator: ",") + "\n"
}

internal func urlFromPath(fileURLWithPath path: String, relativeTo url: URL) -> URL {
    if #available(OSX 10.11, *) {
        return URL(fileURLWithPath: path, relativeTo: url)
    } else {
        fatalError()
    }
}

func encodeJSON<T: Codable>(_ object: T) -> String {
    return String(data: try! JSONEncoder().encode(object), encoding: .utf8)!
}

public extension Encodable {
    var json: Data {
        return try! JSONEncoder().encode(self)
    }
}

// https://stackoverflow.com/a/49547114
extension String {
    func count(of needle: Character) -> Int {
        return reduce(0) {
            $1 == needle ? $0 + 1 : $0
        }
    }
}
