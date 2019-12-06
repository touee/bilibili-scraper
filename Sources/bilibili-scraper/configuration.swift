import TOMLDecoder
import Foundation

let configuration = try! TOMLDecoder()
    .decode(Configuration.self,
            from: FileHandle(
                forReadingAtPath: "Configuration/configuration.toml")!
                .readDataToEndOfFile())

struct Configuration: Decodable {
    let workdir: String?
    
    let environment: Environment
    struct Environment: Decodable {
        let keys: Keys?
        struct Keys: Decodable {
            let appKey: String
            let secretKey: String
        }
        let browser: Browser
        struct Browser: Decodable {
            let userAgent: String
            enum CodingKeys: String, CodingKey {
                case userAgent = "user-agent"
            }
        }
        let mobile7260: Mobile7260
        struct Mobile7260: Decodable {
            let mobiApp: String
            let platform: String
            let userAgent: String
            enum CodingKeys: String, CodingKey {
                case mobiApp
                case platform
                case userAgent = "user-agent"
            }
        }
    }
    
    let entityDB: EntityDB
    struct EntityDB: Decodable {
        let connection: Connection
        struct Connection: Decodable {
            let hostname: String
            let port: Int
            let username: String
            let password: String
            let database: String
        }
    }
    
    enum CodingKeys: String, CodingKey {
        case workdir
        case environment
        case entityDB = "entity-db"
    }
}
