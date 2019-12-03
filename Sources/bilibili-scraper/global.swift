import Foundation

import HeliumLogger
import Logging

import NIO
import AsyncHTTPClient

import BilibiliAPI
import BilibiliEntityDB

let workdir = try! { () throws -> String in
    let workdir = (env("BILIBILI_SCRAPER__WORKDIR") ?? "workdir") + "/"
    try mkdirAll(workdir)
    return workdir
    }()
let rawLogDir = try! { () throws -> String in
    let rawLogDir = workdir + "/raw_logs/"
    try mkdirAll(rawLogDir)
    return rawLogDir
    }()

let logger = { () -> HeliumLogger in
    let logger = HeliumLogger()
    LoggingSystem.bootstrap(logger.makeLogHandler)
    return logger
}()

let PAGINATION_SIZE: UInt64 = 1000

let startLineNumber = { () -> UInt64 in
    let files = try! FileManager.default.contentsOfDirectory(atPath: workdir)
    let rx = try! NSRegularExpression(pattern: #"^raw_(\d+).log.csv$"#, options: [])
    let _maxNumberInFilename = files
        .compactMap { filename -> String? in
            let match = rx.firstMatch(in: filename, options: [],
                                      range: NSRange(filename.startIndex..<filename.endIndex, in: filename))?.range(at: 1)
            if let match = match {
                return String(filename[Range(match, in: filename)!])
            } else {
                return nil
            }
        }.map { Int($0)! }
        .max()
    guard let maxNumberInFilename = _maxNumberInFilename else {
        return 0
    }
    let newsetFile = FileHandle(forReadingAtPath: rawLogDir + "/raw_\(maxNumberInFilename).log.csv")!
    let allData = newsetFile.readDataToEndOfFile()
    let allString = String(data: allData, encoding: .utf8)!
    let linefeeds = allString.count(of: "\n")
    let lines = linefeeds
        + (allString.last == "\n" ? 0 : 1)
        - 1 // 去掉 header
    return UInt64(maxNumberInFilename + lines + 1)
}()
let rawDataLogger
    = RawDataLogger(workdir: rawLogDir,
                    currentLineNumber: startLineNumber,
                    paginationSize: PAGINATION_SIZE)

let browserUserAgent = ProcessInfo.processInfo.environment["BILIBILI_SCRAPER__BROWSER_USER_AGENT"]!
let mobileApp = ProcessInfo.processInfo.environment["BILIBILI_SCRAPER__MOBILE_7260_MOBI_APP"]!
let mobilePlatform = ProcessInfo.processInfo.environment["BILIBILI_SCRAPER__MOBILE_7260_PLATFORM"]!
let mobileUserAgent = ProcessInfo.processInfo.environment["BILIBILI_SCRAPER__MOBILE_7260_USER_AGENT"]!

let apiProvider = { () -> APIProvider in
    let apiProvider = APIProvider(fallbackKeys: nil)
    try! apiProvider.addClientInfo(for: .browser, ClientInfo.forBrowser(userAgent: browserUserAgent, keys: nil))
    try! apiProvider.addClientInfo(for: .iphone7260,
                              ClientInfo(build: 7260, device: "phone",
                                         mobiApp: mobileApp, platform: mobilePlatform,
                                         userAgent: mobileUserAgent,
                                         keys: nil))
    return apiProvider
}()

let entityDB = try! EntityDB(
    path: workdir + "/entities.sqlite3")
let assistantDB = try! AssistantDB(
    path: workdir + "/assistant.sqlite3")
