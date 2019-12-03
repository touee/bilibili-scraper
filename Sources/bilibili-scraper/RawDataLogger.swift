import Foundation
import NIOConcurrencyHelpers
import BilibiliAPI

// 原始数据日志相关
class RawDataLogger {
    let workdir: String
    var currentLineNumber: UInt64
    let paginationSize: UInt64
    var logFile: FileHandle?
    let lock = Lock()
    let jsonEncoder = JSONEncoder()
    init(workdir: String, currentLineNumber: UInt64, paginationSize: UInt64) {
        self.workdir = workdir
        self.currentLineNumber = currentLineNumber
        self.paginationSize = paginationSize
    }
    func log(label: String, query: APIQuery, rawResponse: Response, taskID: Int64) -> UInt64 {
        lock.withLockVoid {
            defer { currentLineNumber += 1 }
            let logFileName = "raw_\(roundDown(self.currentLineNumber, self.paginationSize)).log.csv"
            let logFilePath = workdir + "/" + logFileName
            if self.currentLineNumber % self.paginationSize == 0 {
                if let file = self.logFile {
                    file.closeFile()
                    logFile = nil
                }
                if !createFile(logFilePath) {
                    fatalError("Unable to create raw log file \(logFilePath)")
                }
            }
            if self.logFile == nil {
                self.logFile = FileHandle(forWritingAtPath: logFilePath)
                logFile?.seekToEndOfFile()
            }
            if self.currentLineNumber % self.paginationSize == 0 {
                self.logFile!.write(
                    toCSVRow(["logID", "taskID", "时间", "任务类型", "任务请求 body", "原始结果", "HTTP Status"]).data(using: .utf8)!)
            }
            self.logFile!.write(
                toCSVRow([
                    // trace id
                    String(self.currentLineNumber),
                    // task id
                    String(taskID),
                    // time
                    String(Date().timeIntervalSince1970),
                    // task type
                    String(label),
                    // task query
                    String(data: query.json, encoding: .utf8)!,
                    // raw body
                    String(data: rawResponse.body, encoding: .utf8)!,
                    // status
                    String(rawResponse.statusCode),
                    ]).data(using: .utf8)!)
            self.logFile!.synchronizeFile()
        }
        return currentLineNumber
    }
}
