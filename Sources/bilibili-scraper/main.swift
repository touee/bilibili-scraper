import NIO
import AsyncHTTPClient
import SwiftTask
import Foundation
import JQWrapper

import BilibiliAPI

// 标签评级: designated (手动指定),

//var ipResp = try! httpClient.get(url: "https://httpbin.org/ip").wait()
//print(ipResp.body!.readString(length: ipResp.body!.readableBytes)!)
//exit(0)

//extractVideoStats()
//checkCTime()
//exit(0)

let strategyGroup = { () -> StrategyGroup in
    enum Strategy {
        case demo
    }
    let strategy = Strategy.demo
    
    switch strategy {
    case .demo:
        return StrategyGroup(
            onVideoRelatedVideos: .allUncertain(priority: 0),
            onVideoTags: .allUncertain(priority: 0),
            onUserSubmissions: .allUncertain(priority: 0),
            onUserFolderList: .allUncertain(priority: 0),
            onTagDetail: .allUncertain(priority: 0),
            onTagTop: .allUncertain(priority: 0),
            onFolderVideoList: .allUncertain(priority: 0))
    }
}()

let scheduler = TaskQueue()

scheduler.resultHandler = { label, query, taskID, metadata, report in
//    print("done: [\(taskID)]\(query)")
    return report
}
scheduler.errorHandler = { label, query, taskID, metadata, error in
    var report = TaskReport.failed
    let errorContent = "label: \(label), query: \(query), taskID: \(taskID), metadata: \(String(describing: metadata)), error: \(error)"
    if let _ = error as? ShouldFreezeTaskError {
        report = .shouldFreezeCurrentProgress
    } else if let error = error as? BadAPIResponseCodeError {
        if error.code == 11208 { // "用户隐藏了他的收藏夹"
            entityDB.updateUserHidesFolders(uid: (query as! UserFavoriteFolderListQuery).uid, value: true)
            report = .done
        }
    } else if let error = error as? BadResponseStatusCodeError {
        if error.response.statusCode == 412 { // "由于触发哔哩哔哩安全风控策略，该次访问请求被拒绝。"
            logger.log(.warning, msg: "Triggered 412 on processing task. "
                + errorContent,
                       functionName: #function, lineNum: #line, fileName: #file)
            report = .shouldFreezeCurrentProgress
        } else {
            logger.log(.warning, msg: "Occurred bad response status code on processing task. "
                + errorContent,
                       functionName: #function, lineNum: #line, fileName: #file)
        }
    } else if error is EmptyBodyError {
        if query is TagTopQuery {
            logger.log(.warning, msg: "Empty tag top response. Treated as success. "
                + errorContent,
                       functionName: #function, lineNum: #line, fileName: #file)
            report = .doneOnLastPage
        }
    } else {
        logger.log(.error, msg: "Caught unexpected error on processing task. "
            + errorContent,
            functionName: #function, lineNum: #line, fileName: #file)
        fatalError()
    }
    return report
}

func main() {
    let startTags = { () -> [UInt64] in
        try! entityDB.connection.prepare(#"SELECT * FROM certified_tag"#)
            .map { UInt64($0[0] as! Int64) }
    }()
    var tasks = [EnqueuedTask]()
    for tid in startTags  {
        tasks.append(EnqueuedTask(
            TagDetailQuery(tid: tid).buildTask(),
            priority: 1, referrer: .root))
        tasks.append(EnqueuedTask(
            TagTopQuery(tid: tid).buildTask(),
            priority: 1, referrer: .root))
    }
    scheduler.addTask(tasks)

    //scheduler.resume()
    scheduler.wait()

    //SearchTask(SearchQuery(keyword: "xxx"))
}

main()
