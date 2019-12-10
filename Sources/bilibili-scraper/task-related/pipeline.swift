import Foundation
import SwiftTask
import BilibiliAPI
import SwiftyJSON
import BilibiliEntityDB
import AsyncHTTPClient
    
let taskPipeline: Pipeline<APIQuery, TaskReport>
    = SwiftTask.buildPipeline(forInputType: APIQuery.self)
    | taskPipeline_part1
    | taskPipeline_part2

let taskPipeline_part1 = SwiftTask.buildPipeline(forInputType: APIQuery.self)
    |+ WithData { _, owned in { (query: APIQuery) -> APIQuery in
        reportCenter.reportNewQuery(taskID: owned["taskID"] as! Int64,
                                    query: query)
        return query } }
        
    | callAPIPipeline
    
    // log raw data
    | WithData { _, owned in { resp in
        _ = rawDataLogger.log(label: owned["label"] as! String,
                              query: owned["$input"] as! APIQuery,
                              rawResponse: resp,
                              taskID: owned["taskID"] as! Int64)
        return resp } }
    
    |+ checkAPIError
    
    | processResultPipeline
    
    | updateDatabasePipeline
        
    | WithData { _, owned in { (arg: (EntityCollection, TaskReport)) -> (EntityCollection, TaskReport) in
        let (collection, report) = arg
        reportCenter.reportFound(taskID: owned["taskID"] as! Int64,
                                    found: collection)
        return (collection, report) } }
    
    // filter out duplicated entities
    |+ { (newFounds, report) -> (EntityCollection, TaskReport) in
        removeDuplicatedFounds(newFounds)
        return (newFounds, report) }
        
    |+ WithData { _, owned in { (arg: (EntityCollection, TaskReport)) -> (EntityCollection, TaskReport) in
        let (collection, report) = arg
        reportCenter.reportNewFound(taskID: owned["taskID"] as! Int64,
                                newFounds: collection)
        return (collection, report) } }
        
let taskPipeline_part2//: Pipeline<APIQuery, TaskReport>
        = SwiftTask.buildPipeline(forInputType: (EntityCollection, TaskReport).self)
    // judge entities
    // TODO: judge should receive (queries + meta, strategy) instead of `NewFound`s
    | WithData { _, owned in Blocking { (arg: (EntityCollection, TaskReport)) -> ([EnqueuedTask], TaskReport) in
        let (newFounds, report) = arg
        return judge(newFounds,
              taskID: owned["taskID"] as! Int64,
              source: owned["$input"] as! APIQuery,
              metadata: owned["metadata"] as! JSON?,
              report: report) }}

    | WithData { _, owned in { (arg: ([EnqueuedTask], TaskReport)) -> ([EnqueuedTask], TaskReport) in
        let (tasks, report) = arg
                reportCenter.reportNewTasks(taskID: owned["taskID"] as! Int64,
                                newTasks: tasks)
        return (tasks, report) } }

    |+ WithData { _, owned in { (arg: ([EnqueuedTask], TaskReport)) -> ([EnqueuedTask], TaskReport) in
        let (tasks, report) = arg
        let taskID = owned["taskID"] as! Int64
        let query = owned["$input"] as! APIQuery
        let status = reportCenter.snapshot(of: taskID)
        logger.log(.info, msg: "\nTask \(taskID) [\(query)] reports:\n" + status.description,
                   functionName: #function, lineNum: #line, fileName: #file)
        return (tasks, report) } }

    // add tasks
    | { (tasks: [EnqueuedTask], report: TaskReport) -> TaskReport in
        scheduler.addTask(tasks)
        return report }

// 根据 API 发送请求, 再提取响应 body
let callAPIPipeline = buildPipeline(forInputType: APIQuery.self)
    | WithData { shared, _ in
        Promising { el in { api in
            (shared["client"] as! HTTPClient).execute(
                request: try apiProvider.buildRequest(for: api).asyncHttpClientRequest,
                eventLoop: .delegate(on: el)) } } }
    | { (resp: HTTPClient.Response) -> Response in
        let body: Data
        if var _body = resp.body {
            body = Data(_body.readBytes(length: _body.readableBytes) ?? [])
        } else {
            body = Data(count: 0)
        }
        return Response(body: body, statusCode: resp.status.code)}

let processResultPipeline = buildPipeline(forInputType: Response.self)
    // extract result from response
    |+ WithData { _, owned in { resp -> APIResult in
        try (owned["$input"] as! APIQuery).type
            .extractResult(response: resp) } }
    
    // extract entities from result
    |+ WithData { _, owned in { result in
        try (owned["$input"] as! APIQuery)
            .extractEntitesFromResult(result: result) } }

let updateDatabasePipeline = buildPipeline(forInputType: (EntityCollection, EntityExtra, TaskReport).self)
    // update EntityDB
    | Blocking { (collection, extra, report) -> (EntityCollection, TaskReport) in
        entityDB.update(collection, extra)
        return (collection, report) }

    // update AssistantDB
    | WithData { _, owned in Blocking { (collection, report) -> (EntityCollection, TaskReport) in
        recordAssistantData(collection, source: owned["$input"] as! APIQuery)
        return (collection, report) }}
