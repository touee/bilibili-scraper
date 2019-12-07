import Foundation
import SwiftTask
import BilibiliAPI
import SwiftyJSON
import BilibiliEntityDB

let taskPipeline: Pipeline<APIQuery, TaskReport>
    = SwiftTask.buildPipeline(forInputType: APIQuery.self)
    | callAPI
    
    // log raw data
    |+ WithData { _, owned in { resp in
        _ = rawDataLogger.log(label: owned["label"] as! String,
                              query: owned["$input"] as! APIQuery,
                              rawResponse: resp,
                              taskID: owned["taskID"] as! Int64)
        return resp } }
    
    |+ checkAPIError
    
    // extract result from response
    |+ WithData { _, owned in { resp -> APIResult in
        try (owned["$input"] as! APIQuery).type
            .extractResult(response: resp) } }
    
    // extract entities from result
    |+ WithData { _, owned in { result in
        try (owned["$input"] as! APIQuery)
            .extractEntitesFromResult(result: result) } }
    
    // update EntityDB
    | Blocking { (collection, extra, report) -> (EntityCollection, TaskReport) in
        entityDB.update(collection, extra)
        return (collection, report) }
    
    // update AssistantDB
    | WithData { _, owned in Blocking { (collection, report) -> (EntityCollection, TaskReport) in
        recordAssistantData(collection, source: owned["$input"] as! APIQuery)
        return (collection, report)
    }}
    
    // filter out duplicated entities
    |+ { (newFounds, report) -> (EntityCollection, TaskReport) in
        removeDuplicatedFounds(newFounds)
        return (newFounds, report) }
    
    // judge entities
    // TODO: judge should receive (queries + meta, strategy) instead of `NewFound`s
    | WithData { _, owned in Blocking { (newFounds, report) -> ([EnqueuedTask], TaskReport) in
        return judge(newFounds,
              taskID: owned["taskID"] as! Int64,
              source: owned["$input"] as! APIQuery,
              metadata: owned["metadata"] as! JSON?,
              report: report)
    }}
    
    // TODO: logResult
    
    // add tasks
    | { (tasks, report) -> TaskReport in
        scheduler.addTask(tasks)
        return report
    }
