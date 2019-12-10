import NIO
import AsyncHTTPClient
import SwiftTask
import Foundation
import NIOConcurrencyHelpers
import Dispatch
import BilibiliAPI
import SwiftyJSON

struct ShouldFreezeTaskError: Error {}

let TEST_RUN_LIMIT: Int? = nil

class TaskQueue {
    let elGroup: EventLoopGroup
    // runner
    var runner: Runner
    // stores tasks
    let storage: TaskQueueDB
    
//    public enum Option {
//        case throttle(concurrency: Int?, delay: TimeAmount)
//    }
    let maxConcurrency: Int? = 8
    let delay: TimeAmount? = TimeAmount.milliseconds(100)
    
    public init() {
        self.elGroup = MultiThreadedEventLoopGroup(numberOfThreads: self.maxConcurrency ?? System.coreCount)
        self.runner = SimpleNIORunner(eventLoopGroupProvider: .shared(elGroup))
        runner.sharedData["client"] = { (group) -> HTTPClient in
            var clientConfiguration = HTTPClient.Configuration()
            clientConfiguration.timeout.connect = .seconds(5)
            clientConfiguration.timeout.read = .seconds(5)
            clientConfiguration.proxy = .server(host: "127.0.0.1", port: 1087)
            return HTTPClient(eventLoopGroupProvider: .shared(group),
                              configuration: clientConfiguration)
        }(self.elGroup)
        
        self.storage = try! TaskQueueDB(path: workdir + "/tasks.sqlite3")
        
        self.waitGroup.enter()
        
        self.runner.resultHandler = { task, ownedData, out in
            let outReport = out as! TaskReport
            let report = self.resultHandler?(
                ownedData!["label"] as! String,
                task.input as! APIQuery,
                ownedData!["taskID"] as! Int64,
                ownedData!["metadata"] as! JSON?,
                outReport)
            
            try! self.storage.report(query: task.input as! APIQuery, taskID: ownedData!["taskID"] as! Int64, report ?? outReport)
            self.reportDone()
        }
        self.runner.errorHandler = { (task, ownedData, error) in
            let report = self.errorHandler?(
                ownedData!["label"] as! String,
                task.input as! APIQuery,
                ownedData!["taskID"] as! Int64,
                ownedData!["metadata"] as! JSON?,
                error)
            
            try! self.storage.report(query: task.input as! APIQuery, taskID: ownedData!["taskID"] as! Int64, report ?? .failed)
            self.reportDone()
        }
    }
    
    var resultHandler: ((_ label: String, _ query: APIQuery, _ taskID: Int64, _ metadata: JSON?, _ report: TaskReport) -> TaskReport)? = nil
    var errorHandler: ((_ label: String, _ query: APIQuery, _ taskID: Int64, _ metadata: JSON?, _ error: Error) -> TaskReport)? = nil
    
    var waitGroup = DispatchGroup()
    
    var currentRunningTasks = 0
    var lastEmitTime = NIODeadline.uptimeNanoseconds(0)
    var nextEmitTime: NIODeadline {
        return self.lastEmitTime + (delay ?? TimeAmount.nanoseconds(0))
    }
    
    let localLoop = MultiThreadedEventLoopGroup(numberOfThreads: 1).next()
    
    var isActive = false
    
    func requestSchedule() {
        self.localLoop.execute {
            self.local_requestSchedule()
        }
    }
    func local_requestSchedule() {
        if self.isActive { return }
        self.isActive = true
        self.local_schedule()
    }
    func local_schedule() {
        if let mc = self.maxConcurrency, self.currentRunningTasks >= mc {
            self.isActive = false
            return
        }
        
        guard let task = try! self.storage.dequeue() else {
            if self.currentRunningTasks == 0 {
                self.waitGroup.leave()
            }
            self.isActive = false
            return
        }
        self.currentRunningTasks += 1
        if NIODeadline.now() < self.nextEmitTime {
            self.localLoop.scheduleTask(deadline: self.nextEmitTime, {
                self.local_schedulePart2(task)
            })
        } else {
            local_schedulePart2(task)
        }
    }
    func local_schedulePart2<T: Task>(_ task: T) {
        self.runner.addTask(task)
        self.runner.resume()
        self.lastEmitTime = .now()
        self.local_schedule()
    }
    
    public func addTask(_ tasks: EnqueuedTask...) {
        self.addTask(tasks)
    }
    public func addTask(_ tasks: [EnqueuedTask]) {
        self.localLoop.execute {
            let tasks = tasks.map { task -> EnqueuedTask in
                return task
            }
            //            print("t:", Date(), self.temp_amount)
            try! self.storage.enqueue(tasks)
            self.local_requestSchedule()
        }
    }
    
    var test_taskAmount = 0
    func reportDone() {
//        print("done")
        self.localLoop.execute {
            self.currentRunningTasks -= 1
            self.test_taskAmount += 1
            logger.log(.info, msg: "accumulate tasks: \(self.test_taskAmount), running tasks: \(self.currentRunningTasks)",
                functionName: #function, lineNum: #line, fileName: #file)
            if let TEST_RUN_LIMIT = TEST_RUN_LIMIT,
                self.test_taskAmount > TEST_RUN_LIMIT {
                print("Quit test")
                exit(0)
                
            }
            self.local_requestSchedule()
        }
    }
    
//    public func resume() {
//        self.localLoop.execute {
//            self.runner.resume()
//        }
//    }
    
    func wait() {
        self.waitGroup.wait()
    }

}
