
import Foundation
import BilibiliAPI
import Dispatch

class RawLogIterator {
    static let fileNameRx = try! NSRegularExpression(pattern: #"^raw_(\d+).log.csv$"#, options: [])
    lazy var fileNums = { () -> [Int] in
        let files = try! FileManager.default.contentsOfDirectory(atPath: rawLogDir)
        return files
            .map { ($0, type(of: self).fileNameRx.firstMatch(in: $0, range: NSRange($0.startIndex..<$0.endIndex, in: $0))?.range(at: 1)) }
            .filter { $0.1 != nil }
            .map { String($0.0[Range($0.1!, in: $0.0)!]) }
            .map { Int($0)! }
    }()
    
    var fileCount: Int {
        return self.fileNums.count
    }
    
    func single(i: Int, fileNum: Int, typeFilter: Set<TaskType>, block: (Int, Int, Int, Double, TaskType?, APIQuery?, Any?) -> Void) {
        let fileName = "raw_\(fileNum).log.csv"
        
        let content: String
        do {
            let fh = FileHandle(forReadingAtPath: rawLogDir + "/" + fileName)!
            content = String(data: fh.readDataToEndOfFile(), encoding: .utf8)!
            fh.closeFile()
        }
        
        let reader = CSVReader(forString: content)
        
        var lineNo = -1
        while let cols = reader.next() {
            lineNo += 1
            if lineNo == 0 {
                block(i, fileNum, 0, 0.0, nil, nil, nil)
                continue
            }
            
            let type = TaskType(from: String(cols[3]))
            if !typeFilter.contains(type) {
                continue
            }
            let query = try! type.buildQuery(fromJSON: cols[4].data(using: .utf8)!)
            let timestampString = cols[2]
            let fakeResponse = Response(body: cols[5].data(using: .utf8)!, statusCode: 200)
            do {
                _ = try checkAPIError(fakeResponse)
            } catch {
                var isErrorResolved = false
                do {
                    if let onError = self.onError {
                        try onError(type.label, query, error)
                        isErrorResolved = true
                    }
                } catch {}
                switch true {
                case isErrorResolved//,
//                     (error as? BadAPIResponseCodeError)?.code == 11208,
//                     error is EmptyBodyError
                    :
                    continue
                default:
                    print(error)
                    continue
                }
            }
            let result: Any
            switch type {
            case .search:
                fatalError("Nop")
            case .video_relatedVideos:
                result = try! VideoRelatedVideosResult.extract(from: fakeResponse)
            case .video_tags:
                result = try! VideoTagsResult.extract(from: fakeResponse)
            case .user_submissions:
                result = try! UserSubmissionSearchResult.extract(from: fakeResponse)
            case .user_favoriteFolderList:
                result = try! UserFavoriteFolderListResult.extract(from: fakeResponse)
            case .tag_detail:
                result = try! TagDetailResult.extract(from: fakeResponse)
            case .tag_top:
                result = try! TagTopResult.extract(from: fakeResponse)
            case .folder_videoItems:
                result = try! FavoriteFolderVideosResult.extract(from: fakeResponse)
            }
            block(i, fileNum, lineNo, Double(timestampString)!, type, query, result)
        }
        block(i, fileNum, -1, 0.0, nil, nil, nil)
    }
    
    func randomly(typeFilter: Set<TaskType>, fileFilter: ((Int) -> Bool)? = nil, block: (Int, Int, Int, Double, TaskType?, APIQuery?, Any?) -> Void) {
        DispatchQueue.concurrentPerform(iterations: self.fileCount) { (i) in
            let fileNum = fileNums[i]
            if let fileFilter = fileFilter, !fileFilter(fileNum) {
                return
            }
            single(i: i, fileNum: fileNum, typeFilter: typeFilter, block: block)
        }
    }
    
    var onError: ((String, APIQuery, Error) throws -> ())? = nil
    
}

func extractGeneralizeVideoItems(type: TaskType, result: Any) -> [GeneralVideoItem] {
    switch type {
    case .video_relatedVideos:
        let result = result as! VideoRelatedVideosResult.Result
        return result
    case .tag_detail:
        let result = result as! TagDetailResult.Result
        return result.videos
    case .tag_top:
        let result = result as! TagTopResult.Result
        return result
    case .folder_videoItems:
        let result = result as! FavoriteFolderVideosResult.Result
        return result.archives
    default:
        fatalError("Unexpected task type!")
    }
}

extension TaskType {
    func buildQuery(fromJSON json: Data) throws -> APIQuery {
        let decoder = JSONDecoder()
        switch self {
        case .search:
            return try decoder.decode(SearchQuery.self, from: json)
        case .video_relatedVideos:
            return try decoder.decode(VideoRelatedVideosQuery.self, from: json)
        case .video_tags:
            return try decoder.decode(VideoTagsQuery.self, from: json)
        case .user_submissions:
            return try decoder.decode(UserSubmissionSearchQuery.self, from: json)
        case .user_favoriteFolderList:
            return try decoder.decode(UserFavoriteFolderListQuery.self, from: json)
        case .tag_detail:
            return try decoder.decode(TagDetailQuery.self, from: json)
        case .tag_top:
            return try decoder.decode(TagTopQuery.self, from: json)
        case .folder_videoItems:
            return try decoder.decode(FavoriteFolderVideosQuery.self, from: json)
        }
    }
}
