import Foundation
import SwiftTask
import BilibiliAPI
import SwiftyJSON
import BilibiliEntityDB

public enum TaskSubject {
    case none
    case video
    case user
    case tag
    case folder
}

public enum TaskType: Int {
    case search = 101
    
    case video_relatedVideos = 201
    case video_tags = 202
    
    case user_submissions = 301
    case user_favoriteFolderList = 302
    
    case tag_detail = 401
    case tag_top = 402
    
    case folder_favoriteFolder = 501
    
    var subject: TaskSubject {
        switch self {
        case .search: return .none
        case .video_relatedVideos, .video_tags:
            return .video
        case .user_submissions, .user_favoriteFolderList:
            return .user
        case .tag_detail, .tag_top:
            return .tag
        case .folder_favoriteFolder:
            return .folder
        }
    }
    
    var label: String {
        switch self {
        case .search: return "search"
        case .video_relatedVideos: return "video/related_videos"
        case .video_tags: return "video/tags"
        case .user_submissions: return "user/submissions"
        case .user_favoriteFolderList: return "user/favorite_folder_list"
        case .tag_detail: return "tag/detail"
        case .tag_top: return "tag/top"
        case .folder_favoriteFolder: return "folder/favorite_folder"
        }
    }
    
    init(from label: String) {
        switch label {
        case "search": self = .search
        case "video/related_videos": self = .video_relatedVideos
        case "video/tags": self = .video_tags
        case "user/submissions": self = .user_submissions
        case "user/favorite_folder_list": self = .user_favoriteFolderList
        case "tag/detail": self = .tag_detail
        case "tag/top": self = .tag_top
        case "folder/favorite_folder": self = .folder_favoriteFolder
        default: fatalError("Unknown label!")
        }
    }
    
    init(from queryType: APIQuery.Type) {
        switch queryType {
        case is SearchQuery.Type:
            self = .search
        case is VideoRelatedVideosQuery.Type:
            self = .video_relatedVideos
        case is VideoTagsQuery.Type:
            self = .video_tags
        case is UserSubmissionsQuery.Type,
             is UserSubmissionSearchQuery.Type:
            self = .user_submissions
        case is UserFavoriteFolderListQuery.Type:
            self = .user_favoriteFolderList
        case is TagDetailQuery.Type:
            self = .tag_detail
        case is TagTopQuery.Type:
            self = .tag_top
        case is FavoriteFolderVideosQuery.Type:
            self = .folder_favoriteFolder
        default: fatalError("Unknown query!")
        }
    }
    
    func extractResult(response: Response) throws -> APIResult {
        switch self {
        case .search:
            return try! SearchResult.extract(from: response)
        case .video_relatedVideos:
            return try! VideoRelatedVideosResult.extract(from: response)
        case .video_tags:
            return try! VideoTagsResult.extract(from: response)
        case .user_submissions:
            return try! UserSubmissionSearchResult.extract(from: response)
        case .user_favoriteFolderList:
            return try! UserFavoriteFolderListResult.extract(from: response)
        case .tag_detail:
            return try! TagDetailResult.extract(from: response)
        case .tag_top:
            return try! TagTopResult.extract(from: response)
        case .folder_favoriteFolder:
            return try! FavoriteFolderVideosResult.extract(from: response)
        }
    }
}

extension APIQuery {
    var type: TaskType {
        return TaskType(from: Self.self)
    }
    
    func buildTask(withMetadata metadata: JSON? = nil) -> APITask {
        return APITask(label: self.type.label,
                       query: self,
                       metadata: metadata,
                       pipeline: taskPipeline)
    }
    
    func extractEntitesFromResult(result: APIResult) throws
        -> (EntityCollection, EntityExtra, TaskReport) {
        switch self {
        case let query as SearchResult.Query:
            return try taskResultEntityExtractorGroup
                .extractEntitiesFromSearchResult(
                    result as! SearchResult.Result, query)
        case let query as VideoRelatedVideosResult.Query:
            return try taskResultEntityExtractorGroup
                .extractEntitiesFromVideoRelatedVideosResult(
                    result as! VideoRelatedVideosResult.Result, query)
        case let query as VideoTagsResult.Query:
            return try taskResultEntityExtractorGroup
                .extractEntitiesFromVideoTagsResult(
                    result as! VideoTagsResult.Result, query)
//        case let query as UserSubmissionsResult.Query:
        case let query as UserSubmissionSearchResult.Query:
            return try taskResultEntityExtractorGroup
                .extractEntitiesFromUserSubmissionsResult(
                    result as! UserSubmissionSearchResult.Result, query)
        case let query as UserFavoriteFolderListResult.Query:
            return try taskResultEntityExtractorGroup
                .extractEntitiesFromUserFavoriteFolderListResult(
                    result as! UserFavoriteFolderListResult.Result, query)
        case let query as TagDetailResult.Query:
            return try taskResultEntityExtractorGroup
                .extractEntitiesFromTagDetailResult(
                    result as! TagDetailResult.Result, query)
        case let query as TagTopResult.Query:
            return try taskResultEntityExtractorGroup
                .extractEntitiesFromTagTopResult(
                    result as! TagTopResult.Result, query)
        case let query as FavoriteFolderVideosResult.Query:
            return try taskResultEntityExtractorGroup
                .extractEntitiesFromUserFavoriteFolderResult(
                    result as! FavoriteFolderVideosResult.Result, query)
        default: fatalError()
        }
    }
}

public struct APITask: Task {
    public typealias In = APIQuery
    public typealias Out = TaskReport
    
    let label: String
    let query: APIQuery
    public var input: APIQuery {
        return self.query
    }
    public var metadata: JSON?
    public var ownedData: [String: Any]? {
        var dict = [String: Any]()
        dict["label"] = self.label
        if let metadata = self.metadata {
            dict["metadata"] = metadata
        }
        if let taskID = self.taskID {
            dict["taskID"] = taskID
        }
        return dict
    }
    public let pipeline: Pipeline<In, Out>
    
    public var taskID: Int64?

    public init(label: String, query: APIQuery, metadata: JSON?, pipeline: Pipeline<In, Out>) {
        self.label = label
        self.query = query
        self.metadata = metadata
        self.pipeline = pipeline
    }
}
