struct StrategyGroup {
    enum Decision {
        case pass(priority: Int = 0)
        case judge(priority: Int = 0)
        case freeze
    }
    
    enum UserDecisions {
        case freezeAll
        case `do`(submissions: Decision, favoriteFolderList: Decision)
    }
    enum TagDecisions {
        case freezeAll
        case `do`(detail: Decision, top: Decision)
    }
    enum VideoDecisions {
        case freezeAll
        case `do`(relatedVideos: Decision, tags: Decision)
    }
    enum FolderDecisions {
        case freezeAll
        case `do`(folderVideoList: Decision)
    }
    
    enum VideoRelatedVideosStrategy {
        case freezeAll
        case then(forVideos: VideoDecisions, forUploaders: UserDecisions)
        
    }
    let onVideoRelatedVideos: VideoRelatedVideosStrategy
    
    enum VideoTagsStrategy {
        case freezeAll
        case then(forTags: TagDecisions)
    }
    let onVideoTags: VideoTagsStrategy
    
    enum UserSubmissionsStrategy {
        case freezeAll
        case then(forVideos: VideoDecisions)
    }
    let onUserSubmissions: UserSubmissionsStrategy
    
    enum UserFolderListStrategy {
        case freezeAll
        case then(forFolders: FolderDecisions)
    }
    let onUserFolderList: UserFolderListStrategy
    
    enum TagDetailStrategy {
        case freezeAll
        case then(forRelatedTags: TagDecisions,
            forListedVideos: VideoDecisions,
            forListVideosUploaders: UserDecisions)
    }
    let onTagDetail: TagDetailStrategy
    
    enum TagTopStrategy {
        case freezeAll
        case then(forVideos: VideoDecisions, forUploaders: UserDecisions)
    }
    let onTagTop: TagTopStrategy
    
    enum FolderVideoListStrategy {
        case freezeAll
        case then(forVideos: VideoDecisions, forUploaders: UserDecisions)
    }
    let onFolderVideoList: FolderVideoListStrategy
}
