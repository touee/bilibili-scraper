struct StrategyGroup {
    enum Decision {
        case pass(priority: Double = 0)
        case uncertain(priority: Double = 0)
        case freeze
        
        var isUncertain: Bool {
            if case .uncertain(priority: _) = self {
                return true
            }
            return false
        }
        
        var shouldFreeze: Bool {
            if case .freeze = self {
                return true
            }
            return false
        }
        
        var priority: Double {
            switch self {
            case .pass(let priority):
                return priority
            case .uncertain(let priority):
                return priority
            case .freeze:
                return 0
            }
        }
    }
    
    enum UserDecisions {
        case `do`(submissions: Decision, favoriteFolderList: Decision)
        case passAll(priority: Double = 0)
        case allUncertain(priority: Double = 0)
        case freezeAll
        
        func makeDecision(for new: TaskType) -> Decision {
            switch self {
            case .do(let submissionsDecision, let favoriteFolderListDecision):
                switch new {
                case .user_submissions:
                    return submissionsDecision
                case .user_favoriteFolderList:
                    return favoriteFolderListDecision
                default:
                    fatalError()
                }
            case .passAll(let priority):
                switch new.subject {
                case .user: return .pass(priority: priority)
                default: fatalError()
                }
            case .allUncertain(let priority):
                switch new.subject {
                case .user: return .uncertain(priority: priority)
                default: fatalError()
                }
            case .freezeAll: return .freeze
            }
        }
    }
    enum TagDecisions {
        case `do`(detail: Decision, top: Decision)
        case passAll(priority: Double = 0)
        case allUncertain(priority: Double = 0)
        case freezeAll
        
        func makeDecision(for new: TaskType) -> Decision {
            switch self {
            case .do(let detailDecision, let topDecision):
                switch new {
                case .tag_detail:
                    return detailDecision
                case .tag_top:
                    return topDecision
                default:
                    fatalError()
                }
            case .passAll(let priority):
                switch new.subject {
                case .tag: return .pass(priority: priority)
                default: fatalError()
                }
            case .allUncertain(let priority):
                switch new.subject {
                case .tag: return .uncertain(priority: priority)
                default: fatalError()
                }
            case .freezeAll: return .freeze
            }
        }
    }
    enum VideoDecisions {
        case `do`(relatedVideos: Decision, tags: Decision)
        case passAll(priority: Double = 0)
        case allUncertain(priority: Double = 0)
        case freezeAll
        
        func makeDecision(for new: TaskType) -> Decision {
            switch self {
            case .do(let relatedVideosDecision, let tagsDecision):
                switch new {
                case .video_relatedVideos:
                    return relatedVideosDecision
                case .video_tags:
                    return tagsDecision
                default:
                    fatalError()
                }
            case .passAll(let priority):
                switch new.subject {
                case .video: return .pass(priority: priority)
                default: fatalError()
                }
            case .allUncertain(let priority):
                switch new.subject {
                case .video: return .uncertain(priority: priority)
                default: fatalError()
                }
            case .freezeAll: return .freeze
            }
        }
    }
    enum FolderDecisions {
        case `do`(folderVideoList: Decision)
        case passAll(priority: Double = 0)
        case allUncertain(priority: Double = 0)
        case freezeAll
        
        func makeDecision(for new: TaskType) -> Decision {
            switch self {
            case .do(let folderVideoListDecision):
                switch new {
                case .folder_favoriteFolder:
                    return folderVideoListDecision
                default:
                    fatalError()
                }
            case .passAll(let priority):
                switch new.subject {
                case .folder: return .pass(priority: priority)
                default: fatalError()
                }
            case .allUncertain(let priority):
                switch new.subject {
                case .folder: return .uncertain(priority: priority)
                default: fatalError()
                }
            case .freezeAll: return .freeze
            }
        }
    }
    
    enum VideoRelatedVideosStrategy {
        case then(forVideos: VideoDecisions, forUploaders: UserDecisions)
        case passAll(priority: Double = 0)
        case allUncertain(priority: Double = 0)
        case freezeAll
        
        func makeDecision(for new: TaskType) -> Decision {
            switch self {
            case .then(let videoDecisions, let userDecisions):
                switch new.subject {
                case .video:
                    return videoDecisions.makeDecision(for: new)
                case .user:
                    return userDecisions.makeDecision(for: new)
                default:
                    fatalError()
                }
            case .passAll(let priority):
                switch new.subject {
                case .video, .user: return .pass(priority: priority)
                default: fatalError()
            }
            case .allUncertain(let priority):
                switch new.subject {
                case .video, .user: return .uncertain(priority: priority)
                default: fatalError()
            }
            case .freezeAll: return .freeze
            }
        }
        
    }
    let onVideoRelatedVideos: VideoRelatedVideosStrategy
    
    enum VideoTagsStrategy {
        case then(forTags: TagDecisions)
        case passAll(priority: Double = 0)
        case allUncertain(priority: Double = 0)
        case freezeAll
        
        func makeDecision(for new: TaskType) -> Decision {
            switch self {
            case .then(let tagDecisions):
                switch new.subject {
                case .tag:
                    return tagDecisions.makeDecision(for: new)
                default:
                    fatalError()
                }
            case .passAll(let priority):
                switch new.subject {
                case .tag: return .pass(priority: priority)
                default: fatalError()
            }
            case .allUncertain(let priority):
                switch new.subject {
                case .tag: return .uncertain(priority: priority)
                default: fatalError()
            }
            case .freezeAll: return .freeze
            }
        }
    }
    let onVideoTags: VideoTagsStrategy
    
    enum UserSubmissionsStrategy {
        case then(forVideos: VideoDecisions)
        case passAll(priority: Double = 0)
        case allUncertain(priority: Double = 0)
        case freezeAll
        
        func makeDecision(for new: TaskType) -> Decision {
            switch self {
            case .then(let videoDecisions):
                switch new.subject {
                case .video:
                    return videoDecisions.makeDecision(for: new)
                default:
                    fatalError()
                }
            case .passAll(let priority):
                switch new.subject {
                case .video: return .pass(priority: priority)
                default: fatalError()
            }
            case .allUncertain(let priority):
                switch new.subject {
                case .video: return .uncertain(priority: priority)
                default: fatalError()
            }
            case .freezeAll: return .freeze
            }
        }
    }
    let onUserSubmissions: UserSubmissionsStrategy
    
    enum UserFolderListStrategy {
        case then(forFolders: FolderDecisions)
        case passAll(priority: Double = 0)
        case allUncertain(priority: Double = 0)
        case freezeAll
        
        func makeDecision(for new: TaskType) -> Decision {
            switch self {
            case .then(let folderDecisions):
                switch new.subject {
                case .folder:
                    return folderDecisions.makeDecision(for: new)
                default:
                    fatalError()
                }
            case .passAll(let priority):
                switch new.subject {
                case .folder: return .pass(priority: priority)
                default: fatalError()
            }
            case .allUncertain(let priority):
                switch new.subject {
                case .folder: return .uncertain(priority: priority)
                default: fatalError()
            }
            case .freezeAll: return .freeze
            }
        }
    }
    let onUserFolderList: UserFolderListStrategy
    
    enum TagDetailStrategy {
        case then(forRelatedTags: TagDecisions,
            forListedVideos: VideoDecisions,
            forListVideosUploaders: UserDecisions)
        case passAll(priority: Double = 0)
        case allUncertain(priority: Double = 0)
        case freezeAll
        
        func makeDecision(for new: TaskType) -> Decision {
            switch self {
            case .then(let tagDecisions, let videoDecisions, let userDecisions):
                switch new.subject {
                case .tag:
                    return tagDecisions.makeDecision(for: new)
                case .video:
                    return videoDecisions.makeDecision(for: new)
                case .user:
                    return userDecisions.makeDecision(for: new)
                default:
                    fatalError()
                }
            case .passAll(let priority):
                switch new.subject {
                case .tag, .video, .user: return .pass(priority: priority)
                default: fatalError()
            }
            case .allUncertain(let priority):
                switch new.subject {
                case .tag, .video, .user: return .uncertain(priority: priority)
                default: fatalError()
            }
            case .freezeAll: return .freeze
            }
        }
    }
    let onTagDetail: TagDetailStrategy
    
    enum TagTopStrategy {
        case then(forVideos: VideoDecisions, forUploaders: UserDecisions)
        case passAll(priority: Double = 0)
        case allUncertain(priority: Double = 0)
        case freezeAll
        
        func makeDecision(for new: TaskType) -> Decision {
            switch self {
            case .then(let videoDecisions, let userDecisions):
                switch new.subject {
                case .video:
                    return videoDecisions.makeDecision(for: new)
                case .user:
                    return userDecisions.makeDecision(for: new)
                default:
                    fatalError()
                }
            case .passAll(let priority):
                switch new.subject {
                case .video, .user: return .pass(priority: priority)
                default: fatalError()
            }
            case .allUncertain(let priority):
                switch new.subject {
                case .video, .user: return .uncertain(priority: priority)
                default: fatalError()
            }
            case .freezeAll: return .freeze
            }
        }
    }
    let onTagTop: TagTopStrategy
    
    enum FolderVideoListStrategy {
        case then(forVideos: VideoDecisions, forUploaders: UserDecisions)
        case passAll(priority: Double = 0)
        case allUncertain(priority: Double = 0)
        case freezeAll
        
        func makeDecision(for new: TaskType) -> Decision {
            switch self {
            case .then(let videoDecisions, let userDecisions):
                switch new.subject {
                case .video:
                    return videoDecisions.makeDecision(for: new)
                case .user:
                    return userDecisions.makeDecision(for: new)
                default:
                    fatalError()
                }
            case .passAll(let priority):
                switch new.subject {
                case .video, .user: return .pass(priority: priority)
                default: fatalError()
            }
            case .allUncertain(let priority):
                switch new.subject {
                case .video, .user: return .uncertain(priority: priority)
                default: fatalError()
            }
            case .freezeAll: return .freeze
            }
        }
    }
    let onFolderVideoList: FolderVideoListStrategy
    
    func makeDecision(for new: TaskType, on current: TaskType) -> Decision {
        switch current {
        case .search:
            fatalError()
        case .video_relatedVideos:
            return strategyGroup.onVideoRelatedVideos.makeDecision(for: new)
        case .video_tags:
            return strategyGroup.onVideoTags.makeDecision(for: new)
        case .user_submissions:
            return strategyGroup.onUserSubmissions.makeDecision(for: new)
        case .user_favoriteFolderList:
            return strategyGroup.onUserFolderList.makeDecision(for: new)
        case .tag_detail:
            return strategyGroup.onTagDetail.makeDecision(for: new)
        case .tag_top:
            return strategyGroup.onTagTop.makeDecision(for: new)
        case .folder_favoriteFolder:
            return strategyGroup.onFolderVideoList.makeDecision(for: new)
        }
    }
}
