import BilibiliAPI
import BilibiliEntityDB
import SwiftyJSON
import Foundation

// TODO: 移到 main
let certifiedTags = { () -> Set<UInt64> in
    let tags = try! entityDB.connection.prepare(#"""
        SELECT * FROM certified_tag
        """#).map { UInt64($0[0] as! Int64) }
    return Set(tags)
}()

func concatOptionalArrays<T>(_ a: Array<T>?, _ b: Array<T>?) -> Array<T>? {
    guard let a = a else { return b }
    guard let b = b else { return a }
    return a + b
}

// TODO: 移到 assistantDB 相关的文件中去
func recordAssistantData(_ newFounds: EntityCollection, source query: APIQuery) {
    if let query = query as? APIQueryWithTID {
        assistantDB.addSampleVideos(for: query.tid,
                                    aids: newFounds.videos!.map { $0.aid })
    } else if let query = query as? VideoRelatedVideosQuery {
        for video in newFounds.videos! {
            assistantDB
                .addReversedVideoRelatedVideosReferrer(
                    for: video.aid, referrer: query.aid)
        }
    }
}

let selectTaskWithDirectQueryIDStatement = try! scheduler.storage.db.prepare(#"""
    SELECT * FROM queue WHERE type = ? AND query_id = ?
    """#)
let selectFolderTaskStatement = try! scheduler.storage.db.prepare(#"""
    SELECT * FROM queue WHERE type = \#(TaskType.folder_favoriteFolder) AND query_id = (
        SELECT folder_reference_id FROM query_for_folder
        WHERE owner_uid = ? AND fid = ?
    )
    """#)
// TODO: 移到 taskDB 相关的文件中去
// FIXME: TaskDB 中的 referrers 便没有意义了
func removeDuplicatedFounds(_ newFounds: EntityCollection) {
    func isTaskInQueue(type: TaskType, id: UInt64) -> Bool {
        return selectTaskWithDirectQueryIDStatement.bind(type.rawValue, Int64(id)).fatchNilOrFirstOnlyRow() != nil
    }
    func isFolderTaskInQueue(uid: UInt64, fid: UInt64) -> Bool {
        return selectTaskWithDirectQueryIDStatement.bind(Int64(uid), Int64(fid)).fatchNilOrFirstOnlyRow() != nil
    }
    try! scheduler.storage.db.transaction {
        if newFounds.videos != nil {
            newFounds.videos = newFounds.videos!.filter { !isTaskInQueue(type: .video_relatedVideos, id: $0.aid) }
        }
        if newFounds.users != nil {
            newFounds.users = newFounds.users!.filter { !isTaskInQueue(type: .user_submissions, id: $0.uid) }
        }
        if newFounds.tags != nil {
            newFounds.tags = newFounds.tags!.filter { !isTaskInQueue(type: .tag_detail, id: $0.tid) }
        }
        if newFounds.folders != nil {
            newFounds.folders = newFounds.folders!.filter { !isFolderTaskInQueue(uid: $0.owner_uid, fid: $0.fid) }
        }
    }
}

typealias TaskResultEntityExtractor<ResultContainer> =
    (_ result: ResultContainer.Result, _ query: ResultContainer.Query) throws
    -> (EntityCollection, EntityExtra, TaskReport) where ResultContainer: APIResultContainer
// TODO: `TaskReport` should be moved to other lower places

struct TaskResultEntityExtractorGroup {
    let extractEntitiesFromSearchResult:
    TaskResultEntityExtractor<SearchResult>
    let extractEntitiesFromVideoRelatedVideosResult:
    TaskResultEntityExtractor<VideoRelatedVideosResult>
    let extractEntitiesFromVideoTagsResult:
    TaskResultEntityExtractor<VideoTagsResult>
    let extractEntitiesFromUserSubmissionsResult:
    TaskResultEntityExtractor<UserSubmissionSearchResult>
    let extractEntitiesFromUserFavoriteFolderListResult:
    TaskResultEntityExtractor<UserFavoriteFolderListResult>
    let extractEntitiesFromTagDetailResult:
    TaskResultEntityExtractor<TagDetailResult>
    let extractEntitiesFromTagTopResult:
    TaskResultEntityExtractor<TagTopResult>
    let extractEntitiesFromUserFavoriteFolderResult:
    TaskResultEntityExtractor<FavoriteFolderVideosResult>
}

extension GeneralVideoItem.VideoStats {
    var entityStats: VideoStats {
        return VideoStats(
            views: self.views, danmakus: self.danmakus, replies: self.replies,
            favorites: self.favorites, coins: self.coins, shares: self.shares,
            highest_rank: self.highest_rank, likes: self.likes, dislikes: self.dislikes,
            remained_raw: self.remained_raw)
    }
}

let taskResultEntityExtractorGroup = TaskResultEntityExtractorGroup(
    // 搜索
    extractEntitiesFromSearchResult: { _, _ in
        fatalError("unimplemented")
},
    // 相关视频
    extractEntitiesFromVideoRelatedVideosResult: { (result, query) in
        var videos = [VideoEntity]()
        var users = [UserEntity]()
        var subregions = [SubregionEntity]()
        
        for video in result {
            // 记录视频信息
            subregions.append(SubregionEntity(subregion_id: video.subregion_id, name: video.subregion_name))
            videos.append(VideoEntity(aid: video.aid, title: video.title, uploader_uid: video.uploader_uid, ownership: video.ownership, description: video.description, publish_time: video.times.pub, c_time: video.times.c, subregion_id: video.subregion_id, parts: video.parts, cover_url: video.cover_url, duration: video.duration, cid: video.cid, state: video.state, stats: video.stats.entityStats, volatile: video.other_interesting_stuff))
            // 记录 up 主信息
            users.append(UserEntity(uid: video.uploader_uid, name: video.uploader_name, avatar_url: video.uploader_profile_image_url))
        }
        
        return (.init(users: users, tags: nil,
                      videos: videos, folders: nil,
                      subregions: subregions),
                .init(folderItmes: nil, videosTags: nil,
                      userCurrentVisibleVideoCount: nil),
                .done)
            
},
    // 视频拥有的标签
    extractEntitiesFromVideoTagsResult: { (result, query) in
        var tags = [TagEntity]()
        
        // 记录 tag 所属信息
        let videoTags = (aid: query.aid, tags: result.map { VideoTag(tid: $0.tid, info: (likes: $0.likes, dislikes: $0.dislikes)) })
        
        for tag in result {
            // 记录 tag 信息
            tags.append(TagEntity(tid: tag.tid, name: tag.name, type: tag.type, cover_url: tag.cover_url, head_cover_url: tag.head_cover_url, description: tag.description, short_description: tag.short_description, c_time: tag.c_time, volatile: tag.other_interesting_stuff))
        }
        
        return (.init(users: nil, tags: tags,
                      videos: nil, folders: nil,
                      subregions: nil),
                .init(folderItmes: nil, videosTags: [videoTags],
                      userCurrentVisibleVideoCount: nil),
                .done)
},
    // 用户投稿
    extractEntitiesFromUserSubmissionsResult: { (result, query) in // TODO: 换成了 UserSubmissionSearchResult
        var videos = [VideoEntity]()
        var subregions = [SubregionEntity]()
//        let table = result.subregion_id_name_table ?? [:]
        for video in result.submissions {
            // 记录视频信息
            subregions.append(SubregionEntity(subregion_id: video.subregion_id, name: nil))
            videos.append(VideoEntity(aid: video.aid, title: video.title, uploader_uid: query.uid, ownership: nil, description: video.description,
                                      publish_time: video.publish_time, c_time: nil, // UserSubmissions 及 UserSubmissionSearch 中的 c_time 是 publish_time!
                                      subregion_id: video.subregion_id, parts: nil, cover_url: video.cover_url, duration: video.duration, cid: nil, state: nil, stats: nil, volatile: video.other_interesting_stuff))
        }
        
        // note: result.uploader_name can be nil, that's expected
        let user = UserEntity(uid: query.uid, name: result.uploader_name, avatar_url: nil)
        
        return (.init(users: [user], tags: nil,
                      videos: videos, folders: nil,
                      subregions: subregions),
                .init(folderItmes: nil, videosTags: nil,
                      userCurrentVisibleVideoCount: (
                        query.uid, result.total_count)),
                (result.submissions.count == 0 || (query.pageNumber ?? 1)+1
                    > ((result.total_count-1)/100)+1) ?
                        .done : .shouldTurnPage)
},
    // 用户收藏夹列表
    extractEntitiesFromUserFavoriteFolderListResult: { (result, query) in
        var folders = [FolderEntity]()
        
        for folder in result {
            // 记录收藏夹信息
            folders.append(FolderEntity(owner_uid: query.uid, fid: folder.fid, name: folder.name,
                                        capacity: folder.capacity, current_item_count: folder.current_item_count,
                                        create_time: folder.create_time, modify_time: folder.modify_time,
                                        volatile: folder.other_interesting_stuff))
        }
        
        return (.init(users: nil, tags: nil,
                      videos: nil, folders: folders,
                      subregions: nil),
                .init(folderItmes: nil, videosTags: nil,
                      userCurrentVisibleVideoCount: nil),
                .done)
},
    // 标签页
    extractEntitiesFromTagDetailResult: { (result, query) in
        var videos = [VideoEntity]()
        var users = [UserEntity]()
        var tags = [TagEntity]()
        var videosTags = [(UInt64, [VideoTag])]()
        var subregions = [SubregionEntity]()
        
        // 记录 tag 信息
        let info = result.info
        tags.append(TagEntity(tid: info.tid, name: info.name, type: info.type, cover_url: info.cover_url, head_cover_url: info.head_cover_url, description: info.description, short_description: info.short_description, c_time: info.c_time, volatile: info.other_interesting_stuff))
        
        // 展示的视频
        for video in result.videos {
            // 记录视频信息
            subregions.append(SubregionEntity(subregion_id: video.subregion_id, name: video.subregion_name))
            videos.append(VideoEntity(aid: video.aid, title: video.title, uploader_uid: video.uploader_uid, ownership: video.ownership, description: video.description, publish_time: video.times.pub, c_time: video.times.c, subregion_id: video.subregion_id, parts: video.parts, cover_url: video.cover_url, duration: video.duration, cid: video.cid, state: video.state, stats: video.stats.entityStats, volatile: video.other_interesting_stuff))
            // 记录 up 主信息
            users.append(UserEntity(uid: video.uploader_uid, name: video.uploader_name, avatar_url: video.uploader_profile_image_url))
            // 记录视频标签信息
            videosTags.append((video.aid, [VideoTag(tid: query.tid, info: nil)]))
        }
        
        // 相似 tag
        for tag in result.similar_tags {
            // 记录 tag 信息
            tags.append(TagEntity(tid: tag.tid, name: tag.name, type: nil, cover_url: nil, head_cover_url: nil, description: nil, short_description: nil, c_time: nil, volatile: nil))
        }
        
        return (.init(users: users, tags: tags,
                      videos: videos, folders: nil,
                      subregions: subregions),
                .init(folderItmes: nil, videosTags: videosTags,
                      userCurrentVisibleVideoCount: nil),
                result.videos.count == 0 ? .done : .shouldTurnPage)
},
    // 标签页中的默认排序
    extractEntitiesFromTagTopResult: { (result, query) in
        var videos = [VideoEntity]()
        var users = [UserEntity]()
        var videosTags = [(UInt64, [VideoTag])]()
        var subregions = [SubregionEntity]()
        
        // 展示的视频
        for video in result {
            // 记录视频信息
            subregions.append(SubregionEntity(subregion_id: video.subregion_id, name: video.subregion_name))
            videos.append(VideoEntity(aid: video.aid, title: video.title, uploader_uid: video.uploader_uid, ownership: video.ownership, description: video.description, publish_time: video.times.pub, c_time: video.times.c, subregion_id: video.subregion_id, parts: video.parts, cover_url: video.cover_url, duration: video.duration, cid: video.cid, state: video.state, stats: video.stats.entityStats, volatile: video.other_interesting_stuff))
            // 记录 up 主信息
            users.append(UserEntity(uid: video.uploader_uid, name: video.uploader_name, avatar_url: video.uploader_profile_image_url))
            // 记录视频标签信息
            videosTags.append((video.aid, [VideoTag(tid: query.tid, info: nil)]))
        }
        
        return (.init(users: users, tags: nil,
                      videos: videos, folders: nil,
                      subregions: subregions),
                .init(folderItmes: nil, videosTags: videosTags,
                      userCurrentVisibleVideoCount: nil),
                (result.count == 0 || query.pageNumber == 2) ?
                    .done : .shouldTurnPage)
},
    extractEntitiesFromUserFavoriteFolderResult: { (result, query) in
        var videos = [VideoEntity]()
        var users = [UserEntity]()
        var folderItems = [FolderVideo]()
        var subregions = [SubregionEntity]()
        
        for video in result.archives {
            // 记录视频信息
            subregions.append(SubregionEntity(subregion_id: video.subregion_id, name: video.subregion_name))
            videos.append(VideoEntity(aid: video.aid, title: video.title, uploader_uid: video.uploader_uid, ownership: video.ownership, description: video.description, publish_time: video.times.pub, c_time: video.times.c, subregion_id: video.subregion_id, parts: video.parts, cover_url: video.cover_url, duration: video.duration, cid: video.cid, state: video.state, stats: video.stats.entityStats, volatile: video.other_interesting_stuff))
            // 记录 up 主信息
            users.append(UserEntity(uid: video.uploader_uid, name: video.uploader_name, avatar_url: video.uploader_profile_image_url))
            // 记录收藏信息
            folderItems.append(FolderVideo(aid: video.aid, favorite_time: video.favorite_time))
        }
        
        users.append(UserEntity(uid: query.uid, name: nil, avatar_url: nil))
        
        return (.init(users: users, tags: nil,
                      videos: videos, folders: nil,
                      subregions: subregions),
                .init(folderItmes: (uid: query.uid, fid: query.fid,
                                    items: folderItems),
                      videosTags: nil,
                      userCurrentVisibleVideoCount: nil),
                result.archives.count == 0 ? .done : .shouldTurnPage)
}
)
