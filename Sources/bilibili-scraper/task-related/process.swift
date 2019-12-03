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
func recordAssistantData(_ newFounds: NewFounds, source query: APIQuery) {
    if let query = query as? APIQueryWithTID {
        assistantDB.addSampleVideos(for: query.tid, aids: newFounds.videos)
    } else if let query = query as? VideoRelatedVideosQuery {
        for aid in newFounds.videos {
            assistantDB
                .addReversedVideoRelatedVideosReferrer(
                    for: aid, referrer: query.aid)
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
func removeDuplicatedFounds(_ newFounds: inout NewFounds) {
    func isTaskInQueue(type: TaskType, id: UInt64) -> Bool {
        return selectTaskWithDirectQueryIDStatement.bind(type.rawValue, Int64(id)).fatchNilOrFirstOnlyRow() != nil
    }
    func isFolderTaskInQueue(uid: UInt64, fid: UInt64) -> Bool {
        return selectTaskWithDirectQueryIDStatement.bind(Int64(uid), Int64(fid)).fatchNilOrFirstOnlyRow() != nil
    }
    try! scheduler.storage.db.transaction {
        newFounds.videos = newFounds.videos.filter { !isTaskInQueue(type: .video_relatedVideos, id: $0) }
        newFounds.users = newFounds.users.filter { !isTaskInQueue(type: .user_submissions, id: $0) }
        newFounds.tags = newFounds.tags.filter { !isTaskInQueue(type: .tag_detail, id: $0) }
        newFounds.folders = newFounds.folders.filter { !isFolderTaskInQueue(uid: $0.uid, fid: $0.fid) }
    }
}

func collect(videos: [VideoEntity]?, users: [UserEntity]?,
             tags: [TagEntity]?, folders: [FolderEntity]?,
             folderItmes: (uid: UInt64, fid: UInt64, items: [FolderVideo])?,
             videosTags: [(aid: UInt64, tags: [VideoTag])]?,
             userCurrentVisibleVideoCount: (UInt64, Int)?,
             taskID: Int64, query: APIQuery, metadata: JSON?,
             report: TaskReport) -> TaskReport {
    var videos = videos
    var users = users
    var tags = tags
    var folders = folders
    var folderItmes = folderItmes
    entityDB.update(videos: &videos, users: &users, tags: &tags, folders: &folders, folderItmes: &folderItmes,
                    videosTags: videosTags, userCurrentVisibleVideoCount: userCurrentVisibleVideoCount)
    
    var newFounds = NewFounds(
        videos: concatOptionalArrays(
            videos?.map { $0.aid },
            nil/*folderItmes?.items.map { $0.aid } */) ?? [],
        users: users?.map { $0.uid } ?? [],
        tags: tags?.map { $0.tid } ?? [],
        folders: folders?.map { (uid: $0.owner_uid, fid: $0.fid) }  ?? []
    )
    
    recordAssistantData(newFounds, source: query)
    
    removeDuplicatedFounds(&newFounds)
    
    let (passed, undecided, report) = judge(newFounds, source: query, report: report)
    
    var tasks = [EnqueuedTask]()
    let judgedVideos = passed.videos.map { ($0, false /*shoudFreeze*/) } + undecided.videos.map { ($0, true) }
    _ = judgedVideos.map {
        tasks += [EnqueuedTask(VideoTagsQuery(aid: $0.0).buildTask(), shouldFreeze: false, priority: $0.1 ? 0 : -1, referrer: .ignore)]
//        tasks += [EnqueuedTask(VideoTagsQuery(aid: $0.0).buildTask(), shouldFreeze: false, priority: 1.2, referrer: .ignore)]
        tasks += [EnqueuedTask(VideoRelatedVideosQuery(aid: $0.0).buildTask(), shouldFreeze: $0.1, referrer: .ignore)]
    }
    let judgedUsers = passed.users.map { ($0, false) } + undecided.users.map { ($0, true) }
    _ = judgedUsers.map {
        tasks += [EnqueuedTask(UserFavoriteFolderListQuery(uid: $0.0).buildTask(), shouldFreeze: $0.1, referrer: .ignore)]
//        tasks += [EnqueuedTask(UserSubmissionsQuery(uid: $0.0).buildTask(), shouldFreeze: $0.1, referrer: .ignore)]
        tasks += [EnqueuedTask(UserSubmissionsQuery(uid: $0.0).buildTask(), shouldFreeze: true, referrer: .ignore)]
    }
    let judgedTags = passed.tags.map { ($0, false) } + undecided.tags.map { ($0, true) }
    _ = judgedTags.map {
        tasks += [EnqueuedTask(TagDetailQuery(tid: $0.0).buildTask(), shouldFreeze: $0.1, referrer: .ignore)]
        tasks += [EnqueuedTask(TagTopQuery(tid: $0.0).buildTask(), shouldFreeze: $0.1, referrer: .ignore)]
    }
    let judgedFolders = passed.folders.map { ($0, false) } + undecided.folders.map { ($0, true) }
    _ = judgedFolders.map {
//        tasks += [EnqueuedTask(UserFavoriteFolderQuery(uid: $0.0.uid, fid: $0.0.fid).buildTask(), shouldFreeze: $0.1, referrer: .ignore)]
        tasks += [EnqueuedTask(FavoriteFolderVideosQuery(uid: $0.0.uid, fid: $0.0.fid).buildTask(), shouldFreeze: $0.1, priority: 1.1, referrer: .ignore)]
    }
    
    scheduler.addTask(tasks)
    
    logger.log(.info, msg: #"""
        完成 [\#(taskID)]\#(query):
            存在: 视频 \#(videos?.count ?? 0), 用户 \#(users?.count ?? 0), 标签 \#(tags?.count ?? 0), 收藏夹 \#(folders?.count ?? 0);
            新任务: 视频 \#(judgedVideos.count), 用户 \#(judgedUsers.count), 标签 \#(judgedTags.count), 收藏夹 \#(judgedFolders.count);
            通过评估: 视频 \#(passed.videos.count), 用户 \#(passed.users.count), 标签 \#(passed.tags.count), 收藏夹 \#(passed.folders.count);
            当前任务报告: \#(report)
        """#, functionName: #function, lineNum: #line, fileName: #file)
    
    return report
}

typealias TaskResultProcessor<ResultContainer> = (_ label: String, _ result: ResultContainer.Result, _ query: ResultContainer.Query, _ taskID: Int64, _ metadata: JSON?) throws -> TaskReport where ResultContainer: APIResultContainer

struct TaskProcessorGroup {
    let processSearch:
    TaskResultProcessor<SearchResult>
    let processVideoRelatedVideos:
    TaskResultProcessor<VideoRelatedVideosResult>
    let processVideoTags:
    TaskResultProcessor<VideoTagsResult>
    let processUserSubmissions:
    TaskResultProcessor<UserSubmissionsResult>
    let processUserFavoriteFolderList:
    TaskResultProcessor<UserFavoriteFolderListResult>
    let processTagDetail:
    TaskResultProcessor<TagDetailResult>
    let processTagTop:
    TaskResultProcessor<TagTopResult>
    let processUserFavoriteFolder:
    TaskResultProcessor<FavoriteFolderVideosResult>
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

let taskProcessorGroup = TaskProcessorGroup(
    // 搜索
    processSearch: { _, _, _, _, _ in
        fatalError("unimplemented")
},
    // 相关视频
    processVideoRelatedVideos: { (_, result, query, taskID, metadata) in
        var videos = [VideoEntity]()
        var users = [UserEntity]()
        
        for video in result {
            // 记录视频信息
            var subregion = SubregionEntity(subregion_id: video.subregion_id, name: video.subregion_name)
            entityDB.updateSubregion(subregion: &subregion)
            videos.append(VideoEntity(aid: video.aid, title: video.title, uploader_uid: video.uploader_uid, ownership: video.ownership, description: video.description, publish_time: video.times.pub, c_time: video.times.c, subregion_reference_id: subregion.subregion_reference_id, parts: video.parts, cover_url: video.cover_url, duration: video.duration, cid: video.cid, state: video.state, stats: video.stats.entityStats, volatile: video.other_interesting_stuff))
            // 记录 up 主信息
            users.append(UserEntity(uid: video.uploader_uid, name: video.uploader_name, avatar_url: video.uploader_profile_image_url))
        }
        
        return collect(videos: videos, users: users, tags: nil, folders: nil, folderItmes: nil,
                       videosTags: nil, userCurrentVisibleVideoCount: nil,
                taskID: taskID, query: query, metadata: metadata, report: .done)
},
    // 视频拥有的标签
    processVideoTags: { (_, result, query, taskID, metadata) in
        var tags = [TagEntity]()
        
        // 记录 tag 所属信息
        let videoTags = (aid: query.aid, tags: result.map { VideoTag(tid: $0.tid, info: (likes: $0.likes, dislikes: $0.dislikes)) })
        
        for tag in result {
            // 记录 tag 信息
            tags.append(TagEntity(tid: tag.tid, name: tag.name, type: tag.type, cover_url: tag.cover_url, head_cover_url: tag.head_cover_url, description: tag.description, short_description: tag.short_description, c_time: tag.c_time, volatile: tag.other_interesting_stuff))
        }
        
        return collect(videos: nil, users: nil, tags: tags, folders: nil, folderItmes: nil,
                videosTags: [videoTags], userCurrentVisibleVideoCount: nil,
                taskID: taskID, query: query, metadata: metadata, report: .done)
},
    // 用户投稿
    processUserSubmissions: { (_, result, query, taskID, metadata) in
        var videos = [VideoEntity]()
        
        for video in result.submissions {
            // 记录视频信息
            var subregion = SubregionEntity(subregion_id: nil, name: video.subregion_name)
            entityDB.updateSubregion(subregion: &subregion)
            videos.append(VideoEntity(aid: video.aid, title: video.title, uploader_uid: query.uid, ownership: nil, description: nil,
                                      publish_time: video.c_time, c_time: nil, // UserSubmissions 中的 c_time 是 publish_time!
                                      subregion_reference_id: subregion.subregion_reference_id, parts: nil, cover_url: video.cover_url, duration: video.duration, cid: nil, state: nil, stats: nil, volatile: video.other_interesting_stuff))
        }
        
        return collect(videos: videos, users: nil, tags: nil, folders: nil, folderItmes: nil,
                       videosTags: nil, userCurrentVisibleVideoCount: (query.uid, result.total_count),
                taskID: taskID, query: query, metadata: metadata,
                report: (result.submissions.count == 0
                    || (query.pageNumber ?? 1)+1 > ((result.total_count-1)/20)+1) ? .done : .shouldTurnPage)
},
    // 用户收藏夹列表
    processUserFavoriteFolderList: { (_, result, query, taskID, metadata) in
        var folders = [FolderEntity]()
        
        for folder in result {
            // 记录收藏夹信息
            folders.append(FolderEntity(owner_uid: query.uid, fid: folder.fid, name: folder.name,
                                        capacity: folder.capacity, current_item_count: folder.current_item_count,
                                        create_time: folder.create_time, modify_time: folder.modify_time,
                                        volatile: folder.other_interesting_stuff))
        }
        
        return collect(videos: nil, users: nil, tags: nil, folders: folders, folderItmes: nil,
                       videosTags: nil, userCurrentVisibleVideoCount: nil,
                taskID: taskID, query: query, metadata: metadata, report: .done)
},
    // 标签页
    processTagDetail: { (_, result, query, taskID, metadata) in
        var videos = [VideoEntity]()
        var users = [UserEntity]()
        var tags = [TagEntity]()
        var videosTags = [(UInt64, [VideoTag])]()
        
        // 记录 tag 信息
        let info = result.info
        tags.append(TagEntity(tid: info.tid, name: info.name, type: info.type, cover_url: info.cover_url, head_cover_url: info.head_cover_url, description: info.description, short_description: info.short_description, c_time: info.c_time, volatile: info.other_interesting_stuff))
        
        // 展示的视频
        for video in result.videos {
            // 记录视频信息
            var subregion = SubregionEntity(subregion_id: video.subregion_id, name: video.subregion_name)
            entityDB.updateSubregion(subregion: &subregion)
            videos.append(VideoEntity(aid: video.aid, title: video.title, uploader_uid: video.uploader_uid, ownership: video.ownership, description: video.description, publish_time: video.times.pub, c_time: video.times.c, subregion_reference_id: subregion.subregion_reference_id, parts: video.parts, cover_url: video.cover_url, duration: video.duration, cid: video.cid, state: video.state, stats: video.stats.entityStats, volatile: video.other_interesting_stuff))
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
        
        return collect(videos: videos, users: users, tags: tags, folders: nil, folderItmes: nil,
                       videosTags: videosTags, userCurrentVisibleVideoCount: nil,
                       taskID: taskID, query: query, metadata: metadata,
                       report: result.videos.count == 0 ? .done : .shouldTurnPage)
},
    // 标签页中的默认排序
    processTagTop: { (_, result, query, taskID, metadata) in
        var videos = [VideoEntity]()
        var users = [UserEntity]()
        var videosTags = [(UInt64, [VideoTag])]()
        
        // 展示的视频
        for video in result {
            // 记录视频信息
            var subregion = SubregionEntity(subregion_id: video.subregion_id, name: video.subregion_name)
            entityDB.updateSubregion(subregion: &subregion)
            videos.append(VideoEntity(aid: video.aid, title: video.title, uploader_uid: video.uploader_uid, ownership: video.ownership, description: video.description, publish_time: video.times.pub, c_time: video.times.c, subregion_reference_id: subregion.subregion_reference_id, parts: video.parts, cover_url: video.cover_url, duration: video.duration, cid: video.cid, state: video.state, stats: video.stats.entityStats, volatile: video.other_interesting_stuff))
            // 记录 up 主信息
            users.append(UserEntity(uid: video.uploader_uid, name: video.uploader_name, avatar_url: video.uploader_profile_image_url))
            // 记录视频标签信息
            videosTags.append((video.aid, [VideoTag(tid: query.tid, info: nil)]))
        }
        
        return collect(videos: videos, users: users, tags: nil, folders: nil, folderItmes: nil,
                       videosTags: videosTags, userCurrentVisibleVideoCount: nil,
                       taskID: taskID, query: query, metadata: metadata,
                       report: (result.count == 0 || query.pageNumber == 2) ? .done : .shouldTurnPage)
},
    processUserFavoriteFolder: { (_, result, query, taskID, metadata) in
        var videos = [VideoEntity]()
        var users = [UserEntity]()
        var folderItems = [FolderVideo]()
        
        for video in result.archives {
            // 记录视频信息
            var subregion = SubregionEntity(subregion_id: video.subregion_id, name: video.subregion_name)
            entityDB.updateSubregion(subregion: &subregion)
            videos.append(VideoEntity(aid: video.aid, title: video.title, uploader_uid: video.uploader_uid, ownership: video.ownership, description: video.description, publish_time: video.times.pub, c_time: video.times.c, subregion_reference_id: subregion.subregion_reference_id, parts: video.parts, cover_url: video.cover_url, duration: video.duration, cid: video.cid, state: video.state, stats: video.stats.entityStats, volatile: video.other_interesting_stuff))
            // 记录 up 主信息
            users.append(UserEntity(uid: video.uploader_uid, name: video.uploader_name, avatar_url: video.uploader_profile_image_url))
            // 记录收藏信息
            folderItems.append(FolderVideo(aid: video.aid, favorite_time: video.favorite_time))
        }
        
        return collect(videos: videos, users: users, tags: nil, folders: nil,
                folderItmes: (uid: query.uid, fid: query.fid, items: folderItems),
                videosTags: nil, userCurrentVisibleVideoCount: nil,
                taskID: taskID, query: query, metadata: metadata,
                report: result.archives.count == 0 ? .done : .shouldTurnPage)
}
)
