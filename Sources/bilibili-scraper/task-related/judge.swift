import Foundation

import SwiftyJSON

import BilibiliAPI
import BilibiliEntityDB

let selectCertifiedVideoCountStatement = try! entityDB.connection.prepare(#"""
    SELECT sum(has_certified_tags) AS count_of_certified_videos
    FROM (
        WITH video_item AS (
            SELECT aid, tid
            FROM json_each(?)
            LEFT JOIN video_tag ON value = aid
        )
        SELECT
            max(ifnull(tid IN certified_tag, 0)) AS has_certified_tags
        FROM video_item
        GROUP by aid
    )
    """#)
func getCertifiedVideoCount(among aids: [UInt64]) -> Int? {
    if aids.isEmpty {
        return 0
    }
    if let count = selectCertifiedVideoCountStatement
        .bind(String(data: try! JSONEncoder().encode(aids), encoding: .utf8))
        .fetchFirstOnlyRow()[0] as! Int64? {
        return Int(count)
    }
    return nil
}
func evaluateTag(tag tid: UInt64) -> Bool {
    if certifiedTags.contains(tid) {
        return true
    }
    do { // ¯\_(ツ)_/¯
        let sample = assistantDB.getSampleVideos(for: tid)
        if sample.count == 0 {
            return false
        }
        if let certifiedCount = getCertifiedVideoCount(among: sample),
            certifiedCount * 2 >= sample.count {
            return true
        } else {
            return false
        }
    }
}

let selectIsCertifiedVideoStatement = try! entityDB.connection.prepare(#"""
    SELECT count(*) AS is_certified_video FROM certified_video WHERE aid = ?
    """#)
func isCertifiedVideo(aid: UInt64) -> Bool {
    return (selectIsCertifiedVideoStatement.bind(Int64(aid)).fetchFirstOnlyRow()[0] as! Int64) != 0
}

struct FolderStat {
    let countOfCollectedVideos: Int
    let countOfCertifiedVideos: Int
    let countOfVideosThatHaveCompleteTags: Int
    let countOfCertifiedVideosThatHaveCompleteTags: Int
}
let selectFolderVideoItemStatStatement = try! entityDB.connection.prepare(#"""
    WITH folder_video_item_stat AS
    (
        WITH folder_video_item_tag AS
        (
            SELECT
                item_aid AS video_item_aid,
                (tid IN certified_tag) AS is_certified_tag,
                is_tag_list_complete AS with_complete_tags
            FROM folder_video_item
            LEFT JOIN video ON video_item_aid = video.aid
            LEFT JOIN video_tag ON video.aid = video_tag.aid
            WHERE folder_video_item.folder_reference_id
                = (SELECT folder_reference_id
                    FROM folder
                    WHERE owner_uid = ? AND fid = ?)
        )
        SELECT
            video_item_aid,
            ifnull(max(is_certified_tag), 0) AS has_certified_tag,
            with_complete_tags
        FROM folder_video_item_tag
        GROUP BY video_item_aid
    )
    SELECT
        count(video_item_aid) AS count_of_collected_videos,
        sum(has_certified_tag) AS count_of_certified_videos,
        sum(with_complete_tags) AS count_of_videos_that_have_complete_tags,
        sum(has_certified_tag AND with_complete_tags) AS count_of_certified_videos_that_have_complete_tags
    FROM folder_video_item_stat
    """#)
func getFolderStat(uid: UInt64, fid: UInt64) -> FolderStat {
    let result = selectFolderVideoItemStatStatement.bind(Int64(uid), Int64(fid)).fetchFirstOnlyRow()
    return FolderStat(countOfCollectedVideos: Int(result[0] as! Int64),
                      countOfCertifiedVideos: Int(result[1] as! Int64),
                      countOfVideosThatHaveCompleteTags: Int(result[2] as! Int64),
                      countOfCertifiedVideosThatHaveCompleteTags: Int(result[3] as! Int64))
}
func evaluateFolder(folder fid: UInt64, ofUser uid: UInt64) -> Bool {
    let stat = getFolderStat(uid: uid, fid: fid)
    if stat.countOfCollectedVideos == 0 {
        return false
    } else if stat.countOfCertifiedVideos * 2 >= stat.countOfCollectedVideos {
        return true
        //    } else if stat.countOfCertifiedVideosThatHaveCompleteTags < min(30, stat.countOfCollectedVideos) {
        //        return false
    } else if Float64(stat.countOfCertifiedVideosThatHaveCompleteTags) / Float64(stat.countOfVideosThatHaveCompleteTags) >= 0.8 {
        return true
    }
    return false
}

struct JudgedCollection {
    var users: [UInt64] // userSubmissions userFavoriteFolderList
    var tags: [UInt64] // tagDetail tagTop
    var videos: [UInt64] // videoRelatedVideos videoTags
    var folders: [(uid: UInt64, fid: UInt64)] // folder
}

func judge(_ newFounds: EntityCollection,
             taskID: Int64, source query: APIQuery, metadata: JSON?,
             report: TaskReport) -> ([EnqueuedTask], TaskReport) {
    var report = report
    
    var newTasks = [EnqueuedTask]()
    
    let uncertainUsers: [UInt64]?
    let userSubmissionsUncertainPriority: Double?
    let userFolderListUncertainPriority: Double?
    let uncertainTags: [UInt64]?
    let tagDetailUncertainPriority: Double?
    let tagTopUncertainPriority: Double?
    let uncertainVideos: [UInt64]?
    let videoRelatedVideosUncertainPriority: Double?
    let videoTagsUncertainPriority: Double?
    let uncertainFolders: [(uid: UInt64, fid: UInt64)]?
    let folderVideosUncertainPriority: Double?
    
    // user
    if let users = newFounds.users {
        let submissionsDecision = strategyGroup.makeDecision(for: .user_submissions, on: query.type)
        let folderListDecision = strategyGroup.makeDecision(for: .user_favoriteFolderList, on: query.type)
        if submissionsDecision.isUncertain || folderListDecision.isUncertain {
            uncertainUsers = users.map { $0.uid }
            if submissionsDecision.isUncertain {
                userSubmissionsUncertainPriority = submissionsDecision.priority
            } else {
                newTasks += users.map {
                    $0.buildSubmissionsQuery().buildTask().buildEnqueuedTask(
                        shouldFreeze: submissionsDecision.shouldFreeze,
                        priority: submissionsDecision.priority)
                }
                userSubmissionsUncertainPriority = nil
            }
            if folderListDecision.isUncertain {
                userFolderListUncertainPriority = folderListDecision.priority
            } else {
                newTasks += users.map {
                    $0.buildFavoriteFolderListQuery().buildTask().buildEnqueuedTask(
                        shouldFreeze: folderListDecision.shouldFreeze,
                        priority: folderListDecision.priority)
                }
                userFolderListUncertainPriority = nil
            }
        } else {
            newTasks += users.flatMap {
                [
                    $0.buildSubmissionsQuery().buildTask().buildEnqueuedTask(
                        shouldFreeze: submissionsDecision.shouldFreeze,
                        priority: submissionsDecision.priority),
                    $0.buildFavoriteFolderListQuery().buildTask().buildEnqueuedTask(
                        shouldFreeze: folderListDecision.shouldFreeze,
                        priority: folderListDecision.priority)
                ]
            }
            uncertainUsers = nil
            userSubmissionsUncertainPriority = nil
            userFolderListUncertainPriority = nil
        }
    } else {
        uncertainUsers = nil
        userSubmissionsUncertainPriority = nil
        userFolderListUncertainPriority = nil
    }
    
    // tag
    if let tags = newFounds.tags {
        let detailDecision = strategyGroup.makeDecision(for: .tag_detail, on: query.type)
        let topDecision = strategyGroup.makeDecision(for: .tag_top, on: query.type)
        if detailDecision.isUncertain || topDecision.isUncertain {
            uncertainTags = tags.map { $0.tid }
            if detailDecision.isUncertain {
                tagDetailUncertainPriority = detailDecision.priority
            } else {
                newTasks += tags.map {
                    $0.buildDetailQuery().buildTask().buildEnqueuedTask(
                        shouldFreeze: detailDecision.shouldFreeze,
                        priority: detailDecision.priority)
                }
                tagDetailUncertainPriority = nil
            }
            if topDecision.isUncertain {
                newTasks += tags.map {
                    $0.buildTopQuery().buildTask().buildEnqueuedTask(
                        shouldFreeze: topDecision.shouldFreeze,
                        priority: topDecision.priority)
                }
                tagTopUncertainPriority = topDecision.priority
            } else {
                tagTopUncertainPriority = nil
            }
        } else {
            newTasks += tags.flatMap {
                [
                    $0.buildDetailQuery().buildTask().buildEnqueuedTask(
                        shouldFreeze: detailDecision.shouldFreeze,
                        priority: detailDecision.priority),
                    $0.buildTopQuery().buildTask().buildEnqueuedTask(
                        shouldFreeze: topDecision.shouldFreeze,
                        priority: topDecision.priority)
                ]
            }
            uncertainTags = nil
            tagDetailUncertainPriority = nil
            tagTopUncertainPriority = nil
        }
    } else {
        uncertainTags = nil
        tagDetailUncertainPriority = nil
        tagTopUncertainPriority = nil
    }
    
    // video
    if let videos = newFounds.videos {
        let relatedVideosDecision = strategyGroup.makeDecision(for: .video_relatedVideos, on: query.type)
        let tagsDecision = strategyGroup.makeDecision(for: .video_tags, on: query.type)
        if relatedVideosDecision.isUncertain || tagsDecision.isUncertain {
            uncertainVideos = videos.map { $0.aid }
            if relatedVideosDecision.isUncertain {
                videoRelatedVideosUncertainPriority = relatedVideosDecision.priority
            } else {
                newTasks += videos.map {
                    $0.buildRelatedVideosQuery().buildTask().buildEnqueuedTask(
                        shouldFreeze: relatedVideosDecision.shouldFreeze,
                        priority: relatedVideosDecision.priority)
                }
                videoRelatedVideosUncertainPriority = nil
            }
            if tagsDecision.isUncertain {
                videoTagsUncertainPriority = tagsDecision.priority
            } else {
                newTasks += videos.map {
                    $0.buildTagsQuery().buildTask().buildEnqueuedTask(
                        shouldFreeze: tagsDecision.shouldFreeze,
                        priority: tagsDecision.priority)
                }
                videoTagsUncertainPriority = nil
            }
        } else {
            newTasks += videos.flatMap {
                [
                    $0.buildRelatedVideosQuery().buildTask().buildEnqueuedTask(
                        shouldFreeze: relatedVideosDecision.shouldFreeze,
                        priority: relatedVideosDecision.priority),
                    $0.buildTagsQuery().buildTask().buildEnqueuedTask(
                        shouldFreeze: tagsDecision.shouldFreeze,
                        priority: tagsDecision.priority)
                ]
            }
            uncertainVideos = nil
            videoRelatedVideosUncertainPriority = nil
            videoTagsUncertainPriority = nil
        }
    } else {
        uncertainVideos = nil
        videoRelatedVideosUncertainPriority = nil
        videoTagsUncertainPriority = nil
    }
    
    // folder
    if let folders = newFounds.folders {
        let videosDecision = strategyGroup.makeDecision(for: .folder_favoriteFolder, on: query.type)
        if videosDecision.isUncertain {
            uncertainFolders = folders.map { (uid: $0.owner_uid, fid: $0.fid) }
            folderVideosUncertainPriority = videosDecision.priority
        } else {
            newTasks += folders.map {
                $0.buildVideosQuery().buildTask().buildEnqueuedTask(
                    shouldFreeze: videosDecision.shouldFreeze,
                    priority: videosDecision.priority)
            }
            uncertainFolders = nil
            folderVideosUncertainPriority = nil
        }
    } else {
        uncertainFolders = nil
        folderVideosUncertainPriority = nil
    }
    
    // no subregions
    
//    let (passed, undecided, report) = _judge(newFounds, source: query, report: report)
    
    if uncertainUsers == nil && uncertainTags == nil
        && uncertainVideos == nil && uncertainFolders == nil
        && report != .shouldTurnPage {
        // nothing further need to be done
        return (newTasks, report)
    }
    
    try! entityDB.connection.transaction {
        // Note: since we are always in entityDB's transaction,
        // there is no need to begin assistant's transaction to call `evaluateTag` or `evaluateFolder`
        
        if let uncertainUsers = uncertainUsers {
            // 直接通过
            // TODO: 对于稿件任务, 只通过曾上传过相关视频的用户
            uncertainUsers.forEach {
                var result = [EnqueuedTask]()
                if let priority = userSubmissionsUncertainPriority {
                    newTasks += [UserSubmissionsQuery(uid: $0).buildTask().buildEnqueuedTask(
                        shouldFreeze: false,
                        priority: priority)]
                }
                if let priority = userFolderListUncertainPriority {
                    newTasks += [UserFavoriteFolderListQuery(uid: $0).buildTask().buildEnqueuedTask(
                        shouldFreeze: false,
                        priority: priority)]
                }
            }
        }
        
        if let uncertainTags = uncertainTags {
            // 第一页标签直接通过, 之后每页都会进行评估
            uncertainTags.forEach {
                var result = [EnqueuedTask]()
                if let priority = tagDetailUncertainPriority {
                    newTasks += [TagDetailQuery(tid: $0).buildTask().buildEnqueuedTask(
                        shouldFreeze: false,
                        priority: priority)]
                }
                if let priority = tagTopUncertainPriority {
                    newTasks += [TagTopQuery(tid: $0).buildTask().buildEnqueuedTask(
                        shouldFreeze: false,
                        priority: priority)]
                }
            }
        }
        
        if let uncertainVideos = uncertainVideos {
            var isTagCertified: Bool! = nil
            var folderEvaluationResult: Bool! = nil
            func shouldPassVideo(aid: UInt64, on: APIQuery) -> Bool {
                switch query {
                // 如果视频来源于其他视频的相关视频, 会根据那些其他视频中是否有认证的视频来决定是否冻结
                case is VideoRelatedVideosQuery:
                    let reverseRelatedVideos = assistantDB.getReversedVideoRelatedVideosReferrers(for: aid)
                    return (getCertifiedVideoCount(among: reverseRelatedVideos) ?? 0) > 0
                    
                // 如果视频来源于标签页, 会根据该标签是否认证来决定是否冻结
                case let query as APIQueryWithTID:
                    if isTagCertified == nil {
                        isTagCertified = certifiedTags.contains(query.tid)
                    }
                    return isTagCertified
                
                // 如果视频来源于投稿页, 会冻结
                // TODO: 考虑 up 主倾向?
                case is UserSubmissionsQuery,
                     is UserSubmissionSearchQuery:
                    return true
                    
                // 如果视频来源于收藏夹, 会根据其收藏夹的评判来决定是否冻结
                case let query as FavoriteFolderVideosQuery:
                    if folderEvaluationResult == nil {
                        folderEvaluationResult = evaluateFolder(folder: query.fid, ofUser: query.uid)
                    }
                    return folderEvaluationResult
                    
                default: fatalError()
                }
            }
            uncertainVideos.forEach {
                let shouldFreeze = !shouldPassVideo(aid: $0, on: query)
                if let priority = videoRelatedVideosUncertainPriority {
                    newTasks += [VideoRelatedVideosQuery(aid: $0).buildTask().buildEnqueuedTask(
                        shouldFreeze: shouldFreeze,
                        priority: priority)]
                }
                if let priority = videoTagsUncertainPriority {
                    newTasks += [VideoTagsQuery(aid: $0).buildTask().buildEnqueuedTask(
                        shouldFreeze: shouldFreeze,
                        priority: priority)]
                }
            }
        }
        
        if let uncertainFolders = uncertainFolders {
            // 第一页标签直接通过, 之后每页都会进行评估
            if let priority = folderVideosUncertainPriority {
                newTasks += uncertainFolders.map {
                    FavoriteFolderVideosQuery(uid: $0.uid, fid: $0.fid).buildTask().buildEnqueuedTask(
                        shouldFreeze: false,
                        priority: priority)
                }
            }
        }
        
        if report == .shouldTurnPage {
            switch query {
            case is UserSubmissionsQuery,
                 is UserSubmissionSearchQuery:
                break
            case let query as APIQueryWithTID:
                if !evaluateTag(tag: query.tid) {
                    report = .shouldFreezeFollowUpProgress
                }
            case let query as FavoriteFolderVideosQuery:
                if !evaluateFolder(folder: query.fid, ofUser: query.uid) {
                    report = .shouldFreezeFollowUpProgress
                }
            default: fatalError()
            }
        }
    }
            
//    logger.log(.info, msg: #"""
//
//        完成 [\#(taskID)]\#(query):
//            存在: 视频 \#(videos?.count ?? 0), 用户 \#(users?.count ?? 0), 标签 \#(tags?.count ?? 0), 收藏夹 \#(folders?.count ?? 0);
//            新任务: 视频 \#(judgedVideos.count), 用户 \#(judgedUsers.count), 标签 \#(judgedTags.count), 收藏夹 \#(judgedFolders.count);
//            通过评估: 视频 \#(passed.videos.count), 用户 \#(passed.users.count), 标签 \#(passed.tags.count), 收藏夹 \#(passed.folders.count);
//            当前任务报告: \#(report)
//        """#, functionName: #function, lineNum: #line, fileName: #file)
    
    return (newTasks, report)
}

extension UserEntity {
    func buildSubmissionsQuery() -> UserSubmissionsQuery {
        UserSubmissionsQuery(uid: self.uid)
    }
    func buildFavoriteFolderListQuery() -> UserFavoriteFolderListQuery {
        UserFavoriteFolderListQuery(uid: self.uid)
    }
}
extension TagEntity {
    func buildDetailQuery() -> TagDetailQuery {
        TagDetailQuery(tid: self.tid)
    }
    func buildTopQuery() -> TagTopQuery {
        TagTopQuery(tid: self.tid)
    }
}
extension VideoEntity {
    func buildRelatedVideosQuery() -> VideoRelatedVideosQuery {
        VideoRelatedVideosQuery(aid: self.aid)
    }
    func buildTagsQuery() -> VideoTagsQuery {
        VideoTagsQuery(aid: self.aid)
    }
}
extension FolderEntity {
    func buildVideosQuery() -> FavoriteFolderVideosQuery {
        FavoriteFolderVideosQuery(uid: self.owner_uid, fid: self.fid)
    }
}

extension APITask {
    func buildEnqueuedTask(shouldFreeze: Bool, priority: Double) -> EnqueuedTask {
        return EnqueuedTask(self, initialStatus: shouldFreeze ? .frozen : .pending, priority: priority, referrer: .ignore)
    }
    func buildEnqueuedTask(initialStatus: TaskInitialStatus, priority: Double) -> EnqueuedTask {
        return EnqueuedTask(self, initialStatus: initialStatus, priority: priority, referrer: .ignore)
    }
}
