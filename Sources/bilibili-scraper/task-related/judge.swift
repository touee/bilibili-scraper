import Foundation

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
        // ~~TODO: 合并 TagTop 和 TagDetail, 负进度为 TagTop, 正进度为 TagDetail~~
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
                json_extract(video_item.value, '$[0]') AS video_item_aid,
                (tid IN certified_tag) AS is_certified_tag,
                is_tag_list_complete AS with_complete_tags
            FROM
                folder, json_each(video_items) AS video_item
            LEFT JOIN video ON video_item_aid = video.aid
            LEFT JOIN video_tag ON video.aid = video_tag.aid
            WHERE owner_uid = ? AND fid = ?
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

struct NewFounds {
    var videos: [UInt64] // videoRelatedVideos videoTags
    var users: [UInt64] // userSubmissions userFavoriteFolderList
    var tags: [UInt64] // tagDetail tagTop
    var folders: [(uid: UInt64, fid: UInt64)] // folder
}

// TODO: 独立文件
func judge(_ newFounds: NewFounds, source query: APIQuery, report: TaskReport)
    -> (passed: NewFounds, undecided: NewFounds, report: TaskReport) {
        var report = report
        var passedVideos = [UInt64]()
        var undecidedVideos = [UInt64]()
        var passedUsers = [UInt64]()
        var passedTags = [UInt64]()
        var passedFolders = [(uid: UInt64, fid: UInt64)]()
        
        
        do { // videos
            try! entityDB.connection.transaction {
                for video in newFounds.videos {
                    switch query {
                    // 如果视频来源于其他视频的相关视频, 会根据那些其他视频中是否有认证的视频来决定是否冻结
                    case is VideoRelatedVideosQuery:
                        let reverseRelatedVideos = assistantDB.getReversedVideoRelatedVideosReferrers(for: video)
                        if (getCertifiedVideoCount(among: reverseRelatedVideos) ?? 0) > 0 {
                            passedVideos.append(video)
                        } else {
                            undecidedVideos.append(video)
                        }
                    // 如果视频来源于标签页, 会根据该标签是否认证来决定是否冻结
                    case let query as APIQueryWithTID:
                        if certifiedTags.contains(query.tid) {
                            passedVideos.append(video)
                        } else {
                            undecidedVideos.append(video)
                        }
                        // 如果视频来源于投稿页, 会冻结
                    // TODO: 考虑 up 主倾向?
                    case is UserSubmissionsQuery:
                        undecidedVideos.append(video)
                    // 如果视频来源于收藏夹, 会根据其收藏夹的评判来决定是否冻结
                    case let query as FavoriteFolderVideosQuery:
                        if evaluateFolder(folder: query.fid, ofUser: query.uid) {
                            passedVideos.append(video)
                        } else {
                            undecidedVideos.append(video)
                        }
                        
                    default: fatalError()
                    }
                }
            }
            
            
            do { // users
                // 直接通过
                // TODO: 对于稿件任务, 只通过曾上传过相关视频的用户
                passedUsers = newFounds.users
            }
            
            do { // tags
                // 第一页标签直接通过, 之后每页都会进行评估
                passedTags = newFounds.tags
            }
            
            do { // folders
                // 第一页标签直接通过, 之后每页都会进行评估
                passedFolders = newFounds.folders
            }
            
            try! entityDB.connection.transaction {
                if report == .shouldTurnPage {
                    switch query {
                    case is UserSubmissionsQuery: break
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
        }
        
        return (passed: NewFounds(videos: passedVideos, users: passedUsers,
                                  tags: passedTags, folders: passedFolders),
                undecided: NewFounds(videos: undecidedVideos, users: [],
                                     tags: [], folders: []),
                report: report)
}
