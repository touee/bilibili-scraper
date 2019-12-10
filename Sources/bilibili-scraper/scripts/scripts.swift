
import Foundation
import BilibiliAPI
import BilibiliEntityDB
import Dispatch

func extractVideoStats() {
    let iter = RawLogIterator()
    
    try! entityDB.connection.execute(#"""
        CREATE TABLE IF NOT EXISTS video_stats (
            aid PRIMARY KEY REFERENCES video(aid),
            
            views INTEGER NOT NULL,
            danmakus INTEGER NOT NULL,
            replies INTEGER NOT NULL,
            favorites INTEGER NOT NULL,
            coins INTEGER NOT NULL,
            shares INTEGER NOT NULL,
            highest_rank INTEGER NOT NULL,
            current_rank INTEGER NOT NULL,
            likes INTEGER NOT NULL,
            dislikes INTEGER NOT NULL,
            
            remained_raw TEXT NULL,
            update_time REAL NOT NULL
        )
        """#)
    
    let insertStatement = try! entityDB.connection.prepare(#"""
        INSERT OR REPLACE INTO video_stats(
            aid,
            views, danmakus, replies,
            favorites, coins, shares,
            highest_rank, current_rank,
            likes, dislikes,
            remained_raw, update_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """#)
    
    var videoStatsDict = [UInt64: (Double, GeneralVideoItem.VideoStats)]()
    var lock = NSLock()
    var n = 0
    
    var localVideoStatsDictArray = [[UInt64: (Double, GeneralVideoItem.VideoStats)]](
        repeating: [UInt64: (Double, GeneralVideoItem.VideoStats)](), count: iter.fileCount)
    
    iter.randomly(typeFilter: [.video_relatedVideos, .tag_detail, .tag_top, .folder_videoItems]) { i, fileNum, lineNo, timestamp, type, query, result in
        let fileName = "/raw_\(fileNum).log.csv"
        if lineNo == 0 { // 第一行
            print(fileName)
            return
        }
        
        guard let type = type else { // 完成所有行后
            lock.lock()
            defer { lock.unlock() }
            localVideoStatsDictArray[fileNum].forEach { k, v in
                if let old = videoStatsDict[k] {
                    if old.0 < v.0 {
                        videoStatsDict[k] = v
                    }
                } else {
                    videoStatsDict[k] = v
                }
            }
            n += 1
            print("\(n)/\(iter.fileCount)", fileName, "done")
            return
        }
        
        let videos = extractGeneralizeVideoItems(type: type, result: result!)
        for video in videos {
            localVideoStatsDictArray[fileNum][video.aid] = (timestamp, video.stats)
        }
    }
    
    videoStatsDict.forEach { (arg) in
        let (k, v) = arg
        try! entityDB.connection.transaction {
            try! insertStatement
                .bind(Int64(k),
                      v.1.views, v.1.danmakus, v.1.replies,
                      v.1.favorites, v.1.coins, v.1.shares,
                      v.1.highest_rank, v.1.current_rank,
                      v.1.likes, v.1.dislikes,
                      v.1.remained_raw, v.0)
                .run()
        }
    }
}

func checkCTime() {
    let iter = RawLogIterator()

    var lock = NSLock()
    var n = 0
    
//    try! entityDB.connection.execute(#"""
//        DROP TABLE IF EXISTS video_ctimes;
//        CREATE TABLE video_ctimes (
//            aid INTEGER PRIMARY KEY NOT NULL,
//            c_time INTEGER NULL,
//            c_time_in_user_submission INTEGER NULL
//        )
//        """#)
    
    let updateCTimeStatement = try! entityDB.connection.prepare(#"""
        WITH new (aid, c_time, c_time_in_user_submission) AS ( VALUES(?, ?, ?) )
        INSERT OR REPLACE INTO video_ctimes (aid, c_time, c_time_in_user_submission)
        SELECT
            new.aid,
            (COALESCE(old.c_time, new.c_time)),
            (COALESCE(old.c_time_in_user_submission, new.c_time_in_user_submission))
        FROM new LEFT JOIN video_ctimes AS old ON old.aid = new.aid
        """#)
    
    var localArray = [[(aid: UInt64, c_time: Int64?, c_time_in_user_submission: Int64?)]](repeating: [(aid: UInt64, c_time: Int64?, c_time_in_user_submission: Int64?)](), count: iter.fileCount)
    
    iter.randomly(typeFilter: [.video_relatedVideos, .tag_detail, .tag_top, .folder_videoItems, .user_submissions],
                  fileFilter: { _ in true }) { i, fileNum, lineNo, timestamp, type, query, result in
        let fileName = "/raw_\(fileNum).log.csv"
        if lineNo == 0 { // 第一行
            print(fileName)
            return
        }
        
        guard let type = type else { // 完成所有行后
            lock.lock()
            n += 1
            defer { lock.unlock() }
            
            try! entityDB.connection.transaction {
                for record in localArray[i] {
                    try! updateCTimeStatement.bind(Int64(record.aid), record.c_time, record.c_time_in_user_submission).run()
                }
            }
            localArray[i] = []
            
            print("\(n)/\(iter.fileCount)", fileName, "done")
            return
        }
        
        try! entityDB.connection.transaction {
            if type == .user_submissions {
                let videos = (result! as! UserSubmissionsResult.Result).submissions
                for video in videos {
                    localArray[i].append((aid: video.aid, c_time: nil, c_time_in_user_submission: video.publish_time))
                }
            } else {
                let videos = extractGeneralizeVideoItems(type: type, result: result!)
                for video in videos {
                    localArray[i].append((aid: video.aid, c_time: video.times.c, c_time_in_user_submission:  nil))
                }
            }
        }
    }
}

func recoverUserInfo() {
    let iter = RawLogIterator()
    
    let upsertUserInfo =  try! entityDB.connection.prepare(#"""
        INSERT INTO user (uid, name, avatar_url, hides_folders, current_visible_video_count)
        VALUES (:uid, NULL, NULL, :hides_folders, :current_visible_video_count)
            ON CONFLICT(uid) DO UPDATE
            SET
                hides_folders = COALESCE(excluded.hides_folders, user.hides_folders),
                current_visible_video_count = COALESCE(excluded.current_visible_video_count, user.current_visible_video_count)
        """#)

    var isFolderListAccessibleArray = Array(repeating: nil as Bool?, count: 48001)
    var countArray = Array(repeating: nil as Int?, count: 48001)
    var lock = NSLock()
    var n = 0
    
    iter.onError = { (label, query, error) in
        if (error as? BadAPIResponseCodeError)?.code == 11208 {
            let query = query as! UserFavoriteFolderListQuery
            if (query.uid-1) % 10000 != 0 || query.uid > 480000001 { return }
            lock.lock()
            defer { lock.unlock() }
            isFolderListAccessibleArray[Int((query.uid-1) / 10000)] = false
        }
    }
    
    iter.randomly(typeFilter: [.user_submissions, .user_favoriteFolderList],
                  fileFilter: { _ in true }) { i, fileNum, lineNo, timestamp, type, query, result in
        let fileName = "/raw_\(fileNum).log.csv"
        if lineNo == 0 { // 第一行
            print(fileName)
            return
        }
        
        guard let type = type else { // 完成所有行后
            lock.lock()
            defer { lock.unlock() }
            n += 1
            print("\(n)/\(iter.fileCount)", fileName, "done")
            return
        }
                    
        switch type {
        case .user_favoriteFolderList:
            let query = query as! UserFavoriteFolderListQuery
            if (query.uid-1) % 10000 != 0 || query.uid > 480000001 { break }
            lock.lock()
            defer { lock.unlock() }
            isFolderListAccessibleArray[Int((query.uid-1) / 10000)] = true
        case .user_submissions:
            let query = query as! UserSubmissionSearchQuery
            if (query.uid-1) % 10000 != 0 || query.uid > 480000001 { break }
            let result = result as! UserSubmissionSearchResult.Result
            lock.lock()
            defer { lock.unlock() }
            countArray[Int((query.uid-1) / 10000)] = result.total_count
        default: fatalError()
        }
    }
    
    try! entityDB.connection.transaction {
        for (i, x) in zip(isFolderListAccessibleArray, countArray).enumerated() {
            if i == 0 { continue }
            try! upsertUserInfo.run([
                ":uid": i * 10000 + 1,
                ":hides_folders" : x.0!,
                ":current_visible_video_count": x.1!
            ])
        }
    }
}
