import Foundation
import SQLite

public struct VideoTag {
    public let tid: UInt64
    public let info: (likes: Int, dislikes: Int)?
    public init(tid: UInt64, info: (likes: Int, dislikes: Int)?) {
        self.tid = tid; self.info = info
    }
}

public struct VideoStats: Codable {
    public let views: Int
    public let danmakus: Int
    public let replies: Int
    public let favorites: Int
    public let coins: Int
    public let shares: Int
    public let highest_rank: Int
    public let likes: Int
    public let dislikes: Int
    public let remained_raw: String?
    
    public init(views: Int, danmakus: Int, replies: Int, favorites: Int, coins: Int, shares: Int, highest_rank: Int, likes: Int, dislikes: Int, remained_raw: String?) {
        self.views = views; self.danmakus = danmakus; self.replies = replies; self.favorites = favorites; self.coins = coins; self.shares = shares; self.highest_rank = highest_rank; self.likes = likes; self.dislikes = dislikes; self.remained_raw = remained_raw
    }
}

public struct VideoEntity {
    // 第一遍
    public var aid: UInt64
    public var title: String
    public var uploader_uid: UInt64
    public var ownership: Int?
    public var description: String?
    public var publish_time: Int64?
    public var c_time: Int64?
    public var subregion_reference_id: Int?
    public var parts: Int?
    public var cover_url: String?
    public var duration: Int?
    public var cid: UInt64?
    public var state: Int? //
    public var stats: VideoStats?
    public var volatile: String?
    
    public init(aid: UInt64, title: String, uploader_uid: UInt64, ownership: Int?, description: String?, publish_time: Int64?, c_time: Int64?, subregion_reference_id: Int?, parts: Int?, cover_url: String?, duration: Int?, cid: UInt64?, state: Int?, stats: VideoStats?, volatile: String?) {
        self.aid = aid; self.title = title; self.uploader_uid = uploader_uid; self.ownership = ownership; self.description = description; self.publish_time = publish_time; self.c_time = c_time; self.subregion_reference_id = subregion_reference_id; self.parts = parts; self.cover_url = cover_url; self.duration = duration; self.cid = cid; self.state = state; self.stats = stats; self.volatile = volatile?.trimmingCharacters(in: ["\n"])
    }
    
}

class VideoTable: EntityDBTable {
    weak var db: EntityDB!
    static let name = "video"
    
    static let columns = [
        Column(name: "aid",                     type: .integer, isPrimaryKey: true),
        Column(name: "title",                   type: .text),
        Column(name: "uploader_uid",            type: .integer, references: (table: "user", column: "uid")),
        Column(name: "ownership",               type: .integer, isNullable: true),
        Column(name: "description",             type: .text,    isNullable: true),
        Column(name: "publish_time",            type: .integer, isNullable: true),
        Column(name: "c_time",                  type: .integer, isNullable: true),
        Column(name: "subregion_reference_id",  type: .integer, isNullable: true,
               references: (table: "subregion", column: "subregion_reference_id")),
        Column(name: "parts",                   type: .integer, isNullable: true),
        Column(name: "cover_url",               type: .text,    isNullable: true),
        Column(name: "duration",                type: .integer, isNullable: true),
        Column(name: "cid",                     type: .integer, isNullable: true),
        Column(name: "state",                   type: .integer, isNullable: true),
        Column(name: "volatile",                type: .text,    isNullable: true),
        Column(name: "is_tag_list_complete",    type: .integer), // FIXME: upsertRule: max
    ]
    
    lazy var upsertStatement = try! buildUpsertStatement(
        db: self.connection, table: VideoTable.name,
        columns: VideoTable.columns
            .map { UpsertColumnRule(
                name: $0.name,
                upsertMode: ($0.name == "is_tag_list_complete") ? .max : .coalesceFromNew) },
        matchers: VideoTable.primaryKeyColumnNames)
    lazy var selectStatement = try! self.buildSelectStatement(matchers: VideoTable.primaryKeyColumnNames)
    
    init(db: EntityDB) {
        self.db = db
        try! self.createTableIfNotExists()
        try! self.connection.execute(#"""
            CREATE INDEX IF NOT EXISTS index_video_uploader_uid ON video (
                uploader_uid
            )
            """#)
        
        // TODO: Individual class
        try! self.connection.execute(#"""
            CREATE TABLE IF NOT EXISTS video_tag (
                aid INTEGER NOT NULL,
                tid INTEGER NOT NULL,
                PRIMARY KEY (aid, tid),
                FOREIGN KEY (aid) REFERENCES video (aid),
                FOREIGN KEY (tid) REFERENCES tag (tid)
            )
            """#)
        
        // TODO: Individual class
        try! self.connection.execute(#"""
            CREATE TABLE IF NOT EXISTS video_stats (
                aid INTEGER NOT NULL,
                
                views INTEGER NOT NULL,
                danmakus INTEGER NOT NULL,
                replies INTEGER NOT NULL,
                favorites INTEGER NOT NULL,
                coins INTEGER NOT NULL,
                shares INTEGER NOT NULL,
                highest_rank INTEGER NOT NULL,
                likes INTEGER NOT NULL,
                dislikes INTEGER NOT NULL,
                
                remained_raw TEXT NULL,
                update_time INTEGER NOT NULL,

                PRIMARY KEY (aid),
                FOREIGN KEY (aid) REFERENCES video(aid)
            )
            """#)
    }
    
    lazy var insertVideoStatsStatement = try! self.connection.prepare(#"""
        INSERT OR REPLACE INTO video_stats (aid, views, danmakus, replies, favorites, coins, shares, highest_rank, likes, dislikes, remained_raw, update_time)
        VALUES (:aid, :views, :danmakus, :replies, :favorites, :coins, :shares, :highest_rank, :likes, :dislikes, :remained_raw, CAST(strftime('%s', 'now') AS INTEGER))
        """#)
    
    func update(video: inout VideoEntity) {
//        let oldTags: [VideoTag]?
        let oldVolatile: String?
        if let old = try! self.connection.prepare(#"""
            SELECT volatile FROM video WHERE aid = ?
            """#).bind(Int64(video.aid)).fatchNilOrFirstOnlyRow() {
            oldVolatile = old[0] as! String?
        } else {
            oldVolatile = nil
        }
        if let oldVolatile = oldVolatile {
            if let newVolatile = video.volatile {
                video.volatile = oldVolatile + "\n" + newVolatile
            } else {
                video.volatile = oldVolatile
            }
        }
        
        try! self.upsertStatement.bind([
            ":aid":                      Int64(video.aid),
            ":title":                    video.title,
            ":uploader_uid":             Int64(video.uploader_uid),
            ":ownership":                video.ownership,
            ":description":              video.description,
            ":publish_time":             video.publish_time,
            ":c_time":                   video.c_time,
            ":subregion_reference_id":   video.subregion_reference_id,
            ":parts":                    video.parts,
            ":cover_url":                video.cover_url,
            ":duration":                 video.duration,
            ":cid":                      cast(video.cid, to: Int64.self),
            ":state":                    video.state,
            ":volatile":                 video.volatile,
            ":is_tag_list_complete":     0,
            ]).run()
        
        if let stats = video.stats {
            try! self.insertVideoStatsStatement.bind([
                ":aid":          Int64(video.aid),
                ":views":        stats.views,
                ":danmakus":     stats.danmakus,
                ":replies":      stats.replies,
                ":favorites":    stats.favorites,
                ":coins":        stats.coins,
                ":shares":       stats.shares,
                ":highest_rank": stats.highest_rank,
                ":likes":        stats.likes,
                ":dislikes":     stats.dislikes,
                ":remained_raw": stats.remained_raw
                ]).run()
        }
        
        let updated = try! self.selectStatement.bind([
            ":aid": Int64(video.aid)
            ]).run().fetchFirstOnlyRow()
        
        video.ownership = castBinding(updated[3])
        video.description = castBinding(updated[4])
        video.publish_time = castBinding(updated[5])
        video.c_time = castBinding(updated[6])
        video.subregion_reference_id = castBinding(updated[7])
        //        self.tags = VideoEntity.decodeTags(updated[8] as! String?)
        video.parts = castBinding(updated[8])
        video.cover_url = castBinding(updated[9])
        video.duration = castBinding(updated[10])
        video.cid = castBinding(updated[11])
        video.state = castBinding(updated[12])
    }
    
    lazy var updateIsTagListComplete = try! self.connection.prepare(#"""
        UPDATE video SET is_tag_list_complete = 1 WHERE aid = ?
        """#)
    func updateIsTagListCompleteToTrue(for aid: UInt64) {
        try! self.updateIsTagListComplete.bind(Int64(aid)).run()
    }
    
    lazy var insertVideoTagStatement = try! self.connection.prepare(#"""
        INSERT OR REPLACE INTO video_tag (aid, tid, likes, dislikes)
        VALUES (:aid, :tid, :likes, :dislikes)
        """#)
    func insertVideoTag(for aid: UInt64, tid: UInt64, info: (likes: Int, dislikes: Int)?) {
        try! self.insertVideoTagStatement.bind([
            ":aid": Int64(aid),
            ":tid": Int64(tid),
            ":likes": info?.likes,
            ":dislikes": info?.dislikes,
                ]).run()
    }
}
