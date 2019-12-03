
import SQLite
import Foundation

class AssistantDB {
    
    let connection: Connection
    
    public init(path: String) throws {
        self.connection = try Connection(path)

        self.connection.busyHandler({ (_) in true })

        try! self.connection.execute(#"""
            CREATE TABLE IF NOT EXISTS tag_sample_videos (
                tid             INTEGER NOT NULL,
                sample_videos   TEXT    NOT NULL,

                PRIMARY KEY (tid)
            )
            """#)
        
        try! self.connection.execute(#"""
            CREATE TABLE IF NOT EXISTS reversed_video_related_videos (
                aid         INTEGER NOT NULL,
                referrers   TEXT    NOT NULL,

                PRIMARY KEY (aid)
            )
            """#)
    }

    lazy var selectSampleVideosStatement = try! self.connection.prepare(#"""
        SELECT sample_videos FROM tag_sample_videos WHERE tid = ?
        """#)
    lazy var insertSampleVideosStatement = try! self.connection.prepare(#"""
        INSERT OR REPLACE INTO tag_sample_videos(tid, sample_videos)
        VALUES (?, ?)
        """#)
    public func addSampleVideos(for tid: UInt64, aids: [UInt64]) {
        try! self.connection.transaction {
            let oldSample: [UInt64]
            if let old = self.selectSampleVideosStatement.bind(Int64(tid)).fatchNilOrFirstOnlyRow() {
                oldSample = try! JSONDecoder().decode([UInt64].self, from: (old[0] as! String).data(using: .utf8)!)
            } else {
                oldSample = []
            }
            try! self.insertSampleVideosStatement.bind(
                Int64(tid), String(data: try! JSONEncoder().encode(oldSample + aids), encoding: .utf8))
                .run()
        }
    }
    public func getSampleVideos(for tid: UInt64) -> [UInt64] {
        var out: [UInt64]? = nil
        try! self.connection.transaction {
            if let sample = self.selectSampleVideosStatement.bind(Int64(tid)).fatchNilOrFirstOnlyRow() {
                out = try! JSONDecoder().decode([UInt64].self, from: (sample[0] as! String).data(using: .utf8)!)
            }
        }
        return out ?? []
    }
    
    // copy paste
    lazy var selectReversedVideoRelatedVideosReferrersStatement = try! self.connection.prepare(#"""
        SELECT referrers FROM reversed_video_related_videos WHERE aid = ?
        """#)
    lazy var insertReversedVideoRelatedVideosReferrersStatement = try! self.connection.prepare(#"""
        INSERT OR REPLACE INTO reversed_video_related_videos(aid, referrers)
        VALUES (?, ?)
        """#)
    public func addReversedVideoRelatedVideosReferrer(for aid: UInt64, referrer: UInt64) {
        try! self.connection.transaction {
            let oldReferrers: [UInt64]
            if let old = self.selectReversedVideoRelatedVideosReferrersStatement.bind(Int64(aid)).fatchNilOrFirstOnlyRow() {
                oldReferrers = try! JSONDecoder().decode([UInt64].self, from: (old[0] as! String).data(using: .utf8)!)
            } else {
                oldReferrers = []
            }
            try! self.insertReversedVideoRelatedVideosReferrersStatement.bind(
                Int64(aid), String(data: try! JSONEncoder().encode(oldReferrers + [referrer]), encoding: .utf8))
                .run()
        }
    }
    public func getReversedVideoRelatedVideosReferrers(for aid: UInt64) -> [UInt64] {
        var out: [UInt64]? = nil
        try! self.connection.transaction {
            if let referrers = self.selectReversedVideoRelatedVideosReferrersStatement.bind(Int64(aid)).fatchNilOrFirstOnlyRow() {
                out = try! JSONDecoder().decode([UInt64].self, from: (referrers[0] as! String).data(using: .utf8)!)
            }
        }
        return out ?? []
    }
}
