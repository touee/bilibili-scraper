import XCTest
@testable import BilibiliAPI

final class BilibiliAPITests: XCTestCase {
    func testExample() {
//         This is an example of a functional test case.
//         Use XCTAssert and related functions to verify your tests produce the correct
//         results.
//        XCTAssertEqual(BilibiliAPI().text, "Hello, World!")
    }
    
    func testAPI() {
        let apiProvider = APIProvider()
        for api in [
            SearchQuery(keyword: "东方", order: .danmaku, duration: .all, region: .科技, pageNumber: 1),
//            API.getTagTabInfo(tid: nil, tagName: "东方"),
            TagDetailQuery(tid: 166, pageNumber: 3),
            TagTopQuery(tid: 166, pageNumber: 3),
//            API.getVideoInfo(aid: 640001),
            VideoRelatedVideosQuery(aid: 640001),
            VideoTagsQuery(aid: 2557),
            UserSubmissionsQuery(uid: 364812769, pageNumber: 2),
            UserFavoriteFolderListQuery(uid: 3621415),
            UserFavoriteFolderQuery(uid: 3621415, fid: 1443151, pageNumber: nil),
            ] as [APIQuery] {
            print(try! apiProvider.buildRequest(for: api).url)
        }
    }
    
    static var allTests = [
        ("testExample", testExample),
    ]
}
