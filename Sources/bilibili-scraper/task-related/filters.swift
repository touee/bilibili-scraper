import SwiftTask
import Foundation
import JQWrapper
import NIOHTTP1
import AsyncHTTPClient

import BilibiliAPI

extension Request {
    var asyncHttpClientRequest: HTTPClient.Request {
        return try! HTTPClient.Request(url: self.url, headers: HTTPHeaders(self.headers))
    }
}

struct BadAPIResponseCodeError: Error {
    let code: Int
    let message: String
    let response: Response
}

struct BadResponseStatusCodeError: Error {
    let response: Response
}
struct EmptyBodyError: Error {
    let response: Response
}

func checkAPIError(_ resp: Response) throws -> Response {
    
    if !(200..<300 ~= resp.statusCode) {
        throw BadResponseStatusCodeError(response: resp)
    }
    
    if resp.body.count == 0 {
        throw EmptyBodyError(response: resp)
    }
    
    struct ResponseWrapper: Error, Codable {
        let code: Int
        let message: String
    }
    let wrapper = try JSONDecoder().decode(ResponseWrapper.self, from: resp.body)
//    print(String(data: body, encoding: .utf8)!)
    if wrapper.code != 0 {
        throw BadAPIResponseCodeError(code: wrapper.code, message: wrapper.message, response: resp)
    }
    return resp
}
