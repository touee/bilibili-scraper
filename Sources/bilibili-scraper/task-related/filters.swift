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

// 根据 API 发送请求, 再提取响应 body
let callAPI = buildPipeline(forInputType: APIQuery.self)
    | WithData { shared, _ in
        Promising { el in { api in
            (shared["client"] as! HTTPClient).execute(
                request: try apiProvider.buildRequest(for: api).asyncHttpClientRequest,
                eventLoop: .delegate(on: el)) } } }
    | { (resp: HTTPClient.Response) -> Response in
        let body: Data
        if var _body = resp.body {
            body = Data(_body.readBytes(length: _body.readableBytes) ?? [])
        } else {
            body = Data(count: 0)
        }
        return Response(body: body, statusCode: resp.status.code)}

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

// 从原始数据中提取数据
func ExtractData<T: Task, Result: Decodable>(_ task: T, resultType: Result.Type, transformer: JQ?) -> (Data) throws -> Result {
    return {
        var input = String(data: $0, encoding: .utf8)!
        if let transformer = transformer {
            input = try transformer.executeOne(input: input)
        }
        let result = try JSONDecoder().decode(resultType, from: input.data(using: .utf8)!)
        return result
    }
}
