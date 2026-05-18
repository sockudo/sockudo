import CryptoKit
import Foundation

enum SignedSockudoURLSessionFactory {
    static func make(appKey: String, appSecret: String) -> URLSession {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [SignedSockudoURLProtocol.self]
        configuration.httpAdditionalHeaders = [
            SignedSockudoURLProtocol.appKeyHeader: appKey,
            SignedSockudoURLProtocol.appSecretHeader: appSecret,
        ]
        return URLSession(configuration: configuration)
    }
}

final class SignedSockudoURLProtocol: URLProtocol {
    static let appKeyHeader = "X-Sockudo-Signing-App-Key"
    static let appSecretHeader = "X-Sockudo-Signing-App-Secret"
    private static let handledKey = "SignedSockudoURLProtocolHandled"

    private var dataTask: URLSessionDataTask?

    override class func canInit(with request: URLRequest) -> Bool {
        guard URLProtocol.property(forKey: handledKey, in: request) == nil else {
            return false
        }
        return request.url?.scheme == "http" || request.url?.scheme == "https"
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest {
        request
    }

    override func startLoading() {
        guard let signedRequest = Self.signed(request) else {
            client?.urlProtocol(self, didFailWithError: URLError(.badURL))
            return
        }

        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = []
        dataTask = URLSession(configuration: configuration).dataTask(with: signedRequest) { [weak self] data, response, error in
            guard let self else { return }
            if let error {
                self.client?.urlProtocol(self, didFailWithError: error)
                return
            }
            if let response {
                self.client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            }
            if let data {
                self.client?.urlProtocol(self, didLoad: data)
            }
            self.client?.urlProtocolDidFinishLoading(self)
        }
        dataTask?.resume()
    }

    override func stopLoading() {
        dataTask?.cancel()
    }

    private static func signed(_ request: URLRequest) -> URLRequest? {
        guard let url = request.url,
              let appKey = request.value(forHTTPHeaderField: appKeyHeader),
              let appSecret = request.value(forHTTPHeaderField: appSecretHeader),
              var components = URLComponents(url: url, resolvingAgainstBaseURL: false)
        else {
            return nil
        }

        var queryItems = components.queryItems ?? []
        queryItems.removeAll { item in
            ["auth_key", "auth_timestamp", "auth_version", "auth_signature", "body_md5"].contains(item.name)
        }

        let body = request.httpBody ?? request.httpBodyStream?.readAllData() ?? Data()
        queryItems.append(URLQueryItem(name: "auth_key", value: appKey))
        queryItems.append(URLQueryItem(name: "auth_timestamp", value: String(Int(Date().timeIntervalSince1970))))
        queryItems.append(URLQueryItem(name: "auth_version", value: "1.0"))
        if !body.isEmpty {
            queryItems.append(URLQueryItem(name: "body_md5", value: body.md5Hex))
        }

        let canonicalPairs: [(String, String)] = queryItems
            .map { item in (item.name.lowercased(), item.value ?? "") }
            .sorted { left, right in
                left.0 == right.0 ? left.1 < right.1 : left.0 < right.0
            }
        let canonicalParts = canonicalPairs.map { pair -> String in
            "\(pair.0)=\(pair.1)"
        }
        let canonicalQuery = canonicalParts.joined(separator: "&")
        let path = components.percentEncodedPath.isEmpty ? "/" : components.percentEncodedPath
        let method = request.httpMethod ?? "GET"
        let stringToSign = "\(method)\n\(path)\n\(canonicalQuery)"
        let signature = HMAC<SHA256>.authenticationCode(
            for: Data(stringToSign.utf8),
            using: SymmetricKey(data: Data(appSecret.utf8))
        ).hexString

        queryItems.append(URLQueryItem(name: "auth_signature", value: signature))
        components.queryItems = queryItems

        guard let signedURL = components.url else {
            return nil
        }

        var signedRequest = URLRequest(url: signedURL)
        signedRequest.httpMethod = request.httpMethod
        signedRequest.allHTTPHeaderFields = request.allHTTPHeaderFields
        if !body.isEmpty {
            signedRequest.httpBody = body
        }
        signedRequest.setValue(nil, forHTTPHeaderField: appKeyHeader)
        signedRequest.setValue(nil, forHTTPHeaderField: appSecretHeader)
        let mutableRequest = (signedRequest as NSURLRequest).mutableCopy() as! NSMutableURLRequest
        URLProtocol.setProperty(true, forKey: handledKey, in: mutableRequest)
        return mutableRequest as URLRequest
    }
}

private extension InputStream {
    func readAllData() -> Data {
        open()
        defer { close() }

        var data = Data()
        var buffer = [UInt8](repeating: 0, count: 4096)
        while true {
            let count = read(&buffer, maxLength: buffer.count)
            if count > 0 {
                data.append(buffer, count: count)
            } else {
                break
            }
        }
        return data
    }
}

private extension Data {
    var md5Hex: String {
        Insecure.MD5.hash(data: self).hexString
    }
}

private extension Sequence where Element == UInt8 {
    var hexString: String {
        map { String(format: "%02x", $0) }.joined()
    }
}
