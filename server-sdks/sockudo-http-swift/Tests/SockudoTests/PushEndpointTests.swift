import AnyCodable
import APIota
import XCTest

@testable import Sockudo

final class PushEndpointTests: XCTestCase {
  private let options = TestObjects.ClientOptions.withCustomPort

  func testPublishPushEndpointDefaultsToAsyncAndAdminHeader() throws {
    let endpoint = PushEndpointFactory.publishPush(
      request: [
        "recipients": [["type": "channel", "channel": "orders"]],
        "payload": ["title": "Order", "body": "Updated"],
        "providerOverrides": [["provider": "fcm", "payload": ["android": [:]]]],
        "sync": true,
      ] as [String: Any],
      options: options)

    let request = try endpoint.request(baseUrlComponents: baseComponents())
    let body = try JSONSerialization.jsonObject(with: request.httpBody!) as! [String: Any]
    let overrides = body["providerOverrides"] as! [[String: Any]]

    XCTAssertEqual(request.httpMethod, "POST")
    XCTAssertEqual(request.url?.path, "/apps/123456/push/publish")
    XCTAssertEqual(request.value(forHTTPHeaderField: "X-Sockudo-Push-Capability"), "push-admin")
    XCTAssertEqual(request.value(forHTTPHeaderField: "Content-Type"), "application/json")
    XCTAssertEqual(body["sync"] as? Bool, false)
    XCTAssertEqual(overrides.first?["provider"] as? String, "fcm")
    XCTAssertNotNil(URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?
      .queryItems?
      .first(where: { $0.name == "auth_signature" }))
  }

  func testListChannelSubscriptionsCarriesCursorAndDeviceTokenHeader() throws {
    let params = PushSubscriptionFetchOptions(limit: 10, cursor: "c1", deviceID: "device-1")
    let endpoint = PushEndpointFactory.listChannelPushSubscriptions(
      fetchOptions: params,
      deviceIdentityToken: "identity",
      options: options)

    let request = try endpoint.request(baseUrlComponents: baseComponents())
    let queryItems = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems ?? []

    XCTAssertEqual(request.httpMethod, "GET")
    XCTAssertEqual(request.url?.path, "/apps/123456/push/channelSubscriptions")
    XCTAssertEqual(request.value(forHTTPHeaderField: "X-Sockudo-Push-Capability"), "push-subscribe")
    XCTAssertEqual(request.value(forHTTPHeaderField: "X-Sockudo-Device-Identity-Token"), "identity")
    XCTAssertEqual(queryItems.first(where: { $0.name == "deviceId" })?.value, "device-1")
    XCTAssertEqual(queryItems.first(where: { $0.name == "limit" })?.value, "10")
    XCTAssertEqual(queryItems.first(where: { $0.name == "cursor" })?.value, "c1")

    let timestamp = queryItems.first(where: { $0.name == "auth_timestamp" })!.value!
    let expectedToSign = """
      GET
      /apps/123456/push/channelSubscriptions
      auth_key=auth_key&auth_timestamp=\(timestamp)&auth_version=1.0&cursor=c1&deviceid=device-1&limit=10
      """
    let expectedSignature = CryptoService.sha256HMAC(
      for: expectedToSign.toData(),
      using: TestObjects.ClientOptions.testSecret.toData()
    ).hexEncodedString()
    XCTAssertEqual(queryItems.first(where: { $0.name == "auth_signature" })?.value, expectedSignature)
  }

  private func baseComponents() -> URLComponents {
    var components = URLComponents()
    components.scheme = options.scheme
    components.host = options.host
    components.port = options.port
    return components
  }
}
