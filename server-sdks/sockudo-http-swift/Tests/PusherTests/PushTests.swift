import Foundation
import XCTest

@testable import Pusher

final class PushTests: XCTestCase {
  func testPublishPushEndpointForcesAsyncAdmissionAndPreservesOverrides() throws {
    let endpoint = PushEndpointFactory.publishPush(
      request: [
        "recipients": [["type": "channel", "channel": "orders"]],
        "payload": ["title": "Order", "body": "Updated"],
        "providerOverrides": [["provider": "fcm", "payload": ["android": [:]]]],
      ],
      options: TestObjects.ClientOptions.withCluster
    )

    XCTAssertEqual(endpoint.path, "/apps/123456/push/publish")
    XCTAssertEqual(
      endpoint.headers?.allHTTPHeaderFields["X-Sockudo-Push-Capability"],
      "push-admin")

    let query = Dictionary(uniqueKeysWithValues: (endpoint.queryItems ?? []).map { ($0.name, $0.value ?? "") })
    XCTAssertFalse(query["body_md5"]?.isEmpty ?? true)

    guard let httpBody = endpoint.httpBody else {
      XCTFail("expected request body")
      return
    }

    let json = try JSONSerialization.jsonObject(with: endpoint.encoder.encode(httpBody)) as? [String: Any]
    XCTAssertEqual(json?["sync"] as? Bool, false)
    XCTAssertNotNil(json?["providerOverrides"])
  }

  func testListChannelPushSubscriptionsUsesCursorPaginationAndSubscribeHeaders() {
    let endpoint = PushEndpointFactory.listChannelPushSubscriptions(
      fetchOptions: .init(limit: 10, cursor: "c1", deviceID: "device-1"),
      deviceIdentityToken: "identity-token",
      options: TestObjects.ClientOptions.withCluster
    )

    XCTAssertEqual(endpoint.path, "/apps/123456/push/channelSubscriptions")
    let query = Dictionary(uniqueKeysWithValues: (endpoint.queryItems ?? []).map { ($0.name, $0.value ?? "") })
    XCTAssertEqual(query["limit"], "10")
    XCTAssertEqual(query["cursor"], "c1")
    XCTAssertEqual(query["deviceId"], "device-1")
    let expectedToSign = """
      GET
      /apps/123456/push/channelSubscriptions
      auth_key=auth_key&auth_timestamp=\(query["auth_timestamp"]!)&auth_version=1.0&cursor=c1&deviceid=device-1&limit=10
      """
    let expectedSignature = CryptoService.sha256HMAC(
      for: expectedToSign.toData(),
      using: TestObjects.ClientOptions.testSecret.toData()
    ).hexEncodedString()
    XCTAssertEqual(query["auth_signature"], expectedSignature)
    XCTAssertEqual(
      endpoint.headers?.allHTTPHeaderFields["X-Sockudo-Push-Capability"],
      "push-subscribe")
    XCTAssertEqual(
      endpoint.headers?.allHTTPHeaderFields["X-Sockudo-Device-Identity-Token"],
      "identity-token")
  }

  func testCancelScheduledPushEndpointUsesAdminScope() {
    let endpoint = PushEndpointFactory.cancelScheduledPush(
      publishID: "pub_123",
      options: TestObjects.ClientOptions.withCluster
    )

    XCTAssertEqual(endpoint.path, "/apps/123456/push/scheduled/pub_123")
    XCTAssertEqual(
      endpoint.headers?.allHTTPHeaderFields["X-Sockudo-Push-Capability"],
      "push-admin")
  }

  func testSchedulePushRequiresNotBeforeMs() {
    let expectation = XCTestExpectation(function: #function)
    let client = Pusher(
      apiClient: APIClient(options: TestObjects.ClientOptions.withCluster),
      options: TestObjects.ClientOptions.withCluster)

    client.schedulePush(
      request: [
        "recipients": [["type": "channel", "channel": "orders"]],
        "payload": ["title": "Order"],
      ])
    { result in
      switch result {
      case .success:
        XCTFail("expected failure")
      case .failure(let error):
        guard case .internalError = error else {
          XCTFail("expected internal error, got \(error)")
          break
        }
      }
      expectation.fulfill()
    }

    wait(for: [expectation], timeout: 1.0)
  }
}
