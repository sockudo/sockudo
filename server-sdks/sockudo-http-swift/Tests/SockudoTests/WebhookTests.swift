import APIota
import XCTest

@testable import Sockudo

final class WebhookTests: XCTestCase {

  private static let sockudo = TestObjects.Client.shared

  private func loadForwardCompatFixture(_ name: String) throws -> Data {
    var directory = URL(fileURLWithPath: #file).deletingLastPathComponent()
    while directory.path != directory.deletingLastPathComponent().path {
      let candidate = directory
        .appendingPathComponent("tests")
        .appendingPathComponent("ai-conformance")
        .appendingPathComponent("fixtures")
        .appendingPathComponent("forward-compat")
        .appendingPathComponent(name)
      if FileManager.default.fileExists(atPath: candidate.path) {
        return try Data(contentsOf: candidate)
      }
      directory = directory.deletingLastPathComponent()
    }
    throw NSError(domain: "SockudoTests", code: 1)
  }

  private func signedSockudoWebhookRequest(body: Data) -> URLRequest {
    var request = URLRequest(url: URL(string: "https://google.com")!)
    request.httpBody = body
    request.setValue(
      TestObjects.Client.testKey,
      forHTTPHeaderField: WebhookService.xSockudoKeyHeader)
    request.setValue(
      CryptoService.sha256HMAC(
        for: body,
        using: TestObjects.Client.testSecret.toData()
      ).hexEncodedString(),
      forHTTPHeaderField: WebhookService.xSockudoSignatureHeader)
    return request
  }

  func testVerifyChannelOccupiedWebhookSucceeds() {
    let expectation = XCTestExpectation(function: #function)
    Self.sockudo.verifyWebhook(request: TestObjects.Webhooks.channelOccupiedWebhookRequest) {
      result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { webhook in
        XCTAssertEqual(webhook.createdAt, Date(timeIntervalSince1970: 1_619_602_993))
        XCTAssertEqual(webhook.events.count, 1)
        XCTAssertEqual(webhook.events.first!.eventType, .channelOccupied)
        XCTAssertEqual(webhook.events.first!.channel.name, "my-channel")
        XCTAssertEqual(webhook.events.first!.channel.type, .public)
        XCTAssertNil(webhook.events.first!.event)
        XCTAssertNil(webhook.events.first!.socketId)
        XCTAssertNil(webhook.events.first!.userId)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testVerifyChannelVacatedWebhookSucceeds() {
    let expectation = XCTestExpectation(function: #function)
    Self.sockudo.verifyWebhook(request: TestObjects.Webhooks.channelVacatedWebhookRequest) {
      result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { webhook in
        XCTAssertEqual(webhook.createdAt, Date(timeIntervalSince1970: 1_619_602_993))
        XCTAssertEqual(webhook.events.count, 1)
        XCTAssertEqual(webhook.events.first!.eventType, .channelVacated)
        XCTAssertEqual(webhook.events.first!.channel.name, "my-channel")
        XCTAssertEqual(webhook.events.first!.channel.type, .public)
        XCTAssertNil(webhook.events.first!.event)
        XCTAssertNil(webhook.events.first!.socketId)
        XCTAssertNil(webhook.events.first!.userId)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testVerifyMemberAddedWebhookSucceeds() {
    let expectation = XCTestExpectation(function: #function)
    Self.sockudo.verifyWebhook(request: TestObjects.Webhooks.memberAddedWebhookRequest) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { webhook in
        XCTAssertEqual(webhook.createdAt, Date(timeIntervalSince1970: 1_619_602_993))
        XCTAssertEqual(webhook.events.count, 1)
        XCTAssertEqual(webhook.events.first!.eventType, .memberAdded)
        XCTAssertEqual(webhook.events.first!.channel.name, "my-channel")
        XCTAssertEqual(webhook.events.first!.channel.type, .presence)
        XCTAssertEqual(webhook.events.first!.userId, "user_1")
        XCTAssertNil(webhook.events.first!.event)
        XCTAssertNil(webhook.events.first!.socketId)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testVerifyMemberRemovedWebhookSucceeds() {
    let expectation = XCTestExpectation(function: #function)
    Self.sockudo.verifyWebhook(request: TestObjects.Webhooks.memberRemovedWebhookRequest) {
      result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { webhook in
        XCTAssertEqual(webhook.createdAt, Date(timeIntervalSince1970: 1_619_602_993))
        XCTAssertEqual(webhook.events.count, 1)
        XCTAssertEqual(webhook.events.first!.eventType, .memberRemoved)
        XCTAssertEqual(webhook.events.first!.channel.name, "my-channel")
        XCTAssertEqual(webhook.events.first!.channel.type, .presence)
        XCTAssertEqual(webhook.events.first!.userId, "user_1")
        XCTAssertNil(webhook.events.first!.event)
        XCTAssertNil(webhook.events.first!.socketId)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testVerifyClientEventWebhookSucceeds() throws {
    let expectation = XCTestExpectation(function: #function)
    Self.sockudo.verifyWebhook(request: TestObjects.Webhooks.clientEventWebhookRequest) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { webhook in
        XCTAssertEqual(webhook.createdAt, Date(timeIntervalSince1970: 1_619_602_993))
        XCTAssertEqual(webhook.events.count, 1)
        XCTAssertEqual(webhook.events.first!.eventType, .clientEvent)
        XCTAssertEqual(webhook.events.first!.channel.name, "my-channel")
        XCTAssertEqual(webhook.events.first!.channel.type, .public)
        XCTAssertEqual(webhook.events.first!.event?.name, "my-event")
        XCTAssertNotNil(webhook.events.first!.event?.data)
        let decodedEventData = try? JSONDecoder().decode(
          MockEventData.self,
          from: webhook.events.first!.event!.data)
        XCTAssertEqual(decodedEventData?.name, TestObjects.Events.eventData.name)
        XCTAssertEqual(decodedEventData?.age, TestObjects.Events.eventData.age)
        XCTAssertEqual(decodedEventData?.job, TestObjects.Events.eventData.job)
        XCTAssertEqual(webhook.events.first!.socketId, "socket_1")
        XCTAssertNil(webhook.events.first!.userId)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testVerifyFutureWebhookFixtureSucceeds() throws {
    let expectation = XCTestExpectation(function: #function)
    let body = try loadForwardCompatFixture("future-webhook-events.json")
    Self.sockudo.verifyWebhook(request: signedSockudoWebhookRequest(body: body)) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { webhook in
        XCTAssertEqual(webhook.createdAt, Date(timeIntervalSince1970: 1_710_000_000))
        XCTAssertEqual(webhook.events.count, 3)
        XCTAssertEqual(webhook.events[0].eventType, .unknown("member_updated"))
        XCTAssertEqual(webhook.events[0].eventType.rawValue, "member_updated")
        XCTAssertEqual(webhook.events[0].channel.fullName, "presence-ai-forward")
        XCTAssertEqual(webhook.events[0].userId, "user-1")
        XCTAssertEqual(webhook.events[0].rawPayload["future_field"]?.value as? String, "must-pass-through")
        XCTAssertEqual(webhook.events[1].eventType, .unknown("ai_run_started"))
        XCTAssertEqual(webhook.events[1].rawPayload["run_id"]?.value as? String, "run-1")
        XCTAssertEqual(webhook.events[2].eventType, .unknown("message_version_created"))
        XCTAssertEqual(webhook.events[2].rawPayload["version_serial"]?.value as? String, "ver-1")
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testVerifyNestedFutureWebhookValuesSucceeds() throws {
    let expectation = XCTestExpectation(function: #function)
    let body = """
      {"time_ms":1710000000000,"events":[{"name":"ai_turn_started","channel":"private-ai-forward","data":{"turn_id":"turn-1","tokens":["hello","world"],"done":false,"nullable":null},"future_field":{"nested":true}}]}
      """.data(using: .utf8)!
    Self.sockudo.verifyWebhook(request: signedSockudoWebhookRequest(body: body)) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { webhook in
        XCTAssertEqual(webhook.events[0].eventType, .unknown("ai_turn_started"))
        let data = webhook.events[0].rawPayload["data"]?.value as? [String: Any]
        XCTAssertEqual(data?["turn_id"] as? String, "turn-1")
        XCTAssertEqual(data?["tokens"] as? [String], ["hello", "world"])
        XCTAssertEqual(data?["done"] as? Bool, false)
        XCTAssertTrue(data?["nullable"] is NSNull)
        let futureField = webhook.events[0].rawPayload["future_field"]?.value as? [String: Any]
        XCTAssertEqual(futureField?["nested"] as? Bool, true)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testMissingSockudoKeyHeaderWebhookFails() {
    let expectation = XCTestExpectation(function: #function)
    Self.sockudo.verifyWebhook(request: TestObjects.Webhooks.missingKeyHeaderWebhookRequest) {
      result in
      let expectedError = SockudoError.internalError(
        WebhookService.Error.xSockudoKeyHeaderMissingOrInvalid)
      self.verifyAPIResultFailure(result, expectation: expectation, expectedError: expectedError)
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testInvalidSockudoKeyHeaderWebhookFails() {
    let expectation = XCTestExpectation(function: #function)
    Self.sockudo.verifyWebhook(request: TestObjects.Webhooks.invalidKeyHeaderWebhookRequest) {
      result in
      let expectedError = SockudoError.internalError(
        WebhookService.Error.xSockudoKeyHeaderMissingOrInvalid)
      self.verifyAPIResultFailure(result, expectation: expectation, expectedError: expectedError)
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testMissingSockudoSignatureHeaderWebhookFails() {
    let expectation = XCTestExpectation(function: #function)
    Self.sockudo.verifyWebhook(request: TestObjects.Webhooks.missingSignatureHeaderWebhookRequest) {
      result in
      let expectedError = SockudoError.internalError(
        WebhookService.Error.xSockudoSignatureHeaderMissingOrInvalid)
      self.verifyAPIResultFailure(result, expectation: expectation, expectedError: expectedError)
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testInvalidSockudoSignatureHeaderWebhookFails() {
    let expectation = XCTestExpectation(function: #function)
    Self.sockudo.verifyWebhook(request: TestObjects.Webhooks.invalidSignatureHeaderWebhookRequest) {
      result in
      let expectedError = SockudoError.internalError(
        WebhookService.Error.xSockudoSignatureHeaderMissingOrInvalid)
      self.verifyAPIResultFailure(result, expectation: expectation, expectedError: expectedError)
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testMissingBodyDataWebhookFails() {
    let expectation = XCTestExpectation(function: #function)
    Self.sockudo.verifyWebhook(request: TestObjects.Webhooks.missingBodyDataWebhookRequest) {
      result in
      let expectedError = SockudoError.internalError(WebhookService.Error.bodyDataMissing)
      self.verifyAPIResultFailure(result, expectation: expectation, expectedError: expectedError)
    }
    wait(for: [expectation], timeout: 10.0)
  }
}
