import APIota
import XCTest

@testable import Sockudo

final class WebhookTests: XCTestCase {

  private static let sockudo = TestObjects.Client.shared

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
