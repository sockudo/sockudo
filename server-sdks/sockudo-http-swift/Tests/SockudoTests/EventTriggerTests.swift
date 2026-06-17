import APIota
import XCTest

@testable import Sockudo

final class EventTriggerTests: XCTestCase {

  private static let sockudo = TestObjects.Client.shared

  override func setUpWithError() throws {
    try LiveTestSupport.requireLiveConfig()
  }

  // MARK: - POST single event tests

  func testPostEventToChannelSucceedsForEncryptedChannel() throws {
    let expectation = XCTestExpectation(function: #function)
    Self.sockudo.trigger(event: TestObjects.Events.encrypted) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { channelSummaries in
        XCTAssertEqual(channelSummaries.count, 0)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testPostEventToChannelSucceedsForPrivateChannel() throws {
    let expectation = XCTestExpectation(function: #function)
    Self.sockudo.trigger(event: TestObjects.Events.private) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { channelSummaries in
        XCTAssertEqual(channelSummaries.count, 0)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testPostEventToChannelSucceedsForPublicChannel() throws {
    let expectation = XCTestExpectation(function: #function)
    Self.sockudo.trigger(event: TestObjects.Events.public) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { channelSummaries in
        XCTAssertEqual(channelSummaries.count, 0)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testPostEventToChannelSucceedsForValidMultichannelEvent() throws {
    let expectation = XCTestExpectation(function: #function)
    Self.sockudo.trigger(event: TestObjects.Events.multichannel) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { channelSummaries in
        XCTAssertEqual(channelSummaries.count, 0)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testPostEventWithIdempotencyKeySucceeds() throws {
    let expectation = XCTestExpectation(function: #function)
    Self.sockudo.trigger(event: TestObjects.Events.withIdempotencyKey) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { channelSummaries in
        XCTAssertEqual(channelSummaries.count, 0)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testPostEventWithIdempotencyKeySucceedsForEncryptedChannel() throws {
    let expectation = XCTestExpectation(function: #function)
    Self.sockudo.trigger(event: TestObjects.Events.encryptedWithIdempotencyKey) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { channelSummaries in
        XCTAssertEqual(channelSummaries.count, 0)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testEventIdempotencyKeyIsEncodedInJSON() throws {
    let event = try Event(
      name: "my-event",
      data: TestObjects.Events.eventData,
      channel: TestObjects.Channels.public,
      idempotencyKey: "unique-key-789")
    let encodedData = try JSONEncoder().encode(event)
    let json = try JSONSerialization.jsonObject(with: encodedData) as! [String: Any]
    XCTAssertEqual(json["idempotency_key"] as? String, "unique-key-789")
  }

  func testEventWithoutIdempotencyKeyEncodesNullKey() throws {
    let event = try Event(
      name: "my-event",
      data: TestObjects.Events.eventData,
      channel: TestObjects.Channels.public)
    let encodedData = try JSONEncoder().encode(event)
    let json = try JSONSerialization.jsonObject(with: encodedData) as! [String: Any]
    XCTAssertTrue(json["idempotency_key"] is NSNull)
  }

  func testGenerateIdempotencyKeyReturnsUniqueValues() {
    let key1 = Sockudo.generateIdempotencyKey()
    let key2 = Sockudo.generateIdempotencyKey()
    XCTAssertNotEqual(key1, key2)
    XCTAssertFalse(key1.isEmpty)
    XCTAssertFalse(key2.isEmpty)
  }

  func testGenerateIdempotencyKeyReturnsValidUUID() {
    let key = Sockudo.generateIdempotencyKey()
    XCTAssertNotNil(UUID(uuidString: key))
  }

  func testPostEventToChannelFailsForInvalidMultichannelEvent() throws {
    XCTAssertThrowsError(
      try Event(
        name: "my-multichannel-event",
        data: TestObjects.Events.eventData,
        channels: [
          TestObjects.Channels.encrypted,
          TestObjects.Channels.public,
        ])
    ) { error in
      guard let eventError = error as? Event.Error else {
        XCTFail("The error should be a 'Event.Error'.")

        return
      }

      XCTAssertEqual(eventError, .invalidMultichannelEventConfiguration)
    }
  }

  // MARK: - POST batch event tests

  func testPostBatchEventsToChannelSucceedsForSingleChannelEvents() throws {
    let expectation = XCTestExpectation(function: #function)
    let testEvents = [
      TestObjects.Events.encrypted,
      TestObjects.Events.private,
      TestObjects.Events.public,
    ]
    Self.sockudo.trigger(events: testEvents) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { channelInfoList in
        XCTAssertEqual(channelInfoList.count, 0)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testPostBatchEventsWithIdempotencyKeySucceeds() throws {
    let expectation = XCTestExpectation(function: #function)
    let testEvents = [
      TestObjects.Events.private,
      TestObjects.Events.public,
    ]
    let idempotencyKey = Sockudo.generateIdempotencyKey()
    Self.sockudo.trigger(events: testEvents, idempotencyKey: idempotencyKey) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { channelInfoList in
        XCTAssertEqual(channelInfoList.count, 0)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testPostBatchEventsToChannelFailsForTooLargeBatch() throws {
    let expectation = XCTestExpectation(function: #function)
    let expectedErrorMessage = """
      Batch too large (11 > 10)\n
      """
    let expectedError = SockudoError.failedResponse(
      statusCode: HTTPStatusCode.badRequest.rawValue,
      errorResponse: expectedErrorMessage)
    let testEvents = [
      TestObjects.Events.encrypted,
      TestObjects.Events.private,
      TestObjects.Events.public,
      TestObjects.Events.encrypted,
      TestObjects.Events.private,
      TestObjects.Events.public,
      TestObjects.Events.encrypted,
      TestObjects.Events.private,
      TestObjects.Events.public,
      TestObjects.Events.encrypted,
      TestObjects.Events.private,
    ]
    Self.sockudo.trigger(events: testEvents) { result in
      self.verifyAPIResultFailure(
        result,
        expectation: expectation,
        expectedError: expectedError)
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testPostBatchEventsToChannelFailsForMultichannelEvents() throws {
    let expectation = XCTestExpectation(function: #function)
    let expectedErrorMessage = """
      Missing required parameter: channel\n
      """
    let expectedError = SockudoError.failedResponse(
      statusCode: HTTPStatusCode.badRequest.rawValue,
      errorResponse: expectedErrorMessage)
    let testEvents = [TestObjects.Events.multichannel]
    Self.sockudo.trigger(events: testEvents) { result in
      self.verifyAPIResultFailure(
        result,
        expectation: expectation,
        expectedError: expectedError)
    }
    wait(for: [expectation], timeout: 10.0)
  }
}
