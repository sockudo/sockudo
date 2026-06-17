import APIota
import XCTest

@testable import Pusher

final class EventTriggerTests: XCTestCase {

  private static let pusher = TestObjects.Client.shared

  override func setUpWithError() throws {
    try LiveTestSupport.requireLiveConfig()
  }

  // MARK: - POST single event tests

  func testPostEventToChannelSucceedsForEncryptedChannel() throws {
    let expectation = XCTestExpectation(function: #function)
    Self.pusher.trigger(event: TestObjects.Events.encrypted) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { channelSummaries in
        XCTAssertEqual(channelSummaries.count, 0)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testPostEventToChannelSucceedsForPrivateChannel() throws {
    let expectation = XCTestExpectation(function: #function)
    Self.pusher.trigger(event: TestObjects.Events.private) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { channelSummaries in
        XCTAssertEqual(channelSummaries.count, 0)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testPostEventToChannelSucceedsForPublicChannel() throws {
    let expectation = XCTestExpectation(function: #function)
    Self.pusher.trigger(event: TestObjects.Events.public) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { channelSummaries in
        XCTAssertEqual(channelSummaries.count, 0)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testPostEventToChannelSucceedsForValidMultichannelEvent() throws {
    let expectation = XCTestExpectation(function: #function)
    Self.pusher.trigger(event: TestObjects.Events.multichannel) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { channelSummaries in
        XCTAssertEqual(channelSummaries.count, 0)
      }
    }
    wait(for: [expectation], timeout: 10.0)
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
    Self.pusher.trigger(events: testEvents) { result in
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
    let expectedError = PusherError.failedResponse(
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
    Self.pusher.trigger(events: testEvents) { result in
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
    let expectedError = PusherError.failedResponse(
      statusCode: HTTPStatusCode.badRequest.rawValue,
      errorResponse: expectedErrorMessage)
    let testEvents = [TestObjects.Events.multichannel]
    Self.pusher.trigger(events: testEvents) { result in
      self.verifyAPIResultFailure(
        result,
        expectation: expectation,
        expectedError: expectedError)
    }
    wait(for: [expectation], timeout: 10.0)
  }
}
