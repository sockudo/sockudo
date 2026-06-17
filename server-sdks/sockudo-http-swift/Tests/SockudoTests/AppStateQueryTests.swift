import APIota
import XCTest

@testable import Sockudo

final class AppStateQueryTests: XCTestCase {

  private static let sockudo = TestObjects.Client.shared

  override func setUpWithError() throws {
    try LiveTestSupport.requireLiveConfig()
  }

  // MARK: - GET channels tests

  func testGetChannelsSucceeds() {
    let expectation = XCTestExpectation(function: #function)
    Self.sockudo.channels { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { channelSummaries in
        XCTAssertEqual(channelSummaries.count, 0)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testGetChannelsFailsForInvalidAttributes() {
    let expectation = XCTestExpectation(function: #function)
    let expectedErrorMessage = """
      user_count may only be requested for presence channels - \
      please supply filter_by_prefix begining with presence-\n
      """
    let expectedError = SockudoError.failedResponse(
      statusCode: HTTPStatusCode.badRequest.rawValue,
      errorResponse: expectedErrorMessage)
    Self.sockudo.channels(
      withFilter: .private,
      attributeOptions: .userCount
    ) { result in
      self.verifyAPIResultFailure(
        result,
        expectation: expectation,
        expectedError: expectedError)
    }
    wait(for: [expectation], timeout: 10.0)
  }

  // MARK: - GET channel info tests

  func testGetChannelInfoSucceeds() {
    let expectation = XCTestExpectation(function: #function)
    Self.sockudo.channelInfo(for: TestObjects.Channels.public) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { channelInfo in
        XCTAssertNotNil(channelInfo)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testGetChannelInfoFailsForInvalidAttributes() {
    let expectation = XCTestExpectation(function: #function)
    let expectedErrorMessage = """
      Cannot retrieve the user count unless the channel is a presence channel\n
      """
    let expectedError = SockudoError.failedResponse(
      statusCode: HTTPStatusCode.badRequest.rawValue,
      errorResponse: expectedErrorMessage)
    Self.sockudo.channelInfo(
      for: TestObjects.Channels.public,
      attributeOptions: .userCount
    ) { result in
      self.verifyAPIResultFailure(
        result,
        expectation: expectation,
        expectedError: expectedError)
    }
    wait(for: [expectation], timeout: 10.0)
  }

  // MARK: - GET users tests

  func testGetUsersForChannelSucceedsForPresenceChannel() {
    let expectation = XCTestExpectation(function: #function)
    Self.sockudo.users(for: TestObjects.Channels.presence) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { users in
        XCTAssertEqual(users.count, 0)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testGetUsersForChannelFailsForPublicChannel() {
    let expectation = XCTestExpectation(function: #function)
    let expectedErrorMessage = """
      Users can only be retrieved for presence channels\n
      """
    let expectedError = SockudoError.failedResponse(
      statusCode: HTTPStatusCode.badRequest.rawValue,
      errorResponse: expectedErrorMessage)
    Self.sockudo.users(for: TestObjects.Channels.public) { result in
      self.verifyAPIResultFailure(
        result,
        expectation: expectation,
        expectedError: expectedError)
    }
    wait(for: [expectation], timeout: 10.0)
  }
}
