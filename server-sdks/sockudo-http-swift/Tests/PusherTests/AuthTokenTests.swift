import APIota
import XCTest

@testable import Pusher

final class AuthTokenTests: XCTestCase {

  private static let pusher = TestObjects.Client.shared

  func testAuthenticateEncryptedChannelSucceeds() {
    let expectation = XCTestExpectation(function: #function)
    let expectedSignature = """
      \(TestObjects.Client.testKey):\
      87d2a92fdd48d825606e714d77f63d64b1e6bdf26c309cbb7a847b00f4327b37
      """
    let expectedSharedSecret = "ARWWBbRkHlEsPXKXBcs0QU+MF87GmNyMTvT++YYi0Bw="
    Self.pusher.authenticate(
      channel: TestObjects.Channels.encrypted,
      socketId: TestObjects.AuthSignatures.testSocketId
    ) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { authToken in
        XCTAssertEqual(authToken.signature, expectedSignature)
        XCTAssertNil(authToken.userData)
        XCTAssertEqual(authToken.sharedSecret, expectedSharedSecret)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testAuthenticatePrivateChannelSucceeds() {
    let expectation = XCTestExpectation(function: #function)
    let expectedSignature = """
      \(TestObjects.Client.testKey):\
      3a06323ca30ff1a5e70078a6f995cab691a1005915efe5e68f966c877dd2e990
      """
    Self.pusher.authenticate(
      channel: TestObjects.Channels.private,
      socketId: TestObjects.AuthSignatures.testSocketId
    ) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { authToken in
        XCTAssertEqual(authToken.signature, expectedSignature)
        XCTAssertNil(authToken.userData)
        XCTAssertNil(authToken.sharedSecret)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testAuthenticatePresenceChannelSucceeds() {
    let expectation = XCTestExpectation(function: #function)
    let expectedSignature = """
      \(TestObjects.Client.testKey):\
      fbd326ddf91a43f884782074df8584a9707b5b32ecdd6b174e9cabb6a965580c
      """
    let expectedUserData = "{\"user_id\":\"user_1\"}"
    Self.pusher.authenticate(
      channel: TestObjects.Channels.presence,
      socketId: TestObjects.AuthSignatures.testSocketId,
      userData: TestObjects.AuthSignatures.presenceUserData
    ) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { authToken in
        XCTAssertEqual(authToken.signature, expectedSignature)
        XCTAssertTrue(Self.jsonStringsMatch(authToken.userData, expectedUserData))
        XCTAssertNil(authToken.sharedSecret)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testAuthenticatePresenceChannelWithUserInfoSucceeds() {
    let expectation = XCTestExpectation(function: #function)
    let expectedSignature = """
      \(TestObjects.Client.testKey):\
      18ec1dbc58c9d048c917e44690abf2c87ae954743cdbc7967682b396d36210f4
      """
    let expectedUserData = "{\"user_id\":\"user_1\",\"user_info\":{\"name\":\"Joe Bloggs\"}}"
    Self.pusher.authenticate(
      channel: TestObjects.Channels.presence,
      socketId: TestObjects.AuthSignatures.testSocketId,
      userData: TestObjects.AuthSignatures.presenceUserDataWithUserInfo
    ) { result in
      self.verifyAPIResultSuccess(result, expectation: expectation) { authToken in
        XCTAssertEqual(authToken.signature, expectedSignature)
        XCTAssertTrue(Self.jsonStringsMatch(authToken.userData, expectedUserData))
        XCTAssertNil(authToken.sharedSecret)
      }
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testAuthenticatePresenceChannelWithMissingUserDataFails() {
    let expectation = XCTestExpectation(function: #function)
    let authTokenServiceError = AuthenticationTokenService.Error.missingUserDataForPresenceChannel
    let expectedError = PusherError.internalError(authTokenServiceError)
    Self.pusher.authenticate(
      channel: TestObjects.Channels.presence,
      socketId: TestObjects.AuthSignatures.testSocketId
    ) { result in
      self.verifyAPIResultFailure(result, expectation: expectation, expectedError: expectedError)
    }
    wait(for: [expectation], timeout: 10.0)
  }

  func testAuthenticatePublicChannelFails() {
    let expectation = XCTestExpectation(function: #function)
    let authTokenServiceError = AuthenticationTokenService.Error
      .authenticationAttemptForPublicChannel
    let expectedError = PusherError.internalError(authTokenServiceError)
    Self.pusher.authenticate(
      channel: TestObjects.Channels.public,
      socketId: TestObjects.AuthSignatures.testSocketId
    ) { result in
      self.verifyAPIResultFailure(result, expectation: expectation, expectedError: expectedError)
    }
    wait(for: [expectation], timeout: 10.0)
  }

  private static func jsonStringsMatch(_ lhs: String?, _ rhs: String) -> Bool {
    guard
      let lhs,
      let lhsData = lhs.data(using: .utf8),
      let rhsData = rhs.data(using: .utf8),
      let lhsObject = try? JSONSerialization.jsonObject(with: lhsData),
      let rhsObject = try? JSONSerialization.jsonObject(with: rhsData)
    else {
      return false
    }

    return NSDictionary(dictionary: lhsObject as? [AnyHashable: Any] ?? [:]).isEqual(
      to: rhsObject as? [AnyHashable: Any] ?? [:]
    )
  }
}
