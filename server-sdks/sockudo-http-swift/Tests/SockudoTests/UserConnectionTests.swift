import APIota
import XCTest

@testable import Sockudo

final class UserConnectionTests: XCTestCase {
  private let options = TestObjects.ClientOptions.withCustomPort

  func testTerminateUserConnectionsEndpointBuildsCorrectPath() throws {
    let endpoint = TerminateUserConnectionsEndpoint(userId: "user-123", options: options)
    let request = try endpoint.request(baseUrlComponents: baseComponents())

    XCTAssertEqual(request.httpMethod, "POST")
    XCTAssertEqual(request.url?.path, "/apps/123456/users/user-123/terminate_connections")
    XCTAssertNotNil(
      URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?
        .queryItems?
        .first(where: { $0.name == "auth_signature" }))
  }

  func testForceReconnectUserEndpointBuildsCorrectPath() throws {
    let endpoint = ForceReconnectUserEndpoint(userId: "user-456", options: options)
    let request = try endpoint.request(baseUrlComponents: baseComponents())

    XCTAssertEqual(request.httpMethod, "POST")
    XCTAssertEqual(request.url?.path, "/apps/123456/users/user-456/force_reconnect")
    XCTAssertNotNil(
      URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?
        .queryItems?
        .first(where: { $0.name == "auth_signature" }))
  }

  func testTerminateUserConnectionsEndpointIncludesAuthSignature() throws {
    let endpoint = TerminateUserConnectionsEndpoint(userId: "user-123", options: options)
    let request = try endpoint.request(baseUrlComponents: baseComponents())
    let queryItems =
      URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems ?? []

    let timestamp = queryItems.first(where: { $0.name == "auth_timestamp" })!.value!
    let expectedToSign = """
      POST
      /apps/123456/users/user-123/terminate_connections
      auth_key=auth_key&auth_timestamp=\(timestamp)&auth_version=1.0
      """
    let expectedSignature = CryptoService.sha256HMAC(
      for: expectedToSign.toData(),
      using: TestObjects.ClientOptions.testSecret.toData()
    ).hexEncodedString()
    XCTAssertEqual(
      queryItems.first(where: { $0.name == "auth_signature" })?.value,
      expectedSignature)
  }

  func testForceReconnectUserEndpointIncludesAuthSignature() throws {
    let endpoint = ForceReconnectUserEndpoint(userId: "user-456", options: options)
    let request = try endpoint.request(baseUrlComponents: baseComponents())
    let queryItems =
      URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems ?? []

    let timestamp = queryItems.first(where: { $0.name == "auth_timestamp" })!.value!
    let expectedToSign = """
      POST
      /apps/123456/users/user-456/force_reconnect
      auth_key=auth_key&auth_timestamp=\(timestamp)&auth_version=1.0
      """
    let expectedSignature = CryptoService.sha256HMAC(
      for: expectedToSign.toData(),
      using: TestObjects.ClientOptions.testSecret.toData()
    ).hexEncodedString()
    XCTAssertEqual(
      queryItems.first(where: { $0.name == "auth_signature" })?.value,
      expectedSignature)
  }

  private func baseComponents() -> URLComponents {
    var components = URLComponents()
    components.scheme = options.scheme
    components.host = options.host
    components.port = options.port
    return components
  }
}
