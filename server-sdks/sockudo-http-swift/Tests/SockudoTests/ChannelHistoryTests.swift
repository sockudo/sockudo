import Foundation
import XCTest

@testable import Sockudo

final class ChannelHistoryTests: XCTestCase {
  func testChannelHistoryEndpointBuildsExpectedPathAndQuery() throws {
    let endpoint = GetChannelHistoryEndpoint(
      channel: TestObjects.Channels.public,
      fetchOptions: .init(
        limit: 50,
        direction: "newest_first",
        cursor: "abc",
        startSerial: 10,
        endSerial: 20,
        startTimeMS: 1000,
        endTimeMS: 2000
      ),
      options: TestObjects.ClientOptions.withCluster
    )

    XCTAssertEqual(endpoint.path, "/apps/123456/channels/my-channel/history")
    let query = Dictionary(uniqueKeysWithValues: (endpoint.queryItems ?? []).map { ($0.name, $0.value ?? "") })
    XCTAssertEqual(query["limit"], "50")
    XCTAssertEqual(query["direction"], "newest_first")
    XCTAssertEqual(query["cursor"], "abc")
    XCTAssertEqual(query["start_serial"], "10")
    XCTAssertEqual(query["end_serial"], "20")
    XCTAssertEqual(query["start_time_ms"], "1000")
    XCTAssertEqual(query["end_time_ms"], "2000")
  }

  func testPresenceHistoryEndpointBuildsExpectedPathAndQuery() throws {
    let endpoint = GetPresenceHistoryEndpoint(
      channel: TestObjects.Channels.presence,
      fetchOptions: .init(
        limit: 50,
        direction: "newest_first",
        cursor: "abc",
        startSerial: 10,
        endSerial: 20,
        startTimeMS: 1000,
        endTimeMS: 2000
      ),
      options: TestObjects.ClientOptions.withCluster
    )

    XCTAssertEqual(endpoint.path, "/apps/123456/channels/presence-my-channel/presence/history")
    let query = Dictionary(uniqueKeysWithValues: (endpoint.queryItems ?? []).map { ($0.name, $0.value ?? "") })
    XCTAssertEqual(query["limit"], "50")
    XCTAssertEqual(query["direction"], "newest_first")
    XCTAssertEqual(query["cursor"], "abc")
    XCTAssertEqual(query["start_serial"], "10")
    XCTAssertEqual(query["end_serial"], "20")
    XCTAssertEqual(query["start_time_ms"], "1000")
    XCTAssertEqual(query["end_time_ms"], "2000")
  }

  func testPresenceSnapshotEndpointBuildsExpectedPathAndQuery() throws {
    let endpoint = GetPresenceSnapshotEndpoint(
      channel: TestObjects.Channels.presence,
      fetchOptions: .init(
        atTimeMS: 1000,
        atSerial: 20
      ),
      options: TestObjects.ClientOptions.withCluster
    )

    XCTAssertEqual(endpoint.path, "/apps/123456/channels/presence-my-channel/presence/history/snapshot")
    let query = Dictionary(uniqueKeysWithValues: (endpoint.queryItems ?? []).map { ($0.name, $0.value ?? "") })
    XCTAssertEqual(query["at_time_ms"], "1000")
    XCTAssertEqual(query["at_serial"], "20")
  }
}
