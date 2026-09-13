import ActivityKit
import Foundation

/// Mirrors the server payload: `attributesType: "RideAttributes"`, `attributes: { rideID }`,
/// and `contentState: { status, etaMinutes }`.
struct RideAttributes: ActivityAttributes {
  struct ContentState: Codable, Hashable {
    var status: String
    var etaMinutes: Int
  }

  var rideID: String
}
