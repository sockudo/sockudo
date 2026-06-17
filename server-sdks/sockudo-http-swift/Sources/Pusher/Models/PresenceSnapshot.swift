import Foundation

public struct PresenceSnapshot: Decodable, Equatable {
  public let channel: String
  public let members: [PresenceSnapshotMember]
  public let memberCount: UInt
  public let eventsReplayed: UInt64
  public let snapshotSerial: UInt64?
  public let snapshotTimeMS: Int64?
  public let continuity: PresenceHistoryContinuity

  enum CodingKeys: String, CodingKey {
    case channel
    case members
    case memberCount = "member_count"
    case eventsReplayed = "events_replayed"
    case snapshotSerial = "snapshot_serial"
    case snapshotTimeMS = "snapshot_time_ms"
    case continuity
  }
}

public struct PresenceSnapshotMember: Decodable, Equatable {
  public let userID: String
  public let lastEvent: String
  public let lastEventSerial: UInt64
  public let lastEventAtMS: Int64

  enum CodingKeys: String, CodingKey {
    case userID = "user_id"
    case lastEvent = "last_event"
    case lastEventSerial = "last_event_serial"
    case lastEventAtMS = "last_event_at_ms"
  }
}
