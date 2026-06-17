import Foundation

public struct PresenceSnapshotFetchOptions: Sendable, Equatable {
  public let atTimeMS: Int64?
  public let atSerial: UInt64?

  public init(
    atTimeMS: Int64? = nil,
    atSerial: UInt64? = nil
  ) {
    self.atTimeMS = atTimeMS
    self.atSerial = atSerial
  }

  var queryItems: [URLQueryItem] {
    var items = [URLQueryItem]()
    if let atTimeMS { items.append(URLQueryItem(name: "at_time_ms", value: String(atTimeMS))) }
    if let atSerial { items.append(URLQueryItem(name: "at_serial", value: String(atSerial))) }
    return items
  }
}
