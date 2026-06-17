import Foundation

public struct PresenceHistoryFetchOptions: Sendable, Equatable {
  public let limit: UInt?
  public let direction: String?
  public let cursor: String?
  public let startSerial: UInt64?
  public let endSerial: UInt64?
  public let startTimeMS: Int64?
  public let endTimeMS: Int64?

  public init(
    limit: UInt? = nil,
    direction: String? = nil,
    cursor: String? = nil,
    startSerial: UInt64? = nil,
    endSerial: UInt64? = nil,
    startTimeMS: Int64? = nil,
    endTimeMS: Int64? = nil
  ) {
    self.limit = limit
    self.direction = direction
    self.cursor = cursor
    self.startSerial = startSerial
    self.endSerial = endSerial
    self.startTimeMS = startTimeMS
    self.endTimeMS = endTimeMS
  }

  var queryItems: [URLQueryItem] {
    var items = [URLQueryItem]()
    if let limit { items.append(URLQueryItem(name: "limit", value: String(limit))) }
    if let direction { items.append(URLQueryItem(name: "direction", value: direction)) }
    if let cursor { items.append(URLQueryItem(name: "cursor", value: cursor)) }
    if let startSerial { items.append(URLQueryItem(name: "start_serial", value: String(startSerial))) }
    if let endSerial { items.append(URLQueryItem(name: "end_serial", value: String(endSerial))) }
    if let startTimeMS { items.append(URLQueryItem(name: "start_time_ms", value: String(startTimeMS))) }
    if let endTimeMS { items.append(URLQueryItem(name: "end_time_ms", value: String(endTimeMS))) }
    return items
  }
}
