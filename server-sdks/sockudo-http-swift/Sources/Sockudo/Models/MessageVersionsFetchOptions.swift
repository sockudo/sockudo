import Foundation

public struct MessageVersionsFetchOptions: Sendable, Equatable {
  public let limit: UInt?
  public let direction: String?
  public let cursor: String?

  public init(
    limit: UInt? = nil,
    direction: String? = nil,
    cursor: String? = nil
  ) {
    self.limit = limit
    self.direction = direction
    self.cursor = cursor
  }

  var queryItems: [URLQueryItem] {
    var items = [URLQueryItem]()
    if let limit { items.append(URLQueryItem(name: "limit", value: String(limit))) }
    if let direction { items.append(URLQueryItem(name: "direction", value: direction)) }
    if let cursor { items.append(URLQueryItem(name: "cursor", value: cursor)) }
    return items
  }
}

public struct AnnotationEventsFetchOptions: Sendable, Equatable {
  public let type: String?
  public let fromSerial: String?
  public let limit: UInt?
  public let socketID: String?

  public init(
    type: String? = nil,
    fromSerial: String? = nil,
    limit: UInt? = nil,
    socketID: String? = nil
  ) {
    self.type = type
    self.fromSerial = fromSerial
    self.limit = limit
    self.socketID = socketID
  }

  var queryItems: [URLQueryItem] {
    var items = [URLQueryItem]()
    if let type { items.append(URLQueryItem(name: "type", value: type)) }
    if let fromSerial { items.append(URLQueryItem(name: "from_serial", value: fromSerial)) }
    if let limit { items.append(URLQueryItem(name: "limit", value: String(limit))) }
    if let socketID { items.append(URLQueryItem(name: "socket_id", value: socketID)) }
    return items
  }
}
