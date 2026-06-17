import Foundation

extension JSONEncoder {
  convenience init(
    dateEncodingStrategy: DateEncodingStrategy,
    outputFormatting: OutputFormatting = [],
    keyEncodingStrategy: KeyEncodingStrategy = .useDefaultKeys
  ) {
    self.init()
    self.dateEncodingStrategy = dateEncodingStrategy
    self.outputFormatting = outputFormatting
    self.keyEncodingStrategy = keyEncodingStrategy
  }
}

extension JSONEncoder {
  static let shared = JSONEncoder()
  static let iso8601 = JSONEncoder(dateEncodingStrategy: .iso8601)
  static let iso8601Ordered = JSONEncoder(
    dateEncodingStrategy: .iso8601, outputFormatting: .sortedKeys)
  static let iso8601PrettyPrinted = JSONEncoder(
    dateEncodingStrategy: .iso8601, outputFormatting: .prettyPrinted)
}
