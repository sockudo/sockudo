import Foundation

extension Data {

  // MARK: - Hexadecimal encoding

  /// Options for methods used to Hex encode data.
  struct HexEncodingOptions: OptionSet {
    let rawValue: Int

    /// Set the characters in the encoded string to be uppercase
    /// (e.g `"BE32FA"` instead of `"be32fa"`).
    static let upperCasedCharacters = HexEncodingOptions(rawValue: 1 << 0)
  }

  /// Returns a hexadecimal encoded string.
  /// - Parameter options: The options to use for the encoding. Default value is `[]`.
  /// - Returns: A hexadecimal encoded string representation of the receiver.
  func hexEncodedString(options: HexEncodingOptions = []) -> String {
    let formatString = options.contains(.upperCasedCharacters) ? "%02hhX" : "%02hhx"

    return map { String(format: formatString, $0) }.joined()
  }

  // MARK: - Type conversion

  /// Initializes a `String` from the receiver.
  /// - Returns: The `String` representation of the receiver,
  ///            which is empty if the receiver was not UTF-8 encoded.
  func toString() -> String {
    return String(decoding: self, as: UTF8.self)
  }
}
