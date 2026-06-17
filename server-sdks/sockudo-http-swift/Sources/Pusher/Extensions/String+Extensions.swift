import Foundation

extension String {

  // MARK: - Type conversion

  /// Initializes `Data` from the receiver.
  /// - Returns: The `Data` representation of the receiver,
  ///            which is empty if the receiver was not UTF-8 encoded.
  func toData() -> Data {
    return Data(self.utf8)
  }
}
