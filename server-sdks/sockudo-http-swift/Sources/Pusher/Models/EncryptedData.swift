import Foundation

/// Some encrypted data.
///
/// This can be used to store the encrypted form of some sensitive data,
/// sent as part of a triggered `Event`, or received as a `WebhookEvent`.
struct EncryptedData: Codable {

  /// The nonce data, represented as a base-64 encoded `String`.
  let nonce: String

  /// The encrypted data, represented as a base-64 encoded `String`.
  let ciphertext: String

  // MARK: - Lifecycle

  /// Creates an instance of `EncryptedData` from nonce and ciphertext `Data`.
  /// - Parameters:
  ///   - nonceData: The nonce `Data` used during the encryption.
  ///   - ciphertextData: The encrypted ciphertext `Data`.
  init(nonceData: Data, ciphertextData: Data) {
    self.nonce = nonceData.base64EncodedString()
    self.ciphertext = ciphertextData.base64EncodedString()
  }
}
