import APIota
import Foundation

/// An error encountered whilst using the Pusher Channels HTTP API Swift library.
public enum PusherError: LocalizedError {

  /// The `URLRequest` could not be initialized with a valid `URL`.
  case clientSide

  /// The response body could not be decoded to the specified type.
  case decodingError(_ error: DecodingError)

  /// The request body could not be encoded to the specified type.
  case encodingError(_ error: EncodingError)

  /// The server returned a failed response indicated by a non-successful HTTP `statusCode`.
  ///
  /// The `errorResponse` will contain a message indicating why the request failed, which
  /// can be helpful during development and in production.
  case failedResponse(statusCode: Int, errorResponse: String)

  /// An internal error occured.
  ///
  /// Some internal operation of the library has thrown an error. The reason for the failure
  /// can be inspected via the `localizedDescription` property of the `error` parameter.
  case internalError(_ error: Error)

  /// The server returned a response that was not a `HTTPURLResponse`.
  case unexpectedResponse

  /// A localized human-readable description of the error.
  public var errorDescription: String? {
    switch self {
    case .clientSide:
      return NSLocalizedString(
        "The URLRequest was not initialized with a valid URL",
        comment: "'clientSide' error text")

    case .decodingError(let error):
      return NSLocalizedString(
        "Decoding the response body failed with error: \(error)",
        comment: "'.decodingError(…)' error text")

    case .encodingError(let error):
      return NSLocalizedString(
        "Encoding the response body failed with error: \(error)",
        comment: "'.encodingError(…)' error text")

    case .failedResponse(statusCode: let code, errorResponse: let response):
      return NSLocalizedString(
        "The response failed with HTTP status code: \(code) and response: \(response)",
        comment: "'failedResponse' error text")

    case .internalError(let error):
      return NSLocalizedString(
        "The request failed with error: \(error)",
        comment: "'internalError' error text")

    case .unexpectedResponse:
      return NSLocalizedString(
        "The response was of an unexpected format",
        comment: "'unexpectedResponse' error text")
    }
  }
}

extension PusherError {

  /// Creates a `PusherError` which wraps another `Error`, offering additional context if it can be determined.
  /// - Parameter error: The `Error` to wrap inside the resulting `PusherError`.
  init(from error: Error) {

    // Handle the case where `error` is already a `PusherError`
    if error is PusherError {
      // swiftlint:disable:next force_cast
      self = error as! PusherError
      return
    }

    // Handle mapping from other `Error` types
    guard let apiClientError = error as? APIotaClientError<Data> else {
      if let decodingError = error as? DecodingError {
        self = .decodingError(decodingError)
      } else if let encodingError = error as? EncodingError {
        self = .encodingError(encodingError)
      } else {
        self = .internalError(error)
      }

      return
    }

    // Handle mapping from `APIotaClientError` -> `PusherError`
    switch apiClientError {
    case .clientSide:
      self = .clientSide

    case .decodingError(let error):
      self = .decodingError(error)

    case .encodingError(let error):
      self = .encodingError(error)

    case .failedResponse(statusCode: let code, errorResponseBody: let responseBody):
      let errorMessage = responseBody.toString()
      self = .failedResponse(statusCode: code.rawValue, errorResponse: errorMessage)

    case .internalError(let error):
      self = .internalError(error)

    case .unexpectedResponse:
      self = .unexpectedResponse
    }
  }
}

extension PusherError: Equatable {

  public static func == (lhs: PusherError, rhs: PusherError) -> Bool {
    switch (lhs, rhs) {
    case (.clientSide, .clientSide):
      return true

    case (.decodingError(let errorOne), .decodingError(let errorTwo)):
      return errorOne.localizedDescription == errorTwo.localizedDescription

    case (.encodingError(let errorOne), .encodingError(let errorTwo)):
      return errorOne.localizedDescription == errorTwo.localizedDescription

    case (
      .failedResponse(let codeOne, let responseBodyOne),
      .failedResponse(let codeTwo, let responseBodyTwo)
    ):
      return codeOne == codeTwo && responseBodyOne == responseBodyTwo

    case (.internalError(let errorOne), .internalError(let errorTwo)):
      return errorOne.localizedDescription == errorTwo.localizedDescription

    case (.unexpectedResponse, .unexpectedResponse):
      return true

    default:
      return false
    }
  }
}
