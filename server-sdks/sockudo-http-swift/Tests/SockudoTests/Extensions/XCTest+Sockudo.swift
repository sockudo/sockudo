import XCTest

@testable import Sockudo

extension XCTestCase {

  /// Verify that a result of an API request contains a specific error.
  ///
  /// This helper method is useful when testing against expected error conditions.
  /// - Parameters:
  ///   - result: A `Result<T, SockudoError>` returned from an API request. (Where `T: Decodable`).
  ///   - expectation: An `XCTestExpectation` which will be fulfilled after inspecting `result`.
  ///   - expectedError: The `SockudoError` which is expected to be found inside `result`.
  ///   - file: The source file that called the receiver.
  ///   - function: The function that called the receiver.
  ///   - line: The line number of the call site of the receiver.
  func verifyAPIResultFailure<T>(
    _ result: Result<T, SockudoError>,
    expectation: XCTestExpectation,
    expectedError: SockudoError,
    file: StaticString = #file,
    function: StaticString = #function,
    line: UInt = #line
  ) {
    switch result {
    case .success:
      XCTFail("This test should not succeed.", file: file, line: line)

    case .failure(let error):
      XCTAssertEqual(error, expectedError, file: file, line: line)
    }
    expectation.fulfill()
  }

  /// Verify that a result of an API request contains a successful result.
  /// - Parameters:
  ///   - result: A `Result<T, SockudoError>` returned from an API request. (Where `T: Decodable`).
  ///   - expectation: An `XCTestExpectation` which will be fulfilled after inspecting `result`.
  ///   - validateResultCallback: A closure executed when `result` contains a decoded value.
  ///                             This can be used to inspect the value and determine its correctness.
  ///   - file: The source file that called the receiver.
  ///   - function: The function that called the receiver.
  ///   - line: The line number of the call site of the receiver.
  func verifyAPIResultSuccess<T>(
    _ result: Result<T, SockudoError>,
    expectation: XCTestExpectation,
    validateResultCallback: (T) -> Void,
    file: StaticString = #file,
    function: StaticString = #function,
    line: UInt = #line
  ) {
    switch result {
    case .success(let value):
      validateResultCallback(value)

    case .failure(let error):
      XCTFail(
        "This test should not fail. Failed with error: \(error.localizedDescription)",
        file: file,
        line: line)
    }
    expectation.fulfill()
  }
}

extension XCTestExpectation {

  /// Creates a `XCTestExpectation` with a `description` based on the name of calling function.
  /// - Parameter function: The function that called the receiver.
  convenience init(function: StaticString = #function) {
    let function = String(describing: function).components(separatedBy: "()").first!
    self.init(description: "\(function)Expectation")
  }
}
