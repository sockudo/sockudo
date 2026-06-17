import Foundation
import XCTest

enum LiveTestSupport {
  static var isConfigured: Bool {
    let env = ProcessInfo.processInfo.environment
    return env["TEST_APP_ID"] != nil &&
      env["TEST_APP_KEY"] != nil &&
      env["TEST_APP_SECRET"] != nil &&
      env["TEST_ENCRYPTION_MASTER_KEY"] != nil &&
      env["TEST_APP_CLUSTER"] != nil
  }

  static func requireLiveConfig() throws {
    if !isConfigured {
      throw XCTSkip("Skipping live Swift server SDK tests: TEST_APP_ID/KEY/SECRET/ENCRYPTION_MASTER_KEY/CLUSTER are not set.")
    }
  }
}
