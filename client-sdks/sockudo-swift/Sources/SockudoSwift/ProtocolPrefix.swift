/// Protocol version prefix helpers.
///
/// Protocol v1 uses the original Pusher event prefix ("pusher:" / "pusher_internal:").
/// Protocol v2 (Sockudo-native) uses "sockudo:" / "sockudo_internal:".
struct ProtocolPrefix: Sendable {
  let version: Int
  let eventPrefix: String
  let internalPrefix: String

  init(version: Int) {
    self.version = version
    if version == 2 {
      eventPrefix = "sockudo:"
      internalPrefix = "sockudo_internal:"
    } else {
      eventPrefix = "pusher:"
      internalPrefix = "pusher_internal:"
    }
  }

  func event(_ name: String) -> String {
    "\(eventPrefix)\(name)"
  }

  func `internal`(_ name: String) -> String {
    "\(internalPrefix)\(name)"
  }

  func isInternalEvent(_ name: String) -> Bool {
    name.hasPrefix(internalPrefix)
  }

  func isPlatformEvent(_ name: String) -> Bool {
    name.hasPrefix(eventPrefix)
  }
}
