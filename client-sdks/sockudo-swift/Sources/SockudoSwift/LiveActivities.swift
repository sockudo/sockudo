#if canImport(ActivityKit) && os(iOS)
import ActivityKit
import Foundation

/// ActivityKit token observers return cancellable tasks and emit every token rotation.
@available(iOS 16.1, *)
public enum SockudoLiveActivityTokens {
  public static func hexadecimal(_ token: Data) -> String {
    token.map { String(format: "%02x", $0) }.joined()
  }

  public static func observePushTokens<Attributes: ActivityAttributes>(
    for activity: Activity<Attributes>,
    onUpdate: @escaping @Sendable (_ activityID: String, _ token: String) async -> Void
  ) -> Task<Void, Never> {
    // `Activity` and its `PushTokenUpdates` sequence are not `Sendable`. ActivityKit documents
    // the sequence as safe to consume from any task, so capture only the stream and the id and
    // opt the stream out of region-based isolation checking.
    let activityID = activity.id
    let tokenUpdates = UncheckedSendable(activity.pushTokenUpdates)
    return Task {
      for await token in tokenUpdates.value {
        guard Task.isCancelled == false else { return }
        await onUpdate(activityID, hexadecimal(token))
      }
    }
  }

  @available(iOS 17.2, *)
  public static func observePushToStartTokens<Attributes: ActivityAttributes>(
    for _: Attributes.Type,
    onUpdate: @escaping @Sendable (_ token: String) async -> Void
  ) -> Task<Void, Never> {
    Task {
      for await token in Activity<Attributes>.pushToStartTokenUpdates {
        guard Task.isCancelled == false else { return }
        await onUpdate(hexadecimal(token))
      }
    }
  }
}

/// Wraps a value that is safe to move into a detached task but lacks a `Sendable` conformance.
private struct UncheckedSendable<Value>: @unchecked Sendable {
  let value: Value
  init(_ value: Value) { self.value = value }
}
#endif
