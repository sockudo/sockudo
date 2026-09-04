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
    Task {
      for await token in activity.pushTokenUpdates {
        guard Task.isCancelled == false else { return }
        await onUpdate(activity.id, hexadecimal(token))
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
#endif
