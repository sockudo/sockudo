import ActivityKit
import Flutter
import Foundation

/// Minimal ActivityKit bridge for the Flutter example. Emits token rotations and state changes
/// to Dart, where `sockudo_flutter` turns them into typed backend uploads.
@available(iOS 17.2, *)
final class LiveActivityBridge: NSObject, FlutterStreamHandler {
  private var sink: FlutterEventSink?
  private var tasks: [Task<Void, Never>] = []
  private var tracked: Set<String> = []

  func register(with messenger: FlutterBinaryMessenger) {
    FlutterMethodChannel(name: "sockudo.rides/activitykit", binaryMessenger: messenger)
      .setMethodCallHandler { [weak self] call, result in
        guard let self else { return result(nil) }
        let args = call.arguments as? [String: Any] ?? [:]
        switch call.method {
        case "start":
          self.start(
            rideID: args["rideId"] as? String ?? "ride",
            status: args["status"] as? String ?? "requested",
            eta: args["etaMinutes"] as? Int ?? 0)
          result(nil)
        case "apply":
          Task {
            await self.apply(
              activityID: args["activityId"] as? String ?? "",
              status: args["status"] as? String ?? "",
              eta: args["etaMinutes"] as? Int ?? 0,
              end: args["end"] as? Bool ?? false)
            result(nil)
          }
        case "endAll":
          Task {
            for activity in Activity<RideAttributes>.activities {
              await activity.end(nil, dismissalPolicy: .immediate)
            }
            result(nil)
          }
        default:
          result(FlutterMethodNotImplemented)
        }
      }
    FlutterEventChannel(name: "sockudo.rides/activitykit/events", binaryMessenger: messenger)
      .setStreamHandler(self)
  }

  func onListen(withArguments _: Any?, eventSink: @escaping FlutterEventSink) -> FlutterError? {
    sink = eventSink
    tasks.append(Task { @MainActor in
      for await token in Activity<RideAttributes>.pushToStartTokenUpdates {
        self.emit(["type": "pushToStartToken", "token": Self.hex(token)])
      }
    })
    for activity in Activity<RideAttributes>.activities { track(activity) }
    tasks.append(Task { @MainActor in
      for await activity in Activity<RideAttributes>.activityUpdates { self.track(activity) }
    })
    return nil
  }

  func onCancel(withArguments _: Any?) -> FlutterError? {
    tasks.forEach { $0.cancel() }
    tasks.removeAll()
    sink = nil
    return nil
  }

  private func start(rideID: String, status: String, eta: Int) {
    let content = ActivityContent(
      state: RideAttributes.ContentState(status: status, etaMinutes: eta), staleDate: nil)
    do {
      let activity = try Activity.request(
        attributes: RideAttributes(rideID: rideID), content: content, pushType: .token)
      track(activity)
    } catch {
      emit(["type": "error", "message": "\(error)"])
    }
  }

  private func apply(activityID: String, status: String, eta: Int, end: Bool) async {
    guard let activity = Activity<RideAttributes>.activities.first(where: { $0.id == activityID })
    else { return }
    let content = ActivityContent(
      state: RideAttributes.ContentState(status: status, etaMinutes: eta), staleDate: nil,
      relevanceScore: 1)
    if end {
      await activity.end(content, dismissalPolicy: .after(.now + 600))
    } else {
      await activity.update(content)
    }
  }

  private func track(_ activity: Activity<RideAttributes>) {
    guard !tracked.contains(activity.id) else { return }
    tracked.insert(activity.id)
    let rideID = activity.attributes.rideID
    emitState(activity)
    if let token = activity.pushToken {
      emit(["type": "activityToken", "activityId": activity.id, "rideId": rideID, "token": Self.hex(token)])
    }
    tasks.append(Task { @MainActor in
      for await token in activity.pushTokenUpdates {
        self.emit(["type": "activityToken", "activityId": activity.id, "rideId": rideID, "token": Self.hex(token)])
      }
    })
    tasks.append(Task { @MainActor in
      for await _ in activity.contentUpdates { self.emitState(activity) }
    })
    tasks.append(Task { @MainActor in
      for await _ in activity.activityStateUpdates { self.emitState(activity) }
    })
  }

  private func emitState(_ activity: Activity<RideAttributes>) {
    emit([
      "type": "state", "activityId": activity.id, "rideId": activity.attributes.rideID,
      "status": activity.content.state.status, "etaMinutes": activity.content.state.etaMinutes,
      "lifecycle": Self.describe(activity.activityState),
    ])
  }

  private func emit(_ event: [String: Any]) {
    DispatchQueue.main.async { self.sink?(event) }
  }

  private static func hex(_ data: Data) -> String {
    data.map { String(format: "%02x", $0) }.joined()
  }

  private static func describe(_ state: ActivityState) -> String {
    switch state {
    case .active: return "active"
    case .ended: return "ended"
    case .dismissed: return "dismissed"
    case .stale: return "stale"
    case .pending: return "pending"
    @unknown default: return "unknown"
    }
  }
}
