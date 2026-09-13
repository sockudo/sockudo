import ActivityKit
import Foundation
import SockudoSwift

struct TrackedActivity: Identifiable {
  let id: String
  let rideID: String
  var token: String?
  var state: RideAttributes.ContentState
  var lifecycle: String
}

/// Plays the role of the app layer: observes ActivityKit tokens through the Sockudo Swift
/// client helpers, uploads them to the ride backend, and mirrors activity state for the UI.
@MainActor
final class RideStore: ObservableObject {
  @Published var pushToStartToken: String?
  @Published var activities: [TrackedActivity] = []
  @Published var log: [String] = []

  let userID = "sim-user"
  private let backendURL: URL
  private var tokenTasks: [String: Task<Void, Never>] = [:]
  private var observerTasks: [Task<Void, Never>] = []
  private var localCounter = 0
  private var mirroredPublishIDs: Set<String> = []

  init() {
    let configured = Bundle.main.object(forInfoDictionaryKey: "SockudoBackendURL") as? String
    backendURL = URL(string: configured ?? "http://127.0.0.1:8787")!
  }

  func start() async {
    guard observerTasks.isEmpty else { return }
    append("backend \(backendURL.absoluteString)")
    append("frequent updates enabled: \(ActivityAuthorizationInfo().frequentPushesEnabled)")

    observerTasks.append(
      SockudoLiveActivityTokens.observePushToStartTokens(for: RideAttributes.self) {
        [weak self] token in
        await self?.receivedPushToStartToken(token)
      })

    for activity in Activity<RideAttributes>.activities {
      track(activity)
    }
    observerTasks.append(
      Task { [weak self] in
        for await activity in Activity<RideAttributes>.activityUpdates {
          await self?.track(activity)
        }
      })
    observerTasks.append(
      Task { [weak self] in
        // The Simulator cannot receive `liveactivity` pushes from a mock APNs, so mirror the
        // payload Sockudo last published for each ride onto the on-device activity.
        while !Task.isCancelled {
          await self?.mirrorPublishedStates()
          try? await Task.sleep(for: .seconds(2))
        }
      })
  }

  private struct PublishedState: Decodable {
    var publishId: String?
    var event: String?
    var contentState: RideAttributes.ContentState?
  }

  private func mirrorPublishedStates() async {
    for activity in Activity<RideAttributes>.activities where activity.activityState == .active {
      let rideID = activity.attributes.rideID
      let url = backendURL.appendingPathComponent("/rides/\(rideID)/latest")
      guard let (data, _) = try? await URLSession.shared.data(from: url),
        let published = try? JSONDecoder().decode(PublishedState.self, from: data),
        let publishID = published.publishId, let state = published.contentState,
        !mirroredPublishIDs.contains(publishID)
      else { continue }
      mirroredPublishIDs.insert(publishID)
      let content = ActivityContent(state: state, staleDate: nil, relevanceScore: 1)
      if published.event == "end" {
        await activity.end(content, dismissalPolicy: .after(.now + 600))
      } else {
        await activity.update(content)
      }
      append("mirrored Sockudo publish \(publishID): \(state.status) eta \(state.etaMinutes)")
    }
  }

  func startLocally() {
    localCounter += 1
    let attributes = RideAttributes(rideID: "local-\(localCounter)")
    let state = RideAttributes.ContentState(status: "requested", etaMinutes: 9)
    do {
      let activity = try Activity.request(
        attributes: attributes,
        content: .init(state: state, staleDate: nil),
        pushType: .token)
      append("requested local activity \(activity.id.prefix(8)) for \(attributes.rideID)")
      track(activity)
    } catch {
      append("Activity.request failed: \(error)")
    }
  }

  func endAll() {
    Task {
      for activity in Activity<RideAttributes>.activities {
        await activity.end(nil, dismissalPolicy: .immediate)
      }
      append("ended all activities locally")
    }
  }

  private func receivedPushToStartToken(_ token: String) async {
    pushToStartToken = token
    append("push-to-start token \(token.prefix(12))…")
    await post(path: "/devices/push-to-start", body: ["userId": userID, "token": token])
  }

  private func track(_ activity: Activity<RideAttributes>) {
    if activities.contains(where: { $0.id == activity.id }) { return }
    activities.append(
      TrackedActivity(
        id: activity.id,
        rideID: activity.attributes.rideID,
        token: activity.pushToken.map(SockudoLiveActivityTokens.hexadecimal),
        state: activity.content.state,
        lifecycle: describe(activity.activityState)))
    append("tracking activity \(activity.id.prefix(8)) ride \(activity.attributes.rideID)")
    if let current = activity.pushToken {
      // Re-sync the current token on launch; pushTokenUpdates only emits on rotation.
      Task { await receivedActivityToken(activityID: activity.id, token: SockudoLiveActivityTokens.hexadecimal(current)) }
    }

    tokenTasks[activity.id] = SockudoLiveActivityTokens.observePushTokens(for: activity) {
      [weak self] activityID, token in
      await self?.receivedActivityToken(activityID: activityID, token: token)
    }
    observerTasks.append(
      Task { [weak self] in
        for await content in activity.contentUpdates {
          await self?.update(activity.id) { $0.state = content.state }
          await self?.append(
            "\(activity.attributes.rideID) → \(content.state.status) eta \(content.state.etaMinutes)"
          )
        }
      })
    observerTasks.append(
      Task { [weak self] in
        for await state in activity.activityStateUpdates {
          let label = await self?.describe(state) ?? ""
          await self?.update(activity.id) { $0.lifecycle = label }
          await self?.append("\(activity.attributes.rideID) lifecycle \(label)")
          if state == .dismissed {
            await self?.tokenTasks[activity.id]?.cancel()
          }
        }
      })
  }

  private func receivedActivityToken(activityID: String, token: String) async {
    guard let index = activities.firstIndex(where: { $0.id == activityID }) else { return }
    activities[index].token = token
    let rideID = activities[index].rideID
    append("\(rideID) update token \(token.prefix(12))…")
    await post(path: "/rides/\(rideID)/token", body: ["token": token])
  }

  private func update(_ id: String, _ mutate: (inout TrackedActivity) -> Void) {
    guard let index = activities.firstIndex(where: { $0.id == id }) else { return }
    mutate(&activities[index])
  }

  private func describe(_ state: ActivityState) -> String {
    switch state {
    case .active: return "active"
    case .ended: return "ended"
    case .dismissed: return "dismissed"
    case .stale: return "stale"
    case .pending: return "pending"
    @unknown default: return "unknown"
    }
  }

  private func post(path: String, body: [String: String]) async {
    var request = URLRequest(url: backendURL.appendingPathComponent(path))
    request.httpMethod = "POST"
    request.setValue("application/json", forHTTPHeaderField: "Content-Type")
    request.httpBody = try? JSONSerialization.data(withJSONObject: body)
    do {
      let (_, response) = try await URLSession.shared.data(for: request)
      let status = (response as? HTTPURLResponse)?.statusCode ?? 0
      append("POST \(path) → \(status)")
    } catch {
      append("POST \(path) failed: \(error.localizedDescription)")
    }
  }

  private func append(_ line: String) {
    let stamp = Date().formatted(date: .omitted, time: .standard)
    log.insert("\(stamp) \(line)", at: 0)
    if log.count > 60 { log.removeLast() }
  }
}
