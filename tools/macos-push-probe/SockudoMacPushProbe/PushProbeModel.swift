import AppKit
import Foundation
import SockudoSwift

@MainActor
final class PushProbeModel: ObservableObject {
    @Published var bundleIdentifier = Bundle.main.bundleIdentifier ?? "com.sockudo.macosprobe"
    @Published var authorizationStatus = "unknown"
    @Published var deviceToken = ""
    @Published var sockudoURL = UserDefaults.standard.string(forKey: "sockudoURL") ?? "http://127.0.0.1:6001"
    @Published var appId = UserDefaults.standard.string(forKey: "appId") ?? "app-id"
    @Published var appKey = UserDefaults.standard.string(forKey: "appKey") ?? "app-key"
    @Published var appSecret = UserDefaults.standard.string(forKey: "appSecret") ?? "app-secret"
    @Published var channel = UserDefaults.standard.string(forKey: "channel") ?? "macos-push-probe"
    @Published var clientId = UserDefaults.standard.string(forKey: "clientId") ?? NSUserName()
    @Published var deviceId = UserDefaults.standard.string(forKey: "deviceId") ?? "macos-\(UUID().uuidString)"
    @Published var deviceIdentityToken = UserDefaults.standard.string(forKey: "deviceIdentityToken") ?? ""
    @Published var tokenHash = UserDefaults.standard.string(forKey: "tokenHash") ?? ""
    @Published var lastError = ""
    @Published var lastNotification = ""
    @Published private(set) var logText = ""

    var canCallSockudo: Bool {
        !deviceToken.isEmpty && !sockudoURL.isEmpty && !appId.isEmpty && !appKey.isEmpty && !appSecret.isEmpty
    }

    var canSubscribe: Bool {
        canCallSockudo && !tokenHash.isEmpty && !channel.isEmpty
    }

    var canPublish: Bool {
        canSubscribe
    }

    init() {
        saveDefaults()
        appendLog("Ready. Register with APNs, activate the device, subscribe, then publish.")
    }

    func registerWithAPNs() {
        NSApplication.shared.registerForRemoteNotifications(matching: [.alert, .badge, .sound])
        appendLog("Requested APNs registration.")
    }

    func copyDeviceToken() {
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(deviceToken, forType: .string)
        appendLog("Copied APNs token.")
    }

    func activateDevice() {
        guard canCallSockudo else { return }
        saveDefaults()
        lastError = ""
        appendLog("Activating device \(deviceId)...")

        pushClient().activateDevice(devicePayload()) { [weak self] result in
            Task { @MainActor in
                self?.handleActivation(result)
            }
        }
    }

    func subscribeChannel() {
        guard canSubscribe else { return }
        saveDefaults()
        lastError = ""
        appendLog("Subscribing \(deviceId) to \(channel)...")

        pushClient().upsertChannelSubscription(subscriptionPayload()) { [weak self] result in
            Task { @MainActor in
                switch result {
                case .success(let response):
                    self?.appendLog("Subscribed: \(Self.prettyJSON(response))")
                case .failure(let error):
                    self?.record(error)
                }
            }
        }
    }

    func publishTest() {
        guard canPublish else { return }
        saveDefaults()
        lastError = ""
        let publishId = "macos-probe-\(Int(Date().timeIntervalSince1970 * 1000))"
        appendLog("Publishing \(publishId) to \(channel)...")

        pushClient().publish(publishPayload(publishId: publishId)) { [weak self] result in
            Task { @MainActor in
                switch result {
                case .success(let response):
                    self?.appendLog("Publish accepted: \(Self.prettyJSON(response))")
                case .failure(let error):
                    self?.record(error)
                }
            }
        }
    }

    func appendLog(_ message: String) {
        let timestamp = ISO8601DateFormatter().string(from: Date())
        logText += "[\(timestamp)] \(message)\n"
    }

    private func pushClient() -> SockudoPushRegistration {
        let endpoint = "\(sockudoURL.trimmingCharacters(in: CharacterSet(charactersIn: "/")))/apps/\(appId)/push"
        let session = SignedSockudoURLSessionFactory.make(appKey: appKey, appSecret: appSecret)
        return SockudoPushRegistration(
            options: .init(
                endpoint: endpoint,
                headers: ["x-sockudo-push-capability": "push-admin"]
            ),
            urlSession: session
        )
    }

    private func devicePayload() -> [String: Any] {
        [
            "appId": appId,
            "id": deviceId,
            "clientId": clientId,
            "deviceSecret": deviceIdentityToken.isEmpty ? "macos-probe-bootstrap-secret" : deviceIdentityToken,
            "formFactor": "desktop",
            "platform": "macos",
            "metadata": [
                "bundleId": bundleIdentifier,
                "source": "tools/macos-push-probe",
            ],
            "timezone": TimeZone.current.identifier,
            "locale": Locale.current.identifier,
            "push": [
                "state": "ACTIVE",
                "recipient": [
                    "transportType": "apns",
                    "deviceToken": deviceToken,
                ],
            ],
        ]
    }

    private func subscriptionPayload() -> [String: Any] {
        [
            "appId": appId,
            "channel": channel,
            "deviceId": deviceId,
            "clientId": clientId,
            "provider": "apns",
            "tokenHash": tokenHash,
            "credentialVersion": 1,
        ]
    }

    private func publishPayload(publishId: String) -> [String: Any] {
        [
            "publishId": publishId,
            "recipients": [
                [
                    "type": "channel",
                    "channel": channel,
                ],
            ],
            "payload": [
                "title": "Sockudo macOS probe",
                "body": "Push delivered through Sockudo to \(bundleIdentifier)",
                "collapseKey": publishId,
                "templateData": [
                    "bundleId": bundleIdentifier,
                    "deviceId": deviceId,
                    "source": "tools/macos-push-probe",
                ],
            ],
            "providerOverrides": [],
            "sync": false,
        ]
    }

    private func handleActivation(_ result: Result<[String: Any], Error>) {
        switch result {
        case .success(let response):
            if let responseTokenHash = response["tokenHash"] as? String {
                tokenHash = responseTokenHash
                UserDefaults.standard.set(responseTokenHash, forKey: "tokenHash")
            }
            if let identity = response["deviceIdentityToken"] as? String {
                deviceIdentityToken = identity
                UserDefaults.standard.set(identity, forKey: "deviceIdentityToken")
            }
            appendLog("Activated: \(Self.prettyJSON(response))")
        case .failure(let error):
            record(error)
        }
    }

    private func record(_ error: Error) {
        lastError = error.localizedDescription
        appendLog("Error: \(error.localizedDescription)")
    }

    private func saveDefaults() {
        UserDefaults.standard.set(sockudoURL, forKey: "sockudoURL")
        UserDefaults.standard.set(appId, forKey: "appId")
        UserDefaults.standard.set(appKey, forKey: "appKey")
        UserDefaults.standard.set(appSecret, forKey: "appSecret")
        UserDefaults.standard.set(channel, forKey: "channel")
        UserDefaults.standard.set(clientId, forKey: "clientId")
        UserDefaults.standard.set(deviceId, forKey: "deviceId")
    }

    private static func prettyJSON(_ value: Any) -> String {
        guard JSONSerialization.isValidJSONObject(value),
              let data = try? JSONSerialization.data(withJSONObject: value, options: [.prettyPrinted, .sortedKeys]),
              let string = String(data: data, encoding: .utf8)
        else {
            return String(describing: value)
        }
        return string
    }
}
