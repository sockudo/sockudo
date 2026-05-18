import Foundation
import SwiftUI
import UIKit
import UserNotifications

@MainActor
final class PushProbeModel: ObservableObject {
    @Published var bundleIdentifier: String = Bundle.main.bundleIdentifier ?? "unknown"
    @Published var bridgeURL: String = Bundle.main.object(forInfoDictionaryKey: "SockudoAPNSBridgeURL") as? String ?? ""
    @Published var authorizationStatus: String = "requesting"
    @Published var token: String = ""
    @Published var lastError: String = ""
    @Published var lastBridgeResponse: String = ""
    @Published var lastNotification: String = ""

    func copyToken() {
        UIPasteboard.general.string = token
    }

    func registerAgain() {
        UIApplication.shared.registerForRemoteNotifications()
    }

    func postCurrentTokenToBridge() {
        postTokenToBridge(token)
    }

    func postTokenToBridge(_ token: String) {
        guard !token.isEmpty else { return }
        guard let url = URL(string: bridgeURL), !bridgeURL.isEmpty else {
            lastBridgeResponse = "bridge URL is empty"
            return
        }

        var request = URLRequest(url: url.appendingPathComponent("apns-token"))
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "content-type")
        let payload: [String: String] = [
            "bundleId": bundleIdentifier,
            "token": token
        ]
        request.httpBody = try? JSONSerialization.data(withJSONObject: payload)

        Task {
            do {
                let (data, response) = try await URLSession.shared.data(for: request)
                let statusCode = (response as? HTTPURLResponse)?.statusCode ?? 0
                let body = String(data: data, encoding: .utf8) ?? ""
                print("SOCKUDO_APNS_BRIDGE_RESPONSE=\(statusCode) \(body)")
                await MainActor.run {
                    self.lastBridgeResponse = "\(statusCode) \(body)"
                }
            } catch {
                print("SOCKUDO_APNS_BRIDGE_ERROR=\(error.localizedDescription)")
                await MainActor.run {
                    self.lastBridgeResponse = error.localizedDescription
                }
            }
        }
    }
}

final class AppDelegate: NSObject, UIApplicationDelegate, UNUserNotificationCenterDelegate {
    let model = PushProbeModel()

    func application(
        _ application: UIApplication,
        didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]? = nil
    ) -> Bool {
        UNUserNotificationCenter.current().delegate = self
        requestNotificationAuthorization(application)
        return true
    }

    func application(_ application: UIApplication, didRegisterForRemoteNotificationsWithDeviceToken deviceToken: Data) {
        let token = deviceToken.map { String(format: "%02x", $0) }.joined()
        print("SOCKUDO_APNS_DEVICE_TOKEN=\(token)")
        Task { @MainActor in
            model.token = token
            model.lastError = ""
            model.postTokenToBridge(token)
        }
    }

    func application(_ application: UIApplication, didFailToRegisterForRemoteNotificationsWithError error: Error) {
        print("SOCKUDO_APNS_REGISTRATION_ERROR=\(error.localizedDescription)")
        Task { @MainActor in
            model.lastError = error.localizedDescription
        }
    }

    func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        willPresent notification: UNNotification,
        withCompletionHandler completionHandler: @escaping (UNNotificationPresentationOptions) -> Void
    ) {
        let content = notification.request.content
        let summary = [content.title, content.body].filter { !$0.isEmpty }.joined(separator: " - ")
        print("SOCKUDO_APNS_NOTIFICATION=\(summary)")
        Task { @MainActor in
            model.lastNotification = summary.isEmpty ? notification.request.identifier : summary
        }
        completionHandler([.banner, .list, .sound])
    }

    func requestNotificationAuthorization(_ application: UIApplication = UIApplication.shared) {
        UNUserNotificationCenter.current().requestAuthorization(options: [.alert, .badge, .sound]) { [weak self] granted, error in
            Task { @MainActor in
                if let error {
                    self?.model.authorizationStatus = "failed"
                    self?.model.lastError = error.localizedDescription
                    return
                }
                self?.model.authorizationStatus = granted ? "granted" : "denied"
                guard granted else { return }
                application.registerForRemoteNotifications()
            }
        }
    }

}
