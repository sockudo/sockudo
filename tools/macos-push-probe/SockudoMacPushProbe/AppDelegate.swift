import AppKit
import Foundation
import UserNotifications

final class AppDelegate: NSObject, NSApplicationDelegate, UNUserNotificationCenterDelegate {
    @MainActor let model = PushProbeModel()

    func applicationDidFinishLaunching(_ notification: Notification) {
        UNUserNotificationCenter.current().delegate = self
        requestNotificationAuthorization()
    }

    func application(
        _ application: NSApplication,
        didRegisterForRemoteNotificationsWithDeviceToken deviceToken: Data
    ) {
        let token = deviceToken.map { String(format: "%02x", $0) }.joined()
        print("SOCKUDO_MACOS_APNS_DEVICE_TOKEN=\(token)")
        Task { @MainActor in
            model.deviceToken = token
            model.lastError = ""
            model.appendLog("Received APNs token.")
        }
    }

    func application(
        _ application: NSApplication,
        didFailToRegisterForRemoteNotificationsWithError error: Error
    ) {
        print("SOCKUDO_MACOS_APNS_REGISTRATION_ERROR=\(error.localizedDescription)")
        Task { @MainActor in
            model.lastError = error.localizedDescription
            model.appendLog("APNs registration failed: \(error.localizedDescription)")
        }
    }

    func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        willPresent notification: UNNotification,
        withCompletionHandler completionHandler: @escaping (UNNotificationPresentationOptions) -> Void
    ) {
        let content = notification.request.content
        let summary = [content.title, content.body].filter { !$0.isEmpty }.joined(separator: " - ")
        print("SOCKUDO_MACOS_APNS_NOTIFICATION=\(summary)")
        Task { @MainActor in
            model.lastNotification = summary.isEmpty ? notification.request.identifier : summary
            model.appendLog("Notification received: \(model.lastNotification)")
        }
        completionHandler([.banner, .list, .sound])
    }

    @MainActor
    func requestNotificationAuthorization() {
        model.authorizationStatus = "requesting"
        UNUserNotificationCenter.current().requestAuthorization(options: [.alert, .badge, .sound]) { granted, error in
            Task { @MainActor in
                if let error {
                    self.model.authorizationStatus = "failed"
                    self.model.lastError = error.localizedDescription
                    self.model.appendLog("Authorization failed: \(error.localizedDescription)")
                    return
                }
                self.model.authorizationStatus = granted ? "granted" : "denied"
                self.model.appendLog("Notification authorization \(self.model.authorizationStatus).")
                guard granted else { return }
                NSApplication.shared.registerForRemoteNotifications(matching: [.alert, .badge, .sound])
            }
        }
    }
}
