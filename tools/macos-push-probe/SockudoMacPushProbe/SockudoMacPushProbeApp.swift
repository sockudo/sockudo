import SwiftUI

@main
struct SockudoMacPushProbeApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) private var appDelegate

    var body: some Scene {
        WindowGroup {
            ContentView(model: appDelegate.model)
                .frame(minWidth: 720, minHeight: 640)
        }
        .windowStyle(.titleBar)
    }
}
