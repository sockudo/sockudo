import SwiftUI

struct ContentView: View {
    @ObservedObject var model: PushProbeModel

    var body: some View {
        NavigationStack {
            Form {
                Section("App") {
                    LabeledContent("Bundle ID", value: model.bundleIdentifier)
                    LabeledContent("APNs topic", value: model.bundleIdentifier)
                    LabeledContent("Bridge", value: model.bridgeURL)
                    LabeledContent("Authorization", value: model.authorizationStatus)
                }

                Section("Device Token") {
                    Text(model.token.isEmpty ? "Waiting for APNs token..." : model.token)
                        .font(.system(.footnote, design: .monospaced))
                        .textSelection(.enabled)
                    Button("Copy Token") {
                        model.copyToken()
                    }
                    .disabled(model.token.isEmpty)
                    Button("Register Again") {
                        model.registerAgain()
                    }
                    Button("Send Token To Bridge") {
                        model.postCurrentTokenToBridge()
                    }
                    .disabled(model.token.isEmpty)
                }

                if !model.lastBridgeResponse.isEmpty {
                    Section("Bridge Response") {
                        Text(model.lastBridgeResponse)
                            .font(.footnote)
                            .textSelection(.enabled)
                    }
                }

                if !model.lastNotification.isEmpty {
                    Section("Last Notification") {
                        Text(model.lastNotification)
                    }
                }

                if !model.lastError.isEmpty {
                    Section("Error") {
                        Text(model.lastError)
                            .foregroundStyle(.red)
                            .textSelection(.enabled)
                    }
                }
            }
            .navigationTitle("Sockudo APNs")
        }
    }
}
