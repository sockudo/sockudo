import SwiftUI

struct ContentView: View {
    @ObservedObject var model: PushProbeModel

    var body: some View {
        NavigationSplitView {
            List {
                Section("APNs") {
                    LabeledContent("Bundle ID", value: model.bundleIdentifier)
                    LabeledContent("Authorization", value: model.authorizationStatus)
                    LabeledContent("Device ID", value: model.deviceId)
                }

                Section("Sockudo") {
                    TextField("Sockudo URL", text: $model.sockudoURL)
                    TextField("App ID", text: $model.appId)
                    TextField("App Key", text: $model.appKey)
                    SecureField("App Secret", text: $model.appSecret)
                    TextField("Channel", text: $model.channel)
                    TextField("Client ID", text: $model.clientId)
                }
            }
            .navigationTitle("Sockudo Push")
            .frame(minWidth: 280)
        } detail: {
            VStack(alignment: .leading, spacing: 14) {
                GroupBox("Device Token") {
                    VStack(alignment: .leading, spacing: 8) {
                        Text(model.deviceToken.isEmpty ? "Waiting for APNs token..." : model.deviceToken)
                            .font(.system(.footnote, design: .monospaced))
                            .textSelection(.enabled)
                            .frame(maxWidth: .infinity, alignment: .leading)

                        HStack {
                            Button("Register With APNs") {
                                model.registerWithAPNs()
                            }
                            Button("Copy Token") {
                                model.copyDeviceToken()
                            }
                            .disabled(model.deviceToken.isEmpty)
                        }
                    }
                    .padding(4)
                }

                HStack {
                    Button("Activate Device") {
                        model.activateDevice()
                    }
                    .disabled(!model.canCallSockudo)

                    Button("Subscribe Channel") {
                        model.subscribeChannel()
                    }
                    .disabled(!model.canSubscribe)

                    Button("Publish Test") {
                        model.publishTest()
                    }
                    .disabled(!model.canPublish)
                }

                if !model.lastNotification.isEmpty {
                    GroupBox("Last Notification") {
                        Text(model.lastNotification)
                            .frame(maxWidth: .infinity, alignment: .leading)
                    }
                }

                if !model.lastError.isEmpty {
                    GroupBox("Error") {
                        Text(model.lastError)
                            .foregroundStyle(.red)
                            .textSelection(.enabled)
                            .frame(maxWidth: .infinity, alignment: .leading)
                    }
                }

                GroupBox("Log") {
                    ScrollView {
                        Text(model.logText)
                            .font(.system(.footnote, design: .monospaced))
                            .textSelection(.enabled)
                            .frame(maxWidth: .infinity, alignment: .leading)
                    }
                    .frame(minHeight: 220)
                }
            }
            .padding(18)
            .navigationTitle("macOS APNs Probe")
        }
    }
}
