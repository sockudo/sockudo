import SwiftUI

struct ContentView: View {
  @EnvironmentObject private var store: RideStore

  var body: some View {
    NavigationStack {
      List {
        Section("Push-to-start token") {
          Text(store.pushToStartToken.map { String($0.prefix(24)) + "…" } ?? "waiting for ActivityKit…")
            .font(.system(.footnote, design: .monospaced))
            .accessibilityIdentifier("pushToStartToken")
        }
        Section("Activities") {
          if store.activities.isEmpty {
            Text("none").foregroundStyle(.secondary)
          }
          ForEach(store.activities) { activity in
            VStack(alignment: .leading, spacing: 4) {
              Text(activity.rideID).font(.headline)
              Text("\(activity.state.status) · ETA \(activity.state.etaMinutes) min · \(activity.lifecycle)")
                .accessibilityIdentifier("state-\(activity.rideID)")
              Text(activity.token.map { String($0.prefix(24)) + "…" } ?? "no update token yet")
                .font(.system(.caption2, design: .monospaced))
                .foregroundStyle(.secondary)
            }
          }
        }
        Section("Log") {
          ForEach(Array(store.log.enumerated()), id: \.offset) { _, line in
            Text(line).font(.system(.caption2, design: .monospaced))
          }
        }
      }
      .navigationTitle("Sockudo Rides")
      .toolbar {
        ToolbarItemGroup(placement: .bottomBar) {
          Button("Start locally") { store.startLocally() }
          Spacer()
          Button("End all", role: .destructive) { store.endAll() }
        }
      }
    }
  }
}
