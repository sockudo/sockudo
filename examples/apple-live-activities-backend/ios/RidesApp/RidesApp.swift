import SwiftUI

@main
struct RidesApp: App {
  @StateObject private var store = RideStore()

  var body: some Scene {
    WindowGroup {
      ContentView()
        .environmentObject(store)
        .task { await store.start() }
    }
  }
}
