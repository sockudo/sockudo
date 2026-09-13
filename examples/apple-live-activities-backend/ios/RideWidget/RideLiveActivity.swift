import ActivityKit
import SwiftUI
import WidgetKit

struct RideLiveActivity: Widget {
  var body: some WidgetConfiguration {
    ActivityConfiguration(for: RideAttributes.self) { context in
      HStack {
        Image(systemName: "car.fill").font(.title2)
        VStack(alignment: .leading) {
          Text("Ride \(context.attributes.rideID)").font(.headline)
          Text(context.state.status.capitalized)
        }
        Spacer()
        VStack {
          Text("\(context.state.etaMinutes)").font(.title).bold()
          Text("min").font(.caption)
        }
      }
      .padding()
      .activityBackgroundTint(.black.opacity(0.7))
    } dynamicIsland: { context in
      DynamicIsland {
        DynamicIslandExpandedRegion(.leading) { Text(context.attributes.rideID) }
        DynamicIslandExpandedRegion(.trailing) { Text("\(context.state.etaMinutes) min") }
        DynamicIslandExpandedRegion(.bottom) { Text(context.state.status.capitalized) }
      } compactLeading: {
        Image(systemName: "car.fill")
      } compactTrailing: {
        Text("\(context.state.etaMinutes)m")
      } minimal: {
        Text("\(context.state.etaMinutes)")
      }
    }
  }
}
