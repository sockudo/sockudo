// Main entry point for bundling with Bun
// Import all required libraries and make them globally available

import Pusher from "@sockudo/client";
import * as fossilDelta from "fossil-delta";
import vcdiffDecoder from "@ably/vcdiff-decoder";

// Make libraries globally available for app.js
window.Pusher = Pusher;
window.fossilDelta = fossilDelta;
window.vcdiff = vcdiffDecoder;

// Import the main app code
import "./app.js";

// Log available libraries
console.log("[Bundle] Libraries loaded:");
console.log("- Pusher:", typeof Pusher !== "undefined");
console.log("- fossilDelta:", typeof fossilDelta !== "undefined");
console.log("- vcdiff:", typeof vcdiffDecoder !== "undefined");
