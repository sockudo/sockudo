// Bundle entry point for Bun
// This file imports all dependencies and makes them available globally

import Pusher from "@sockudo/client";
import { Filter, FilterExamples } from "@sockudo/client/filter";

// Make Pusher available globally for app.js
window.Pusher = Pusher;
window.Filter = Filter;
window.FilterExamples = FilterExamples;
window.FilterExamples = FilterExamples;

// Import the main app
import "./app.js";
