// Keep this bootstrap in the public website's shared head only. Sensitive pages
// must use a separate root layout without the snippet or consent controls.
export const typeUIInsightsSnippet = String.raw`
(function () {
  var consentGranted = false;
  var loading = false;
  var pendingEvents = [];
  var pendingRevenue = [];
  var siteKey = "tui_pk_aFpbPiCR0tDQNjkb6j5GfG-6q3fh046q";
  var trackerUrl = "https://cdn.typeui.sh/insights/v1.js";

  window.enableTypeUIInsights = function () {
    consentGranted = true;
    if (window.TypeUIInsights) {
      window.TypeUIInsights.init({ siteKey: siteKey });
      window.TypeUIInsights.setConsent(true);
      return;
    }
    if (loading) return;
    loading = true;
    var tracker = document.createElement("script");
    tracker.src = trackerUrl;
    tracker.async = true;
    tracker.onload = function () {
      loading = false;
      window.TypeUIInsights.init({ siteKey: siteKey });
      window.TypeUIInsights.setConsent(consentGranted);
      var events = pendingEvents;
      pendingEvents = [];
      events.forEach(function (event) {
        if (typeof window.TypeUIInsights.track === "function") {
          window.TypeUIInsights.track(event.name, event.options);
        }
      });
      var revenue = pendingRevenue;
      pendingRevenue = [];
      revenue.forEach(function (event) {
        if (typeof window.TypeUIInsights.trackRevenue === "function") {
          window.TypeUIInsights.trackRevenue(event);
        }
      });
    };
    tracker.onerror = function () {
      loading = false;
    };
    document.head.appendChild(tracker);
  };

  window.disableTypeUIInsights = function () {
    consentGranted = false;
    pendingEvents = [];
    pendingRevenue = [];
    if (window.TypeUIInsights) {
      window.TypeUIInsights.setConsent(false);
    }
  };

  window.trackTypeUIEvent = function (name, options) {
    if (!consentGranted) return false;
    if (
      window.TypeUIInsights &&
      typeof window.TypeUIInsights.track === "function"
    ) {
      return window.TypeUIInsights.track(name, options);
    }
    if (pendingEvents.length >= 20) return false;
    pendingEvents.push({ name: name, options: options });
    window.enableTypeUIInsights();
    return true;
  };

  window.trackTypeUIRevenue = function (options) {
    if (!consentGranted) return false;
    if (
      window.TypeUIInsights &&
      typeof window.TypeUIInsights.trackRevenue === "function"
    ) {
      return window.TypeUIInsights.trackRevenue(options);
    }
    if (pendingRevenue.length >= 20) return false;
    pendingRevenue.push(options);
    window.enableTypeUIInsights();
    return true;
  };
})();
`;
