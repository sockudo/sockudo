// app.js - Enhanced Pusher WebSocket Testing Dashboard

document.addEventListener("DOMContentLoaded", () => {
  // DOM Elements
  const elements = {
    // Connection
    configDisplay: document.getElementById("config-display"),
    connectBtn: document.getElementById("connect-btn"),
    disconnectBtn: document.getElementById("disconnect-btn"),
    connectionStatus: document.getElementById("connection-status"),
    statusDot: document.getElementById("status-dot"),

    // Channels
    channelNameInput: document.getElementById("channel-name"),
    subscribeBtn: document.getElementById("subscribe-btn"),
    subscribedChannels: document.getElementById("subscribed-channels"),
    channelCount: document.getElementById("channel-count"),

    // Tag Filtering
    enableTagFilter: document.getElementById("enable-tag-filter"),
    tagFilterControls: document.getElementById("tag-filter-controls"),
    filterPreset: document.getElementById("filter-preset"),
    filterJson: document.getElementById("filter-json"),

    // Server Events
    serverEventChannel: document.getElementById("server-event-channel"),
    serverEventName: document.getElementById("server-event-name"),
    serverEventData: document.getElementById("server-event-data"),
    serverEventTags: document.getElementById("server-event-tags"),
    conflationKey: document.getElementById("conflation-key"),
    sendServerEventBtn: document.getElementById("send-server-event-btn"),
    sendBatchEventsBtn: document.getElementById("send-batch-events-btn"),

    // Client Events
    clientEventChannel: document.getElementById("client-event-channel"),
    clientEventName: document.getElementById("client-event-name"),
    clientEventData: document.getElementById("client-event-data"),
    sendClientEventBtn: document.getElementById("send-client-event-btn"),

    // Events Log
    eventsLog: document.getElementById("events-log"),
    clearEventsBtn: document.getElementById("clear-events-btn"),
    exportEventsBtn: document.getElementById("export-events-btn"),

    // Presence
    presenceChannelName: document.getElementById("presence-channel-name"),
    presenceCount: document.getElementById("presence-count"),
    presenceMembers: document.getElementById("presence-members"),

    // Statistics
    totalEvents: document.getElementById("total-events"),
    totalChannels: document.getElementById("total-channels"),
    connectionTime: document.getElementById("connection-time"),
    webhookCount: document.getElementById("webhook-count"),

    // Webhooks
    webhooksLog: document.getElementById("webhooks-log"),
    fetchWebhooksBtn: document.getElementById("fetch-webhooks-btn"),
    clearWebhooksBtn: document.getElementById("clear-webhooks-btn"),

    // Delta Compression
    deltaCompressionToggle: document.getElementById("delta-compression-toggle"),
    deltaEnabled: document.getElementById("delta-enabled"),
    deltaMessages: document.getElementById("delta-messages"),
    fullMessages: document.getElementById("full-messages"),
    bandwidthSaved: document.getElementById("bandwidth-saved"),
  };

  // Application State
  let state = {
    pusher: null,
    config: null,
    channels: new Map(),
    currentPresenceChannel: null,
    events: [],
    webhooks: [],
    stats: {
      totalEvents: 0,
      connectionStartTime: null,
      connectionTimer: null,
    },
    currentEventFilter: "all",
    tagFiltering: {
      enabled: false,
      currentFilter: null,
    },
    deltaCompression: {
      enabled: false,
      channelStates: new Map(), // Store last message per channel for delta decoding
      stats: {
        deltaMessages: 0,
        fullMessages: 0,
        totalBytesWithoutCompression: 0,
        totalBytesWithCompression: 0,
      },
    },
  };

  // Utility Functions
  const utils = {
    formatTime(timestamp) {
      return new Date(timestamp).toLocaleTimeString();
    },

    formatJSON(obj) {
      try {
        return JSON.stringify(obj, null, 2);
      } catch (e) {
        console.log(e);
        return String(obj);
      }
    },

    getChannelType(channelName) {
      if (channelName.startsWith("presence-")) return "presence";
      if (channelName.startsWith("private-")) return "private";
      return "public";
    },

    exportEvents() {
      const dataStr = JSON.stringify(state.events, null, 2);
      const dataBlob = new Blob([dataStr], { type: "application/json" });
      const url = URL.createObjectURL(dataBlob);
      const link = document.createElement("a");
      link.href = url;
      link.download = `pusher-events-${Date.now()}.json`;
      link.click();
      URL.revokeObjectURL(url);
    },

    updateConnectionTimer() {
      if (state.stats.connectionStartTime) {
        const elapsed = Date.now() - state.stats.connectionStartTime;
        const minutes = Math.floor(elapsed / 60000);
        const seconds = Math.floor((elapsed % 60000) / 1000);
        elements.connectionTime.textContent = `${minutes
          .toString()
          .padStart(2, "0")}:${seconds.toString().padStart(2, "0")}`;
      }
    },

    addAnimation(element, animation = "fade-in") {
      element.classList.add(animation);
      setTimeout(() => element.classList.remove(animation), 300);
    },
  };

  // Event Management
  const eventManager = {
    add(event) {
      event.id = Date.now() + Math.random();
      state.events.unshift(event);
      state.stats.totalEvents++;

      // Keep only latest 500 events
      if (state.events.length > 500) {
        state.events = state.events.slice(0, 500);
      }

      eventManager.render();
      eventManager.updateStats();
    },

    render() {
      const filteredEvents = state.events.filter((event) => {
        if (state.currentEventFilter === "all") return true;
        return event.type === state.currentEventFilter;
      });

      elements.eventsLog.innerHTML = "";

      filteredEvents.forEach((event) => {
        const li = document.createElement("li");
        li.className = "event-item";
        li.innerHTML = `
          <div class="event-header">
            <div>
              <span class="event-type ${event.type}">${event.type}</span>
              <span class="event-title">${event.title}</span>
            </div>
            <span class="event-timestamp">${utils.formatTime(
              event.timestamp,
            )}</span>
          </div>
          ${
            event.data
              ? `<div class="event-data">${utils.formatJSON(event.data)}</div>`
              : ""
          }
        `;
        utils.addAnimation(li);
        elements.eventsLog.appendChild(li);
      });
    },

    updateStats() {
      elements.totalEvents.textContent = state.stats.totalEvents;
      elements.totalChannels.textContent = state.channels.size;
    },

    clear() {
      state.events = [];
      state.stats.totalEvents = 0;
      eventManager.render();
      eventManager.updateStats();
    },
  };

  // Channel Management
  const channelManager = {
    subscribe(channelName) {
      if (!state.pusher || state.pusher.connection.state !== "connected") {
        eventManager.add({
          type: "error",
          title: "Cannot subscribe: Not connected",
          timestamp: Date.now(),
        });
        return;
      }

      if (state.channels.has(channelName)) {
        eventManager.add({
          type: "system",
          title: `Already subscribed to ${channelName}`,
          timestamp: Date.now(),
        });
        return;
      }

      // Get tag filter if enabled
      let tagsFilter = null;
      if (state.tagFiltering.enabled && state.tagFiltering.currentFilter) {
        tagsFilter = state.tagFiltering.currentFilter;
        eventManager.add({
          type: "system",
          title: `Subscribing to ${channelName} with tag filter`,
          timestamp: Date.now(),
          data: tagsFilter,
        });
      }

      const channel = state.pusher.subscribe(channelName, tagsFilter);
      state.channels.set(channelName, {
        channel,
        filter: tagsFilter,
      });

      channelManager.bindChannelEvents(channel, channelName);
      channelManager.render();
      channelManager.updateDropdowns();
    },

    unsubscribe(channelName) {
      if (state.channels.has(channelName)) {
        state.pusher.unsubscribe(channelName);
        state.channels.delete(channelName);

        if (
          state.currentPresenceChannel &&
          state.currentPresenceChannel.name === channelName
        ) {
          presenceManager.clear();
        }

        channelManager.render();
        channelManager.updateDropdowns();
      }
    },

    bindChannelEvents(channel, channelName) {
      // Subscription events
      channel.bind("pusher:subscription_succeeded", (data) => {
        eventManager.add({
          type: "system",
          title: `✅ Subscribed to ${channelName}`,
          timestamp: Date.now(),
          data: data,
        });

        if (channelName.startsWith("presence-")) {
          state.currentPresenceChannel = channel;
          presenceManager.update(channel.members);
        }
      });

      channel.bind("pusher:subscription_error", (status) => {
        eventManager.add({
          type: "error",
          title: `❌ Subscription failed: ${channelName}`,
          timestamp: Date.now(),
          data: { status, channelName },
        });
      });

      // Presence events
      if (channelName.startsWith("presence-")) {
        channel.bind("pusher:member_added", (member) => {
          eventManager.add({
            type: "member",
            title: `👋 Member joined ${channelName}`,
            timestamp: Date.now(),
            data: member,
          });
          if (state.currentPresenceChannel === channel) {
            presenceManager.update(channel.members);
          }
        });

        channel.bind("pusher:member_removed", (member) => {
          eventManager.add({
            type: "member",
            title: `👋 Member left ${channelName}`,
            timestamp: Date.now(),
            data: member,
          });
          if (state.currentPresenceChannel === channel) {
            presenceManager.update(channel.members);
          }
        });
      }

      // Custom events (catch-all)
      // Note: This seamlessly receives BOTH full messages AND automatically decoded delta messages!
      // The sockudo-js library handles delta decoding transparently, so you don't need to
      // manually bind to "pusher:delta" - all events come through here already decoded.
      channel.bind_global((eventName, data) => {
        if (!eventName.startsWith("pusher:")) {
          console.log(`[Event] "${eventName}" on ${channelName}:`, data);

          const eventType = eventName.startsWith("client-")
            ? "client"
            : "custom";

          eventManager.add({
            type: eventType,
            title: `📡 ${eventName} on ${channelName}`,
            timestamp: Date.now(),
            data: data,
          });
        }
      });
    },

    render() {
      elements.subscribedChannels.innerHTML = "";

      state.channels.forEach((channelData, channelName) => {
        const div = document.createElement("div");
        div.className = "channel-item";

        const channelType = utils.getChannelType(channelName);
        const hasFilter = channelData.filter
          ? ' <i class="fas fa-filter" title="Filtered"></i>'
          : "";
        div.innerHTML = `
          <div>
            <span class="channel-name">${channelName}${hasFilter}</span>
            <span class="channel-type ${channelType}">${channelType}</span>
          </div>
          <button class="btn btn-small btn-danger" onclick="channelManager.unsubscribe('${channelName}')">
            <i class="fas fa-times"></i> Unsubscribe
          </button>
        `;

        utils.addAnimation(div);
        elements.subscribedChannels.appendChild(div);
      });

      elements.channelCount.textContent = state.channels.size;
    },

    updateDropdowns() {
      [elements.serverEventChannel, elements.clientEventChannel].forEach(
        (select) => {
          const currentValue = select.value;
          select.innerHTML = '<option value="">Select channel...</option>';

          state.channels.forEach((channelData, channelName) => {
            const option = document.createElement("option");
            option.value = channelName;
            option.textContent = channelName;
            if (channelName === currentValue) {
              option.selected = true;
            }
            select.appendChild(option);
          });
        },
      );

      // Update client event button state
      const hasSelectedChannel = elements.clientEventChannel.value !== "";
      const isConnected = state.pusher?.connection?.state === "connected";
      elements.sendClientEventBtn.disabled =
        !hasSelectedChannel || !isConnected;
    },
  };

  // Presence Management
  const presenceManager = {
    update(members) {
      if (!members) {
        presenceManager.clear();
        return;
      }

      elements.presenceChannelName.textContent =
        state.currentPresenceChannel?.name || "None";
      elements.presenceCount.textContent = members.count || 0;

      elements.presenceMembers.innerHTML = "";

      if (members.count > 0) {
        members.each((member) => {
          const div = document.createElement("div");
          div.className = "member-item";
          console.log("Member:", member);

          const isMe = member.id === members.me?.id;
          div.innerHTML = `
            <img src="${
              member.info.user_info?.avatar ||
              `https://ui-avatars.com/api/?name=${encodeURIComponent(
                member.info.user_info.name,
              )}&background=random`
            }" alt="${member.info.user_info.name}" class="member-avatar">
            <div class="member-info">
              <div class="member-name">${member.info.user_info.name}</div>
              <div class="member-id">${member.info.user_id}</div>
            </div>
            ${isMe ? '<span class="member-badge">You</span>' : ""}
          `;

          utils.addAnimation(div);
          elements.presenceMembers.appendChild(div);
        });
      } else {
        elements.presenceMembers.innerHTML =
          '<div class="member-item">No members present</div>';
      }
    },

    clear() {
      elements.presenceChannelName.textContent = "None";
      elements.presenceCount.textContent = "0";
      elements.presenceMembers.innerHTML =
        '<div class="member-item">Not subscribed to a presence channel</div>';
      state.currentPresenceChannel = null;
    },
  };

  // Connection Management
  const connectionManager = {
    async connect() {
      if (!state.config) {
        eventManager.add({
          type: "error",
          title: "Configuration not loaded",
          timestamp: Date.now(),
        });
        return;
      }

      if (state.pusher && state.pusher.connection.state !== "disconnected") {
        eventManager.add({
          type: "system",
          title: "Already connected or connecting",
          timestamp: Date.now(),
        });
        return;
      }

      connectionManager.updateStatus("connecting", "Connecting...");

      const pusherConfig = {
        cluster: state.config.pusherCluster || "mt1",
        wsHost: state.config.pusherHost || "localhost",
        wsPort: state.config.pusherPort || 6001,
        forceTLS: false,
        disableStats: true,
        enabledTransports: ["ws"],
        authEndpoint: state.config.authEndpoint,
        authTransport: "ajax",
        // Enable delta compression with automatic decoding
        deltaCompression: {
          enabled: true,
          algorithms: ["fossil", "xdelta3"],
          debug: false,
          onStats: (stats) => {
            console.log("Delta Stats Update:", stats);
            // Update UI with stats from the library
            elements.deltaMessages.textContent = stats.deltaMessages;
            elements.fullMessages.textContent = stats.fullMessages;
            elements.bandwidthSaved.textContent =
              stats.bandwidthSavedPercent.toFixed(1) + "%";
          },
          onError: (error) => {
            console.error("Delta Compression Error:", error);
          },
        },
      };

      state.pusher = new Pusher(state.config.pusherKey, pusherConfig);
      connectionManager.bindConnectionEvents();
    },

    disconnect() {
      if (state.pusher) {
        state.pusher.disconnect();
      }
    },

    bindConnectionEvents() {
      state.pusher.connection.bind("connected", () => {
        state.stats.connectionStartTime = Date.now();
        state.stats.connectionTimer = setInterval(
          utils.updateConnectionTimer,
          1000,
        );

        connectionManager.updateStatus(
          "connected",
          `Connected (${state.pusher.connection.socket_id})`,
        );

        eventManager.add({
          type: "system",
          title: `🚀 Connected to WebSocket server`,
          timestamp: Date.now(),
          data: { socketId: state.pusher.connection.socket_id },
        });

        // Enable delta compression toggle now that we're connected
        elements.deltaCompressionToggle.disabled = false;

        // Disable delta compression by default - wait for user to enable via toggle
        if (state.pusher.deltaCompression) {
          state.pusher.deltaCompression.disable();
          elements.deltaEnabled.textContent = "No";
        }

        elements.connectBtn.disabled = true;
        elements.disconnectBtn.disabled = false;
        elements.subscribeBtn.disabled = false;
        channelManager.updateDropdowns();

        // Note: Delta compression is now handled internally by the sockudo-js library
        // No manual WebSocket hooks needed!
      });

      state.pusher.connection.bind("disconnected", () => {
        if (state.stats.connectionTimer) {
          clearInterval(state.stats.connectionTimer);
          state.stats.connectionTimer = null;
        }

        connectionManager.updateStatus("disconnected", "Disconnected");

        eventManager.add({
          type: "system",
          title: "🔌 Disconnected from server",
          timestamp: Date.now(),
        });

        elements.connectBtn.disabled = false;
        elements.disconnectBtn.disabled = true;
        elements.subscribeBtn.disabled = true;

        // Clear channels and presence
        state.channels.clear();
        channelManager.render();
        channelManager.updateDropdowns();
        presenceManager.clear();
      });

      state.pusher.connection.bind("connecting", () => {
        connectionManager.updateStatus("connecting", "Connecting...");
      });

      state.pusher.connection.bind("error", (err) => {
        let errorMsg = "Connection Error";
        if (err.error?.data) {
          errorMsg += `: ${err.error.data.code} - ${err.error.data.message}`;
        } else if (err.message) {
          errorMsg += `: ${err.message}`;
        }

        eventManager.add({
          type: "error",
          title: errorMsg,
          timestamp: Date.now(),
          data: err,
        });

        connectionManager.updateStatus("error", "Connection Error");
      });

      state.pusher.connection.bind("failed", () => {
        eventManager.add({
          type: "error",
          title: "❌ Connection failed permanently",
          timestamp: Date.now(),
        });

        connectionManager.updateStatus("failed", "Connection Failed");
        elements.disconnectBtn.disabled = true;
      });

      // Delta compression events - bind globally to catch system-level events
      state.pusher.bind_global((eventName, data, metadata) => {
        console.log(`[Delta] Global event: ${eventName}, metadata:`, metadata);

        if (eventName === "pusher:delta_compression_enabled") {
          state.deltaCompression.enabled = true;

          eventManager.add({
            type: "system",
            title: "✅ Delta compression enabled by server",
            timestamp: Date.now(),
            data: data,
          });
        }
      });

      state.pusher.connection.bind("pusher:error", (error) => {
        if (error.data?.message?.includes("delta")) {
          eventManager.add({
            type: "error",
            title: `Delta compression error: ${error.data.message}`,
            timestamp: Date.now(),
            data: error,
          });
        }
      });
    },

    updateStatus(status, text) {
      elements.connectionStatus.textContent = text;
      elements.statusDot.className = `status-dot ${status}`;
    },
  };

  // Server Events
  const serverEventManager = {
    async send() {
      const channel = elements.serverEventChannel.value;
      const eventName = elements.serverEventName.value.trim();
      const eventDataStr = elements.serverEventData.value.trim();
      const eventTagsStr = elements.serverEventTags.value.trim();
      const conflationKey = elements.conflationKey.value.trim();

      if (!channel || !eventName) {
        eventManager.add({
          type: "error",
          title: "Channel and event name are required",
          timestamp: Date.now(),
        });
        return;
      }

      let eventData = {};
      if (eventDataStr) {
        try {
          eventData = JSON.parse(eventDataStr);
        } catch (e) {
          eventManager.add({
            type: "error",
            title: `Invalid JSON data: ${e.message}`,
            timestamp: Date.now(),
          });
          return;
        }
      }

      let eventTags = null;
      if (eventTagsStr) {
        try {
          eventTags = JSON.parse(eventTagsStr);
        } catch (e) {
          eventManager.add({
            type: "error",
            title: `Invalid JSON tags: ${e.message}`,
            timestamp: Date.now(),
          });
          return;
        }
      }

      try {
        const payload = { channel, event: eventName, data: eventData };
        if (eventTags) {
          payload.tags = eventTags;
        }
        if (conflationKey) {
          payload.conflation_key = conflationKey;
        }

        const response = await fetch("/trigger-event", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });

        const result = await response.json();

        if (result.success) {
          const details = [];
          if (eventTags) details.push("tags");
          if (conflationKey) details.push(`conflation: ${conflationKey}`);
          const detailsStr = details.length ? ` (${details.join(", ")})` : "";

          eventManager.add({
            type: "system",
            title: `📤 Server event sent: ${eventName} → ${channel}${detailsStr}`,
            timestamp: Date.now(),
            data: { eventData, tags: eventTags, conflation_key: conflationKey },
          });

          // Clear form
          elements.serverEventName.value = "";
          elements.serverEventData.value = "";
          elements.serverEventTags.value = "";
          elements.conflationKey.value = "";
        } else {
          throw new Error(result.error || "Failed to send event");
        }
      } catch (error) {
        eventManager.add({
          type: "error",
          title: `Failed to send server event: ${error.message}`,
          timestamp: Date.now(),
        });
      }
    },

    async sendBatch() {
      const channel = elements.serverEventChannel.value;

      if (!channel) {
        eventManager.add({
          type: "error",
          title: "Channel is required for batch events",
          timestamp: Date.now(),
        });
        return;
      }

      try {
        const response = await fetch("/trigger-batch-events", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ channel, count: 5, delay: 500 }),
        });

        const result = await response.json();

        if (result.success) {
          eventManager.add({
            type: "system",
            title: `📤 Batch events triggered on ${channel}`,
            timestamp: Date.now(),
            data: { message: result.message },
          });
        } else {
          throw new Error(result.error || "Failed to trigger batch events");
        }
      } catch (error) {
        eventManager.add({
          type: "error",
          title: `Failed to trigger batch events: ${error.message}`,
          timestamp: Date.now(),
        });
      }
    },
  };

  // Client Events
  const clientEventManager = {
    send() {
      const channelName = elements.clientEventChannel.value;
      const eventName = elements.clientEventName.value.trim();
      const eventDataStr = elements.clientEventData.value.trim();

      if (!channelName || !eventName) {
        eventManager.add({
          type: "error",
          title: "Channel and event name are required",
          timestamp: Date.now(),
        });
        return;
      }

      if (!eventName.startsWith("client-")) {
        eventManager.add({
          type: "error",
          title: 'Client event names must start with "client-"',
          timestamp: Date.now(),
        });
        return;
      }

      let eventData = {};
      if (eventDataStr) {
        try {
          eventData = JSON.parse(eventDataStr);
        } catch (e) {
          eventManager.add({
            type: "error",
            title: `Invalid JSON data: ${e.message}`,
            timestamp: Date.now(),
          });
          return;
        }
      }

      const channelData = state.channels.get(channelName);
      if (!channelData) {
        eventManager.add({
          type: "error",
          title: `Not subscribed to channel: ${channelName}`,
          timestamp: Date.now(),
        });
        return;
      }

      try {
        const triggered = channelData.channel.trigger(eventName, eventData);
        if (triggered) {
          eventManager.add({
            type: "client",
            title: `📱 Client event sent: ${eventName} → ${channelName}`,
            timestamp: Date.now(),
            data: eventData,
          });

          // Clear form
          elements.clientEventName.value = "";
          elements.clientEventData.value = "";
        } else {
          throw new Error("Failed to trigger client event");
        }
      } catch (error) {
        eventManager.add({
          type: "error",
          title: `Failed to send client event: ${error.message}`,
          timestamp: Date.now(),
        });
      }
    },
  };

  // Webhook Management
  const webhookManager = {
    async fetch() {
      try {
        const response = await fetch("/webhooks-log");
        const webhooks = await response.json();

        elements.webhooksLog.innerHTML = "";
        elements.webhookCount.textContent = webhooks.length;

        if (webhooks.length === 0) {
          elements.webhooksLog.innerHTML =
            '<li class="webhook-item">No webhooks received yet</li>';
          return;
        }

        webhooks.forEach((webhook) => {
          const li = document.createElement("li");
          li.className = "webhook-item";

          const events =
            webhook.body?.events
              ?.map((e) => `${e.name} (${e.channel || "N/A"})`)
              .join(", ") || "No events";

          li.innerHTML = `
            <div class="event-header">
              <div class="event-title">🪝 ${events}</div>
              <div class="event-timestamp">${utils.formatTime(
                webhook.timestamp,
              )}</div>
            </div>
            <div class="event-data">${utils.formatJSON(webhook.body)}</div>
          `;

          utils.addAnimation(li);
          elements.webhooksLog.appendChild(li);
        });
      } catch (error) {
        eventManager.add({
          type: "error",
          title: `Failed to fetch webhooks: ${error.message}`,
          timestamp: Date.now(),
        });
      }
    },

    clear() {
      elements.webhooksLog.innerHTML = "";
      elements.webhookCount.textContent = "0";
    },
  };

  // Configuration Loading
  const loadConfig = async () => {
    try {
      const response = await fetch("/config");
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      state.config = await response.json();
      elements.configDisplay.textContent = utils.formatJSON(state.config);
      elements.connectBtn.disabled = false;

      eventManager.add({
        type: "system",
        title: "⚙️ Configuration loaded successfully",
        timestamp: Date.now(),
      });
    } catch (error) {
      elements.configDisplay.textContent = `Error loading config: ${error.message}`;
      elements.connectBtn.disabled = true;

      eventManager.add({
        type: "error",
        title: `Configuration load failed: ${error.message}`,
        timestamp: Date.now(),
      });
    }
  };

  // Delta Compression Management
  // Simplified Delta Compression Manager
  // The sockudo-js library now handles all delta decoding internally!
  // This manager only handles UI updates and enable/disable controls
  const deltaCompressionManager = {
    enable() {
      if (!state.pusher) {
        console.error("Pusher not initialized");
        eventManager.add({
          type: "error",
          title: "Cannot enable delta compression: Not connected",
          timestamp: Date.now(),
        });
        return;
      }

      if (!state.pusher.deltaCompression) {
        console.error("Delta compression not available on Pusher instance");
        eventManager.add({
          type: "error",
          title:
            "Delta compression not available. Make sure deltaCompression config is set.",
          timestamp: Date.now(),
        });
        return;
      }

      // Enable delta compression in the library
      state.pusher.deltaCompression.enable();
      state.deltaCompression.enabled = true;

      // Update UI
      elements.deltaEnabled.textContent = "Yes";

      eventManager.add({
        type: "system",
        title: "🗜️ Delta compression enabled",
        timestamp: Date.now(),
      });
    },

    disable() {
      if (!state.pusher || !state.pusher.deltaCompression) {
        return;
      }

      // Disable delta compression in the library
      state.pusher.deltaCompression.disable();
      state.deltaCompression.enabled = false;

      // Update UI
      elements.deltaEnabled.textContent = "No";

      eventManager.add({
        type: "system",
        title: "Delta compression disabled",
        timestamp: Date.now(),
      });
    },
  };

  // Event Listeners
  elements.connectBtn.addEventListener("click", connectionManager.connect);
  elements.disconnectBtn.addEventListener(
    "click",
    connectionManager.disconnect,
  );

  elements.subscribeBtn.addEventListener("click", () => {
    const channelName = elements.channelNameInput.value.trim();
    if (channelName) {
      channelManager.subscribe(channelName);
      elements.channelNameInput.value = "";
    }
  });

  elements.deltaCompressionToggle.addEventListener("change", (e) => {
    if (e.target.checked) {
      deltaCompressionManager.enable();
    } else {
      deltaCompressionManager.disable();
    }
  });

  // Tag Filtering event listeners
  elements.enableTagFilter.addEventListener("change", (e) => {
    state.tagFiltering.enabled = e.target.checked;
    elements.tagFilterControls.style.display = e.target.checked
      ? "block"
      : "none";

    if (!e.target.checked) {
      state.tagFiltering.currentFilter = null;
      elements.filterJson.value = "";
    }
  });

  elements.filterPreset.addEventListener("change", (e) => {
    const preset = e.target.value;
    let filter = null;

    // Access Filter from window object
    const Filter = window.Filter;
    if (!Filter) {
      console.error("Filter class not available");
      return;
    }

    switch (preset) {
      case "goals":
        filter = Filter.eq("event_type", "goal");
        break;
      case "goals-shots":
        filter = Filter.in("event_type", ["goal", "shot"]);
        break;
      case "high-xg":
        filter = Filter.gte("xG", "0.8");
        break;
      case "goals-high-xg":
        filter = Filter.or(
          Filter.eq("event_type", "goal"),
          Filter.and(Filter.eq("event_type", "shot"), Filter.gte("xG", "0.8")),
        );
        break;
    }

    if (filter) {
      elements.filterJson.value = JSON.stringify(filter, null, 2);
      state.tagFiltering.currentFilter = filter;
    }
  });

  elements.filterJson.addEventListener("input", (e) => {
    try {
      if (e.target.value.trim()) {
        const filter = JSON.parse(e.target.value);
        state.tagFiltering.currentFilter = filter;
      } else {
        state.tagFiltering.currentFilter = null;
      }
    } catch (err) {
      console.log(err);
      // Invalid JSON, ignore
    }
  });

  elements.channelNameInput.addEventListener("keypress", (e) => {
    if (e.key === "Enter") {
      elements.subscribeBtn.click();
    }
  });

  elements.sendServerEventBtn.addEventListener(
    "click",
    serverEventManager.send,
  );
  elements.sendBatchEventsBtn.addEventListener(
    "click",
    serverEventManager.sendBatch,
  );

  elements.sendClientEventBtn.addEventListener(
    "click",
    clientEventManager.send,
  );
  elements.clientEventChannel.addEventListener(
    "change",
    channelManager.updateDropdowns,
  );

  elements.clearEventsBtn.addEventListener("click", eventManager.clear);
  elements.exportEventsBtn.addEventListener("click", utils.exportEvents);

  elements.fetchWebhooksBtn.addEventListener("click", webhookManager.fetch);
  elements.clearWebhooksBtn.addEventListener("click", webhookManager.clear);

  // Event filter buttons
  document.querySelectorAll(".filter-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".filter-btn").forEach((b) => {
        b.classList.remove("active");
      });
      btn.classList.add("active");
      state.currentEventFilter = btn.dataset.filter;
      eventManager.render();
    });
  });

  // Make managers globally available for onclick handlers
  window.channelManager = channelManager;

  // Initialize
  loadConfig();
  eventManager.render();
  presenceManager.clear();
  webhookManager.fetch();

  // Auto-refresh webhooks every 30 seconds
  setInterval(webhookManager.fetch, 30000);
});
