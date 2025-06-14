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

    // Server Events
    serverEventChannel: document.getElementById("server-event-channel"),
    serverEventName: document.getElementById("server-event-name"),
    serverEventData: document.getElementById("server-event-data"),
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
            event.timestamp
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

      const channel = state.pusher.subscribe(channelName);
      state.channels.set(channelName, channel);

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
      channel.bind_global((eventName, data) => {
        if (!eventName.startsWith("pusher:")) {
          const eventType = eventName.startsWith("client-") ? "client" : "custom";
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

      state.channels.forEach((channel, channelName) => {
        const div = document.createElement("div");
        div.className = "channel-item";

        const channelType = utils.getChannelType(channelName);
        div.innerHTML = `
          <div>
            <span class="channel-name">${channelName}</span>
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

            state.channels.forEach((channel, channelName) => {
              const option = document.createElement("option");
              option.value = channelName;
              option.textContent = channelName;
              if (channelName === currentValue) {
                option.selected = true;
              }
              select.appendChild(option);
            });
          }
      );

      // Update client event button state
      const hasSelectedChannel = elements.clientEventChannel.value !== "";
      const isConnected = state.pusher?.connection?.state === "connected";
      elements.sendClientEventBtn.disabled = !hasSelectedChannel || !isConnected;
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
                  member.info.user_info.name
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

      if (
          state.pusher &&
          state.pusher.connection.state !== "disconnected"
      ) {
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
        wsHost: state.config.pusherHost,
        wsPort: state.config.pusherPort,
        // wssPort: state.config.pusherPort,
        forceTLS: state.config.pusherUseTLS,
        enabledTransports: ["ws"],
        disabledTransports: ["sockjs"],
        authEndpoint: state.config.authEndpoint,
        authTransport: "ajax",
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
            1000
        );

        connectionManager.updateStatus(
            "connected",
            `Connected (${state.pusher.connection.socket_id})`
        );

        eventManager.add({
          type: "system",
          title: `🚀 Connected to WebSocket server`,
          timestamp: Date.now(),
          data: { socketId: state.pusher.connection.socket_id },
        });

        elements.connectBtn.disabled = true;
        elements.disconnectBtn.disabled = false;
        elements.subscribeBtn.disabled = false;
        channelManager.updateDropdowns();
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

      try {
        const response = await fetch("/trigger-event", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ channel, event: eventName, data: eventData }),
        });

        const result = await response.json();

        if (result.success) {
          eventManager.add({
            type: "system",
            title: `📤 Server event sent: ${eventName} → ${channel}`,
            timestamp: Date.now(),
            data: eventData,
          });

          // Clear form
          elements.serverEventName.value = "";
          elements.serverEventData.value = "";
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

      const channel = state.channels.get(channelName);
      if (!channel) {
        eventManager.add({
          type: "error",
          title: `Not subscribed to channel: ${channelName}`,
          timestamp: Date.now(),
        });
        return;
      }

      try {
        const triggered = channel.trigger(eventName, eventData);
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
              webhook.timestamp
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

  // Event Listeners
  elements.connectBtn.addEventListener("click", connectionManager.connect);
  elements.disconnectBtn.addEventListener("click", connectionManager.disconnect);

  elements.subscribeBtn.addEventListener("click", () => {
    const channelName = elements.channelNameInput.value.trim();
    if (channelName) {
      channelManager.subscribe(channelName);
      elements.channelNameInput.value = "";
    }
  });

  elements.channelNameInput.addEventListener("keypress", (e) => {
    if (e.key === "Enter") {
      elements.subscribeBtn.click();
    }
  });

  elements.sendServerEventBtn.addEventListener("click", serverEventManager.send);
  elements.sendBatchEventsBtn.addEventListener(
      "click",
      serverEventManager.sendBatch
  );

  elements.sendClientEventBtn.addEventListener(
      "click",
      clientEventManager.send
  );
  elements.clientEventChannel.addEventListener(
      "change",
      channelManager.updateDropdowns
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