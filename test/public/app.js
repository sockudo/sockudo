// public/app.js

document.addEventListener("DOMContentLoaded", () => {
  // --- DOM Elements ---
  const configDisplay = document.getElementById("config-display");
  const connectBtn = document.getElementById("connect-btn");
  const disconnectBtn = document.getElementById("disconnect-btn");
  const connectionStatus = document.getElementById("connection-status");
  const channelNameInput = document.getElementById("channel-name");
  const subscribeBtn = document.getElementById("subscribe-btn");
  const subscribedChannelsList = document.getElementById("subscribed-channels");
  const clientEventChannelSelect = document.getElementById(
    "client-event-channel-select",
  );
  const clientEventNameInput = document.getElementById("client-event-name");
  const clientEventDataInput = document.getElementById("client-event-data");
  const sendClientEventBtn = document.getElementById("send-client-event-btn");
  const eventsLog = document.getElementById("events-log");
  const clearEventsLogBtn = document.getElementById("clear-events-log-btn");
  const presenceChannelName = document.getElementById("presence-channel-name");
  const presenceMembersCount = document.getElementById(
    "presence-members-count",
  );
  const presenceMembersList = document.getElementById("presence-members");
  const fetchWebhooksBtn = document.getElementById("fetch-webhooks-btn");
  const clearWebhooksLogBtn = document.getElementById("clear-webhooks-log-btn");
  const webhooksLog = document.getElementById("webhooks-log");

  // --- State ---
  let pusher = null;
  let config = null;
  let channels = {}; // Store subscribed channel objects { channelName: channel }
  let currentPresenceChannel = null; // Track the latest subscribed presence channel for UI display

  // --- Logging Helper ---
  function logEvent(message, type = "info", data = null) {
    const li = document.createElement("li");
    const timestamp = new Date().toLocaleTimeString();
    li.classList.add(`event-${type}`);
    li.innerHTML = `<span class="timestamp">[${timestamp}]</span> ${message}`;
    if (data) {
      try {
        li.innerHTML += `<pre class="data">${JSON.stringify(data, null, 2)}</pre>`;
      } catch (e) {
        li.innerHTML += `<pre class="data">Error stringifying data: ${e.message}</pre>`;
      }
    }
    eventsLog.prepend(li); // Add new events to the top
  }

  function logWebhook(webhook) {
    const li = document.createElement("li");
    const timestamp = new Date(webhook.timestamp).toLocaleString();
    let events =
      webhook.body?.events
        ?.map(
          (e) =>
            `<span class="event-meta">${e.name} (${e.channel || "N/A"})</span>`,
        )
        .join(", ") || "No events array";
    li.innerHTML = `<span class="timestamp">[${timestamp}]</span> Events: ${events}`;
    li.innerHTML += `<pre class="data">${JSON.stringify(webhook.body, null, 2)}</pre>`;
    webhooksLog.appendChild(li); // Add new webhooks to the bottom (chronological)
  }

  // --- Configuration Fetch ---
  async function fetchConfig() {
    try {
      const response = await fetch("/config");
      if (!response.ok)
        throw new Error(`HTTP error("{}", status: ${response.status}`);
      config = await response.json();
      configDisplay.textContent = JSON.stringify(config, null, 2);
      connectBtn.disabled = false;
      logEvent("Configuration loaded successfully", "system");
    } catch (error) {
      configDisplay.textContent = `Error loading config: ${error.message}`;
      logEvent(`Error loading config: ${error.message}`, "error");
      connectBtn.disabled = true;
    }
  }

  // --- Pusher Connection ---
  function connect() {
    if (!config) {
      logEvent("Configuration not loaded.", "error");
      return;
    }
    if (pusher && pusher.connection.state !== "disconnected") {
      logEvent("Already connected or connecting.", "system");
      return;
    }

    logEvent("Connecting...", "system");
    connectionStatus.textContent = "Connecting...";
    connectionStatus.style.color = "orange";

    // --- IMPORTANT: pusher-js configuration for self-hosted ---
    const pusherConfig = {
      cluster: config.pusherCluster || "mt1", // Often required syntactically
      wsHost: config.pusherHost,
      wsPort: config.pusherPort,
      wssPort: config.pusherPort, // Use same port for WSS if TLS enabled
      forceTLS: config.pusherUseTLS,
      enabledTransports: ["ws", "wss"], // Prioritize WebSocket
      disabledTransports: ["sockjs"], // Disable SockJS if not supported/needed
      authEndpoint: config.authEndpoint,
      authTransport: "ajax", // Or 'jsonp' if needed, 'ajax' is default/modern
      auth: {
        headers: {
          // Add any custom headers needed for your auth endpoint if required
          // 'X-CSRF-Token': 'some-token'
        },
      },
    };

    console.log("Initializing Pusher with config:", pusherConfig);
    pusher = new Pusher(config.pusherKey, pusherConfig);

    // --- Global Connection Event Bindings ---
    pusher.connection.bind("connected", () => {
      logEvent(
        `Connected! Socket ID: ${pusher.connection.socket_id}`,
        "system",
      );
      connectionStatus.textContent = `Connected (${pusher.connection.socket_id})`;
      connectionStatus.style.color = "green";
      connectBtn.disabled = true;
      disconnectBtn.disabled = false;
      subscribeBtn.disabled = false;
      updateClientEventSendability();
    });

    pusher.connection.bind("disconnected", () => {
      logEvent("Disconnected.", "system");
      connectionStatus.textContent = "Disconnected";
      connectionStatus.style.color = "red";
      connectBtn.disabled = false;
      disconnectBtn.disabled = true;
      subscribeBtn.disabled = true;
      updateClientEventSendability();
      // Clear presence info on disconnect
      clearPresenceInfo();
      // Optionally clear subscribed channels list or mark them as inactive
      channels = {};
      subscribedChannelsList.innerHTML = "";
      updateClientEventChannelDropdown();
    });

    pusher.connection.bind("connecting", () => {
      logEvent("Connecting...", "system");
      connectionStatus.textContent = "Connecting...";
      connectionStatus.style.color = "orange";
    });

    pusher.connection.bind("unavailable", () => {
      logEvent("Connection unavailable. Will attempt to reconnect.", "error");
      connectionStatus.textContent = "Unavailable";
      connectionStatus.style.color = "red";
    });

    pusher.connection.bind("error", (err) => {
      let errorMsg = "Connection Error";
      if (err.error && err.error.data) {
        errorMsg += `: ${err.error.data.code} - ${err.error.data.message}`;
      } else if (err.message) {
        errorMsg += `: ${err.message}`;
      }
      logEvent(errorMsg, "error", err);
      connectionStatus.textContent = "Error";
      connectionStatus.style.color = "red";
      // Depending on the error, pusher might try to reconnect or move to disconnected/failed
    });

    pusher.connection.bind("failed", () => {
      logEvent(
        "Connection failed permanently (check host/port/TLS settings).",
        "error",
      );
      connectionStatus.textContent = "Failed";
      connectionStatus.style.color = "red";
      disconnectBtn.disabled = true; // Can't disconnect if failed
    });

    // Global event catcher (optional)
    pusher.bind_global((eventName, data) => {
      // console.log(`Globally caught event: ${eventName}`, data);
      // Avoid logging internal pusher events here if they are handled elsewhere
      if (!eventName.startsWith("pusher:")) {
        logEvent(
          `Global Event: <span class="event-meta">${eventName}</span>`,
          "info",
          data,
        );
      }
    });
  }

  function disconnect() {
    if (pusher) {
      logEvent("Disconnecting...", "system");
      pusher.disconnect();
      // State updates handled by 'disconnected' event binding
    }
  }

  // --- Channel Subscription ---
  function subscribeToChannel(channelName) {
    if (!pusher || pusher.connection.state !== "connected") {
      logEvent("Cannot subscribe: Not connected.", "error");
      return;
    }
    if (!channelName) {
      logEvent("Cannot subscribe: Channel name is empty.", "error");
      return;
    }
    if (channels[channelName]) {
      logEvent(
        `Already subscribed or subscribing to ${channelName}.`,
        "system",
      );
      return;
    }

    logEvent(`Subscribing to channel: ${channelName}...`, "system");
    const channel = pusher.subscribe(channelName);
    channels[channelName] = channel; // Store channel object immediately
    updateSubscribedChannelsUI(); // Add to list immediately
    updateClientEventChannelDropdown();

    // --- Channel Specific Event Bindings ---

    channel.bind("pusher:subscription_succeeded", (data) => {
      logEvent(
        `Subscription successful: <span class="event-meta channel">${channelName}</span>`,
        "system",
        data,
      );
      // If it's a presence channel, update member list
      if (channelName.startsWith("presence-")) {
        currentPresenceChannel = channel; // Track this one
        presenceChannelName.textContent = channelName;
        updatePresenceMembers(channel.members);
      }
      // Re-enable buttons associated with this channel if needed
      updateClientEventSendability();
    });

    channel.bind("pusher:subscription_error", (status) => {
      logEvent(
        `Subscription failed: <span class="event-meta channel">${channelName}</span> - Status: ${status}`,
        "error",
      );
      // Handle specific statuses (e.g., 401/403 for auth issues)
      if (status === 403 || status === 401) {
        logEvent(
          `Auth failed for ${channelName}. Check backend auth endpoint and credentials.`,
          "error",
        );
      } else {
        logEvent(
          `Check WebSocket server logs for ${channelName} subscription error.`,
          "error",
        );
      }
      delete channels[channelName]; // Remove failed subscription
      updateSubscribedChannelsUI();
      updateClientEventChannelDropdown();
      if (currentPresenceChannel === channel) clearPresenceInfo(); // Clear presence if this was the one
    });

    // --- Bindings for Presence Channels ---
    if (channelName.startsWith("presence-")) {
      channel.bind("pusher:member_added", (member) => {
        logEvent(
          `Member joined <span class="event-meta channel">${channelName}</span>: ${member.info.name || member.id}`,
          "member",
          member,
        );
        if (currentPresenceChannel === channel) {
          updatePresenceMembers(channel.members); // Update list with fresh data
        }
      });

      channel.bind("pusher:member_removed", (member) => {
        logEvent(
          `Member left <span class="event-meta channel">${channelName}</span>: ${member.info.name || member.id}`,
          "member",
          member,
        );
        if (currentPresenceChannel === channel) {
          updatePresenceMembers(channel.members); // Update list with fresh data
        }
      });
    }

    // Catch-all for custom events on this channel
    channel.bind_global((eventName, data) => {
      // Avoid double-logging internal events if also using pusher.bind_global
      if (!eventName.startsWith("pusher:")) {
        const eventType = eventName.startsWith("client-") ? "client" : "custom";
        logEvent(
          `Event on <span class="event-meta channel">${channelName}</span>: <span class="event-meta">${eventName}</span>`,
          eventType,
          data,
        );
      }
    });
  }

  function unsubscribeFromChannel(channelName) {
    if (!pusher) return;
    const channel = channels[channelName];
    if (channel) {
      logEvent(`Unsubscribing from ${channelName}...`, "system");
      // Only call unsubscribe if subscription might have succeeded previously
      // Check internal state if available, otherwise just call it
      // if (channel.subscribed) { // pusher-js doesn't expose 'subscribed' reliably like this anymore in v8+
      pusher.unsubscribe(channelName);
      // } else {
      //    logEvent(`Channel ${channelName} was not fully subscribed, removing locally.`, 'system');
      // }

      if (currentPresenceChannel === channel) {
        clearPresenceInfo();
      }
      delete channels[channelName];
      updateSubscribedChannelsUI();
      updateClientEventChannelDropdown();
      updateClientEventSendability();
      logEvent(`Unsubscribed from ${channelName}.`, "system");
    } else {
      logEvent(`Not subscribed to ${channelName}.`, "system");
    }
  }

  // --- Update UI Elements ---

  function updateSubscribedChannelsUI() {
    subscribedChannelsList.innerHTML = "";
    Object.keys(channels).forEach((name) => {
      const li = document.createElement("li");
      li.textContent = name;
      const unsubBtn = document.createElement("button");
      unsubBtn.textContent = "Unsubscribe";
      unsubBtn.onclick = () => unsubscribeFromChannel(name);
      li.appendChild(unsubBtn);
      subscribedChannelsList.appendChild(li);
    });
  }

  function updateClientEventChannelDropdown() {
    const currentSelection = clientEventChannelSelect.value;
    clientEventChannelSelect.innerHTML =
      '<option value="">-- Select Channel --</option>';
    let foundSelected = false;
    Object.keys(channels).forEach((name) => {
      // Typically, client events need auth or server enablement, often used with private/presence
      // Allow selecting any subscribed channel for testing flexibility
      const option = document.createElement("option");
      option.value = name;
      option.textContent = name;
      clientEventChannelSelect.appendChild(option);
      if (name === currentSelection) {
        option.selected = true;
        foundSelected = true;
      }
    });
    clientEventChannelSelect.disabled = Object.keys(channels).length === 0;
    // If previous selection is gone, reset selection
    if (!foundSelected) {
      clientEventChannelSelect.value = "";
    }
    updateClientEventSendability(); // Update button based on selection
  }

  function updatePresenceMembers(members) {
    presenceMembersList.innerHTML = "";
    let count = 0;
    // members object structure: { count: number, me: myInfo, hash: { user_id1: userInfo1, ... } }
    if (members && members.count > 0) {
      count = members.count;
      members.each((member) => {
        // Use the 'each' iterator
        const li = document.createElement("li");
        li.textContent = `ID: ${member.id} - Info: ${JSON.stringify(member.info)}`;
        if (member.id === members.me.id) {
          li.textContent += " (You)";
          li.style.fontWeight = "bold";
        }
        presenceMembersList.appendChild(li);
      });
    } else {
      count = members?.count ?? 0; // Handle cases where members might be null/undefined initially
      const li = document.createElement("li");
      li.textContent =
        count === 0 ? "No members present." : "Member data not available.";
      presenceMembersList.appendChild(li);
    }
    presenceMembersCount.textContent = count;
  }

  function clearPresenceInfo() {
    presenceChannelName.textContent = "N/A";
    presenceMembersCount.textContent = "0";
    presenceMembersList.innerHTML =
      "<li>Not subscribed to a presence channel.</li>";
    currentPresenceChannel = null;
  }

  function updateClientEventSendability() {
    const channelName = clientEventChannelSelect.value;
    const channel = channels[channelName];
    // Check connection and if the selected channel exists and is likely subscribed
    // Note: pusher-js v8+ doesn't have a simple channel.subscribed flag.
    // We rely on the channel object existing in our 'channels' map as a proxy.
    const canSend = pusher?.connection?.state === "connected" && channel;
    sendClientEventBtn.disabled = !canSend;
  }

  // --- Sending Client Events ---
  function sendClientEvent() {
    const channelName = clientEventChannelSelect.value;
    const eventName = clientEventNameInput.value.trim();
    const eventDataStr = clientEventDataInput.value.trim();

    if (!channelName) {
      logEvent("Select a channel to send a client event.", "error");
      return;
    }
    if (!eventName) {
      logEvent("Client event name cannot be empty.", "error");
      return;
    }
    if (!eventName.startsWith("client-")) {
      logEvent('Client event names must start with "client-".', "error");
      return;
    }

    let eventData;
    try {
      eventData = eventDataStr ? JSON.parse(eventDataStr) : {};
    } catch (e) {
      logEvent(`Invalid JSON data for client event: ${e.message}`, "error");
      return;
    }

    const channel = channels[channelName];
    if (!channel) {
      logEvent(`Cannot send event: Not subscribed to ${channelName}.`, "error");
      updateClientEventSendability(); // Re-check state
      return;
    }

    logEvent(
      `Attempting to send client event <span class="event-meta">${eventName}</span> on <span class="event-meta channel">${channelName}</span>...`,
      "client",
      eventData,
    );

    // Use trigger() - check pusher-js docs for limitations (e.g., payload size)
    try {
      const triggered = channel.trigger(eventName, eventData);
      if (triggered) {
        logEvent(
          `Client event <span class="event-meta">${eventName}</span> sent successfully (check if received by other clients).`,
          "client",
        );
        clientEventNameInput.value = ""; // Clear fields on success
        clientEventDataInput.value = "";
      } else {
        // This 'else' might occur if the channel isn't ready, connection dropped, etc.
        // Pusher-js v8 might not return false directly on simple trigger failure,
        // errors are often handled via connection state or specific error events.
        logEvent(
          `Failed to send client event <span class="event-meta">${eventName}</span>. Channel might not be ready or client events disabled on server.`,
          "error",
        );
      }
    } catch (error) {
      logEvent(
        `Error triggering client event <span class="event-meta">${eventName}</span>: ${error.message}`,
        "error",
      );
    }
  }

  // --- Webhook Fetching ---
  async function fetchWebhooks() {
    logEvent("Fetching webhooks log from backend...", "system");
    webhooksLog.innerHTML = "<li>Loading...</li>"; // Clear previous logs
    try {
      const response = await fetch("/webhooks-log");
      if (!response.ok)
        throw new Error(`HTTP error!("{}", status: ${response.status}`);
      const webhooks = await response.json();
      webhooksLog.innerHTML = ""; // Clear loading message
      if (webhooks.length === 0) {
        webhooksLog.innerHTML =
          "<li>No webhooks received by the backend yet.</li>";
      } else {
        webhooks.forEach(logWebhook); // Display received webhooks
      }
    } catch (error) {
      webhooksLog.innerHTML = `<li>Error fetching webhooks: ${error.message}</li>`;
      logEvent(`Error fetching webhooks: ${error.message}`, "error");
    }
  }

  // --- Event Listeners ---
  connectBtn.addEventListener("click", connect);
  disconnectBtn.addEventListener("click", disconnect);
  subscribeBtn.addEventListener("click", () =>
    subscribeToChannel(channelNameInput.value.trim()),
  );
  sendClientEventBtn.addEventListener("click", sendClientEvent);
  clearEventsLogBtn.addEventListener("click", () => (eventsLog.innerHTML = ""));
  fetchWebhooksBtn.addEventListener("click", fetchWebhooks);
  clearWebhooksLogBtn.addEventListener(
    "click",
    () => (webhooksLog.innerHTML = ""),
  );
  clientEventChannelSelect.addEventListener(
    "change",
    updateClientEventSendability,
  );

  // --- Initial Load ---
  fetchConfig();
  clearPresenceInfo(); // Set initial state for presence UI
  updateClientEventChannelDropdown(); // Set initial state for dropdown/button
});
