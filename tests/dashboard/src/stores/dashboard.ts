import { defineStore } from "pinia";
import { ref, computed } from "vue";

export type ConnectionState =
  | "disconnected"
  | "connecting"
  | "connected"
  | "reconnecting"
  | "failed";
export type Tab =
  | "connection"
  | "channels"
  | "events"
  | "presence"
  | "api"
  | "filters"
  | "delta";

export interface EventLogEntry {
  id: string;
  timestamp: number;
  direction: "in" | "out" | "system";
  event: string;
  channel?: string;
  data?: unknown;
}

export interface ChannelInfo {
  name: string;
  type:
    | "public"
    | "private"
    | "presence"
    | "encrypted"
    | "cache"
    | "wildcard"
    | "meta"
    | "user-limited"
    | "namespaced";
  subscribed: boolean;
  members?: PresenceMember[];
}

export interface PresenceMember {
  id: string;
  info: Record<string, unknown>;
}

export interface ConnectionConfig {
  appKey: string;
  appSecret: string;
  appId: string;
  host: string;
  port: number;
  useTLS: boolean;
  cluster: string;
  wireFormat: "json" | "messagepack" | "protobuf";
  authEndpoint: string;
  userAuthEndpoint: string;
}

let eventIdCounter = 0;

export const useDashboardStore = defineStore("dashboard", () => {
  const activeTab = ref<Tab>("connection");

  const config = ref<ConnectionConfig>({
    appKey: "app-key",
    appSecret: "app-secret",
    appId: "app-id",
    host: "127.0.0.1",
    port: 6001,
    useTLS: false,
    cluster: "mt1",
    wireFormat: "json",
    authEndpoint: "/pusher/auth",
    userAuthEndpoint: "/pusher/user-auth",
  });

  const connectionState = ref<ConnectionState>("disconnected");
  const socketId = ref<string | null>(null);

  const channels = ref<Map<string, ChannelInfo>>(new Map());
  const presenceMembers = ref<Map<string, PresenceMember[]>>(new Map());

  const eventLog = ref<EventLogEntry[]>([]);
  const maxEvents = 500;

  function addEvent(entry: Omit<EventLogEntry, "id" | "timestamp">) {
    eventLog.value.unshift({
      ...entry,
      id: `evt-${++eventIdCounter}`,
      timestamp: Date.now(),
    });
    if (eventLog.value.length > maxEvents) {
      eventLog.value.length = maxEvents;
    }
  }

  function clearEvents() {
    eventLog.value = [];
  }

  function addChannel(ch: ChannelInfo) {
    channels.value.set(ch.name, ch);
    // trigger reactivity
    channels.value = new Map(channels.value);
  }

  function removeChannel(name: string) {
    channels.value.delete(name);
    channels.value = new Map(channels.value);
  }

  function updateChannel(name: string, partial: Partial<ChannelInfo>) {
    const existing = channels.value.get(name);
    if (existing) {
      channels.value.set(name, { ...existing, ...partial });
      channels.value = new Map(channels.value);
    }
  }

  function setPresenceMembers(channel: string, members: PresenceMember[]) {
    presenceMembers.value.set(channel, members);
    presenceMembers.value = new Map(presenceMembers.value);
  }

  const deltaStats = ref({
    totalMessages: 0,
    deltaMessages: 0,
    fullMessages: 0,
    totalBytesWithoutCompression: 0,
    totalBytesWithCompression: 0,
    bandwidthSaved: 0,
    bandwidthSavedPercent: 0,
    errors: 0,
  });

  function updateDeltaStats(stats: any) {
    deltaStats.value = {
      totalMessages: stats.totalMessages ?? 0,
      deltaMessages: stats.deltaMessages ?? 0,
      fullMessages: stats.fullMessages ?? 0,
      totalBytesWithoutCompression: stats.totalBytesWithoutCompression ?? 0,
      totalBytesWithCompression: stats.totalBytesWithCompression ?? 0,
      bandwidthSaved: stats.bandwidthSaved ?? 0,
      bandwidthSavedPercent: stats.bandwidthSavedPercent ?? 0,
      errors: stats.errors ?? 0,
    };
  }

  const channelList = computed(() => Array.from(channels.value.values()));

  return {
    activeTab,
    config,
    connectionState,
    socketId,
    channels,
    channelList,
    presenceMembers,
    eventLog,
    deltaStats,
    updateDeltaStats,
    addEvent,
    clearEvents,
    addChannel,
    removeChannel,
    updateChannel,
    setPresenceMembers,
  };
});
