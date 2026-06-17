// @vitest-environment jsdom

import { beforeAll, afterEach, describe, expect, it } from "vitest";
import { act, createElement } from "react";
import { createRoot, type Root } from "react-dom/client";
import { createApp, defineComponent, h, nextTick } from "vue";
import { createHash, createHmac, randomUUID } from "node:crypto";
import type SockudoType from "../src/index";
import { useChannel as useReactChannel } from "../src/framework-react/index";
import {
  createSockudoPlugin,
  useChannel as useVueChannel,
} from "../src/framework-vue/index";

let Sockudo: typeof SockudoType;

const liveTestsEnabled = () => process.env.SOCKUDO_LIVE_TESTS === "1";

const waitForValue = async <T>(
  supplier: () => T | undefined,
  timeoutMs = 10000,
): Promise<T> => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = supplier();
    if (value !== undefined) {
      return value;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }

  throw new Error("Timed out waiting for live framework state");
};

const publishToLocalSockudo = async ({
  channel,
  eventName,
  payload,
}: {
  channel: string;
  eventName: string;
  payload: Record<string, unknown>;
}): Promise<void> => {
  const path = "/apps/app-id/events";
  const body = JSON.stringify({
    name: eventName,
    channels: [channel],
    data: JSON.stringify(payload),
  });
  const bodyMd5 = createHash("md5").update(body).digest("hex");
  const timestamp = Math.floor(Date.now() / 1000).toString();
  const params = new URLSearchParams({
    auth_key: "app-key",
    auth_timestamp: timestamp,
    auth_version: "1.0",
    body_md5: bodyMd5,
  });
  const canonicalQuery = [...params.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}=${value}`)
    .join("&");
  const authSignature = createHmac("sha256", "app-secret")
    .update(`POST\n${path}\n${canonicalQuery}`)
    .digest("hex");

  const response = await fetch(
    `http://127.0.0.1:6001${path}?${canonicalQuery}&auth_signature=${authSignature}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body,
    },
  );

  expect([200, 202]).toContain(response.status);
};

const createLiveClient = () =>
  new Sockudo("app-key", {
    cluster: "local",
    forceTLS: false,
    enabledTransports: ["ws"],
    wsHost: "127.0.0.1",
    wsPort: 6001,
    wssPort: 6001,
    protocolVersion: 2,
  });

beforeAll(async () => {
  Object.assign(globalThis, {
    VERSION: "test-version",
    CDN_HTTP: "",
    CDN_HTTPS: "",
    DEPENDENCY_SUFFIX: "",
  });

  ({ default: Sockudo } = await import("../src/index"));
});

describe("live framework integrations", () => {
  let reactRoot: Root | null = null;
  let reactContainer: HTMLDivElement | null = null;
  let vueApp: ReturnType<typeof createApp> | null = null;
  let vueContainer: HTMLDivElement | null = null;
  const clients: Array<InstanceType<typeof Sockudo>> = [];

  afterEach(async () => {
    for (const client of clients.splice(0)) {
      client.disconnect();
    }

    if (reactRoot) {
      await act(async () => {
        reactRoot?.unmount();
      });
      reactRoot = null;
    }
    reactContainer?.remove();
    reactContainer = null;

    if (vueApp) {
      vueApp.unmount();
      vueApp = null;
    }
    vueContainer?.remove();
    vueContainer = null;
  });

  it("updates React hook state from live websocket traffic", async () => {
    if (!liveTestsEnabled()) {
      return;
    }

    const client = createLiveClient();
    clients.push(client);
    const channelName = `react-live-${randomUUID()}`;
    const eventName = "react-live-event";
    let latest: ReturnType<typeof useReactChannel> | null = null;

    function Consumer() {
      latest = useReactChannel(channelName, { client });
      return null;
    }

    reactContainer = document.createElement("div");
    reactRoot = createRoot(reactContainer);

    await act(async () => {
      reactRoot?.render(createElement(Consumer));
    });

    client.connect();

    await waitForValue(() =>
      latest?.subscribed === true && latest.channel ? true : undefined,
    );

    await publishToLocalSockudo({
      channel: channelName,
      eventName,
      payload: { source: "react", marker: channelName },
    });

    await waitForValue(() =>
      latest?.lastEventName === eventName ? latest.lastEventData : undefined,
    );

    expect(latest?.lastEventName).toBe(eventName);
    expect((latest?.lastEventData as Record<string, unknown>)?.source).toBe(
      "react",
    );
  }, 20000);

  it("updates Vue composable state from live websocket traffic", async () => {
    if (!liveTestsEnabled()) {
      return;
    }

    const client = createLiveClient();
    clients.push(client);
    const channelName = `vue-live-${randomUUID()}`;
    const eventName = "vue-live-event";
    let latest: ReturnType<typeof useVueChannel> | null = null;

    const RootComponent = defineComponent({
      setup() {
        latest = useVueChannel(channelName);
        return () => h("div");
      },
    });

    vueApp = createApp(RootComponent);
    vueApp.use(createSockudoPlugin(client));
    vueContainer = document.createElement("div");
    vueApp.mount(vueContainer);

    client.connect();

    await waitForValue(() =>
      latest?.subscribed.value === true && latest.channel.value
        ? true
        : undefined,
    );

    await publishToLocalSockudo({
      channel: channelName,
      eventName,
      payload: { source: "vue", marker: channelName },
    });
    await nextTick();

    await waitForValue(() =>
      latest?.lastEventName.value === eventName
        ? latest.lastEventData.value
        : undefined,
    );

    expect(latest?.lastEventName.value).toBe(eventName);
    expect(
      (latest?.lastEventData.value as Record<string, unknown>)?.source,
    ).toBe("vue");
  }, 20000);
});
