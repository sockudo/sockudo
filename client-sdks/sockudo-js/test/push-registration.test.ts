import { describe, expect, it } from "vitest";
import type { ApnsLiveActivityPublishRequest } from "../types/core/push";

import { SockudoPushRegistration } from "../src/core/push";

describe("SockudoPushRegistration", () => {
  it("uses a backend proxy and keeps push publish async by default", async () => {
    const requests: Array<{ url: string; init: RequestInit }> = [];
    const client = new SockudoPushRegistration({
      endpoint: "https://api.example.test/push",
      headers: { Authorization: "Bearer session" },
      fetch: (async (url: string, init: RequestInit) => {
        requests.push({ url, init });
        return new Response(JSON.stringify({ publish_id: "pub_123" }), {
          status: 202,
          headers: { "Content-Type": "application/json" },
        });
      }) as typeof fetch,
    });

    const response = await client.publish({
      recipients: [{ type: "channel", channel: "orders" }],
      payload: { title: "Order", body: "Updated" },
      providerOverrides: [{ provider: "fcm", payload: { android: {} } }],
    });

    expect(response).toEqual({ publish_id: "pub_123" });
    expect(requests[0].url).toBe("https://api.example.test/push/publish");
    expect(requests[0].init.method).toBe("POST");
    expect(JSON.parse(requests[0].init.body as string)).toMatchObject({
      sync: false,
      recipients: [{ type: "channel", channel: "orders" }],
    });
    expect(requests[0].init.headers).toMatchObject({
      Authorization: "Bearer session",
      "Content-Type": "application/json",
    });
  });

  it("passes device identity token only to device update requests", async () => {
    const requests: Array<{ url: string; init: RequestInit }> = [];
    const client = new SockudoPushRegistration({
      endpoint: "https://api.example.test/push",
      fetch: (async (url: string, init: RequestInit) => {
        requests.push({ url, init });
        return new Response(JSON.stringify({ change: "updated" }), {
          status: 201,
          headers: { "Content-Type": "application/json" },
        });
      }) as typeof fetch,
    });

    await client.updateDeviceRegistration(
      {
        id: "device-1",
        formFactor: "phone",
        platform: "android",
        timezone: "UTC",
        locale: "en",
        push: {
          recipient: { transportType: "gcm", registrationToken: "rotated" },
        },
      },
      "identity",
    );

    expect(requests[0].url).toBe("https://api.example.test/push/deviceRegistrations");
    expect(requests[0].init.headers).toMatchObject({
      "X-Sockudo-Device-Identity-Token": "identity",
    });
  });

  it("publishes typed direct and broadcast Live Activity requests", async () => {
    const bodies: Array<Record<string, unknown>> = [];
    const client = new SockudoPushRegistration({
      endpoint: "https://api.example.test/push",
      fetch: (async (_url: string, init: RequestInit) => {
        bodies.push(JSON.parse(init.body as string));
        return new Response(JSON.stringify({ publish_id: "live_123" }), {
          status: 202,
          headers: { "Content-Type": "application/json" },
        });
      }) as typeof fetch,
    });

    const directRequest: ApnsLiveActivityPublishRequest = {
      publishId: "ride-42-start",
      recipients: [
        {
          type: "recipient",
          recipient: {
            transportType: "apnsLiveActivity",
            activityToken: "push-to-start-token",
          },
        },
      ],
      liveActivity: {
        event: "start",
        timestamp: 1725000000,
        contentState: { status: "assigned" },
        attributesType: "RideAttributes",
        attributes: { rideId: "ride-42" },
        alert: { title: "Driver assigned" },
        inputPushToken: true,
        priority: "immediate",
      },
    };
    await client.publishLiveActivity(directRequest);
    await client.publishLiveActivity({
      recipients: [
        {
          type: "recipient",
          recipient: {
            transportType: "apnsLiveActivityBroadcast",
            channelId: "dHN0LWNoYW5uZWw=",
            storagePolicy: "mostRecent",
          },
        },
      ],
      liveActivity: {
        event: "update",
        timestamp: 1725000001,
        contentState: { home: 2, away: 1 },
        priority: "lowPower",
      },
      expiresAtMs: 1725003600000,
    });

    expect(bodies[0]).toMatchObject({
      sync: false,
      payload: {},
      recipients: [
        {
          type: "recipient",
          recipient: { transportType: "apnsLiveActivity" },
        },
      ],
      liveActivity: { event: "start", inputPushToken: true },
    });
    expect(bodies[1]).toMatchObject({
      sync: false,
      recipients: [
        {
          type: "recipient",
          recipient: {
            transportType: "apnsLiveActivityBroadcast",
            storagePolicy: "mostRecent",
          },
        },
      ],
      liveActivity: { event: "update", priority: "lowPower" },
    });
  });

  it("uses cursor pagination params on list calls", async () => {
    const urls: string[] = [];
    const client = new SockudoPushRegistration({
      endpoint: "https://api.example.test/push",
      fetch: (async (url: string, _init: RequestInit) => {
        urls.push(url);
        return new Response(JSON.stringify({ items: [], has_more: false }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }) as typeof fetch,
    });

    await client.listChannelSubscriptions({
      deviceId: "device-1",
      limit: 10,
      cursor: "c1",
    });

    expect(urls[0]).toBe(
      "https://api.example.test/push/channelSubscriptions?deviceId=device-1&limit=10&cursor=c1",
    );
  });
});
