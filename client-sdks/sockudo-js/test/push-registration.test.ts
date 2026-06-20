import { describe, expect, it } from "vitest";

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
