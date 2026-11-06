import { beforeAll, describe, expect, it, vi } from "vitest";

let Sockudo: typeof import("../src/core/sockudo").default;

beforeAll(async () => {
  Object.assign(globalThis, {
    VERSION: "test-version",
    CDN_HTTP: "",
    CDN_HTTPS: "",
    DEPENDENCY_SUFFIX: "",
  });
  ({ default: Sockudo } = await import("../src/core/sockudo"));
});

describe("connection recovery resume ordering", () => {
  it("resumes each channel only after its subscription succeeds", () => {
    const client = new Sockudo("app-key", {
      cluster: "local",
      protocolVersion: 2,
      connectionRecovery: true,
    });
    const send = vi.spyOn(client, "send_event").mockReturnValue(true);
    client.setRecoveryPositions({
      "private-a": { stream_id: "s-a", serial: 127 },
      "public-b": { stream_id: "s-b", serial: 5 },
    });
    (client.connection as unknown as { state: string }).state = "connected";

    client.connection.emit("connected", undefined);
    const resumes = () => send.mock.calls.filter(([event]) => event === "sockudo:resume");
    expect(resumes()).toHaveLength(0);

    // The ack's own serial (the attach position) must not replace the
    // position the resume is sent from.
    client.connection.emit("message", {
      event: "sockudo_internal:subscription_succeeded",
      channel: "private-a",
      data: {},
      stream_id: "s-a",
      serial: 129,
    });
    expect(resumes()).toHaveLength(1);
    expect(JSON.parse(resumes()[0][1] as string)).toEqual({
      channel_positions: { "private-a": { stream_id: "s-a", serial: 127 } },
    });

    // A repeated ack for the same channel does not resume twice.
    client.connection.emit("message", {
      event: "sockudo_internal:subscription_succeeded",
      channel: "private-a",
      data: {},
    });
    expect(resumes()).toHaveLength(1);

    client.connection.emit("message", {
      event: "sockudo_internal:subscription_succeeded",
      channel: "public-b",
      data: {},
    });
    expect(resumes()).toHaveLength(2);
    expect(JSON.parse(resumes()[1][1] as string)).toEqual({
      channel_positions: { "public-b": { stream_id: "s-b", serial: 5 } },
    });
    client.disconnect();
  });
});
