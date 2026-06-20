import { describe, expect, it } from "vitest";

import { ErrorCode } from "../errors.js";
import { createMockClient } from "./mocks.js";

describe("mock realtime client", () => {
  it("publishes, delivers, mutates, and reads history deterministically", async () => {
    const client = createMockClient({
      clientId: "client-1",
      providers: {
        now: () => 123,
      },
    });
    const channel = client.channels.get("chat");
    const delivered: string[] = [];
    channel.subscribe((message) => delivered.push(message.action));

    const create = await channel.publish({ name: "ai-output", data: "hello" });
    const append = await channel.appendMessage(create.messageSerial, " world");
    const update = await channel.updateMessage(create.messageSerial, {
      data: "hello world",
    });

    expect(client.connection.clientId).toBe("client-1");
    expect(create).toMatchObject({ messageSerial: "msg-1", historySerial: 1 });
    expect(append.historySerial).toBe(2);
    expect(update.historySerial).toBe(3);
    expect(delivered).toEqual(["create", "append", "update"]);

    const history = await channel.history({ limit: 10 });
    expect(history.items.map((message) => message.action)).toEqual(["create", "append", "update"]);
  });

  it("supports presence and continuity-loss injection", async () => {
    const client = createMockClient();
    const channel = client.channels.get("presence-chat");
    const presenceEvents: string[] = [];
    const continuityCodes: number[] = [];
    channel.presence.subscribe((event) => presenceEvents.push(event));
    channel.on("continuity_lost", (error) => continuityCodes.push(error.code));

    await channel.presence.enter({ online: true });
    await channel.presence.update({ online: false });
    await channel.presence.leave();
    client.injectContinuityLost("presence-chat", "stream_reset");

    expect(presenceEvents).toEqual(["enter", "update", "leave"]);
    expect(continuityCodes).toEqual([ErrorCode.ChannelContinuityLost]);
  });
});
