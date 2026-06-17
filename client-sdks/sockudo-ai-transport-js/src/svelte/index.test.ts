import { get } from "svelte/store";
import { describe, expect, it } from "vitest";

import { createMockClient } from "../realtime/mocks.js";
import { UIMessageCodec } from "../vercel/codec/index.js";
import { createTransportStore, createViewStore } from "./index.js";

describe("Svelte transport stores", () => {
  it("creates transport and view stores from an explicit client", () => {
    const client = createMockClient({ clientId: "svelte-client" });
    const transport = createTransportStore({
      client,
      channelName: "chat",
      codec: UIMessageCodec,
      api: "/api/chat",
      closeOnDestroy: false,
    });
    const state = get(transport);
    const view = createViewStore({ transport });

    expect(state.transportError).toBeUndefined();
    expect(state.transport).toBeDefined();
    expect(get(view).messages).toEqual([]);
  });
});
