// @vitest-environment happy-dom

import { createApp, defineComponent, h } from "vue";
import { describe, expect, it } from "vitest";

import { createMockClient } from "../realtime/mocks.js";
import { UIMessageCodec } from "../vercel/codec/index.js";
import {
  provideTransport,
  useClientTransport,
  useView,
  type UseClientTransportResult,
  type ViewHandle,
} from "./index.js";

describe("Vue transport composables", () => {
  it("provides and resolves a typed client transport", () => {
    const client = createMockClient({ clientId: "vue-client" });
    let provided: UseClientTransportResult<unknown, unknown, unknown, unknown> | undefined;
    let resolved: UseClientTransportResult<unknown, unknown, unknown, unknown> | undefined;
    let view: ViewHandle<unknown> | undefined;

    const Child = defineComponent({
      setup() {
        resolved = useClientTransport();
        view = useView();
        return () => null;
      },
    });
    const app = createApp(
      defineComponent({
        setup() {
          provided = provideTransport({
            client,
            channelName: "chat",
            codec: UIMessageCodec,
            api: "/api/chat",
          });
          return () => h(Child);
        },
      }),
    );

    app.mount(document.createElement("div"));

    expect(provided?.transportError.value).toBeUndefined();
    expect(resolved?.transportError.value).toBeUndefined();
    expect(resolved?.transport.value).toBe(provided?.transport.value);
    expect(view?.messages.value).toEqual([]);

    app.unmount();
  });
});
