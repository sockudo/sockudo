export { version } from "../../version.js";

import { onDestroy } from "svelte";
import { get, readable, type Readable } from "svelte/store";
import { ErrorCode, ErrorInfo } from "../../errors.js";
import {
  createTransportStore,
  getClientTransport,
  setTransportContext,
  createActiveTurnsStore as createGenericActiveTurnsStore,
  createOwnedViewStore as createGenericOwnedViewStore,
  createSockudoMessagesStore as createGenericSockudoMessagesStore,
  createTreeHandle as createGenericTreeHandle,
  createViewStore as createGenericViewStore,
  type ActiveTurnsStoreOptions,
  type ClientTransportState,
  type ClientTransportStore,
  type GetClientTransportOptions,
  type SockudoMessagesStoreOptions,
  type TransportStoreOptions,
  type ViewStore,
  type ViewStoreOptions,
} from "../../svelte/index.js";
import {
  createChatTransport,
  type ChatTransport,
  type ChatTransportOptions,
} from "../transport/index.js";
import {
  UIMessageCodec,
  type AI,
  type VercelInput,
  type VercelOutput,
  type VercelProjection,
} from "../codec/index.js";

/**
 * Svelte store options for the Vercel AI SDK transport layer.
 */
export type ChatTransportStoreOptions = Omit<
  TransportStoreOptions<VercelInput, VercelOutput, VercelProjection, AI.UIMessage>,
  "api" | "codec"
> & {
  /** Server endpoint URL for the route handler.
   *
   * @defaultValue `"/api/chat"`.
   */
  api?: string;
  /** Optional Vercel chat adapter hooks. */
  chatOptions?: ChatTransportOptions;
};

/**
 * Svelte Vercel chat transport state.
 */
export interface ChatTransportState {
  /** Resolved Vercel chat transport. */
  chatTransport?: ChatTransport;
  /** Resolved underlying client transport. */
  transport?: ClientTransportState<
    VercelInput,
    VercelOutput,
    VercelProjection,
    AI.UIMessage
  >["transport"];
  /** Chat transport lookup or construction error. */
  chatTransportError?: ErrorInfo;
  /** Underlying client transport lookup or construction error. */
  transportError?: ErrorInfo;
}

/**
 * Svelte Vercel chat transport store.
 */
export interface ChatTransportStore extends Readable<ChatTransportState> {
  /** Channel registry key. */
  readonly channelName?: string;
  /** Closes the chat and client transport. */
  close(): Promise<void>;
}

/**
 * Creates a Sockudo-backed Vercel `ChatTransport` Svelte store.
 */
export function createChatTransportStore(options: ChatTransportStoreOptions): ChatTransportStore {
  const { chatOptions, api = "/api/chat", closeOnDestroy, ...transportOptions } = options;
  const client = createTransportStore({
    ...transportOptions,
    api,
    codec: UIMessageCodec,
    ...(closeOnDestroy !== undefined ? { closeOnDestroy } : {}),
  });
  let chatTransport: ChatTransport | undefined;
  let chatTransportError: ErrorInfo | undefined;
  const clientState = get(client);
  if (clientState.transport) {
    try {
      chatTransport = createChatTransport(clientState.transport, chatOptions);
    } catch (error) {
      chatTransportError = toChatTransportError(error);
    }
  } else {
    chatTransportError = clientState.transportError;
  }
  const store = readable<ChatTransportState>(
    {
      ...(chatTransport !== undefined ? { chatTransport } : {}),
      ...(clientState.transport !== undefined ? { transport: clientState.transport } : {}),
      ...(chatTransportError !== undefined ? { chatTransportError } : {}),
      ...(clientState.transportError !== undefined
        ? { transportError: clientState.transportError }
        : {}),
    },
    (set) =>
      client.subscribe((state) => {
        set({
          ...(chatTransport !== undefined ? { chatTransport } : {}),
          ...(state.transport !== undefined ? { transport: state.transport } : {}),
          ...(chatTransportError !== undefined ? { chatTransportError } : {}),
          ...(state.transportError !== undefined ? { transportError: state.transportError } : {}),
        });
      }),
  );
  const chatStore: ChatTransportStore = {
    subscribe: store.subscribe,
    async close() {
      await chatTransport?.close();
      await client.close();
    },
    ...(options.channelName !== undefined ? { channelName: options.channelName } : {}),
  };
  if (closeOnDestroy !== false) {
    safeOnDestroy(() => {
      void chatStore.close();
    });
  }
  return chatStore;
}

/**
 * Creates, stores, and provides a Vercel chat transport in one call.
 */
export function provideChatTransport(options: ChatTransportStoreOptions): ChatTransportStore {
  const store = createChatTransportStore(options);
  const clientState = get(store);
  if (clientState.transport !== undefined) {
    setTransportContext(createStaticTransportStore(options.channelName, clientState.transport));
  }
  return store;
}

/**
 * Reads the nearest or named Vercel chat transport store.
 */
export function getChatTransport(
  options: GetClientTransportOptions = {},
): Readable<ChatTransportState> {
  const client = getClientTransport<VercelInput, VercelOutput, VercelProjection, AI.UIMessage>(
    options,
  );
  const state = get(client);
  if (options.skip === true) {
    return readable({});
  }
  if (!state.transport) {
    return readable({
      ...(state.transportError !== undefined ? { transportError: state.transportError } : {}),
      chatTransportError: missingChatTransportError(options.channelName),
    });
  }
  const chatTransport = createChatTransport(state.transport);
  return readable(
    {
      transport: state.transport,
      chatTransport,
    },
    () => () => {
      void chatTransport.close();
    },
  );
}

/**
 * Creates a Vercel-typed client transport store.
 */
export function createClientTransportStore(
  options: Omit<
    TransportStoreOptions<VercelInput, VercelOutput, VercelProjection, AI.UIMessage>,
    "codec"
  >,
): ClientTransportStore<VercelInput, VercelOutput, VercelProjection, AI.UIMessage> {
  return createTransportStore({
    ...options,
    codec: UIMessageCodec,
  });
}

/**
 * Creates a Vercel UIMessage view store.
 */
export function createViewStore(
  options: ViewStoreOptions<VercelInput, AI.UIMessage> = {},
): ViewStore<AI.UIMessage> {
  return createGenericViewStore(options);
}

/**
 * Creates and owns an additional Vercel UIMessage view store.
 */
export function createOwnedViewStore(
  options: Omit<ViewStoreOptions<VercelInput, AI.UIMessage>, "view"> = {},
): ViewStore<AI.UIMessage> {
  return createGenericOwnedViewStore(options);
}

/**
 * Creates stable tree callbacks for the Vercel transport.
 */
export function createTreeHandle(options: ActiveTurnsStoreOptions<VercelInput, AI.UIMessage> = {}) {
  return createGenericTreeHandle(options);
}

/**
 * Subscribes to active/suspended Vercel turn ownership.
 */
export function createActiveTurnsStore(
  options: ActiveTurnsStoreOptions<VercelInput, AI.UIMessage> = {},
) {
  return createGenericActiveTurnsStore(options);
}

/**
 * Subscribes to raw normalized Sockudo inbound messages for the Vercel
 * transport.
 */
export function createSockudoMessagesStore(
  options: SockudoMessagesStoreOptions<VercelInput, AI.UIMessage> = {},
) {
  return createGenericSockudoMessagesStore(options);
}

function createStaticTransportStore(
  channelName: string | undefined,
  transport: NonNullable<ChatTransportState["transport"]>,
): ClientTransportStore<VercelInput, VercelOutput, VercelProjection, AI.UIMessage> {
  const store = readable<
    ClientTransportState<VercelInput, VercelOutput, VercelProjection, AI.UIMessage>
  >({ transport });
  return {
    subscribe: store.subscribe,
    close() {
      return transport.close();
    },
    ...(channelName !== undefined ? { channelName } : {}),
  };
}

function missingChatTransportError(channelName: string | undefined): ErrorInfo {
  return new ErrorInfo({
    code: ErrorCode.InvalidArgument,
    statusCode: 400,
    message:
      channelName === undefined
        ? "unable to resolve chat transport; no Svelte ChatTransport context is available"
        : `unable to resolve chat transport; no Svelte ChatTransport context for channel ${channelName}`,
  });
}

function toChatTransportError(error: unknown): ErrorInfo {
  if (error instanceof ErrorInfo) {
    return error;
  }
  return new ErrorInfo({
    code: ErrorCode.InvalidArgument,
    statusCode: 400,
    message: "unable to create chat transport in Svelte ChatTransport store",
    cause: error,
  });
}

function safeOnDestroy(cleanup: () => void): void {
  try {
    onDestroy(cleanup);
  } catch {
    // Store factories can be used outside component initialization in tests and
    // plain modules. In that case callers own cleanup through `close()`.
  }
}
