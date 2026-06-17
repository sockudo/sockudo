export { version } from "../../version.js";

import {
  inject,
  onScopeDispose,
  provide,
  shallowRef,
  type ComputedRef,
  type InjectionKey,
  type Ref,
  type ShallowRef,
} from "vue";
import { ErrorCode, ErrorInfo } from "../../errors.js";
import {
  createTransportScope as createVueTransportScope,
  type TransportProviderOptions,
  type UseActiveTurnsOptions,
  type UseClientTransportOptions,
  type UseClientTransportResult,
  type UseCreateViewOptions,
  type UseSockudoMessagesOptions,
  type UseTreeOptions,
  type UseViewOptions,
  type ViewHandle,
  type TreeHandle,
} from "../../vue/index.js";
import type { InboundMessage } from "../../realtime/index.js";
import type { ClientTransport } from "../../core/transport/index.js";
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

type VercelTransport = ClientTransport<
  VercelInput,
  VercelOutput,
  VercelProjection,
  AI.UIMessage
>;

/**
 * Provider options for the Vercel AI SDK Vue transport layer.
 */
export type ChatTransportProviderOptions = Omit<
  TransportProviderOptions<
    VercelInput,
    VercelOutput,
    VercelProjection,
    AI.UIMessage
  >,
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
 * Result returned by {@link provideChatTransport} and {@link useChatTransport}.
 */
export interface UseChatTransportResult {
  /** Resolved Vercel chat transport ref. */
  chatTransport: ShallowRef<ChatTransport | undefined>;
  /** Resolved underlying client transport ref. */
  transport: ShallowRef<VercelTransport | undefined>;
  /** Chat transport lookup or construction error ref. */
  chatTransportError: ShallowRef<ErrorInfo | undefined>;
  /** Underlying client transport lookup or construction error ref. */
  transportError: ShallowRef<ErrorInfo | undefined>;
}

interface ChatTransportSlot {
  chatTransport: ShallowRef<ChatTransport | undefined>;
  error: ShallowRef<ErrorInfo | undefined>;
}

interface ChatTransportRegistry {
  defaultChannelName?: string;
  slots: Map<string, ChatTransportSlot>;
}

const chatTransportKey: InjectionKey<ChatTransportRegistry> = Symbol(
  "sockudo-ai-transport-vercel-vue",
);
const vercelScope = createVueTransportScope<
  VercelInput,
  VercelOutput,
  VercelProjection,
  AI.UIMessage
>();

/**
 * Provides a Vercel `ChatTransport` and underlying Vercel-typed client
 * transport for one Sockudo channel.
 */
export function provideChatTransport(
  options: ChatTransportProviderOptions,
): UseChatTransportResult {
  const parent = inject(chatTransportKey, undefined);
  const { chatOptions, api = "/api/chat", ...transportOptions } = options;
  const client = vercelScope.provideTransport({
    ...transportOptions,
    api,
    codec: UIMessageCodec,
  });
  const chatTransport = shallowRef<ChatTransport | undefined>();
  const chatTransportError = shallowRef<ErrorInfo | undefined>();
  if (client.transport.value) {
    try {
      chatTransport.value = createChatTransport(
        client.transport.value,
        chatOptions,
      );
    } catch (error) {
      chatTransportError.value = toChatTransportError(error);
    }
  } else {
    chatTransportError.value = client.transportError.value;
  }
  const slots = new Map(parent?.slots);
  slots.set(options.channelName, {
    chatTransport,
    error: chatTransportError,
  });
  const defaultChannelName = options.channelName || parent?.defaultChannelName;
  provide(
    chatTransportKey,
    defaultChannelName === undefined
      ? { slots }
      : { slots, defaultChannelName },
  );
  onScopeDispose(() => {
    void chatTransport.value?.close();
  });
  return {
    chatTransport,
    transport: client.transport,
    chatTransportError,
    transportError: client.transportError,
  };
}

/**
 * Reads the nearest or named Vercel chat transport.
 */
export function useChatTransport(
  options: UseClientTransportOptions = {},
): UseChatTransportResult {
  const client = useClientTransport(options);
  if (options.skip === true) {
    return {
      chatTransport: shallowRef(undefined),
      transport: client.transport,
      chatTransportError: shallowRef(undefined),
      transportError: client.transportError,
    };
  }
  const registry = inject(chatTransportKey, undefined);
  const slot = resolveChatSlot(registry, options.channelName);
  const chatTransportError = shallowRef<ErrorInfo | undefined>(
    slot?.error.value ?? missingChatTransportError(options.channelName),
  );
  return {
    chatTransport: slot?.chatTransport ?? shallowRef(undefined),
    transport: client.transport,
    chatTransportError:
      slot?.chatTransport.value === undefined ? chatTransportError : slot.error,
    transportError: client.transportError,
  };
}

/**
 * Creates a Vercel-typed generic transport scope.
 */
export function createTransportScope() {
  return createVueTransportScope<
    VercelInput,
    VercelOutput,
    VercelProjection,
    AI.UIMessage
  >();
}

/**
 * Reads the nearest or named Vercel client transport.
 */
export function useClientTransport(
  options?: UseClientTransportOptions,
): UseClientTransportResult<
  VercelInput,
  VercelOutput,
  VercelProjection,
  AI.UIMessage
> {
  return vercelScope.useClientTransport(options);
}

/**
 * Subscribes to a Vercel UIMessage view.
 */
export function useView(
  options?: UseViewOptions<VercelInput, AI.UIMessage>,
): ViewHandle<AI.UIMessage> {
  return vercelScope.useView(options);
}

/**
 * Creates and owns an additional Vercel UIMessage view.
 */
export function useCreateView(
  options?: UseCreateViewOptions<VercelInput, AI.UIMessage>,
): ViewHandle<AI.UIMessage> {
  return vercelScope.useCreateView(options);
}

/**
 * Returns stable tree callbacks for the Vercel transport.
 */
export function useTree(
  options?: UseTreeOptions<VercelInput, AI.UIMessage>,
): TreeHandle {
  return vercelScope.useTree(options);
}

/**
 * Subscribes to active/suspended Vercel turn ownership.
 */
export function useActiveTurns(
  options?: UseActiveTurnsOptions<VercelInput, AI.UIMessage>,
): ComputedRef<Map<string, Set<string>>> {
  return vercelScope.useActiveTurns(options);
}

/**
 * Subscribes to raw normalized Sockudo inbound messages for the Vercel
 * transport.
 */
export function useSockudoMessages(
  options?: UseSockudoMessagesOptions<VercelInput, AI.UIMessage>,
): Ref<readonly InboundMessage[]> {
  return vercelScope.useSockudoMessages(options);
}

function resolveChatSlot(
  registry: ChatTransportRegistry | undefined,
  channelName: string | undefined,
): ChatTransportSlot | undefined {
  const key = channelName ?? registry?.defaultChannelName;
  return key === undefined ? undefined : registry?.slots.get(key);
}

function missingChatTransportError(channelName: string | undefined): ErrorInfo {
  return new ErrorInfo({
    code: ErrorCode.InvalidArgument,
    statusCode: 400,
    message:
      channelName === undefined
        ? "unable to resolve chat transport; no Vue ChatTransport provider is available"
        : `unable to resolve chat transport; no Vue ChatTransport provider for channel ${channelName}`,
  });
}

function toChatTransportError(error: unknown): ErrorInfo {
  if (error instanceof ErrorInfo) {
    return error;
  }
  return new ErrorInfo({
    code: ErrorCode.InvalidArgument,
    statusCode: 400,
    message: "unable to create chat transport in Vue ChatTransport provider",
    cause: error,
  });
}
