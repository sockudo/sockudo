export { version } from "../../version.js";

import {
  createContext,
  createElement,
  useContext,
  useEffect,
  useMemo,
  useRef,
  type ReactNode,
} from "react";
import { ErrorCode, ErrorInfo } from "../../errors.js";
import {
  createTransportHooks as createGenericTransportHooks,
  type TransportHooks,
  type TransportProviderProps,
  type TreeHandle,
  type UseActiveTurnsOptions,
  type UseClientTransportOptions,
  type UseClientTransportResult,
  type UseCreateViewOptions,
  type UseSockudoMessagesOptions,
  type UseTreeOptions,
  type UseViewOptions,
  type ViewHandle,
} from "../../react/index.js";
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

type MessageSetter = (
  value:
    | readonly AI.UIMessage[]
    | ((messages: readonly AI.UIMessage[]) => readonly AI.UIMessage[]),
) => void;

/**
 * Provider props for the Vercel `useChat` transport layer.
 *
 * `chatOptions` is captured by `useMemo([transport, chatOptions])`; callers
 * should pass a referentially stable object to avoid replacing the chat
 * transport between renders.
 *
 * @defaultValue `api` defaults to `"/api/chat"`.
 */
export type ChatTransportProviderProps = Omit<
  TransportProviderProps<
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
  /** Optional Vercel chat adapter hooks; keep this object stable. */
  chatOptions?: ChatTransportOptions;
  /** Child React tree. */
  children?: ReactNode;
};

/**
 * Options for {@link useChatTransport}.
 *
 * Missing, skipped, and failed providers return throwing stubs. Error fields
 * are set except when `skip` is true.
 */
export interface UseChatTransportOptions {
  /**
   * Provider channel name.
   *
   * @defaultValue Nearest chat transport provider.
   */
  channelName?: string;
  /**
   * Suppresses lookup and returns throwing stubs with no error fields.
   *
   * @defaultValue `false`.
   */
  skip?: boolean;
}

/**
 * Result returned by {@link useChatTransport}.
 *
 * Stub access throws {@link ErrorInfo} with
 * {@link ErrorCode.InvalidArgument}; async methods on real transports reject
 * with {@link ErrorInfo}.
 */
export interface UseChatTransportResult {
  /** Resolved Vercel chat transport or a throwing stub. */
  chatTransport: ChatTransport;
  /** Resolved underlying client transport or a throwing stub. */
  transport: VercelTransport;
  /**
   * Chat transport lookup or construction error.
   *
   * @defaultValue `undefined` when resolved or skipped.
   */
  chatTransportError?: ErrorInfo;
  /**
   * Underlying client transport lookup or construction error.
   *
   * @defaultValue `undefined` when resolved or skipped.
   */
  transportError?: ErrorInfo;
}

/**
 * Options for {@link useMessageSync}.
 */
export interface UseMessageSyncOptions {
  /** Vercel `useChat` `setMessages` function. */
  setMessages: MessageSetter;
  /**
   * Provider channel name.
   *
   * @defaultValue Nearest chat transport provider.
   */
  channelName?: string;
  /**
   * Suppresses subscriptions.
   *
   * @defaultValue `false`.
   */
  skip?: boolean;
}

interface ChatTransportSlot {
  chatTransport?: ChatTransport;
  error?: ErrorInfo;
}

interface ChatTransportRegistry {
  defaultChannelName?: string;
  slots: Map<string, ChatTransportSlot>;
}

const ChatTransportContext = createContext<ChatTransportRegistry | undefined>(
  undefined,
);
const emptyChatOptions: ChatTransportOptions = {};
const vercelHooks = createGenericTransportHooks<
  VercelInput,
  VercelOutput,
  VercelProjection,
  AI.UIMessage
>();

/**
 * Creates a Vercel-typed generic transport hook bundle.
 *
 * @defaultValue Type parameters are fixed to the Vercel UIMessage codec.
 */
export function createTransportHooks(): TransportHooks<
  VercelInput,
  VercelOutput,
  VercelProjection,
  AI.UIMessage
> {
  return createGenericTransportHooks<
    VercelInput,
    VercelOutput,
    VercelProjection,
    AI.UIMessage
  >();
}

/**
 * Provides a Vercel `ChatTransport` and the underlying Vercel-typed client
 * transport for one Sockudo channel.
 *
 * This component wraps the generic {@link TransportProvider} with
 * {@link UIMessageCodec}. It does not close the chat transport on unmount; the
 * underlying generic transport provider owns lifecycle and strict-mode cleanup.
 */
export function ChatTransportProvider(
  props: ChatTransportProviderProps,
): ReturnType<typeof createElement> {
  const {
    children,
    chatOptions,
    api = "/api/chat",
    channelName,
    ...transportOptions
  } = props;
  const innerProps =
    chatOptions === undefined ? { channelName } : { channelName, chatOptions };
  return createElement(
    VercelTransportProvider,
    {
      ...transportOptions,
      api,
      channelName,
      codec: UIMessageCodec,
    },
    createElement(ChatTransportProviderInner, innerProps, children),
  );
}

/**
 * Reads the nearest or named Vercel chat transport.
 *
 * @defaultValue Uses the nearest provider when `channelName` is omitted.
 *
 * Missing, skipped, and failed providers return throwing stubs; error fields are
 * omitted only when skipped.
 */
export function useChatTransport(
  options: UseChatTransportOptions = {},
): UseChatTransportResult {
  const registry = useContext(ChatTransportContext);
  const client = useClientTransport({
    ...(options.channelName !== undefined
      ? { channelName: options.channelName }
      : {}),
    ...(options.skip !== undefined ? { skip: options.skip } : {}),
  });
  if (options.skip === true) {
    return {
      chatTransport: throwingChatTransportStub(),
      transport: client.transport,
    };
  }
  const slot = resolveChatSlot(registry, options.channelName);
  const chatTransport = slot?.chatTransport;
  const chatError =
    slot?.error ?? missingChatTransportError(options.channelName);
  const result: UseChatTransportResult = {
    chatTransport: chatTransport ?? throwingChatTransportStub(),
    transport: client.transport,
  };
  if (!chatTransport) {
    result.chatTransportError = chatError;
  }
  if (client.transportError !== undefined) {
    result.transportError = client.transportError;
  }
  return result;
}

/**
 * Synchronizes Sockudo view updates into Vercel `useChat` state.
 *
 * While this client owns an active stream, synchronization is suppressed to
 * avoid Vercel optimistic id replacement flicker. When streaming transitions to
 * `false`, a sync runs immediately.
 */
export function useMessageSync(options: UseMessageSyncOptions): void {
  const { setMessages, channelName, skip } = options;
  const setMessagesRef = useRef(setMessages);
  setMessagesRef.current = setMessages;
  const { chatTransport, transport, chatTransportError, transportError } =
    useChatTransport({
      ...(channelName !== undefined ? { channelName } : {}),
      ...(skip !== undefined ? { skip } : {}),
    });
  useEffect(() => {
    if (
      skip === true ||
      chatTransportError !== undefined ||
      transportError !== undefined
    ) {
      return;
    }
    const view = transport.view;
    const sync = (): void => {
      setMessagesRef.current((overlay) =>
        mergeMessages(view.getMessages(), overlay),
      );
    };
    const syncIfIdle = (): void => {
      if (!chatTransport.streaming) {
        sync();
      }
    };
    const unsubscribeView = view.on("update", syncIfIdle);
    const unsubscribeMessages = transport.on("message", () => undefined);
    const unsubscribeStreaming = chatTransport.onStreamingChange(
      (streaming) => {
        if (!streaming) {
          sync();
        }
      },
    );
    syncIfIdle();
    return () => {
      unsubscribeView();
      unsubscribeMessages();
      unsubscribeStreaming();
    };
  }, [
    channelName,
    chatTransport,
    chatTransportError,
    skip,
    transport,
    transportError,
  ]);
}

/**
 * Merges Sockudo tree messages with Vercel's local optimistic overlay.
 *
 * Tree message order is preserved. Assistant tool-resolution parts from the
 * overlay win only over matching unresolved tree tool parts, preserving the
 * tree part's `type`. Overlay messages unknown to the tree are appended.
 */
export function mergeMessages(
  treeMessages: readonly AI.UIMessage[],
  overlayMessages: readonly AI.UIMessage[],
): readonly AI.UIMessage[] {
  const overlayById = new Map<string, AI.UIMessage>();
  for (const message of overlayMessages) {
    overlayById.set(message.id, message);
  }
  const seen = new Set<string>();
  const merged: AI.UIMessage[] = [];
  for (const treeMessage of treeMessages) {
    seen.add(treeMessage.id);
    const overlay = overlayById.get(treeMessage.id);
    merged.push(
      overlay?.role === "assistant" && treeMessage.role === "assistant"
        ? mergeAssistantMessage(treeMessage, overlay)
        : treeMessage,
    );
  }
  for (const overlay of overlayMessages) {
    if (!seen.has(overlay.id)) {
      merged.push(overlay);
    }
  }
  return merged;
}

/**
 * Reads the nearest or named Vercel client transport.
 *
 * @defaultValue Uses the nearest provider when `channelName` is omitted.
 */
export function useClientTransport(
  options?: UseClientTransportOptions,
): UseClientTransportResult<
  VercelInput,
  VercelOutput,
  VercelProjection,
  AI.UIMessage
> {
  return vercelHooks.useClientTransport(options);
}

/**
 * Subscribes to a Vercel UIMessage view.
 *
 * @defaultValue Uses the context transport's default view.
 */
export function useView(
  options?: UseViewOptions<VercelInput, AI.UIMessage>,
): ViewHandle<AI.UIMessage> {
  return vercelHooks.useView(options);
}

/**
 * Creates and owns an additional Vercel UIMessage view.
 *
 * @defaultValue Uses the context transport.
 */
export function useCreateView(
  options?: UseCreateViewOptions<VercelInput, AI.UIMessage>,
): ViewHandle<AI.UIMessage> {
  return vercelHooks.useCreateView(options);
}

/**
 * Returns stable tree callbacks for the Vercel transport.
 *
 * @defaultValue Uses the context transport.
 */
export function useTree(
  options?: UseTreeOptions<VercelInput, AI.UIMessage>,
): TreeHandle {
  return vercelHooks.useTree(options);
}

/**
 * Subscribes to active/suspended Vercel turn ownership.
 *
 * @defaultValue Uses the context transport.
 */
export function useActiveTurns(
  options?: UseActiveTurnsOptions<VercelInput, AI.UIMessage>,
): Map<string, Set<string>> {
  return vercelHooks.useActiveTurns(options);
}

/**
 * Subscribes to raw normalized Sockudo inbound messages for the Vercel
 * transport.
 *
 * @defaultValue Uses the context transport.
 */
export function useSockudoMessages(
  options?: UseSockudoMessagesOptions<VercelInput, AI.UIMessage>,
): readonly InboundMessage[] {
  return vercelHooks.useSockudoMessages(options);
}

function ChatTransportProviderInner({
  channelName,
  chatOptions,
  children,
}: {
  channelName: string;
  chatOptions?: ChatTransportOptions;
  children?: ReactNode;
}): ReturnType<typeof createElement> {
  const parent = useContext(ChatTransportContext);
  const { transport, transportError } = useClientTransport({ channelName });
  const options = chatOptions ?? emptyChatOptions;
  const slot = useMemo<ChatTransportSlot>(() => {
    if (transportError !== undefined) {
      return { error: transportError };
    }
    try {
      return { chatTransport: createChatTransport(transport, options) };
    } catch (error) {
      return { error: toChatTransportError(error) };
    }
  }, [transport, options]);
  const registry = useMemo<ChatTransportRegistry>(() => {
    const slots = new Map(parent?.slots);
    slots.set(channelName, slot);
    return { slots, defaultChannelName: channelName };
  }, [channelName, parent, slot]);
  return createElement(
    ChatTransportContext.Provider,
    { value: registry },
    children,
  );
}

function mergeAssistantMessage(
  treeMessage: AI.UIMessage,
  overlay: AI.UIMessage,
): AI.UIMessage {
  const overlayParts = resolvedToolPartsByCallId(overlay.parts);
  let parts: AI.UIMessagePart[] | undefined;
  for (let index = 0; index < treeMessage.parts.length; index += 1) {
    const part = treeMessage.parts[index];
    if (part === undefined) {
      continue;
    }
    const toolCallId = toolCallIdOf(part);
    const overlayPart = toolCallId ? overlayParts.get(toolCallId) : undefined;
    if (
      overlayPart === undefined ||
      !isUnresolvedToolPart(part) ||
      !isResolvedOverlayToolPart(overlayPart)
    ) {
      parts?.push(part);
      continue;
    }
    parts ??= treeMessage.parts.slice(0, index);
    parts.push(mergeToolPartKeepingTreeType(part, overlayPart));
  }
  return parts === undefined ? treeMessage : { ...treeMessage, parts };
}

function resolvedToolPartsByCallId(
  parts: readonly AI.UIMessagePart[],
): Map<string, AI.UIMessagePart> {
  const result = new Map<string, AI.UIMessagePart>();
  for (const part of parts) {
    const toolCallId = toolCallIdOf(part);
    if (toolCallId && isResolvedOverlayToolPart(part)) {
      result.set(toolCallId, part);
    }
  }
  return result;
}

function mergeToolPartKeepingTreeType(
  treePart: AI.UIMessagePart,
  overlayPart: AI.UIMessagePart,
): AI.UIMessagePart {
  return {
    ...recordPart(treePart),
    ...recordPart(overlayPart),
    type: recordPart(treePart).type,
  } as AI.UIMessagePart;
}

function isUnresolvedToolPart(part: AI.UIMessagePart): boolean {
  const state = stateOf(part);
  return (
    state === "input-streaming" ||
    state === "input-available" ||
    state === "approval-requested"
  );
}

function isResolvedOverlayToolPart(part: AI.UIMessagePart): boolean {
  const state = stateOf(part);
  return (
    state === "output-available" ||
    state === "output-error" ||
    state === "approval-responded" ||
    state === "output-denied"
  );
}

function toolCallIdOf(part: AI.UIMessagePart): string | undefined {
  const record = recordPart(part);
  return typeof record.toolCallId === "string" ? record.toolCallId : undefined;
}

function stateOf(part: AI.UIMessagePart): string | undefined {
  const record = recordPart(part);
  return typeof record.state === "string" ? record.state : undefined;
}

function recordPart(part: AI.UIMessagePart): Record<string, unknown> {
  return part;
}

function VercelTransportProvider(
  props: TransportProviderProps<
    VercelInput,
    VercelOutput,
    VercelProjection,
    AI.UIMessage
  >,
): ReturnType<typeof createElement> {
  return vercelHooks.TransportProvider(props);
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
        ? "unable to resolve chat transport; no ChatTransportProvider is available"
        : `unable to resolve chat transport; no ChatTransportProvider for channel ${channelName}`,
  });
}

function toChatTransportError(error: unknown): ErrorInfo {
  if (error instanceof ErrorInfo) {
    return error;
  }
  return new ErrorInfo({
    code: ErrorCode.InvalidArgument,
    statusCode: 400,
    message: "unable to create chat transport in ChatTransportProvider",
    cause: error,
  });
}

function throwingChatTransportStub(): ChatTransport {
  return new Proxy(
    {},
    {
      get(): never {
        throw missingChatTransportError(undefined);
      },
    },
  ) as ChatTransport;
}
