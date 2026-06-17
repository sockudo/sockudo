export { version } from "../version.js";
import {
  createClientTransport as createCoreClientTransport,
  createServerTransport as createCoreServerTransport,
  type ClientTransport,
  type ClientTransportOptions,
  type ServerTransport,
  type ServerTransportOptions,
} from "../core/transport/index.js";
export * from "./codec/index.js";
import {
  UIMessageCodec,
  type AI,
  type VercelInput,
  type VercelOutput,
  type VercelProjection,
} from "./codec/index.js";

/**
 * Client transport options for Vercel UI messages.
 *
 * @defaultValue `api` defaults to `"/api/chat"`.
 */
export type VercelClientTransportOptions = Omit<
  ClientTransportOptions<
    VercelInput,
    VercelOutput,
    VercelProjection,
    AI.UIMessage
  >,
  "api" | "codec"
> & {
  /** Server endpoint URL for the HTTP poke.
   *
   * @defaultValue `"/api/chat"`.
   */
  api?: string;
};

/**
 * Server transport options for Vercel UI messages.
 */
export type VercelServerTransportOptions = Omit<
  ServerTransportOptions<
    VercelInput,
    VercelOutput,
    VercelProjection,
    AI.UIMessage
  >,
  "codec"
>;

/**
 * Creates a Sockudo client transport pre-bound to {@link UIMessageCodec}.
 *
 * Async methods reject with `ErrorInfo`; synchronous misuse throws `ErrorInfo`
 * with `InvalidArgument`.
 */
export function createClientTransport(
  options: VercelClientTransportOptions,
): ClientTransport<VercelInput, VercelOutput, VercelProjection, AI.UIMessage> {
  return createCoreClientTransport({
    ...options,
    api: options.api ?? "/api/chat",
    codec: UIMessageCodec,
  });
}

/**
 * Creates a Sockudo server transport pre-bound to {@link UIMessageCodec}.
 *
 * Public methods reject with `ErrorInfo`; synchronous misuse throws
 * `ErrorInfo` with `InvalidArgument`.
 */
export function createServerTransport(
  options: VercelServerTransportOptions,
): ServerTransport<VercelOutput, VercelProjection, AI.UIMessage> {
  return createCoreServerTransport({
    ...options,
    codec: UIMessageCodec,
  });
}

export * from "./transport/index.js";
