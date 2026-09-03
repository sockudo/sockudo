import { ChannelAuthorizationOptions, UserAuthenticationOptions } from "./auth/options";
import {
  ChannelAuthorizerGenerator,
  DeprecatedAuthOptions,
} from "./auth/deprecated_channel_authorizer";
import { AuthTransport, Transport } from "./config";
import * as nacl from "tweetnacl";
import Logger from "./logger";
import { DeltaOptions } from "./delta/types";

export interface PresenceHistoryOptions {
  endpoint: string;
  headers?: Record<string, string>;
  headersProvider?: () => Record<string, string>;
}

export interface VersionedMessagesOptions {
  endpoint: string;
  headers?: Record<string, string>;
  headersProvider?: () => Record<string, string>;
}

export type AppendMode = "delta" | "full";

export type AuthTokenReason = "initial" | "reconnect" | "refresh" | "expired";

export interface AuthTokenRequest {
  socketId?: string;
  reason: AuthTokenReason;
}

export interface AuthTokenData {
  token: string;
  exp?: number;
  iat?: number;
  expiresAt?: number;
  issuedAt?: number;
  expiresAtMs?: number;
  issuedAtMs?: number;
  expiresIn?: number;
}

export type AuthTokenResult = string | AuthTokenData;
export type AuthTokenCallback = (
  request: AuthTokenRequest,
) => AuthTokenResult | Promise<AuthTokenResult>;

export interface CapabilityTokenAuthData {
  clientId?: string;
  jti?: string;
  exp?: number;
}

export interface CapabilityTokenExpiredData {
  code?: number;
  reason?: string;
}

export interface Options {
  activityTimeout?: number;

  auth?: DeprecatedAuthOptions; // DEPRECATED use channelAuthorization instead
  authEndpoint?: string; // DEPRECATED use channelAuthorization instead
  authTransport?: AuthTransport; // DEPRECATED use channelAuthorization instead
  authorizer?: ChannelAuthorizerGenerator; // DEPRECATED use channelAuthorization instead

  channelAuthorization?: ChannelAuthorizationOptions;
  userAuthentication?: UserAuthenticationOptions;
  presenceHistory?: PresenceHistoryOptions;
  versionedMessages?: VersionedMessagesOptions;

  cluster?: string;
  protocolVersion?: number;
  /** Static Protocol V2 capability token used for the first connection. */
  token?: string;
  /** Endpoint that returns an AuthTokenResult for connection and refresh requests. */
  authUrl?: string;
  /** Provider used to obtain fresh Protocol V2 capability tokens. */
  authCallback?: AuthTokenCallback;
  wireFormat?: "json" | "messagepack" | "msgpack" | "protobuf" | "proto";
  appendMode?: AppendMode;
  deltaCompression?: DeltaOptions;
  messageDeduplication?: boolean;
  messageDeduplicationCapacity?: number;
  connectionRecovery?: boolean;
  maxReconnectAttempts?: number | null;
  maxReconnectGapInSeconds?: number;
  /** Fraction of the reconnect delay to randomize, 0 (off) to 1 (full jitter). */
  reconnectJitter?: number;
  echoMessages?: boolean;
  enableStats?: boolean;
  disableStats?: boolean;
  disabledTransports?: Transport[];
  enabledTransports?: Transport[];
  forceTLS?: boolean;
  httpHost?: string;
  httpPath?: string;
  httpPort?: number;
  httpsPort?: number;
  ignoreNullOrigin?: boolean;
  nacl?: nacl;
  pongTimeout?: number;
  statsHost?: string;
  timelineParams?: any;
  unavailableTimeout?: number;
  wsHost?: string;
  wsPath?: string;
  wsPort?: number;
  wssPort?: number;
}

export function validateOptions(options) {
  if (options == null) {
    throw "You must pass an options object";
  }
  if (options.cluster == null && options.wsHost == null) {
    throw "Options object must provide a cluster or wsHost";
  }
  const hasCapabilityTokenAuth =
    options.token != null || options.authUrl != null || options.authCallback != null;
  if (hasCapabilityTokenAuth && (options.protocolVersion ?? 7) !== 2) {
    throw "Capability-token authentication requires protocolVersion: 2";
  }
  if (options.token != null && (typeof options.token !== "string" || options.token.length === 0)) {
    throw "token must be a non-empty string";
  }
  if (
    options.authUrl != null &&
    (typeof options.authUrl !== "string" || options.authUrl.length === 0)
  ) {
    throw "authUrl must be a non-empty string";
  }
  if (options.authCallback != null && typeof options.authCallback !== "function") {
    throw "authCallback must be a function";
  }
  if ("disableStats" in options) {
    Logger.warn("The disableStats option is deprecated in favor of enableStats");
  }
}
