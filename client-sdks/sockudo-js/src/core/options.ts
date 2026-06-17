import {
  ChannelAuthorizationOptions,
  UserAuthenticationOptions,
} from "./auth/options";
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
  wireFormat?: "json" | "messagepack" | "msgpack" | "protobuf" | "proto";
  deltaCompression?: DeltaOptions;
  messageDeduplication?: boolean;
  messageDeduplicationCapacity?: number;
  connectionRecovery?: boolean;
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
  if ("disableStats" in options) {
    Logger.warn(
      "The disableStats option is deprecated in favor of enableStats",
    );
  }
}
