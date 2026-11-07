import Timeline from "../timeline/timeline";
import type { AppendMode } from "../options";

interface StrategyOptions {
  echoMessages?: boolean;
  failFast?: boolean;
  hostNonTLS?: string;
  hostTLS?: string;
  httpPath?: string;
  ignoreNullOrigin?: boolean;
  key?: string;
  loop?: boolean;
  retryDelay?: number;
  retryDelayLimit?: number;
  timeline?: Timeline;
  timeout?: number;
  timeoutLimit?: number;
  ttl?: number;
  useTLS?: boolean;
  wireFormat?: "json" | "messagepack" | "msgpack" | "protobuf" | "proto";
  appendMode?: AppendMode;
  getAuthToken?: () => string | undefined;
}

export default StrategyOptions;
