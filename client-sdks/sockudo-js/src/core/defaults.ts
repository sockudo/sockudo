import {
  ChannelAuthorizationOptions,
  UserAuthenticationOptions,
} from "./auth/options";
import { AuthTransport } from "./config";

export interface DefaultConfig {
  VERSION: string;
  PROTOCOL: number;
  wsPort: number;
  wssPort: number;
  wsPath: string;
  httpHost: string;
  wsHost: string;
  httpPort: number;
  httpsPort: number;
  httpPath: string;
  stats_host: string;
  authEndpoint: string;
  authTransport: AuthTransport;
  activityTimeout: number;
  pongTimeout: number;
  unavailableTimeout: number;
  userAuthentication: UserAuthenticationOptions;
  channelAuthorization: ChannelAuthorizationOptions;

  cdn_http?: string;
  cdn_https?: string;
  dependency_suffix?: string;
}

const Defaults: DefaultConfig = {
  VERSION: VERSION,
  PROTOCOL: 2,

  wsPort: 80,
  wssPort: 443,
  wsPath: "",
  // DEPRECATED: SockJS fallback parameters
  httpHost: "sockjs.sockudo.io",
  wsHost: "ws.sockudo.io",
  httpPort: 80,
  httpsPort: 443,
  httpPath: "/sockudo",
  // DEPRECATED: Stats
  stats_host: "stats.sockudo.io",
  // DEPRECATED: Other settings
  authEndpoint: "/sockudo/auth",
  authTransport: "fetch",
  activityTimeout: 120000,
  pongTimeout: 30000,
  unavailableTimeout: 10000,
  userAuthentication: {
    endpoint: "/sockudo/user-auth",
    transport: "fetch",
  },
  channelAuthorization: {
    endpoint: "/sockudo/auth",
    transport: "fetch",
  },

  // CDN configuration
  cdn_http: CDN_HTTP,
  cdn_https: CDN_HTTPS,
  dependency_suffix: DEPENDENCY_SUFFIX,
};

export default Defaults;
