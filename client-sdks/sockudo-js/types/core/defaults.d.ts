import { ChannelAuthorizationOptions, UserAuthenticationOptions } from './auth/options';
import { AuthTransport } from './config';
export interface DefaultConfig {
    VERSION: string;
    PROTOCOL: number;
    wsPort: number;
    wssPort: number;
    wsPath: string;
    httpHost: string;
    httpPort: number;
    httpsPort: number;
    httpPath: string;
    stats_host: string;
    authEndpoint: string;
    authTransport: AuthTransport;
    activityTimeout: number;
    pongTimeout: number;
    unavailableTimeout: number;
    maxReconnectAttempts: number;
    maxReconnectGapInSeconds: number;
    reconnectJitter: number;
    retryJitterMs: number;
    userAuthentication: UserAuthenticationOptions;
    channelAuthorization: ChannelAuthorizationOptions;
}
declare var Defaults: DefaultConfig;
export default Defaults;
