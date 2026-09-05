import { ChannelAuthorizationOptions, UserAuthenticationOptions } from './auth/options';
import { ChannelAuthorizerGenerator, DeprecatedAuthOptions } from './auth/deprecated_channel_authorizer';
import { AuthTransport, Transport } from './config';
import * as nacl from 'tweetnacl';
import { DeltaOptions } from './delta/types';
export type AppendMode = 'delta' | 'full';
export type AuthTokenReason = 'initial' | 'reconnect' | 'refresh' | 'expired';
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
export type AuthTokenCallback = (request: AuthTokenRequest) => AuthTokenResult | Promise<AuthTokenResult>;
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
    auth?: DeprecatedAuthOptions;
    authEndpoint?: string;
    authTransport?: AuthTransport;
    authorizer?: ChannelAuthorizerGenerator;
    channelAuthorization?: ChannelAuthorizationOptions;
    userAuthentication?: UserAuthenticationOptions;
    cluster?: string;
    token?: string;
    authUrl?: string;
    authCallback?: AuthTokenCallback;
    deltaCompression?: DeltaOptions;
    protocolVersion?: number;
    appendMode?: AppendMode;
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
export declare function validateOptions(options: any): void;
