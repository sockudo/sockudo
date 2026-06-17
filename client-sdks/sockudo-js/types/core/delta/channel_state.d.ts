import { CachedMessage, CacheSyncData, ChannelDeltaStats } from './types';
export default class ChannelState {
    channelName: string;
    conflationKey: string | null;
    maxMessagesPerKey: number;
    conflationCaches: Map<string, CachedMessage[]>;
    baseMessage: string | null;
    baseSequence: number | null;
    lastSequence: number | null;
    deltaCount: number;
    fullMessageCount: number;
    constructor(channelName: string);
    initializeFromCacheSync(data: CacheSyncData): void;
    setBase(message: string, sequence: number): void;
    getBaseMessage(conflationKeyValue?: string, baseIndex?: number): string | null;
    updateConflationCache(conflationKeyValue: string | undefined, message: string, sequence: number): void;
    hasBase(): boolean;
    isValidSequence(sequence: number): boolean;
    updateSequence(sequence: number): void;
    recordDelta(): void;
    recordFullMessage(): void;
    getStats(): ChannelDeltaStats;
}
