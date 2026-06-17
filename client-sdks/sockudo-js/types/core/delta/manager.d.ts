import { PusherEvent } from '../connection/protocol/message-types';
import { DeltaOptions, DeltaStats, DeltaMessage, DeltaAlgorithm, CacheSyncData } from './types';
export default class DeltaCompressionManager {
    private options;
    private enabled;
    private channelStates;
    private stats;
    private availableAlgorithms;
    private sendEventCallback;
    private defaultAlgorithm;
    constructor(options: DeltaOptions, sendEventCallback: (event: string, data: any) => boolean);
    private detectAvailableAlgorithms;
    enable(): void;
    disable(): void;
    handleEnabled(data: any): void;
    handleCacheSync(channel: string, data: CacheSyncData): void;
    handleDeltaMessage(channel: string, deltaData: DeltaMessage): PusherEvent | null;
    handleFullMessage(channel: string, rawMessage: string, sequence?: number, conflationKey?: string): void;
    private requestResync;
    private updateStats;
    getStats(): DeltaStats;
    resetStats(): void;
    clearChannelState(channel?: string): void;
    isEnabled(): boolean;
    getAvailableAlgorithms(): DeltaAlgorithm[];
    private log;
    private error;
}
