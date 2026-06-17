/**
 * Delta Compression Types and Interfaces
 */

export type DeltaAlgorithm = "fossil" | "xdelta3";

export interface DeltaOptions {
  enabled?: boolean;
  algorithms?: DeltaAlgorithm[];
  debug?: boolean;
  onStats?: (stats: DeltaStats) => void;
  onError?: (error: unknown) => void;
}

export interface DeltaStats {
  totalMessages: number;
  deltaMessages: number;
  fullMessages: number;
  totalBytesWithoutCompression: number;
  totalBytesWithCompression: number;
  bandwidthSaved: number;
  bandwidthSavedPercent: number;
  errors: number;
  channelCount: number;
  channels: ChannelDeltaStats[];
}

export interface ChannelDeltaStats {
  channelName: string;
  conflationKey: string | null;
  conflationGroupCount: number;
  deltaCount: number;
  fullMessageCount: number;
  totalMessages: number;
}

export interface DeltaMessage {
  event: string;
  delta: string; // base64 encoded
  seq: number;
  algorithm?: DeltaAlgorithm;
  conflation_key?: string;
  base_index?: number;
}

export interface CacheSyncData {
  conflation_key?: string;
  max_messages_per_key?: number;
  states?: {
    [key: string]: Array<{
      content: string;
      seq: number;
    }>;
  };
}

export interface CachedMessage {
  content: string;
  sequence: number;
}
