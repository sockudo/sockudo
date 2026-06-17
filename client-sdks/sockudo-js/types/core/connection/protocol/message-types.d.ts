export interface MessageExtras {
    headers?: Record<string, string | number | boolean>;
    ephemeral?: boolean;
    idempotency_key?: string;
    echo?: boolean;
}
export interface RecoveryPosition {
    stream_id?: string;
    serial: number;
    last_message_id?: string;
}
export interface ResumeRecoveredChannel {
    channel: string;
    source: string;
    replayed: number;
}
export interface ResumeFailedChannel {
    channel: string;
    code: string;
    reason: string;
    expected_stream_id?: string;
    current_stream_id?: string;
    oldest_available_serial?: number;
    newest_available_serial?: number;
}
export interface ResumeSuccessData {
    recovered: ResumeRecoveredChannel[];
    failed: ResumeFailedChannel[];
}
export interface RewindCompleteData {
    historical_count: number;
    live_count: number;
    complete: boolean;
    truncated_by_retention: boolean;
    truncated_by_limit: boolean;
}
interface PusherEvent {
    event: string;
    channel?: string;
    data?: any;
    user_id?: string;
    stream_id?: string;
    message_id?: string;
    serial?: number;
    extras?: MessageExtras;
    rawMessage?: string;
    sequence?: number;
    conflation_key?: string;
}
export { PusherEvent };
