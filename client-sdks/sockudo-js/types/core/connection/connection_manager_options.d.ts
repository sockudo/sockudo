import Timeline from '../timeline/timeline';
import Strategy from '../strategies/strategy';
interface ConnectionManagerOptions {
    timeline: Timeline;
    getStrategy: (StrategyOptions: any) => Strategy;
    unavailableTimeout: number;
    pongTimeout: number;
    activityTimeout: number;
    useTLS: boolean;
    maxReconnectAttempts: number | null;
    maxReconnectGapInSeconds: number;
    reconnectJitter: number;
    beforeConnect?: (reason: 'initial' | 'reconnect') => void | Promise<void>;
}
export default ConnectionManagerOptions;
