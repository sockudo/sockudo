import Timeline from "../timeline/timeline";
import Strategy from "../strategies/strategy";
import StrategyOptions from "../strategies/strategy_options";

interface ConnectionManagerOptions {
  timeline: Timeline;
  getStrategy: (options: StrategyOptions) => Strategy;
  unavailableTimeout: number;
  pongTimeout: number;
  activityTimeout: number;
  useTLS: boolean;
  maxReconnectAttempts: number | null;
  maxReconnectGapInSeconds: number;
  reconnectJitter: number;
  beforeConnect?: (reason: "initial" | "reconnect") => void | Promise<void>;
}

export default ConnectionManagerOptions;
