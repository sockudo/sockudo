import StrategyRunner from "./strategy_runner";

interface Strategy {
  isSupported(): boolean;
  connect(
    minPriority: number,
    callback: (...args: any[]) => any,
  ): StrategyRunner;
}

export default Strategy;
