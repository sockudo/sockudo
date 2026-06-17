import { OneOffTimer as Timer } from "../utils/timers";
import Strategy from "./strategy";

/** Runs substrategy after specified delay.
 *
 * Options:
 * - delay - time in miliseconds to delay the substrategy attempt
 *
 * @param {Strategy} strategy
 * @param {Object} options
 */
export default class DelayedStrategy implements Strategy {
  strategy: Strategy;
  options: { delay: number };

  constructor(strategy: Strategy, { delay: number }) {
    this.strategy = strategy;
    this.options = { delay: number };
  }

  isSupported(): boolean {
    return this.strategy.isSupported();
  }

  connect(minPriority: number, callback: (...args: any[]) => any) {
    const strategy = this.strategy;
    let runner;
    const timer = new Timer(this.options.delay, function () {
      runner = strategy.connect(minPriority, callback);
    });

    return {
      abort: function () {
        timer.ensureAborted();
        if (runner) {
          runner.abort();
        }
      },
      forceMinPriority: function (p) {
        minPriority = p;
        if (runner) {
          runner.forceMinPriority(p);
        }
      },
    };
  }
}
