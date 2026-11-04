import * as Collections from "../utils/collections";
import Util from "../util";
import { OneOffTimer as Timer } from "../utils/timers";
import Runtime from "runtime";
import Strategy from "./strategy";
import StrategyOptions from "./strategy_options";

/** Loops through strategies with optional timeouts.
 *
 * Options:
 * - loop - whether it should loop through the substrategy list
 * - timeout - initial timeout for a single substrategy
 * - timeoutLimit - maximum timeout
 * - retryDelay - base delay before retrying a substrategy that failed before
 *   its timeout; doubles per attempt with jitter. Without it, a fast failure
 *   waits out the full timeout.
 * - retryDelayLimit - maximum retryDelay
 *
 * @param {Strategy[]} strategies
 * @param {Object} options
 */
export default class SequentialStrategy implements Strategy {
  strategies: Strategy[];
  loop: boolean;
  failFast: boolean;
  timeout: number;
  timeoutLimit: number;
  retryDelay: number;
  retryDelayLimit: number;

  constructor(strategies: Strategy[], options: StrategyOptions) {
    this.strategies = strategies;
    this.loop = Boolean(options.loop);
    this.failFast = Boolean(options.failFast);
    this.timeout = options.timeout;
    this.timeoutLimit = options.timeoutLimit;
    this.retryDelay = options.retryDelay;
    this.retryDelayLimit = options.retryDelayLimit;
  }

  isSupported(): boolean {
    return Collections.any(this.strategies, Util.method("isSupported"));
  }

  connect(minPriority: number, callback: (...args: any[]) => any) {
    const strategies = this.strategies;
    let current = 0;
    let timeout = this.timeout;
    let runner = null;
    let attempts = 0;

    const nextRetryDelay = () => {
      if (!this.retryDelay) {
        return 0;
      }
      const delay = Math.min(
        this.retryDelay * 2 ** attempts,
        this.retryDelayLimit || Number.POSITIVE_INFINITY,
      );
      attempts += 1;
      // Equal jitter keeps clients dropped together from retrying in lockstep.
      const half = Math.floor(delay / 2);
      return delay - half + Runtime.randomInt(half + 1);
    };

    const tryNextStrategy = (error, handshake) => {
      if (handshake) {
        callback(null, handshake);
      } else {
        current = current + 1;
        if (this.loop) {
          current = current % strategies.length;
        }

        if (current < strategies.length) {
          if (timeout) {
            timeout = timeout * 2;
            if (this.timeoutLimit) {
              timeout = Math.min(timeout, this.timeoutLimit);
            }
          }
          runner = this.tryStrategy(
            strategies[current],
            minPriority,
            { timeout, failFast: this.failFast, retryDelay: nextRetryDelay() },
            tryNextStrategy,
          );
        } else {
          callback(true);
        }
      }
    };

    runner = this.tryStrategy(
      strategies[current],
      minPriority,
      { timeout: timeout, failFast: this.failFast, retryDelay: nextRetryDelay() },
      tryNextStrategy,
    );

    return {
      abort: function () {
        runner.abort();
      },
      forceMinPriority: function (p) {
        minPriority = p;
        if (runner) {
          runner.forceMinPriority(p);
        }
      },
    };
  }

  private tryStrategy(
    strategy: Strategy,
    minPriority: number,
    options: StrategyOptions,
    callback: (...args: any[]) => any,
  ) {
    let timer = null;
    let runner = null;

    if (options.timeout > 0) {
      timer = new Timer(options.timeout, function () {
        runner.abort();
        callback(true);
      });
    }

    runner = strategy.connect(minPriority, function (error, handshake) {
      if (error && timer && timer.isRunning() && !options.failFast) {
        if (options.retryDelay > 0) {
          // retry after a short backoff instead of waiting out the timeout
          timer.ensureAborted();
          timer = new Timer(options.retryDelay, function () {
            callback(error);
          });
        }
        // otherwise advance to the next strategy after the timeout
        return;
      }
      if (timer) {
        timer.ensureAborted();
      }
      callback(error, handshake);
    });

    return {
      abort: function () {
        if (timer) {
          timer.ensureAborted();
        }
        runner.abort();
      },
      forceMinPriority: function (p) {
        runner.forceMinPriority(p);
      },
    };
  }
}
