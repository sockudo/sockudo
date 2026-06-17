import Util from "../util";
import Runtime from "runtime";
import Strategy from "./strategy";
import SequentialStrategy from "./sequential_strategy";
import StrategyOptions from "./strategy_options";
import TransportStrategy from "./transport_strategy";
import Timeline from "../timeline/timeline";
import * as Collections from "../utils/collections";

export interface TransportStrategyDictionary {
  [key: string]: TransportStrategy;
}

/** Caches the last successful transport and, after the first few attempts,
 *  uses the cached transport for subsequent attempts.
 *
 * @param {Strategy} strategy
 * @param {Object} transports
 * @param {Object} options
 */
export default class WebSocketPrioritizedCachedStrategy implements Strategy {
  strategy: Strategy;
  transports: TransportStrategyDictionary;
  ttl: number;
  usingTLS: boolean;
  timeline: Timeline;

  constructor(
    strategy: Strategy,
    transports: TransportStrategyDictionary,
    options: StrategyOptions,
  ) {
    this.strategy = strategy;
    this.transports = transports;
    this.ttl = options.ttl || 1800 * 1000;
    this.usingTLS = options.useTLS;
    this.timeline = options.timeline;
  }

  isSupported(): boolean {
    return this.strategy.isSupported();
  }

  connect(minPriority: number, callback: (...args: any[]) => any) {
    const usingTLS = this.usingTLS;
    const info = fetchTransportCache(usingTLS);
    let cacheSkipCount = info && info.cacheSkipCount ? info.cacheSkipCount : 0;

    const strategies = [this.strategy];
    if (info && info.timestamp + this.ttl >= Util.now()) {
      const transport = this.transports[info.transport];
      if (transport) {
        if (["ws", "wss"].includes(info.transport) || cacheSkipCount > 3) {
          this.timeline.info({
            cached: true,
            transport: info.transport,
            latency: info.latency,
          });
          strategies.push(
            new SequentialStrategy([transport], {
              timeout: info.latency * 2 + 1000,
              failFast: true,
            }),
          );
        } else {
          cacheSkipCount++;
        }
      }
    }

    let startTimestamp = Util.now();
    let runner = strategies
      .pop()
      .connect(minPriority, function cb(error, handshake) {
        if (error) {
          flushTransportCache(usingTLS);
          if (strategies.length > 0) {
            startTimestamp = Util.now();
            runner = strategies.pop().connect(minPriority, cb);
          } else {
            callback(error);
          }
        } else {
          storeTransportCache(
            usingTLS,
            handshake.transport.name,
            Util.now() - startTimestamp,
            cacheSkipCount,
          );
          callback(null, handshake);
        }
      });

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
}

function getTransportCacheKey(usingTLS: boolean): string {
  return "sockudoTransport" + (usingTLS ? "TLS" : "NonTLS");
}

function fetchTransportCache(usingTLS: boolean): any {
  const storage = Runtime.getLocalStorage();
  if (storage) {
    try {
      const serializedCache = storage[getTransportCacheKey(usingTLS)];
      if (serializedCache) {
        return JSON.parse(serializedCache);
      }
    } catch {
      flushTransportCache(usingTLS);
    }
  }
  return null;
}

function storeTransportCache(
  usingTLS: boolean,
  transport: TransportStrategy,
  latency: number,
  cacheSkipCount: number,
) {
  const storage = Runtime.getLocalStorage();
  if (storage) {
    try {
      storage[getTransportCacheKey(usingTLS)] = Collections.safeJSONStringify({
        timestamp: Util.now(),
        transport: transport,
        latency: latency,
        cacheSkipCount: cacheSkipCount,
      });
    } catch {
      // catch over quota exceptions raised by localStorage
    }
  }
}

function flushTransportCache(usingTLS: boolean) {
  const storage = Runtime.getLocalStorage();
  if (storage) {
    try {
      delete storage[getTransportCacheKey(usingTLS)];
    } catch {
      // catch exceptions raised by localStorage
    }
  }
}
