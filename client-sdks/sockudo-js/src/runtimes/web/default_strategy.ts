import TransportManager from "core/transports/transport_manager";
import Strategy from "core/strategies/strategy";
import StrategyOptions from "core/strategies/strategy_options";
import SequentialStrategy from "core/strategies/sequential_strategy";
import BestConnectedEverStrategy from "core/strategies/best_connected_ever_strategy";
import WebSocketPrioritizedCachedStrategy, {
  TransportStrategyDictionary,
} from "core/strategies/websocket_prioritized_cached_strategy";
import DelayedStrategy from "core/strategies/delayed_strategy";
import FirstConnectedStrategy from "core/strategies/first_connected_strategy";
import { Config } from "core/config";

const getDefaultStrategy = (
  config: Config,
  baseOptions: StrategyOptions,
  defineTransport: (...args: any[]) => any,
): Strategy => {
  const definedTransports = <TransportStrategyDictionary>{};

  const defineTransportStrategy = (
    name: string,
    type: string,
    priority: number,
    options: StrategyOptions,
    manager?: TransportManager,
  ) => {
    const transport = defineTransport(
      config,
      name,
      type,
      priority,
      options,
      manager,
    );

    definedTransports[name] = transport;
    return transport;
  };

  const wsOptions: StrategyOptions = Object.assign({}, baseOptions, {
    hostNonTLS: `${config.wsHost}:${config.wsPort}`,
    hostTLS: `${config.wsHost}:${config.wssPort}`,
    httpPath: config.wsPath,
    echoMessages: config.echoMessages,
    wireFormat: config.wireFormat,
  });
  const wssOptions: StrategyOptions = Object.assign({}, wsOptions, {
    useTLS: true,
  });

  const timeouts = {
    loop: true,
    timeout: 15000,
    timeoutLimit: 60000,
  };

  const wsManager = new TransportManager({
    minPingDelay: 10000,
    maxPingDelay: config.activityTimeout,
  });

  const wsTransport = defineTransportStrategy(
    "ws",
    "ws",
    3,
    wsOptions,
    wsManager,
  );
  const wssTransport = defineTransportStrategy(
    "wss",
    "ws",
    3,
    wssOptions,
    wsManager,
  );

  const wsLoop = new SequentialStrategy([wsTransport], timeouts);
  const wssLoop = new SequentialStrategy([wssTransport], timeouts);

  const wsStrategy: Strategy = baseOptions.useTLS
    ? wsLoop
    : new BestConnectedEverStrategy([
        wsLoop,
        new DelayedStrategy(wssLoop, { delay: 2000 }),
      ]);

  return new WebSocketPrioritizedCachedStrategy(
    new FirstConnectedStrategy(wsStrategy),
    definedTransports,
    {
      ttl: 1800000,
      timeline: baseOptions.timeline,
      useTLS: baseOptions.useTLS,
    },
  );
};

export default getDefaultStrategy;
