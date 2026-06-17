import * as Collections from "../utils/collections";
import Util from "../util";
import TransportManager from "../transports/transport_manager";
import * as Errors from "../errors";
import Strategy from "./strategy";
import TransportStrategy from "./transport_strategy";
import StrategyOptions from "../strategies/strategy_options";
import { Config } from "../config";
import Runtime from "runtime";

const { Transports } = Runtime;

export const defineTransport = function (
  config: Config,
  name: string,
  type: string,
  priority: number,
  options: StrategyOptions,
  manager?: TransportManager,
): Strategy {
  const transportClass = Transports[type];
  if (!transportClass) {
    throw new Errors.UnsupportedTransport(type);
  }

  const enabled =
    (!config.enabledTransports ||
      Collections.arrayIndexOf(config.enabledTransports, name) !== -1) &&
    (!config.disabledTransports ||
      Collections.arrayIndexOf(config.disabledTransports, name) === -1);

  let transport;
  if (enabled) {
    options = Object.assign(
      { ignoreNullOrigin: config.ignoreNullOrigin },
      options,
    );

    transport = new TransportStrategy(
      name,
      priority,
      manager ? manager.getAssistant(transportClass) : transportClass,
      options,
    );
  } else {
    transport = UnsupportedStrategy;
  }

  return transport;
};

const UnsupportedStrategy: Strategy = {
  isSupported: function () {
    return false;
  },
  connect: function (_, callback) {
    const deferred = Util.defer(function () {
      callback(new Errors.UnsupportedStrategy());
    });
    return {
      abort: function () {
        deferred.ensureAborted();
      },
      forceMinPriority: function () {},
    };
  },
};
