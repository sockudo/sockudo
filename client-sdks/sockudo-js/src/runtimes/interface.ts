import type { AuthTransports } from "../core/auth/auth_transports";
import type TimelineTransport from "../core/timeline/timeline_transport";
import type Ajax from "../core/http/ajax";
import type Reachability from "../core/reachability";
import type TransportsTable from "../core/transports/transports_table";
import type Socket from "../core/socket";
import type HTTPFactory from "../core/http/http_factory";
import type Sockudo from "../core/sockudo";
import type Strategy from "../core/strategies/strategy";
import type { Config } from "../core/config";
import type StrategyOptions from "../core/strategies/strategy_options";

/*
This interface is implemented in web/runtime, node/runtime, react-native/runtime
and worker/runtime. Its job is to be the only point of contact to platform-specific
code for the core library. When the core library imports "runtime", the bundler will
look for src/runtimes/<platform>/runtime.ts. This is how the Sockudo client
keeps core and platform-specific code separate.
*/
export default interface Runtime {
  setup(SockudoClass: {
    new (key: string, options: unknown): Sockudo;
    ready(): void;
  }): void;
  getProtocol(): string;
  getAuthorizers(): AuthTransports;
  getLocalStorage(): any;
  TimelineTransport: TimelineTransport;
  createXHR(): Ajax;
  createWebSocket(url: string): Socket;
  getNetwork(): Reachability;
  getDefaultStrategy(
    config: Config,
    options: StrategyOptions,
    defineTransport: (...args: any[]) => any,
  ): Strategy;
  Transports: TransportsTable;
  getWebSocketAPI(): new (url: string) => Socket;
  getXHRAPI(): new () => Ajax;
  addUnloadListener(listener: (...args: any[]) => any): void;
  removeUnloadListener(listener: (...args: any[]) => any): void;
  transportConnectionInitializer: (...args: any[]) => any;
  HTTPFactory: HTTPFactory;
  isXHRSupported(): boolean;
  randomInt(max: number): number;

  // these methods/types are only implemented in the web Runtime, so they're
  // optional but must be included in the interface
  getDocument?(): Document;
}
