import * as Collections from "core/utils/collections";
import Transports from "isomorphic/transports/transports";
import Ajax from "core/http/ajax";
import getDefaultStrategy from "./default_strategy";
import TransportsTable from "core/transports/transports_table";
import transportConnectionInitializer from "./transports/transport_connection_initializer";
import HTTPFactory from "./http/http";

const Isomorphic: any = {
  getDefaultStrategy,
  Transports: <TransportsTable>Transports,
  transportConnectionInitializer,
  HTTPFactory,

  setup(SockudoClass): void {
    SockudoClass.ready();
  },

  getLocalStorage(): any {
    return undefined;
  },

  getClientFeatures(): any[] {
    return Collections.keys(
      Collections.filterObject({ ws: Transports.ws }, function (t) {
        return t.isSupported({});
      }),
    );
  },

  getProtocol(): string {
    return "http:";
  },

  isXHRSupported(): boolean {
    return true;
  },

  createXHR(): Ajax {
    const Constructor = this.getXHRAPI();
    return new Constructor();
  },

  createWebSocket(url: string): any {
    const Constructor = this.getWebSocketAPI();
    return new Constructor(url);
  },

  addUnloadListener(_listener: any) {},
  removeUnloadListener(_listener: any) {},
};

export default Isomorphic;
