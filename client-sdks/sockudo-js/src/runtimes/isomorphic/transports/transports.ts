import * as URLSchemes from "core/transports/url_schemes";
import Transport from "core/transports/transport";
import TransportHooks from "core/transports/transport_hooks";
import TransportsTable from "core/transports/transports_table";

type WebSocketCtor = new (url: string) => WebSocket;

const getWebSocketConstructor = (): WebSocketCtor | undefined => {
  return globalThis.WebSocket as unknown as WebSocketCtor | undefined;
};

/** WebSocket transport using the native runtime WebSocket implementation. */
const WSTransport = new Transport(<TransportHooks>{
  urls: URLSchemes.ws,
  handlesActivityChecks: false,
  supportsPing: false,

  isInitialized: function () {
    return Boolean(getWebSocketConstructor());
  },
  isSupported: function (): boolean {
    return Boolean(getWebSocketConstructor());
  },
  getSocket: function (url, options) {
    const Constructor = getWebSocketConstructor();
    if (!Constructor) {
      throw new Error("WebSocket is not available in this environment.");
    }
    const socket = new Constructor(url);
    if (options?.wireFormat && options.wireFormat !== "json") {
      (socket as any).binaryType = "arraybuffer";
    }
    return socket;
  },
});

const Transports: TransportsTable = {
  ws: WSTransport,
};

export default Transports;
