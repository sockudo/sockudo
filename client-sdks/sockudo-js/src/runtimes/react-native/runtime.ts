import Isomorphic from "isomorphic/runtime";
import Runtime from "../interface";
import { Network } from "./net_info";
import fetchAuth from "isomorphic/auth/fetch_auth";
import { AuthTransports } from "core/auth/auth_transports";
import fetchTimeline from "isomorphic/timeline/fetch_timeline";

const {
  getDefaultStrategy,
  Transports,
  setup,
  getProtocol,
  isXHRSupported,
  getLocalStorage,
  createXHR,
  createWebSocket,
  addUnloadListener,
  removeUnloadListener,
  transportConnectionInitializer,
  HTTPFactory,
} = Isomorphic;

const ReactNative: Runtime = {
  getDefaultStrategy,
  Transports,
  setup,
  getProtocol,
  isXHRSupported,
  getLocalStorage,
  createXHR,
  createWebSocket,
  addUnloadListener,
  removeUnloadListener,
  transportConnectionInitializer,
  HTTPFactory,

  TimelineTransport: fetchTimeline,

  getAuthorizers(): AuthTransports {
    return { fetch: fetchAuth, ajax: fetchAuth };
  },

  getWebSocketAPI() {
    return WebSocket;
  },

  getXHRAPI() {
    return XMLHttpRequest;
  },

  getNetwork() {
    return Network;
  },

  randomInt(max: number): number {
    return Math.floor(Math.random() * max);
  },
};

export default ReactNative;
