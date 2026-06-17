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
  getLocalStorage,
  createWebSocket,
  addUnloadListener,
  removeUnloadListener,
  transportConnectionInitializer,
  HTTPFactory,
} = Isomorphic;

const NodeJS: Runtime = {
  getDefaultStrategy,
  Transports,
  setup,
  getProtocol,
  getLocalStorage,
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
    return globalThis.WebSocket as unknown as new (url: string) => any;
  },

  getXHRAPI() {
    return (globalThis as any).XMLHttpRequest;
  },

  getNetwork() {
    return Network;
  },

  createXHR() {
    throw new Error(
      "XMLHttpRequest is not available in the node runtime. Use fetch-based auth/timeline or provide a custom authorizer.",
    );
  },

  isXHRSupported(): boolean {
    return false;
  },

  randomInt(max: number): number {
    if (!Number.isFinite(max) || max <= 0) {
      return 0;
    }

    const crypto = globalThis.crypto;
    if (crypto?.getRandomValues) {
      const random = crypto.getRandomValues(new Uint32Array(1))[0];
      return Math.floor((random / 2 ** 32) * max);
    }

    return Math.floor(Math.random() * max);
  },
};

export default NodeJS;
