import Isomorphic from "isomorphic/runtime";
import Runtime from "../interface";
import { Network } from "../node/net_info";
import fetchAuth from "isomorphic/auth/fetch_auth";
import { AuthTransports } from "core/auth/auth_transports";
import fetchTimeline from "isomorphic/timeline/fetch_timeline";

const {
  getDefaultStrategy,
  Transports,
  setup,
  getProtocol,
  getLocalStorage,
  addUnloadListener,
  removeUnloadListener,
  transportConnectionInitializer,
  HTTPFactory,
} = Isomorphic;

const NativeScript: Runtime = {
  getDefaultStrategy,
  Transports,
  setup,
  getProtocol,
  getLocalStorage,
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
    const Constructor = this.getXHRAPI();
    if (!Constructor) {
      throw new Error(
        "XMLHttpRequest is not available in the NativeScript runtime.",
      );
    }

    return new Constructor();
  },

  createWebSocket(url: string) {
    const Constructor = this.getWebSocketAPI();
    if (!Constructor) {
      throw new Error(
        "WebSocket is not available in the NativeScript runtime. Import '@valor/nativescript-websockets' before initializing Sockudo.",
      );
    }

    return new Constructor(url);
  },

  isXHRSupported(): boolean {
    return typeof this.getXHRAPI() === "function";
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

export default NativeScript;
