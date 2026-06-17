import Browser from "./browser";
import { AuthTransports } from "core/auth/auth_transports";
import fetchAuth from "isomorphic/auth/fetch_auth";
import fetchTimeline from "isomorphic/timeline/fetch_timeline";
import Transports from "./transports/transports";
import Ajax from "core/http/ajax";
import { Network } from "./net_info";
import getDefaultStrategy from "./default_strategy";
import transportConnectionInitializer from "isomorphic/transports/transport_connection_initializer";
import HTTPFactory from "./http/http";

const Runtime: Browser = {
  getDefaultStrategy,
  Transports,
  transportConnectionInitializer,
  HTTPFactory,

  TimelineTransport: fetchTimeline,

  getXHRAPI() {
    return window.XMLHttpRequest;
  },

  getWebSocketAPI() {
    return window.WebSocket || window.MozWebSocket;
  },

  setup(SockudoClass): void {
    (window as any).Sockudo = SockudoClass;
    this.onDocumentBody(SockudoClass.ready);
  },

  getDocument(): Document {
    return document;
  },

  getProtocol(): string {
    return this.getDocument().location.protocol;
  },

  getAuthorizers(): AuthTransports {
    return { fetch: fetchAuth, ajax: fetchAuth };
  },

  onDocumentBody(callback: (...args: any[]) => any) {
    if (document.body) {
      callback();
      return;
    }

    setTimeout(() => {
      this.onDocumentBody(callback);
    }, 0);
  },

  getLocalStorage() {
    try {
      return window.localStorage;
    } catch {
      return undefined;
    }
  },

  createXHR(): Ajax {
    return this.createXMLHttpRequest();
  },

  createXMLHttpRequest(): Ajax {
    const Constructor = this.getXHRAPI();
    if (!Constructor) {
      throw new Error("XMLHttpRequest is not available in this environment.");
    }
    return new Constructor();
  },

  getNetwork() {
    return Network;
  },

  createWebSocket(url: string): any {
    const Constructor = this.getWebSocketAPI();
    return new Constructor(url);
  },

  isXHRSupported(): boolean {
    const Constructor = this.getXHRAPI();
    return (
      Boolean(Constructor) && new Constructor().withCredentials !== undefined
    );
  },

  addUnloadListener(listener: any) {
    if (window.addEventListener !== undefined) {
      window.addEventListener("unload", listener, false);
    } else if (window.attachEvent !== undefined) {
      window.attachEvent("onunload", listener);
    }
  },

  removeUnloadListener(listener: any) {
    if (window.addEventListener !== undefined) {
      window.removeEventListener("unload", listener, false);
    } else if (window.detachEvent !== undefined) {
      window.detachEvent("onunload", listener);
    }
  },

  randomInt(max: number): number {
    if (!Number.isFinite(max) || max <= 0) {
      return 0;
    }

    const crypto = window.crypto || window["msCrypto"];
    if (crypto?.getRandomValues) {
      const random = crypto.getRandomValues(new Uint32Array(1))[0];
      return Math.floor((random / 2 ** 32) * max);
    }

    return Math.floor(Math.random() * max);
  },
};

export default Runtime;
