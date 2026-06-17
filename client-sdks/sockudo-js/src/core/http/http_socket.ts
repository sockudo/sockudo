import URLLocation from "./url_location";
import State from "./state";
import Socket from "../socket";
import SocketHooks from "./socket_hooks";
import Util from "../util";
import HTTPRequest from "./http_request";

let autoIncrement = 1;

type RequestCreator = (method: string, url: string) => HTTPRequest;

class HTTPSocket implements Socket {
  hooks: SocketHooks;
  session: string;
  location: URLLocation;
  readyState: State;
  stream: HTTPRequest;
  createRequest: RequestCreator;

  onopen: () => void;
  onerror: (error: any) => void;
  onclose: (closeEvent: any) => void;
  onmessage: (message: any) => void;
  onactivity: () => void;

  constructor(hooks: SocketHooks, url: string, createRequest: RequestCreator) {
    this.hooks = hooks;
    this.createRequest = createRequest;
    this.session = randomNumber(1000) + "/" + randomString(8);
    this.location = getLocation(url);
    this.readyState = State.CONNECTING;
    this.openStream();
  }

  send(payload: any) {
    return this.sendRaw(JSON.stringify([payload]));
  }

  ping() {
    this.hooks.sendHeartbeat(this);
  }

  close(code: any, reason: any) {
    this.onClose(code, reason, true);
  }

  /** For internal use only */
  sendRaw(payload: any): boolean {
    if (this.readyState === State.OPEN) {
      try {
        this.createRequest(
          "POST",
          getUniqueURL(getSendURL(this.location, this.session)),
        ).start(payload);
        return true;
      } catch {
        return false;
      }
    } else {
      return false;
    }
  }

  /** For internal use only */
  reconnect() {
    this.closeStream();
    this.openStream();
  }

  /** For internal use only */
  onClose(code, reason, wasClean) {
    this.closeStream();
    this.readyState = State.CLOSED;
    if (this.onclose) {
      this.onclose({
        code: code,
        reason: reason,
        wasClean: wasClean,
      });
    }
  }

  private onChunk(chunk) {
    if (chunk.status !== 200) {
      return;
    }
    if (this.readyState === State.OPEN) {
      this.onActivity();
    }

    let payload;
    const type = chunk.data.slice(0, 1);
    switch (type) {
      case "o":
        payload = JSON.parse(chunk.data.slice(1) || "{}");
        this.onOpen(payload);
        break;
      case "a":
        payload = JSON.parse(chunk.data.slice(1) || "[]");
        for (let i = 0; i < payload.length; i++) {
          this.onEvent(payload[i]);
        }
        break;
      case "m":
        payload = JSON.parse(chunk.data.slice(1) || "null");
        this.onEvent(payload);
        break;
      case "h":
        this.hooks.onHeartbeat(this);
        break;
      case "c":
        payload = JSON.parse(chunk.data.slice(1) || "[]");
        this.onClose(payload[0], payload[1], true);
        break;
    }
  }

  private onOpen(options) {
    if (this.readyState === State.CONNECTING) {
      if (options && options.hostname) {
        this.location.base = replaceHost(this.location.base, options.hostname);
      }
      this.readyState = State.OPEN;

      if (this.onopen) {
        this.onopen();
      }
    } else {
      this.onClose(1006, "Server lost session", true);
    }
  }

  private onEvent(event) {
    if (this.readyState === State.OPEN && this.onmessage) {
      this.onmessage({ data: event });
    }
  }

  private onActivity() {
    if (this.onactivity) {
      this.onactivity();
    }
  }

  private onError(error) {
    if (this.onerror) {
      this.onerror(error);
    }
  }

  private openStream() {
    this.stream = this.createRequest(
      "POST",
      getUniqueURL(this.hooks.getReceiveURL(this.location, this.session)),
    );

    this.stream.bind("chunk", (chunk) => {
      this.onChunk(chunk);
    });
    this.stream.bind("finished", (status) => {
      this.hooks.onFinished(this, status);
    });
    this.stream.bind("buffer_too_long", () => {
      this.reconnect();
    });

    try {
      this.stream.start();
    } catch (error) {
      Util.defer(() => {
        this.onError(error);
        this.onClose(1006, "Could not start streaming", false);
      });
    }
  }

  private closeStream() {
    if (this.stream) {
      this.stream.unbind_all();
      this.stream.close();
      this.stream = null;
    }
  }
}

function getLocation(url): URLLocation {
  const parts = /([^?]*)\/*(\??.*)/.exec(url);
  return {
    base: parts[1],
    queryString: parts[2],
  };
}

function getSendURL(url: URLLocation, session: string): string {
  return url.base + "/" + session + "/xhr_send";
}

function getUniqueURL(url: string): string {
  const separator = url.indexOf("?") === -1 ? "?" : "&";
  return url + separator + "t=" + +new Date() + "&n=" + autoIncrement++;
}

function replaceHost(url: string, hostname: string): string {
  const urlParts = /(https?:\/\/)([^/:]+)((\/|:)?.*)/.exec(url);
  return urlParts[1] + hostname + urlParts[3];
}

function randomNumber(max: number): number {
  if (!Number.isFinite(max) || max <= 0) {
    return 0;
  }

  const crypto = globalThis.crypto as
    | { getRandomValues: (arr: Uint32Array) => Uint32Array }
    | undefined;
  if (crypto?.getRandomValues) {
    const random = crypto.getRandomValues(new Uint32Array(1))[0];
    return Math.floor((random / 2 ** 32) * max);
  }

  return Math.floor(Math.random() * max);
}

function randomString(length: number): string {
  const result = [];

  for (let i = 0; i < length; i++) {
    result.push(randomNumber(32).toString(32));
  }

  return result.join("");
}

export default HTTPSocket;
