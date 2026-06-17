import { WebHookError } from "./errors";
import Token = require("./token");
import type { TokenShape, WebHookData, WebHookRequest } from "./types";

class WebHook {
  token: Token;
  key: string | undefined;
  signature: string | undefined;
  contentType: string | undefined;
  body: string;
  data?: WebHookData;

  constructor(token: Token, request: WebHookRequest) {
    this.token = token;
    this.key = request.headers["x-pusher-key"];
    this.signature = request.headers["x-pusher-signature"];
    this.contentType = request.headers["content-type"];
    this.body = request.rawBody;

    if (this.isContentTypeValid()) {
      try {
        this.data = JSON.parse(this.body) as WebHookData;
      } catch {
        // ignored
      }
    }
  }

  isValid(extraTokens?: TokenShape | TokenShape[]): boolean {
    if (!this.isBodyValid()) {
      return false;
    }

    const normalized = extraTokens
      ? Array.isArray(extraTokens)
        ? extraTokens
        : [extraTokens]
      : [];

    const tokens: Array<Token | TokenShape> = [this.token, ...normalized];
    for (let token of tokens) {
      let candidate: Token;
      if (token instanceof Token) {
        candidate = token;
      } else {
        candidate = new Token(token.key, token.secret);
      }
      if (this.key === candidate.key && this.signature) {
        if (candidate.verify(this.body, this.signature)) {
          return true;
        }
      }
    }

    return false;
  }

  isContentTypeValid(): boolean {
    return this.contentType === "application/json";
  }

  isBodyValid(): boolean {
    return this.data !== undefined;
  }

  getData(): WebHookData {
    if (!this.isBodyValid()) {
      throw new WebHookError(
        "Invalid WebHook body",
        this.contentType,
        this.body,
        this.signature,
      );
    }
    return this.data!;
  }

  getEvents() {
    return this.getData().events;
  }

  getTime(): Date {
    return new Date(this.getData().time_ms);
  }
}

export = WebHook;
