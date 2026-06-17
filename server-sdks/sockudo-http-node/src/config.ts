import isBase64 from "is-base64";
import Token = require("./token");
import type { BaseOptions } from "./types";

class Config {
  scheme: string;
  port?: string | number;
  appId: string | number;
  token: Token;
  timeout?: number;
  agent?: BaseOptions["agent"];
  encryptionMasterKey?: Buffer;
  host!: string;

  constructor(options: BaseOptions = { appId: "", key: "", secret: "" }) {
    let useTLS = false;
    if (options.useTLS !== undefined && options.encrypted !== undefined) {
      throw new Error(
        "Cannot set both `useTLS` and `encrypted` configuration options",
      );
    } else if (options.useTLS !== undefined) {
      useTLS = options.useTLS;
    } else if (options.encrypted !== undefined) {
      console.warn("`encrypted` option is deprecated in favor of `useTLS`");
      useTLS = options.encrypted;
    }

    this.scheme = options.scheme || (useTLS ? "https" : "http");
    this.port = options.port;
    this.appId = options.appId;
    this.token = new Token(options.key, options.secret);
    this.timeout = options.timeout;
    this.agent = options.agent;

    if (options.encryptionMasterKey !== undefined) {
      if (options.encryptionMasterKeyBase64 !== undefined) {
        throw new Error(
          "Do not specify both encryptionMasterKey and encryptionMasterKeyBase64. encryptionMasterKey is deprecated, please specify only encryptionMasterKeyBase64.",
        );
      }
      console.warn(
        "`encryptionMasterKey` option is deprecated in favor of `encryptionMasterKeyBase64`",
      );
      if (typeof options.encryptionMasterKey !== "string") {
        throw new Error("encryptionMasterKey must be a string");
      }
      if (options.encryptionMasterKey.length !== 32) {
        throw new Error(
          `encryptionMasterKey must be 32 bytes long, but the string '${options.encryptionMasterKey}' is ${options.encryptionMasterKey.length} bytes long`,
        );
      }

      this.encryptionMasterKey = Buffer.from(options.encryptionMasterKey);
    }

    if (options.encryptionMasterKeyBase64 !== undefined) {
      if (typeof options.encryptionMasterKeyBase64 !== "string") {
        throw new Error("encryptionMasterKeyBase64 must be a string");
      }
      if (!isBase64(options.encryptionMasterKeyBase64)) {
        throw new Error("encryptionMasterKeyBase64 must be valid base64");
      }

      const decodedKey = Buffer.from(
        options.encryptionMasterKeyBase64,
        "base64",
      );
      if (decodedKey.length !== 32) {
        throw new Error(
          `encryptionMasterKeyBase64 must decode to 32 bytes, but the string '${options.encryptionMasterKeyBase64}' decodes to ${decodedKey.length} bytes`,
        );
      }

      this.encryptionMasterKey = decodedKey;
    }
  }

  prefixPath(): string {
    throw new Error(
      "NotImplementedError: #prefixPath should be implemented by subclasses",
    );
  }

  getBaseURL(): string {
    const port = this.port ? `:${this.port}` : "";
    return `${this.scheme}://${this.host}${port}`;
  }
}

export = Config;
