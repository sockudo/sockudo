import crypto from "crypto";
import { secureCompare } from "./util";

class Token {
  key: string;
  secret: string;

  constructor(key: string, secret: string) {
    this.key = key;
    this.secret = secret;
  }

  sign(value: string): string {
    return crypto
      .createHmac("sha256", this.secret)
      .update(Buffer.from(value))
      .digest("hex");
  }

  verify(value: string, signature: string): boolean {
    return secureCompare(this.sign(value), signature);
  }
}

export = Token;
