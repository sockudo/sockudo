import Config = require("./config");
import type { Options } from "./types";

class SockudoConfig extends Config {
  constructor(options: Options) {
    super(options);

    if ("host" in options && options.host) {
      this.host = options.host;
    } else if ("cluster" in options && options.cluster) {
      this.host = `api-${options.cluster}.sockudo.com`;
    } else {
      this.host = "localhost";
    }
  }

  prefixPath(subPath = ""): string {
    return `/apps/${this.appId}${subPath}`;
  }
}

export = SockudoConfig;
