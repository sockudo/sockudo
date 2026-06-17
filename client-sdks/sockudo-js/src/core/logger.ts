import { stringify } from "./utils/collections";

// Logger configuration - can be set by Sockudo class
export interface LoggerConfig {
  log?: (message: string) => void;
  logToConsole?: boolean;
}

let config: LoggerConfig = {
  logToConsole: false,
};

export function setLoggerConfig(newConfig: LoggerConfig) {
  config = { ...config, ...newConfig };
}

class Logger {
  debug(...args: any[]) {
    this.log(this.globalLog, args);
  }

  warn(...args: any[]) {
    this.log(this.globalLogWarn, args);
  }

  error(...args: any[]) {
    this.log(this.globalLogError, args);
  }

  private globalLog = (message: string) => {
    if (global.console && global.console.log) {
      global.console.log(message);
    }
  };

  private globalLogWarn(message: string) {
    if (global.console && global.console.warn) {
      global.console.warn(message);
    } else {
      this.globalLog(message);
    }
  }

  private globalLogError(message: string) {
    if (global.console && global.console.error) {
      global.console.error(message);
    } else {
      this.globalLogWarn(message);
    }
  }

  private log(
    defaultLoggingFunction: (message: string) => void,
    ..._args: any[]
  ) {
    const message = stringify.apply(this, arguments);
    if (config.log) {
      config.log(message);
    } else if (config.logToConsole) {
      const log = defaultLoggingFunction.bind(this);
      log(message);
    }
  }
}

export default new Logger();
