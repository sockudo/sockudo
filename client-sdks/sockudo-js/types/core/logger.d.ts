export interface LoggerConfig {
    log?: (message: string) => void;
    logToConsole?: boolean;
}
export declare function setLoggerConfig(newConfig: LoggerConfig): void;
declare class Logger {
    debug(...args: any[]): void;
    warn(...args: any[]): void;
    error(...args: any[]): void;
    private globalLog;
    private globalLogWarn;
    private globalLogError;
    private log;
}
declare const _default: Logger;
export default _default;
