import Timeline from '../timeline/timeline';
import { AppendMode } from '../options';
interface StrategyOptions {
    echoMessages?: boolean;
    failFast?: boolean;
    hostNonTLS?: string;
    hostTLS?: string;
    httpPath?: string;
    ignoreNullOrigin?: boolean;
    key?: string;
    loop?: boolean;
    timeline?: Timeline;
    timeout?: number;
    timeoutLimit?: number;
    ttl?: number;
    useTLS?: boolean;
    appendMode?: AppendMode;
}
export default StrategyOptions;
