import Action from './action';
import { SockudoEvent } from './message-types';
declare const Protocol: {
    decodeMessage: (messageEvent: MessageEvent) => SockudoEvent;
    encodeMessage: (event: SockudoEvent) => string;
    processHandshake: (messageEvent: MessageEvent) => Action;
    getCloseAction: (closeEvent: any) => string;
    getCloseError: (closeEvent: any) => any;
};
export default Protocol;
