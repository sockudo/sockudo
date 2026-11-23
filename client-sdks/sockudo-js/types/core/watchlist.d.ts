import Sockudo from './sockudo';
import EventsDispatcher from './events/dispatcher';
export default class WatchlistFacade extends EventsDispatcher {
    private sockudo;
    constructor(sockudo: Sockudo);
    handleEvent(sockudoEvent: any): void;
    private bindWatchlistInternalEvent;
}
