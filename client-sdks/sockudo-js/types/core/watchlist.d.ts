import Sockudo from './sockudo';
import EventsDispatcher from './events/dispatcher';
export default class WatchlistFacade extends EventsDispatcher {
    private pusher;
    constructor(pusher: Sockudo);
    handleEvent(pusherEvent: any): void;
    private bindWatchlistInternalEvent;
}
