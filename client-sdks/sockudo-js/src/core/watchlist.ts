import Logger from "./logger";
import Sockudo from "./sockudo";
import EventsDispatcher from "./events/dispatcher";
import { prefixedInternal } from "./protocol_prefix";

export default class WatchlistFacade extends EventsDispatcher {
  private sockudo: Sockudo;

  public constructor(sockudo: Sockudo) {
    super(function (eventName, _data) {
      Logger.debug(`No callbacks on watchlist events for ${eventName}`);
    });

    this.sockudo = sockudo;
    this.bindWatchlistInternalEvent();
  }

  handleEvent(sockudoEvent) {
    sockudoEvent.data.events.forEach((watchlistEvent) => {
      this.emit(watchlistEvent.name, watchlistEvent);
    });
  }

  private bindWatchlistInternalEvent() {
    this.sockudo.connection.bind("message", (sockudoEvent) => {
      const eventName = sockudoEvent.event;
      if (eventName === prefixedInternal("watchlist_events")) {
        this.handleEvent(sockudoEvent);
      }
    });
  }
}
