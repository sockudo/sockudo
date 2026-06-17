import { default as EventsDispatcher } from "core/events/dispatcher";
import Reachability from "core/reachability";

export class NetInfo extends EventsDispatcher implements Reachability {
  isOnline(): boolean {
    return true;
  }
}

export const Network = new NetInfo();
