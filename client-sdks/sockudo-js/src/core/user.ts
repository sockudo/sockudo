import Sockudo from "./sockudo";
import Logger from "./logger";
import {
  UserAuthenticationData,
  UserAuthenticationCallback,
} from "./auth/options";
import Channel from "./channels/channel";
import WatchlistFacade from "./watchlist";
import EventsDispatcher from "./events/dispatcher";
import flatPromise from "./utils/flat_promise";
import {
  prefixedEvent,
  isInternalEvent,
  isPlatformEvent,
} from "./protocol_prefix";

export default class UserFacade extends EventsDispatcher {
  sockudo: Sockudo;
  signin_requested: boolean = false;
  user_data: any = null;
  serverToUserChannel: Channel = null;
  signinDonePromise: Promise<any> = null;
  watchlist: WatchlistFacade;
  private _signinDoneResolve: (...args: any[]) => any = null;

  public constructor(sockudo: Sockudo) {
    super(function (eventName, _data) {
      Logger.debug("No callbacks on user for " + eventName);
    });
    this.sockudo = sockudo;
    this.sockudo.connection.bind("state_change", ({ previous, current }) => {
      if (previous !== "connected" && current === "connected") {
        this._signin();
      }
      if (previous === "connected" && current !== "connected") {
        this._cleanup();
        this._newSigninPromiseIfNeeded();
      }
    });

    this.watchlist = new WatchlistFacade(sockudo);

    this.sockudo.connection.bind("message", (event) => {
      const eventName = event.event;
      if (eventName === prefixedEvent("signin_success")) {
        this._onSigninSuccess(event.data);
      }
      if (
        this.serverToUserChannel &&
        this.serverToUserChannel.name === event.channel
      ) {
        this.serverToUserChannel.handleEvent(event);
      }
    });
  }

  public signin() {
    if (this.signin_requested) {
      return;
    }

    this.signin_requested = true;
    this._signin();
  }

  private _signin() {
    if (!this.signin_requested) {
      return;
    }

    this._newSigninPromiseIfNeeded();

    if (this.sockudo.connection.state !== "connected") {
      // Signin will be attempted when the connection is connected
      return;
    }

    this.sockudo.config.userAuthenticator(
      {
        socketId: this.sockudo.connection.socket_id,
      },
      this._onAuthorize,
    );
  }

  private _onAuthorize: UserAuthenticationCallback = (
    err,
    authData: UserAuthenticationData,
  ) => {
    if (err) {
      Logger.warn(`Error during signin: ${err}`);
      this._cleanup();
      return;
    }

    this.sockudo.send_event(prefixedEvent("signin"), {
      auth: authData.auth,
      user_data: authData.user_data,
    });

    // Later when we get signin_success event, the user will be marked as signed in
  };

  private _onSigninSuccess(data: any) {
    try {
      this.user_data = JSON.parse(data.user_data);
    } catch {
      Logger.error(`Failed parsing user data after signin: ${data.user_data}`);
      this._cleanup();
      return;
    }

    if (typeof this.user_data.id !== "string" || this.user_data.id === "") {
      Logger.error(
        `user_data doesn't contain an id. user_data: ${this.user_data}`,
      );
      this._cleanup();
      return;
    }

    // Signin succeeded
    this._signinDoneResolve();
    this._subscribeChannels();
  }

  private _subscribeChannels() {
    const ensure_subscribed = (channel) => {
      if (channel.subscriptionPending && channel.subscriptionCancelled) {
        channel.reinstateSubscription();
      } else if (
        !channel.subscriptionPending &&
        this.sockudo.connection.state === "connected"
      ) {
        channel.subscribe();
      }
    };

    this.serverToUserChannel = new Channel(
      `#server-to-user-${this.user_data.id}`,
      this.sockudo,
    );
    this.serverToUserChannel.bind_global((eventName, data) => {
      if (isInternalEvent(eventName) || isPlatformEvent(eventName)) {
        // ignore internal events
        return;
      }
      this.emit(eventName, data);
    });
    ensure_subscribed(this.serverToUserChannel);
  }

  private _cleanup() {
    this.user_data = null;
    if (this.serverToUserChannel) {
      this.serverToUserChannel.unbind_all();
      this.serverToUserChannel.disconnect();
      this.serverToUserChannel = null;
    }

    if (this.signin_requested) {
      // If signin is in progress and cleanup is called,
      // Mark the current signin process as done.
      this._signinDoneResolve();
    }
  }

  private _newSigninPromiseIfNeeded() {
    if (!this.signin_requested) {
      return;
    }

    // If there is a promise and it is not resolved, return without creating a new one.
    if (this.signinDonePromise && !(this.signinDonePromise as any).done) {
      return;
    }

    // This promise is never rejected.
    // It gets resolved when the signin process is done whether it failed or succeeded
    const { promise, resolve, reject: _ } = flatPromise();
    (promise as any).done = false;
    const setDone = () => {
      (promise as any).done = true;
    };
    promise.then(setDone).catch(setDone);
    this.signinDonePromise = promise;
    this._signinDoneResolve = resolve;
  }
}
