import Sockudo from './sockudo';
import Channel from './channels/channel';
import WatchlistFacade from './watchlist';
import EventsDispatcher from './events/dispatcher';
export default class UserFacade extends EventsDispatcher {
    sockudo: Sockudo;
    signin_requested: boolean;
    user_data: any;
    serverToUserChannel: Channel;
    signinDonePromise: Promise<any>;
    watchlist: WatchlistFacade;
    private _signinDoneResolve;
    constructor(sockudo: Sockudo);
    signin(): void;
    private _signin;
    private _onAuthorize;
    private _onSigninSuccess;
    private _subscribeChannels;
    private _cleanup;
    private _newSigninPromiseIfNeeded;
}
