import PrivateChannel from './private_channel';
import Members from './members';
import Sockudo from '../sockudo';
import { SockudoEvent } from '../connection/protocol/message-types';
export default class PresenceChannel extends PrivateChannel {
    members: Members;
    constructor(name: string, pusher: Sockudo);
    authorize(socketId: string, callback: Function): void;
    handleEvent(event: SockudoEvent): void;
    handleInternalEvent(event: SockudoEvent): void;
    handleSubscriptionSucceededEvent(event: SockudoEvent): void;
    disconnect(): void;
}
