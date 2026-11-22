import PrivateChannel from './private_channel';
import Sockudo from '../sockudo';
import { SockudoEvent } from '../connection/protocol/message-types';
import { ChannelAuthorizationCallback } from '../auth/options';
import * as nacl from 'tweetnacl';
export default class EncryptedChannel extends PrivateChannel {
    key: Uint8Array;
    nacl: nacl;
    constructor(name: string, pusher: Sockudo, nacl: nacl);
    authorize(socketId: string, callback: ChannelAuthorizationCallback): void;
    trigger(event: string, data: any): boolean;
    handleEvent(event: SockudoEvent): void;
    private handleEncryptedEvent;
    getDataToEmit(bytes: Uint8Array): string;
}
