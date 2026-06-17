import AssistantToTheTransportManager from "../transports/assistant_to_the_transport_manager";
import PingDelayOptions from "../transports/ping_delay_options";
import Transport from "../transports/transport";
import TransportManager from "../transports/transport_manager";
import Handshake from "../connection/handshake";
import HandshakePayload from "../connection/handshake/handshake_payload";
import TransportConnection from "../transports/transport_connection";

import Timeline from "../timeline/timeline";
import {
  default as TimelineSender,
  TimelineSenderOptions,
} from "../timeline/timeline_sender";
import PresenceChannel from "../channels/presence_channel";
import PrivateChannel from "../channels/private_channel";
import EncryptedChannel from "../channels/encrypted_channel";
import Channel from "../channels/channel";
import ConnectionManager from "../connection/connection_manager";
import ConnectionManagerOptions from "../connection/connection_manager_options";
import Channels from "../channels/channels";
import Sockudo from "../sockudo";
import * as nacl from "tweetnacl";

const Factory = {
  createChannels(): Channels {
    return new Channels();
  },

  createConnectionManager(
    key: string,
    options: ConnectionManagerOptions,
  ): ConnectionManager {
    return new ConnectionManager(key, options);
  },

  createChannel(name: string, sockudo: Sockudo): Channel {
    return new Channel(name, sockudo);
  },

  createPrivateChannel(name: string, sockudo: Sockudo): PrivateChannel {
    return new PrivateChannel(name, sockudo);
  },

  createPresenceChannel(name: string, sockudo: Sockudo): PresenceChannel {
    return new PresenceChannel(name, sockudo);
  },

  createEncryptedChannel(
    name: string,
    sockudo: Sockudo,
    nacl: nacl,
  ): EncryptedChannel {
    return new EncryptedChannel(name, sockudo, nacl);
  },

  createTimelineSender(timeline: Timeline, options: TimelineSenderOptions) {
    return new TimelineSender(timeline, options);
  },

  createHandshake(
    transport: TransportConnection,
    callback: (result: HandshakePayload) => void,
  ): Handshake {
    return new Handshake(transport, callback);
  },

  createAssistantToTheTransportManager(
    manager: TransportManager,
    transport: Transport,
    options: PingDelayOptions,
  ): AssistantToTheTransportManager {
    return new AssistantToTheTransportManager(manager, transport, options);
  },
};

export default Factory;
