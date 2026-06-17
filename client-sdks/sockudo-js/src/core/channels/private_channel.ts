import Channel from "./channel";
import { ChannelAuthorizationCallback } from "../auth/options";

/** Extends public channels to provide private channel interface.
 *
 * @param {String} name
 * @param {Sockudo} sockudo
 */
export default class PrivateChannel extends Channel {
  /** Authorizes the connection to use the channel.
   *
   * @param  {String} socketId
   * @param  {(...args: any[]) => any} callback
   */
  authorize(socketId: string, callback: ChannelAuthorizationCallback) {
    return this.sockudo.config.channelAuthorizer(
      {
        channelName: this.name,
        socketId: socketId,
      },
      callback,
    );
  }
}
