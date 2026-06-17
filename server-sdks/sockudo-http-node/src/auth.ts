import { isEncryptedChannel } from "./util";
import type {
  ChannelAuthResponse,
  UserAuthResponse,
  UserChannelData,
  PresenceChannelData,
} from "./types";
import type Token = require("./token");
import type Sockudo = require("./sockudo");

export function getSocketSignatureForUser(
  token: Token,
  socketId: string,
  userData: UserChannelData,
): UserAuthResponse {
  const serializedUserData = JSON.stringify(userData);
  const signature = token.sign(`${socketId}::user::${serializedUserData}`);
  return {
    auth: `${token.key}:${signature}`,
    user_data: serializedUserData,
  };
}

export function getSocketSignature(
  sockudo: Sockudo,
  token: Token,
  channel: string,
  socketID: string,
  data?: PresenceChannelData,
): ChannelAuthResponse {
  const result: ChannelAuthResponse = { auth: "" };
  const signatureData = [socketID, channel];

  if (data) {
    const serializedData = JSON.stringify(data);
    signatureData.push(serializedData);
    result.channel_data = serializedData;
  }

  result.auth = `${token.key}:${token.sign(signatureData.join(":"))}`;

  if (isEncryptedChannel(channel)) {
    if (sockudo.config.encryptionMasterKey === undefined) {
      throw new Error(
        "Cannot generate shared_secret because encryptionMasterKey is not set",
      );
    }

    result.shared_secret = Buffer.from(
      sockudo.channelSharedSecret(channel),
    ).toString("base64");
  }

  return result;
}
