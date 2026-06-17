import {
  decode as decodeMsgpack,
  encode as encodeMsgpack,
} from "@msgpack/msgpack";
import protobuf from "protobufjs/light";
import Action from "./action";
import { SockudoEvent } from "./message-types";
import { prefixedEvent } from "../../protocol_prefix";
import { wireFormat } from "../../wire_format";

const { Field, MapField, OneOf, Root, Type } = protobuf;

const ProtoExtrasValue = new Type("ProtoExtrasValue")
  .add(new OneOf("kind", ["stringValue", "numberValue", "boolValue"]))
  .add(new Field("stringValue", 1, "string"))
  .add(new Field("numberValue", 2, "double"))
  .add(new Field("boolValue", 3, "bool"));

const ProtoMessageExtras = new Type("ProtoMessageExtras")
  .add(new MapField("headers", 1, "string", "ProtoExtrasValue"))
  .add(new Field("ephemeral", 2, "bool"))
  .add(new Field("idempotencyKey", 3, "string"))
  .add(new Field("echo", 4, "bool"));

const ProtoStructuredData = new Type("ProtoStructuredData")
  .add(new Field("channelData", 1, "string"))
  .add(new Field("channel", 2, "string"))
  .add(new Field("userData", 3, "string"))
  .add(new MapField("extra", 4, "string", "string"));

const ProtoMessageData = new Type("ProtoMessageData")
  .add(new OneOf("kind", ["stringValue", "structured", "jsonValue"]))
  .add(new Field("stringValue", 1, "string"))
  .add(new Field("structured", 2, "ProtoStructuredData"))
  .add(new Field("jsonValue", 3, "string"));

const ProtoPusherMessage = new Type("ProtoPusherMessage")
  .add(new Field("event", 1, "string"))
  .add(new Field("channel", 2, "string"))
  .add(new Field("data", 3, "ProtoMessageData"))
  .add(new Field("name", 4, "string"))
  .add(new Field("userId", 5, "string"))
  .add(new MapField("tags", 6, "string", "string"))
  .add(new Field("sequence", 7, "uint64"))
  .add(new Field("conflationKey", 8, "string"))
  .add(new Field("messageId", 9, "string"))
  .add(new Field("streamId", 10, "string"))
  .add(new Field("serial", 11, "uint64"))
  .add(new Field("idempotencyKey", 12, "string"))
  .add(new Field("extras", 13, "ProtoMessageExtras"))
  .add(new Field("deltaSequence", 14, "uint64"))
  .add(new Field("deltaConflationKey", 15, "string"));

new Root()
  .define("sockudo")
  .add(ProtoExtrasValue)
  .add(ProtoMessageExtras)
  .add(ProtoStructuredData)
  .add(ProtoMessageData)
  .add(ProtoPusherMessage);

type Envelope = Record<string, any>;
const MSGPACK_ENVELOPE_FIELDS = [
  "event",
  "channel",
  "data",
  "name",
  "user_id",
  "tags",
  "sequence",
  "conflation_key",
  "message_id",
  "stream_id",
  "serial",
  "idempotency_key",
  "extras",
  "__delta_seq",
  "__conflation_key",
] as const;

function toUint8Array(payload: unknown): Uint8Array {
  if (payload instanceof Uint8Array) {
    return payload;
  }
  if (payload instanceof ArrayBuffer) {
    return new Uint8Array(payload);
  }
  if (ArrayBuffer.isView(payload)) {
    return new Uint8Array(
      payload.buffer,
      payload.byteOffset,
      payload.byteLength,
    );
  }
  throw new Error("Unsupported binary payload");
}

function normalizeNumeric(value: unknown): number | undefined {
  if (typeof value === "number") {
    return value;
  }
  if (
    value != null &&
    typeof value === "object" &&
    "toNumber" in (value as Record<string, unknown>) &&
    typeof (value as { toNumber: () => number }).toNumber === "function"
  ) {
    return (value as { toNumber: () => number }).toNumber();
  }
  return undefined;
}

function parseEventData(data: unknown): any {
  if (typeof data === "string") {
    try {
      return JSON.parse(data);
    } catch {
      return data;
    }
  }
  return data;
}

function stringifyEnvelope(messageData: Envelope): string {
  return JSON.stringify(messageData);
}

function encodeMsgpackValue(value: any): any {
  if (value === undefined) {
    return null;
  }
  if (Array.isArray(value)) {
    return value.map((entry) => encodeMsgpackValue(entry));
  }
  if (value && typeof value === "object") {
    if (
      "headers" in value ||
      "ephemeral" in value ||
      "idempotency_key" in value ||
      "echo" in value
    ) {
      const headers = value.headers
        ? Object.fromEntries(
            Object.entries(value.headers).map(([key, headerValue]) => {
              if (typeof headerValue === "number") {
                return [key, ["number", headerValue]];
              }
              if (typeof headerValue === "boolean") {
                return [key, ["bool", headerValue]];
              }
              return [key, ["string", String(headerValue)]];
            }),
          )
        : undefined;
      return {
        headers,
        ephemeral: value.ephemeral ?? null,
        idempotency_key: value.idempotency_key ?? null,
        echo: value.echo ?? null,
      };
    }
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [
        key,
        encodeMsgpackValue(entry),
      ]),
    );
  }
  return value;
}

function encodeMsgpackEnvelope(event: SockudoEvent): any[] {
  const envelope = eventToEnvelope(event);
  const rawData = envelope.data;
  const encodedData =
    rawData === undefined
      ? null
      : typeof rawData === "string"
        ? ["string", rawData]
        : ["json", JSON.stringify(rawData)];

  return [
    envelope.event ?? null,
    envelope.channel ?? null,
    encodedData,
    envelope.name ?? null,
    envelope.user_id ?? null,
    envelope.tags ?? null,
    envelope.sequence ?? null,
    envelope.conflation_key ?? null,
    envelope.message_id ?? null,
    envelope.stream_id ?? null,
    envelope.serial ?? null,
    envelope.idempotency_key ?? null,
    encodeMsgpackValue(envelope.extras),
    envelope.__delta_seq ?? null,
    envelope.__conflation_key ?? null,
  ];
}

function decodeMsgpackValue(value: any): any {
  if (Array.isArray(value)) {
    if (value.length === 2 && typeof value[0] === "string") {
      const [kind, payload] = value;
      switch (kind) {
        case "string":
        case "json":
        case "number":
        case "bool":
          return payload;
        case "structured":
          return decodeMsgpackValue(payload);
        default:
          return value.map((entry) => decodeMsgpackValue(entry));
      }
    }
    return value.map((entry) => decodeMsgpackValue(entry));
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [
        key,
        decodeMsgpackValue(entry),
      ]),
    );
  }
  return value;
}

function decodeMsgpackEnvelope(payload: any): Envelope {
  if (Array.isArray(payload)) {
    const envelope: Envelope = {};
    MSGPACK_ENVELOPE_FIELDS.forEach((field, index) => {
      const value = payload[index];
      if (value !== null && value !== undefined) {
        envelope[field] =
          field === "data"
            ? decodeMsgpackValue(value)
            : decodeMsgpackValue(value);
      }
    });
    return envelope;
  }
  return decodeMsgpackValue(payload) as Envelope;
}

function extrasToProto(extras: Record<string, any> | undefined) {
  if (!extras) {
    return undefined;
  }
  const headers = Object.fromEntries(
    Object.entries(extras.headers || {}).map(([key, value]) => {
      if (typeof value === "number") {
        return [key, { numberValue: value }];
      }
      if (typeof value === "boolean") {
        return [key, { boolValue: value }];
      }
      return [key, { stringValue: String(value) }];
    }),
  );
  return {
    headers,
    ephemeral: extras.ephemeral,
    idempotencyKey: extras.idempotency_key,
    echo: extras.echo,
  };
}

function extrasFromProto(extras: Record<string, any> | undefined) {
  if (!extras) {
    return undefined;
  }
  const headers = extras.headers
    ? Object.fromEntries(
        Object.entries(extras.headers).map(([key, value]: [string, any]) => {
          if (typeof value?.numberValue === "number") {
            return [key, value.numberValue];
          }
          if (typeof value?.boolValue === "boolean") {
            return [key, value.boolValue];
          }
          return [key, value?.stringValue ?? ""];
        }),
      )
    : undefined;
  return {
    headers,
    ephemeral: extras.ephemeral,
    idempotency_key: extras.idempotencyKey,
    echo: extras.echo,
  };
}

function eventToEnvelope(event: SockudoEvent): Envelope {
  const envelope: Envelope = {
    event: event.event,
    channel: event.channel,
    user_id: event.user_id,
    extras: event.extras,
  };
  if (event.data !== undefined) {
    envelope.data = event.data;
  }
  if (typeof event.sequence === "number") {
    envelope.__delta_seq = event.sequence;
  }
  if (typeof event.conflation_key === "string") {
    envelope.__conflation_key = event.conflation_key;
  }
  if (typeof (event as any).serial === "number") {
    envelope.serial = event.serial;
  }
  if (typeof event.message_id === "string") {
    envelope.message_id = event.message_id;
  }
  if (typeof event.stream_id === "string") {
    envelope.stream_id = event.stream_id;
  }
  return envelope;
}

function envelopeToEvent(
  messageData: Envelope,
  rawMessage: string,
): SockudoEvent {
  const decodedEvent: SockudoEvent = {
    event: messageData.event,
    channel: messageData.channel,
    data: parseEventData(messageData.data),
    rawMessage,
  };
  if (typeof messageData.name === "string") {
    decodedEvent.name = messageData.name;
  }
  if (messageData.user_id) {
    decodedEvent.user_id = messageData.user_id;
  }
  if (messageData.extras) {
    decodedEvent.extras = messageData.extras;
  }
  const sequence = messageData.__delta_seq ?? messageData.sequence;
  const conflationKey =
    messageData.__conflation_key ?? messageData.conflation_key;
  if (typeof sequence === "number") {
    decodedEvent.sequence = sequence;
  }
  if (typeof conflationKey === "string") {
    decodedEvent.conflation_key = conflationKey;
  }
  if (typeof messageData.serial === "number") {
    decodedEvent.serial = messageData.serial;
  }
  if (typeof messageData.message_id === "string") {
    decodedEvent.message_id = messageData.message_id;
  }
  if (typeof messageData.stream_id === "string") {
    decodedEvent.stream_id = messageData.stream_id;
  }
  return decodedEvent;
}

function encodeProtobuf(event: SockudoEvent): Uint8Array {
  const envelope = eventToEnvelope(event);
  const payload: Record<string, any> = {
    event: envelope.event,
    channel: envelope.channel,
    userId: envelope.user_id,
    streamId: envelope.stream_id,
    serial: envelope.serial,
    messageId: envelope.message_id,
    deltaSequence: envelope.__delta_seq,
    deltaConflationKey: envelope.__conflation_key,
    extras: extrasToProto(envelope.extras),
  };

  if (envelope.data !== undefined) {
    if (typeof envelope.data === "string") {
      payload.data = { stringValue: envelope.data };
    } else {
      payload.data = { jsonValue: JSON.stringify(envelope.data) };
    }
  }

  return ProtoPusherMessage.encode(payload).finish();
}

function decodeProtobuf(payload: Uint8Array): Envelope {
  const decoded = ProtoPusherMessage.toObject(
    ProtoPusherMessage.decode(payload),
    {
      longs: Number,
      defaults: false,
    },
  ) as Record<string, any>;

  const envelope: Envelope = {
    event: decoded.event,
    channel: decoded.channel,
    user_id: decoded.userId,
    stream_id: decoded.streamId,
    serial: normalizeNumeric(decoded.serial),
    message_id: decoded.messageId,
    __delta_seq: normalizeNumeric(decoded.deltaSequence),
    __conflation_key: decoded.deltaConflationKey,
    extras: extrasFromProto(decoded.extras),
  };

  if (decoded.data?.stringValue !== undefined) {
    envelope.data = decoded.data.stringValue;
  } else if (decoded.data?.jsonValue !== undefined) {
    try {
      envelope.data = JSON.parse(decoded.data.jsonValue);
    } catch {
      envelope.data = decoded.data.jsonValue;
    }
  }

  return envelope;
}

const Protocol = {
  decodeMessage: function (messageEvent: MessageEvent): SockudoEvent {
    try {
      let messageData: Envelope;
      let rawMessage: string;

      switch (wireFormat()) {
        case "messagepack": {
          messageData = decodeMsgpackEnvelope(
            decodeMsgpack(toUint8Array(messageEvent.data)),
          );
          rawMessage = stringifyEnvelope(messageData);
          break;
        }
        case "protobuf": {
          messageData = decodeProtobuf(toUint8Array(messageEvent.data));
          rawMessage = stringifyEnvelope(messageData);
          break;
        }
        case "json":
        default: {
          rawMessage = String(messageEvent.data);
          messageData = JSON.parse(rawMessage);
          break;
        }
      }

      return envelopeToEvent(messageData, rawMessage);
    } catch (e) {
      throw { type: "MessageParseError", error: e, data: messageEvent.data };
    }
  },

  encodeMessage: function (event: SockudoEvent): string | Uint8Array {
    switch (wireFormat()) {
      case "messagepack":
        return encodeMsgpack(encodeMsgpackEnvelope(event));
      case "protobuf":
        return encodeProtobuf(event);
      case "json":
      default:
        return JSON.stringify(event);
    }
  },

  processHandshake: function (messageEvent: MessageEvent): Action {
    const message = Protocol.decodeMessage(messageEvent);

    if (message.event === prefixedEvent("connection_established")) {
      if (!message.data.activity_timeout) {
        throw "No activity timeout specified in handshake";
      }
      return {
        action: "connected",
        id: message.data.socket_id,
        activityTimeout: message.data.activity_timeout * 1000,
      };
    } else if (message.event === prefixedEvent("error")) {
      return {
        action: this.getCloseAction(message.data),
        error: this.getCloseError(message.data),
      };
    } else {
      throw "Invalid handshake";
    }
  },

  getCloseAction: function (closeEvent): string {
    if (closeEvent.code < 4000) {
      if (closeEvent.code >= 1002 && closeEvent.code <= 1004) {
        return "backoff";
      } else {
        return null;
      }
    } else if (closeEvent.code === 4000) {
      return "tls_only";
    } else if (closeEvent.code < 4100) {
      return "refused";
    } else if (closeEvent.code < 4200) {
      return "backoff";
    } else if (closeEvent.code < 4300) {
      return "retry";
    } else {
      return "refused";
    }
  },

  getCloseError: function (closeEvent): any {
    if (closeEvent.code !== 1000 && closeEvent.code !== 1001) {
      return {
        type: "SockudoError",
        data: {
          code: closeEvent.code,
          message: closeEvent.reason || closeEvent.message,
        },
      };
    } else {
      return null;
    }
  },
};

export default Protocol;
