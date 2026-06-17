using MessagePack;
using System.Buffers.Binary;
using System.Text;

namespace Sockudo.Client;

public static class ProtocolCodec
{
    private static readonly string[] MessagePackEnvelopeFields =
    {
        "event",
        "channel",
        "data",
        "name",
        "user_id",
        "tags",
        "sequence",
        "conflation_key",
        "message_id",
        "serial",
        "idempotency_key",
        "extras",
        "__delta_seq",
        "__conflation_key",
        "stream_id",
    };

    public static object EncodeEnvelope(IDictionary<string, object?> envelope, SockudoWireFormat wireFormat)
    {
        return wireFormat switch
        {
            SockudoWireFormat.Json => JsonSupport.Encode(envelope),
            SockudoWireFormat.MessagePack => EncodeMessagePack(envelope),
            _ => EncodeProtobuf(envelope),
        };
    }

    public static SockudoEvent DecodeEvent(object rawMessage, SockudoWireFormat wireFormat)
    {
        var (envelope, rawText) = DecodeEnvelope(rawMessage, wireFormat);
        var rawData = envelope.TryGetValue("data", out var dataValue) ? dataValue : null;
        object? data = rawData;
        if (rawData is string text)
        {
            try
            {
                data = JsonSupport.Decode(text);
            }
            catch
            {
                data = text;
            }
        }

        return new SockudoEvent(
            Event: envelope["event"] as string ?? throw new SockudoException("Unable to decode event envelope"),
            Channel: envelope.TryGetValue("channel", out var channel) ? channel as string : null,
            Data: data,
            UserId: envelope.TryGetValue("user_id", out var userId) ? userId as string : null,
            MessageId: envelope.TryGetValue("message_id", out var messageId) ? messageId as string : null,
            StreamId: envelope.TryGetValue("stream_id", out var streamId) ? streamId as string : null,
            RawMessage: rawText,
            Sequence: CoerceInt(envelope.TryGetValue("__delta_seq", out var deltaSeq) ? deltaSeq : envelope.Get("sequence")),
            ConflationKey: envelope.TryGetValue("__conflation_key", out var deltaKey) ? deltaKey as string : envelope.Get("conflation_key") as string,
            Serial: CoerceInt(envelope.Get("serial")),
            Extras: DecodeExtras(envelope.Get("extras"))
        );
    }

    public static (Dictionary<string, object?> Envelope, string RawMessage) DecodeEnvelope(object rawMessage, SockudoWireFormat wireFormat)
    {
        switch (wireFormat)
        {
            case SockudoWireFormat.Json:
                {
                    var text = rawMessage switch
                    {
                        string s => s,
                        byte[] bytes => Encoding.UTF8.GetString(bytes),
                        _ => throw new SockudoException("Unsupported socket payload type"),
                    };
                    var decoded = JsonSupport.Decode(text) as Dictionary<string, object?>;
                    if (decoded is null)
                    {
                        throw new SockudoException("Unable to decode event envelope");
                    }
                    return (decoded, text);
                }
            case SockudoWireFormat.MessagePack:
                {
                    var payload = rawMessage switch
                    {
                        byte[] bytes => bytes,
                        string s => Encoding.UTF8.GetBytes(s),
                        _ => throw new SockudoException("Unsupported socket payload type"),
                    };
                    var envelope = DecodeMessagePack(payload);
                    return (envelope, JsonSupport.Encode(envelope));
                }
            default:
                {
                    var payload = rawMessage switch
                    {
                        byte[] bytes => bytes,
                        string s => Encoding.UTF8.GetBytes(s),
                        _ => throw new SockudoException("Unsupported socket payload type"),
                    };
                    var envelope = DecodeProtobuf(payload);
                    return (envelope, JsonSupport.Encode(envelope));
                }
        }
    }

    private static byte[] EncodeMessagePack(IDictionary<string, object?> envelope)
    {
        var payload = new object?[]
        {
            envelope.Get("event"),
            envelope.Get("channel"),
            EncodeMessagePackData(envelope.Get("data")),
            envelope.Get("name"),
            envelope.Get("user_id"),
            envelope.Get("tags"),
            envelope.Get("sequence"),
            envelope.Get("conflation_key"),
            envelope.Get("message_id"),
            envelope.Get("serial"),
            envelope.Get("idempotency_key"),
            EncodeMessagePackExtras(envelope.Get("extras")),
            envelope.Get("__delta_seq"),
            envelope.Get("__conflation_key"),
            envelope.Get("stream_id"),
        };
        return MessagePackSerializer.Serialize(payload);
    }

    private static Dictionary<string, object?> DecodeMessagePack(byte[] payload)
    {
        var value = MessagePackSerializer.Deserialize<object?>(payload);
        if (value is object?[] array)
        {
            var envelope = new Dictionary<string, object?>(StringComparer.Ordinal);
            for (var index = 0; index < MessagePackEnvelopeFields.Length && index < array.Length; index++)
            {
                var decoded = DecodeMessagePackValue(array[index]);
                if (decoded is not null)
                {
                    envelope[MessagePackEnvelopeFields[index]] = decoded;
                }
            }
            return envelope;
        }

        if (value is IDictionary<object, object> map)
        {
            return map.ToDictionary(entry => entry.Key.ToString()!, entry => DecodeMessagePackValue(entry.Value), StringComparer.Ordinal);
        }

        throw new SockudoException("Unable to decode event envelope");
    }

    private static object? EncodeMessagePackData(object? data)
    {
        return data switch
        {
            null => null,
            string text => new object?[] { "string", text },
            _ => new object?[] { "json", JsonSupport.Encode(data) },
        };
    }

    private static object? EncodeMessagePackExtras(object? rawExtras)
    {
        var extras = DecodeExtras(rawExtras);
        if (extras is null)
        {
            return null;
        }

        var payload = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (extras.Headers is not null)
        {
            payload["headers"] = extras.Headers.ToDictionary(
                entry => entry.Key,
                entry => entry.Value switch
                {
                    bool flag => new object?[] { "bool", flag },
                    int number => new object?[] { "number", (double)number },
                    long number => new object?[] { "number", (double)number },
                    double number => new object?[] { "number", number },
                    _ => new object?[] { "string", entry.Value?.ToString() ?? string.Empty },
                },
                StringComparer.Ordinal
            );
        }
        if (extras.Ephemeral is not null)
        {
            payload["ephemeral"] = extras.Ephemeral.Value;
        }
        if (extras.IdempotencyKey is not null)
        {
            payload["idempotency_key"] = extras.IdempotencyKey;
        }
        if (extras.Echo is not null)
        {
            payload["echo"] = extras.Echo.Value;
        }
        return payload;
    }

    private static object? DecodeMessagePackValue(object? value)
    {
        if (value is object?[] array)
        {
            if (array.Length == 2 && array[0] is string tag && tag is "string" or "json" or "number" or "bool")
            {
                return array[1];
            }

            return array.Select(DecodeMessagePackValue).ToList();
        }

        if (value is IDictionary<object, object> map)
        {
            return map.ToDictionary(entry => entry.Key.ToString()!, entry => DecodeMessagePackValue(entry.Value), StringComparer.Ordinal);
        }

        return value;
    }

    private static byte[] EncodeProtobuf(IDictionary<string, object?> envelope)
    {
        var output = new List<byte>();
        ProtoWriter.WriteString(output, 1, envelope.Get("event") as string);
        ProtoWriter.WriteString(output, 2, envelope.Get("channel") as string);
        if (envelope.TryGetValue("data", out var data) && data is not null)
        {
            var nested = new List<byte>();
            if (data is string text)
            {
                ProtoWriter.WriteString(nested, 1, text);
            }
            else
            {
                ProtoWriter.WriteString(nested, 3, JsonSupport.Encode(data));
            }
            ProtoWriter.WriteBytes(output, 3, nested.ToArray());
        }
        ProtoWriter.WriteString(output, 5, envelope.Get("user_id") as string);
        ProtoWriter.WriteUInt64(output, 7, CoerceUInt64(envelope.Get("sequence")));
        ProtoWriter.WriteString(output, 8, envelope.Get("conflation_key") as string);
        ProtoWriter.WriteString(output, 9, envelope.Get("message_id") as string);
        ProtoWriter.WriteUInt64(output, 10, CoerceUInt64(envelope.Get("serial")));
        var extras = EncodeProtobufExtras(envelope.Get("extras"));
        if (extras is not null)
        {
            ProtoWriter.WriteBytes(output, 12, extras);
        }
        ProtoWriter.WriteUInt64(output, 13, CoerceUInt64(envelope.Get("__delta_seq")));
        ProtoWriter.WriteString(output, 14, envelope.Get("__conflation_key") as string);
        ProtoWriter.WriteString(output, 15, envelope.Get("stream_id") as string);
        return output.ToArray();
    }

    private static byte[]? EncodeProtobufExtras(object? rawExtras)
    {
        var extras = DecodeExtras(rawExtras);
        if (extras is null)
        {
            return null;
        }

        var output = new List<byte>();
        if (extras.Headers is not null)
        {
            foreach (var entry in extras.Headers)
            {
                var nested = new List<byte>();
                ProtoWriter.WriteString(nested, 1, entry.Key);
                var valueBytes = new List<byte>();
                switch (entry.Value)
                {
                    case bool flag:
                        ProtoWriter.WriteBool(valueBytes, 3, flag);
                        break;
                    case int number:
                        ProtoWriter.WriteDouble(valueBytes, 2, number);
                        break;
                    case long number:
                        ProtoWriter.WriteDouble(valueBytes, 2, number);
                        break;
                    case double number:
                        ProtoWriter.WriteDouble(valueBytes, 2, number);
                        break;
                    default:
                        ProtoWriter.WriteString(valueBytes, 1, entry.Value?.ToString());
                        break;
                }
                ProtoWriter.WriteBytes(nested, 2, valueBytes.ToArray());
                ProtoWriter.WriteBytes(output, 1, nested.ToArray());
            }
        }
        ProtoWriter.WriteOptionalBool(output, 2, extras.Ephemeral);
        ProtoWriter.WriteString(output, 3, extras.IdempotencyKey);
        ProtoWriter.WriteOptionalBool(output, 4, extras.Echo);
        return output.ToArray();
    }

    private static Dictionary<string, object?> DecodeProtobuf(byte[] payload)
    {
        var reader = new ProtoReader(payload);
        var envelope = new Dictionary<string, object?>(StringComparer.Ordinal);
        while (reader.TryReadTag(out var field, out var wireType))
        {
            switch (field)
            {
                case 1:
                    envelope["event"] = reader.ReadString();
                    break;
                case 2:
                    envelope["channel"] = reader.ReadString();
                    break;
                case 3:
                    envelope["data"] = DecodeProtoData(reader.ReadBytes());
                    break;
                case 5:
                    envelope["user_id"] = reader.ReadString();
                    break;
                case 7:
                    envelope["sequence"] = (long)reader.ReadUInt64();
                    break;
                case 8:
                    envelope["conflation_key"] = reader.ReadString();
                    break;
                case 9:
                    envelope["message_id"] = reader.ReadString();
                    break;
                case 10:
                    envelope["serial"] = (long)reader.ReadUInt64();
                    break;
                case 12:
                    envelope["extras"] = DecodeProtoExtras(reader.ReadBytes());
                    break;
                case 13:
                    envelope["__delta_seq"] = (long)reader.ReadUInt64();
                    break;
                case 14:
                    envelope["__conflation_key"] = reader.ReadString();
                    break;
                case 15:
                    envelope["stream_id"] = reader.ReadString();
                    break;
                default:
                    reader.Skip(wireType);
                    break;
            }
        }
        return envelope;
    }

    private static object? DecodeProtoData(byte[] payload)
    {
        var reader = new ProtoReader(payload);
        while (reader.TryReadTag(out var field, out var wireType))
        {
            switch (field)
            {
                case 1:
                    return reader.ReadString();
                case 3:
                    return reader.ReadString();
                default:
                    reader.Skip(wireType);
                    break;
            }
        }
        return null;
    }

    private static Dictionary<string, object?> DecodeProtoExtras(byte[] payload)
    {
        var reader = new ProtoReader(payload);
        var result = new Dictionary<string, object?>(StringComparer.Ordinal);
        var headers = new Dictionary<string, object>(StringComparer.Ordinal);
        while (reader.TryReadTag(out var field, out var wireType))
        {
            switch (field)
            {
                case 1:
                    {
                        var (key, value) = DecodeProtoHeaderEntry(reader.ReadBytes());
                        if (key is not null)
                        {
                            headers[key] = value!;
                        }
                        break;
                    }
                case 2:
                    result["ephemeral"] = reader.ReadBool();
                    break;
                case 3:
                    result["idempotency_key"] = reader.ReadString();
                    break;
                case 4:
                    result["echo"] = reader.ReadBool();
                    break;
                default:
                    reader.Skip(wireType);
                    break;
            }
        }
        if (headers.Count > 0)
        {
            result["headers"] = headers;
        }
        return result;
    }

    private static (string? Key, object? Value) DecodeProtoHeaderEntry(byte[] payload)
    {
        var reader = new ProtoReader(payload);
        string? key = null;
        object? value = null;
        while (reader.TryReadTag(out var field, out var wireType))
        {
            switch (field)
            {
                case 1:
                    key = reader.ReadString();
                    break;
                case 2:
                    value = DecodeProtoExtraValue(reader.ReadBytes());
                    break;
                default:
                    reader.Skip(wireType);
                    break;
            }
        }
        return (key, value);
    }

    private static object? DecodeProtoExtraValue(byte[] payload)
    {
        var reader = new ProtoReader(payload);
        while (reader.TryReadTag(out var field, out var wireType))
        {
            switch (field)
            {
                case 1:
                    return reader.ReadString();
                case 2:
                    return reader.ReadDouble();
                case 3:
                    return reader.ReadBool();
                default:
                    reader.Skip(wireType);
                    break;
            }
        }
        return null;
    }

    private static MessageExtras? DecodeExtras(object? rawExtras)
    {
        if (rawExtras is null)
        {
            return null;
        }

        if (rawExtras is MessageExtras extras)
        {
            return extras;
        }

        if (rawExtras is not IDictionary<string, object?> map)
        {
            return null;
        }

        IDictionary<string, object>? headers = null;
        if (map.TryGetValue("headers", out var rawHeaders) && rawHeaders is IDictionary<string, object> headerMap)
        {
            headers = headerMap;
        }

        return new MessageExtras(
            Headers: headers,
            Ephemeral: map.TryGetValue("ephemeral", out var ephemeral) ? ephemeral as bool? : null,
            IdempotencyKey: map.TryGetValue("idempotency_key", out var idempotencyKey) ? idempotencyKey as string : null,
            Echo: map.TryGetValue("echo", out var echo) ? echo as bool? : null
        );
    }

    internal static int? CoerceInt(object? value)
    {
        return value switch
        {
            int number => number,
            long number => (int)number,
            double number => (int)number,
            _ => null,
        };
    }

    private static ulong? CoerceUInt64(object? value)
    {
        return value switch
        {
            null => null,
            int number => (ulong)number,
            long number => (ulong)number,
            ulong number => number,
            double number => (ulong)number,
            _ => null,
        };
    }
}

internal static class ProtoWriter
{
    public static void WriteString(List<byte> output, int field, string? value)
    {
        if (value is null)
        {
            return;
        }
        WriteTag(output, field, 2);
        var payload = Encoding.UTF8.GetBytes(value);
        WriteVarint(output, (ulong)payload.Length);
        output.AddRange(payload);
    }

    public static void WriteBytes(List<byte> output, int field, byte[] payload)
    {
        WriteTag(output, field, 2);
        WriteVarint(output, (ulong)payload.Length);
        output.AddRange(payload);
    }

    public static void WriteUInt64(List<byte> output, int field, ulong? value)
    {
        if (value is null)
        {
            return;
        }
        WriteTag(output, field, 0);
        WriteVarint(output, value.Value);
    }

    public static void WriteOptionalBool(List<byte> output, int field, bool? value)
    {
        if (value is null)
        {
            return;
        }
        WriteBool(output, field, value.Value);
    }

    public static void WriteBool(List<byte> output, int field, bool value)
    {
        WriteTag(output, field, 0);
        WriteVarint(output, value ? 1UL : 0UL);
    }

    public static void WriteDouble(List<byte> output, int field, double value)
    {
        WriteTag(output, field, 1);
        Span<byte> bytes = stackalloc byte[8];
        BinaryPrimitives.WriteDoubleLittleEndian(bytes, value);
        output.AddRange(bytes.ToArray());
    }

    private static void WriteTag(List<byte> output, int field, int wireType) => WriteVarint(output, (ulong)((field << 3) | wireType));

    private static void WriteVarint(List<byte> output, ulong value)
    {
        while (value >= 0x80)
        {
            output.Add((byte)((value & 0x7FUL) | 0x80UL));
            value >>= 7;
        }
        output.Add((byte)value);
    }
}

internal ref struct ProtoReader
{
    private readonly ReadOnlySpan<byte> _data;
    private int _index;

    public ProtoReader(ReadOnlySpan<byte> data)
    {
        _data = data;
        _index = 0;
    }

    public bool TryReadTag(out int field, out int wireType)
    {
        if (_index >= _data.Length)
        {
            field = 0;
            wireType = 0;
            return false;
        }
        var tag = ReadVarint();
        field = (int)(tag >> 3);
        wireType = (int)(tag & 0x7);
        return true;
    }

    public string ReadString() => Encoding.UTF8.GetString(ReadBytes());
    public ulong ReadUInt64() => ReadVarint();
    public bool ReadBool() => ReadVarint() != 0;
    public double ReadDouble()
    {
        var value = BinaryPrimitives.ReadDoubleLittleEndian(_data.Slice(_index, 8));
        _index += 8;
        return value;
    }

    public byte[] ReadBytes()
    {
        var length = (int)ReadVarint();
        var value = _data.Slice(_index, length).ToArray();
        _index += length;
        return value;
    }

    public void Skip(int wireType)
    {
        switch (wireType)
        {
            case 0:
                ReadVarint();
                return;
            case 1:
                _index += 8;
                return;
            case 2:
                _index += (int)ReadVarint();
                return;
            case 5:
                _index += 4;
                return;
            default:
                throw new SockudoException($"Unsupported protobuf wire type: {wireType}");
        }
    }

    private ulong ReadVarint()
    {
        ulong result = 0;
        var shift = 0;
        while (true)
        {
            var value = _data[_index++];
            result |= (ulong)(value & 0x7F) << shift;
            if ((value & 0x80) == 0)
            {
                return result;
            }
            shift += 7;
        }
    }
}
