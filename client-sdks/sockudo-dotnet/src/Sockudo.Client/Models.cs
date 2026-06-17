using System.Collections;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Sockudo.Client;

public enum SockudoTransport
{
    Ws,
    Wss,
}

public enum SockudoWireFormat
{
    Json,
    MessagePack,
    Protobuf,
}

public enum ConnectionState
{
    Initialized,
    Connecting,
    Connected,
    Disconnected,
    Unavailable,
    Failed,
}

public enum DeltaAlgorithm
{
    Fossil,
    Xdelta3,
}

public sealed record ChannelDeltaSettings(bool? Enabled = null, DeltaAlgorithm? Algorithm = null)
{
    public object SubscriptionValue()
    {
        if (Enabled is null && Algorithm is not null)
        {
            return Algorithm.Value.ToString().ToLowerInvariant();
        }

        if (Enabled == false && Algorithm is null)
        {
            return false;
        }

        if (Enabled == true && Algorithm is null)
        {
            return true;
        }

        var payload = new Dictionary<string, object>();
        if (Enabled is not null)
        {
            payload["enabled"] = Enabled.Value;
        }
        if (Algorithm is not null)
        {
            payload["algorithm"] = Algorithm.Value.ToString().ToLowerInvariant();
        }
        return payload;
    }
}

public sealed record MessageExtras(
    IDictionary<string, object>? Headers = null,
    bool? Ephemeral = null,
    string? IdempotencyKey = null,
    bool? Echo = null
);

public sealed record SubscriptionOptions(
    FilterNode? Filter = null,
    ChannelDeltaSettings? Delta = null,
    IReadOnlyList<string>? Events = null,
    SubscriptionRewind? Rewind = null,
    bool AnnotationSubscribe = false
);

public abstract record SubscriptionRewind
{
    internal abstract object SubscriptionValue();

    public sealed record Count(int Value) : SubscriptionRewind
    {
        internal override object SubscriptionValue() => Value;
    }

    public sealed record Seconds(int Value) : SubscriptionRewind
    {
        internal override object SubscriptionValue() =>
            new Dictionary<string, object> { ["seconds"] = Value };
    }
}

public sealed record DeltaOptions(
    bool? Enabled = null,
    IReadOnlyList<DeltaAlgorithm>? Algorithms = null,
    bool Debug = false,
    Action<DeltaStats>? OnStats = null,
    Action<Exception>? OnError = null
)
{
    public IReadOnlyList<DeltaAlgorithm> EffectiveAlgorithms => Algorithms ?? new[] { DeltaAlgorithm.Fossil, DeltaAlgorithm.Xdelta3 };
}

public sealed record DeltaStats(
    int TotalMessages,
    int DeltaMessages,
    int FullMessages,
    int TotalBytesWithoutCompression,
    int TotalBytesWithCompression,
    int Errors
)
{
    public int BandwidthSaved => TotalBytesWithoutCompression - TotalBytesWithCompression;
    public double BandwidthSavedPercent => TotalBytesWithoutCompression == 0 ? 0.0 : (double)BandwidthSaved / TotalBytesWithoutCompression * 100.0;
}

public sealed record PresenceMember(string Id, object? Info);

public sealed record ChannelAuthorizationData(string Auth, string? ChannelData = null, string? SharedSecret = null);
public sealed record UserAuthenticationData(string Auth, string UserData);
public sealed record ChannelAuthorizationRequest(string SocketId, string ChannelName);
public sealed record UserAuthenticationRequest(string SocketId);

public delegate Task<ChannelAuthorizationData> ChannelAuthorizationHandler(ChannelAuthorizationRequest request);
public delegate Task<UserAuthenticationData> UserAuthenticationHandler(UserAuthenticationRequest request);

public sealed record ChannelAuthorizationOptions(
    string Endpoint = "/sockudo/auth",
    IDictionary<string, string>? Headers = null,
    IDictionary<string, object>? Params = null,
    Func<IDictionary<string, string>>? HeadersProvider = null,
    Func<IDictionary<string, object>>? ParamsProvider = null,
    ChannelAuthorizationHandler? CustomHandler = null
);

public sealed record UserAuthenticationOptions(
    string Endpoint = "/sockudo/user-auth",
    IDictionary<string, string>? Headers = null,
    IDictionary<string, object>? Params = null,
    Func<IDictionary<string, string>>? HeadersProvider = null,
    Func<IDictionary<string, object>>? ParamsProvider = null,
    UserAuthenticationHandler? CustomHandler = null
);

public sealed record PresenceHistoryOptions(
    string Endpoint,
    IDictionary<string, string>? Headers = null,
    Func<IDictionary<string, string>>? HeadersProvider = null
);

public sealed record VersionedMessagesOptions(
    string Endpoint,
    IDictionary<string, string>? Headers = null,
    Func<IDictionary<string, string>>? HeadersProvider = null
);

public sealed record PresenceHistoryParams(
    string? Direction = null,
    int? Limit = null,
    string? Cursor = null,
    long? StartSerial = null,
    long? EndSerial = null,
    long? StartTimeMs = null,
    long? EndTimeMs = null,
    long? Start = null,
    long? End = null
)
{
    public IDictionary<string, object> ToPayload()
    {
        var payload = new Dictionary<string, object>(StringComparer.Ordinal);
        if (Direction is not null) payload["direction"] = Direction;
        if (Limit is not null) payload["limit"] = Limit.Value;
        if (Cursor is not null) payload["cursor"] = Cursor;
        if (StartSerial is not null) payload["start_serial"] = StartSerial.Value;
        if (EndSerial is not null) payload["end_serial"] = EndSerial.Value;
        if (StartTimeMs is not null) payload["start_time_ms"] = StartTimeMs.Value;
        else if (Start is not null) payload["start_time_ms"] = Start.Value;
        if (EndTimeMs is not null) payload["end_time_ms"] = EndTimeMs.Value;
        else if (End is not null) payload["end_time_ms"] = End.Value;
        return payload;
    }
}

public sealed record PresenceSnapshotParams(
    long? AtTimeMs = null,
    long? At = null,
    long? AtSerial = null
)
{
    public IDictionary<string, object> ToPayload()
    {
        var payload = new Dictionary<string, object>(StringComparer.Ordinal);
        if (AtTimeMs is not null) payload["at_time_ms"] = AtTimeMs.Value;
        else if (At is not null) payload["at_time_ms"] = At.Value;
        if (AtSerial is not null) payload["at_serial"] = AtSerial.Value;
        return payload;
    }
}

public sealed record PresenceHistoryItem(
    string StreamId,
    long Serial,
    long PublishedAtMs,
    string Event,
    string Cause,
    string UserId,
    string? ConnectionId,
    string? DeadNodeId,
    int PayloadSizeBytes,
    IDictionary<string, object?> PresenceEvent
);

public sealed record PresenceHistoryBounds(
    long? StartSerial,
    long? EndSerial,
    long? StartTimeMs,
    long? EndTimeMs
);

public sealed record PresenceHistoryContinuity(
    string? StreamId,
    long? OldestAvailableSerial,
    long? NewestAvailableSerial,
    long? OldestAvailablePublishedAtMs,
    long? NewestAvailablePublishedAtMs,
    long RetainedEvents,
    long RetainedBytes,
    bool Degraded,
    bool Complete,
    bool TruncatedByRetention
);

public sealed class PresenceHistoryPage
{
    private readonly Func<string, Task<PresenceHistoryPage>>? _fetchNext;

    public PresenceHistoryPage(
        IReadOnlyList<PresenceHistoryItem> items,
        string direction,
        int limit,
        bool hasMore,
        string? nextCursor,
        PresenceHistoryBounds bounds,
        PresenceHistoryContinuity continuity,
        Func<string, Task<PresenceHistoryPage>>? fetchNext = null)
    {
        Items = items;
        Direction = direction;
        Limit = limit;
        HasMore = hasMore;
        NextCursor = nextCursor;
        Bounds = bounds;
        Continuity = continuity;
        _fetchNext = fetchNext;
    }

    public IReadOnlyList<PresenceHistoryItem> Items { get; }
    public string Direction { get; }
    public int Limit { get; }
    public bool HasMore { get; }
    public string? NextCursor { get; }
    public PresenceHistoryBounds Bounds { get; }
    public PresenceHistoryContinuity Continuity { get; }

    public bool HasNext() => HasMore && !string.IsNullOrEmpty(NextCursor);

    public Task<PresenceHistoryPage> NextAsync()
    {
        if (!HasNext() || _fetchNext is null || NextCursor is null)
        {
            throw new SockudoException("No more pages available");
        }
        return _fetchNext(NextCursor);
    }
}

public sealed record PresenceSnapshotMember(
    string UserId,
    string LastEvent,
    long LastEventSerial,
    long LastEventAtMs
);

public sealed record PresenceSnapshot(
    string Channel,
    IReadOnlyList<PresenceSnapshotMember> Members,
    int MemberCount,
    long EventsReplayed,
    long? SnapshotSerial,
    long? SnapshotTimeMs,
    PresenceHistoryContinuity Continuity
);

public sealed record ChannelHistoryParams(
    string? Direction = null,
    int? Limit = null,
    string? Cursor = null,
    long? StartSerial = null,
    long? EndSerial = null,
    long? StartTimeMs = null,
    long? EndTimeMs = null
)
{
    public IDictionary<string, object> ToPayload()
    {
        var payload = new Dictionary<string, object>(StringComparer.Ordinal);
        if (Direction is not null) payload["direction"] = Direction;
        if (Limit is not null) payload["limit"] = Limit.Value;
        if (Cursor is not null) payload["cursor"] = Cursor;
        if (StartSerial is not null) payload["start_serial"] = StartSerial.Value;
        if (EndSerial is not null) payload["end_serial"] = EndSerial.Value;
        if (StartTimeMs is not null) payload["start_time_ms"] = StartTimeMs.Value;
        if (EndTimeMs is not null) payload["end_time_ms"] = EndTimeMs.Value;
        return payload;
    }
}

public sealed class ChannelHistoryPageProxy
{
    private readonly Func<string, Task<ChannelHistoryPageProxy>>? _fetchNext;

    public ChannelHistoryPageProxy(
        IReadOnlyList<Dictionary<string, object?>> items,
        string direction,
        int limit,
        bool hasMore,
        string? nextCursor,
        IDictionary<string, object?> bounds,
        IDictionary<string, object?> continuity,
        Func<string, Task<ChannelHistoryPageProxy>>? fetchNext = null)
    {
        Items = items;
        Direction = direction;
        Limit = limit;
        HasMore = hasMore;
        NextCursor = nextCursor;
        Bounds = bounds;
        Continuity = continuity;
        _fetchNext = fetchNext;
    }

    public IReadOnlyList<Dictionary<string, object?>> Items { get; }
    public string Direction { get; }
    public int Limit { get; }
    public bool HasMore { get; }
    public string? NextCursor { get; }
    public IDictionary<string, object?> Bounds { get; }
    public IDictionary<string, object?> Continuity { get; }

    public bool HasNext() => HasMore && NextCursor is not null;

    public Task<ChannelHistoryPageProxy> NextAsync()
    {
        if (NextCursor is null || _fetchNext is null || !HasNext())
        {
            throw new InvalidOperationException("No more pages available");
        }
        return _fetchNext(NextCursor);
    }
}

public sealed record MessageVersionsParams(
    string? Direction = null,
    int? Limit = null,
    string? Cursor = null
)
{
    public IDictionary<string, object> ToPayload()
    {
        var payload = new Dictionary<string, object>(StringComparer.Ordinal);
        if (Direction is not null) payload["direction"] = Direction;
        if (Limit is not null) payload["limit"] = Limit.Value;
        if (Cursor is not null) payload["cursor"] = Cursor;
        return payload;
    }
}

public sealed class MessageVersionsPage
{
    private readonly Func<string, Task<MessageVersionsPage>>? _fetchNext;

    public MessageVersionsPage(
        string channel,
        IReadOnlyList<Dictionary<string, object?>> items,
        string direction,
        int limit,
        bool hasMore,
        string? nextCursor,
        Func<string, Task<MessageVersionsPage>>? fetchNext = null)
    {
        Channel = channel;
        Items = items;
        Direction = direction;
        Limit = limit;
        HasMore = hasMore;
        NextCursor = nextCursor;
        _fetchNext = fetchNext;
    }

    public string Channel { get; }
    public IReadOnlyList<Dictionary<string, object?>> Items { get; }
    public string Direction { get; }
    public int Limit { get; }
    public bool HasMore { get; }
    public string? NextCursor { get; }

    public bool HasNext() => HasMore && NextCursor is not null;

    public Task<MessageVersionsPage> NextAsync()
    {
        if (NextCursor is null || _fetchNext is null || !HasNext())
        {
            throw new InvalidOperationException("No more pages available");
        }
        return _fetchNext(NextCursor);
    }
}

public sealed record PublishAnnotationRequest(
    string Type,
    string? Name = null,
    int? Count = null,
    IDictionary<string, object?>? Data = null,
    string? ClientId = null,
    IDictionary<string, object?>? Extras = null,
    string? IdempotencyKey = null
)
{
    public IDictionary<string, object?> ToPayload()
    {
        var payload = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["type"] = Type,
        };
        if (Name is not null) payload["name"] = Name;
        if (Count is not null) payload["count"] = Count.Value;
        if (Data is not null) payload["data"] = Data;
        if (ClientId is not null) payload["clientId"] = ClientId;
        if (Extras is not null) payload["extras"] = Extras;
        if (IdempotencyKey is not null) payload["idempotencyKey"] = IdempotencyKey;
        return payload;
    }
}

public sealed record PublishAnnotationResponse(
    IDictionary<string, object?> Annotation,
    IDictionary<string, object?>? Summary = null
);

public sealed record DeleteAnnotationResponse(
    bool Deleted,
    string AnnotationSerial,
    IDictionary<string, object?>? Summary = null
);

public sealed record AnnotationEventsParams(
    string? Direction = null,
    int? Limit = null,
    string? Cursor = null,
    string? Type = null,
    string? FromSerial = null
)
{
    public IDictionary<string, object> ToPayload()
    {
        var payload = new Dictionary<string, object>(StringComparer.Ordinal);
        if (Direction is not null) payload["direction"] = Direction;
        if (Limit is not null) payload["limit"] = Limit.Value;
        if (Cursor is not null) payload["cursor"] = Cursor;
        if (Type is not null) payload["type"] = Type;
        if (FromSerial is not null) payload["from_serial"] = FromSerial;
        return payload;
    }
}

public sealed class AnnotationEventsPage
{
    private readonly Func<string, Task<AnnotationEventsPage>>? _fetchNext;

    public AnnotationEventsPage(
        IReadOnlyList<Dictionary<string, object?>> items,
        string direction,
        int limit,
        bool hasMore,
        string? nextCursor,
        Func<string, Task<AnnotationEventsPage>>? fetchNext = null)
    {
        Items = items;
        Direction = direction;
        Limit = limit;
        HasMore = hasMore;
        NextCursor = nextCursor;
        _fetchNext = fetchNext;
    }

    public IReadOnlyList<Dictionary<string, object?>> Items { get; }
    public string Direction { get; }
    public int Limit { get; }
    public bool HasMore { get; }
    public string? NextCursor { get; }

    public bool HasNext() => HasMore && NextCursor is not null;

    public Task<AnnotationEventsPage> NextAsync()
    {
        if (NextCursor is null || _fetchNext is null || !HasNext())
        {
            throw new InvalidOperationException("No more pages available");
        }
        return _fetchNext(NextCursor);
    }
}

public sealed record SockudoOptions(
    string Cluster,
    int ProtocolVersion = 2,
    TimeSpan? ActivityTimeout = null,
    bool? ForceTls = null,
    IReadOnlyList<SockudoTransport>? EnabledTransports = null,
    IReadOnlyList<SockudoTransport>? DisabledTransports = null,
    string? WsHost = null,
    int WsPort = 80,
    int WssPort = 443,
    string WsPath = "",
    string? HttpHost = null,
    int HttpPort = 80,
    int HttpsPort = 443,
    string HttpPath = "/sockudo",
    TimeSpan? PongTimeout = null,
    TimeSpan? UnavailableTimeout = null,
    bool EnableStats = false,
    string StatsHost = "stats.sockudo.io",
    IDictionary<string, object>? TimelineParams = null,
    ChannelAuthorizationOptions? ChannelAuthorization = null,
    UserAuthenticationOptions? UserAuthentication = null,
    PresenceHistoryOptions? PresenceHistory = null,
    VersionedMessagesOptions? VersionedMessages = null,
    DeltaOptions? DeltaCompression = null,
    bool MessageDeduplication = true,
    int MessageDeduplicationCapacity = 1000,
    bool ConnectionRecovery = false,
    bool EchoMessages = true,
    SockudoWireFormat WireFormat = SockudoWireFormat.Json
)
{
    public TimeSpan EffectiveActivityTimeout => ActivityTimeout ?? TimeSpan.FromSeconds(120);
    public TimeSpan EffectivePongTimeout => PongTimeout ?? TimeSpan.FromSeconds(30);
    public TimeSpan EffectiveUnavailableTimeout => UnavailableTimeout ?? TimeSpan.FromSeconds(10);
    public ChannelAuthorizationOptions EffectiveChannelAuthorization => ChannelAuthorization ?? new ChannelAuthorizationOptions();
    public UserAuthenticationOptions EffectiveUserAuthentication => UserAuthentication ?? new UserAuthenticationOptions();
}

public sealed record EventMetadata(string? UserId = null);

public sealed record StateChange(string Previous, string Current);

public sealed record PresenceSubscriptionData(
    IReadOnlyDictionary<string, object?> Hash,
    int Count
);

public sealed record SockudoEvent(
    string Event,
    string? Channel,
    object? Data,
    string? UserId,
    string? MessageId,
    string? StreamId,
    string RawMessage,
    int? Sequence = null,
    string? ConflationKey = null,
    int? Serial = null,
    MessageExtras? Extras = null
);

internal sealed record RecoveryPosition(
    int Serial,
    string? StreamId = null,
    string? LastMessageId = null
);

internal static class JsonSupport
{
    internal static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public static string Encode(object? value) => JsonSerializer.Serialize(Normalize(value), SerializerOptions);

    public static object? Decode(string text) => NormalizeElement(JsonDocument.Parse(text).RootElement);

    public static object? Normalize(object? value)
    {
        return value switch
        {
            null => null,
            JsonElement element => NormalizeElement(element),
            FilterNode filter => filter,
            IDictionary dictionary => dictionary.Keys.Cast<object>().ToDictionary(key => key.ToString()!, key => Normalize(dictionary[key])),
            IEnumerable enumerable when value is not string => enumerable.Cast<object?>().Select(Normalize).ToList(),
            Enum e => e.ToString().ToLowerInvariant(),
            _ => value,
        };
    }

    public static object? NormalizeElement(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.Object => element.EnumerateObject().ToDictionary(prop => prop.Name, prop => NormalizeElement(prop.Value)),
            JsonValueKind.Array => element.EnumerateArray().Select(NormalizeElement).ToList(),
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number when element.TryGetInt64(out var number) => number,
            JsonValueKind.Number => element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => null,
        };
    }
}

internal sealed class EventDispatcher
{
    private readonly Action<string, object?>? _failThrough;
    private readonly Dictionary<string, OrderedDictionary<string, Action<object?, EventMetadata?>>> _callbacks = new();
    private readonly OrderedDictionary<string, Action<string, object?>> _globalCallbacks = new();

    public EventDispatcher(Action<string, object?>? failThrough = null)
    {
        _failThrough = failThrough;
    }

    public string Bind(string eventName, Action<object?, EventMetadata?> callback)
    {
        var token = Guid.NewGuid().ToString("N");
        if (!_callbacks.TryGetValue(eventName, out var callbacks))
        {
            callbacks = new OrderedDictionary<string, Action<object?, EventMetadata?>>();
            _callbacks[eventName] = callbacks;
        }
        callbacks[token] = callback;
        return token;
    }

    public string BindGlobal(Action<string, object?> callback)
    {
        var token = Guid.NewGuid().ToString("N");
        _globalCallbacks[token] = callback;
        return token;
    }

    public void Unbind(string? eventName = null, string? token = null)
    {
        if (eventName is not null && token is null)
        {
            _callbacks.Remove(eventName);
            return;
        }

        if (eventName is not null && token is not null)
        {
            if (_callbacks.TryGetValue(eventName, out var callbacks))
            {
                callbacks.Remove(token);
                if (callbacks.Count == 0)
                {
                    _callbacks.Remove(eventName);
                }
            }
            return;
        }

        if (token is not null)
        {
            foreach (var callbacks in _callbacks.Values)
            {
                callbacks.Remove(token);
            }
            _globalCallbacks.Remove(token);
            return;
        }

        _callbacks.Clear();
        _globalCallbacks.Clear();
    }

    public void Emit(string eventName, object? data, EventMetadata? metadata = null)
    {
        foreach (var callback in _globalCallbacks.Values)
        {
            callback(eventName, data);
        }

        if (!_callbacks.TryGetValue(eventName, out var callbacks) || callbacks.Count == 0)
        {
            _failThrough?.Invoke(eventName, data);
            return;
        }

        foreach (var callback in callbacks.Values)
        {
            callback(data, metadata);
        }
    }
}

internal sealed class OrderedDictionary<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>> where TKey : notnull
{
    private readonly Dictionary<TKey, TValue> _map = new();
    private readonly LinkedList<TKey> _order = new();

    public int Count => _map.Count;
    public IEnumerable<TValue> Values => _order.Select(key => _map[key]);

    public TValue this[TKey key]
    {
        get => _map[key];
        set
        {
            if (!_map.ContainsKey(key))
            {
                _order.AddLast(key);
            }
            _map[key] = value;
        }
    }

    public bool Remove(TKey key)
    {
        if (!_map.Remove(key))
        {
            return false;
        }
        _order.Remove(key);
        return true;
    }

    public void Clear()
    {
        _map.Clear();
        _order.Clear();
    }

    public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() => _order.Select(key => new KeyValuePair<TKey, TValue>(key, _map[key])).GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

internal static class DictionaryValue
{
    public static TValue? Get<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key) where TKey : notnull
    {
        return dictionary.TryGetValue(key, out var value) ? value : default;
    }
}

internal sealed class MessageDeduplicator
{
    private readonly int _capacity;
    private readonly OrderedDictionary<string, bool> _seen = new();

    public MessageDeduplicator(int capacity)
    {
        _capacity = capacity;
    }

    public bool IsDuplicate(string messageId) => _seen.Any(entry => entry.Key == messageId);

    public void Track(string messageId)
    {
        _seen.Remove(messageId);
        _seen[messageId] = true;
        while (_seen.Count > _capacity)
        {
            _seen.Remove(_seen.First().Key);
        }
    }
}

internal sealed class ProtocolPrefix
{
    public ProtocolPrefix(int version)
    {
        Version = version >= 2 ? "2" : "7";
        EventPrefix = version >= 2 ? "sockudo:" : "pusher:";
        InternalPrefix = version >= 2 ? "sockudo_internal:" : "pusher_internal:";
    }

    public string Version { get; }
    public string EventPrefix { get; }
    public string InternalPrefix { get; }

    public string Event(string name) => $"{EventPrefix}{name}";
    public string Internal(string name) => $"{InternalPrefix}{name}";
    public bool IsInternalEvent(string name) => name.StartsWith(InternalPrefix, StringComparison.Ordinal);
    public bool IsPlatformEvent(string name) => name.StartsWith(EventPrefix, StringComparison.Ordinal);
}

internal static class QueryString
{
    public static string Encode(IDictionary<string, object> parameters)
    {
        return string.Join(
            "&",
            parameters
                .OrderBy(entry => entry.Key, StringComparer.Ordinal)
                .Select(entry => $"{Uri.EscapeDataString(entry.Key)}={Uri.EscapeDataString(ValueToString(entry.Value))}")
        );
    }

    private static string ValueToString(object value) => value switch
    {
        bool flag => flag ? "true" : "false",
        Enum e => e.ToString().ToLowerInvariant(),
        _ => value.ToString() ?? string.Empty,
    };
}
