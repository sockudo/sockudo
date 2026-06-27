using System.Collections.Concurrent;
using System.Net.Http.Json;
using System.Net.Http.Headers;
using System.Net.WebSockets;
using System.Text;
using Sodium;
using VCDiff.Decoders;

namespace Sockudo.Client;

public class SockudoException : Exception
{
    public SockudoException(string message) : base(message)
    {
    }
}

public sealed class AuthFailure : SockudoException
{
    public AuthFailure(int? statusCode, string message) : base(message)
    {
        StatusCode = statusCode;
    }

    public int? StatusCode { get; }
}

public class TokenAuthException : SockudoException
{
    public TokenAuthException(int code, string reason, string message) : base(message)
    {
        Code = code;
        Reason = reason;
    }

    public int StatusCode => 401;
    public int Code { get; }
    public string Reason { get; }
}

public sealed class TokenExpiredException : TokenAuthException
{
    public TokenExpiredException(string reason = "expired")
        : base(40142, reason, "Sockudo capability token expired")
    {
    }
}

public sealed class TokenRevokedException : TokenAuthException
{
    public TokenRevokedException(string reason = "revoked")
        : base(40160, reason, "Sockudo capability token was revoked")
    {
    }
}

public sealed class UnsupportedFeature : SockudoException
{
    public UnsupportedFeature(string message) : base(message)
    {
    }
}

public sealed class BadEventName : SockudoException
{
    public BadEventName(string message) : base(message)
    {
    }
}

public sealed class SockudoClient : IAsyncDisposable
{
    private readonly HttpClient _httpClient;
    private readonly ProtocolPrefix _prefix;
    private readonly EventDispatcher _dispatcher = new();
    private readonly Dictionary<string, SockudoChannel> _channels = new(StringComparer.Ordinal);
    private readonly MessageDeduplicator? _deduplicator;
    private readonly SemaphoreSlim _socketGate = new(1, 1);
    private readonly Dictionary<string, RecoveryPosition> _channelPositions = new(StringComparer.Ordinal);
    private readonly DeltaCompressionManager? _deltaManager;
    private ClientWebSocket? _socket;
    private CancellationTokenSource? _socketCts;
    private Task? _receiveLoop;
    private Task? _activityLoop;
    private Task? _retryLoop;
    private Task? _unavailableLoop;
    private Task? _authRefreshLoop;
    private CancellationTokenSource? _authRefreshCts;
    private bool _manuallyDisconnected;
    private SockudoTransport? _currentTransport;
    private bool _attemptedFallback;
    private string? _authToken;

    public SockudoClient(string key, SockudoOptions options, HttpClient? httpClient = null)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            throw new SockudoException("You must pass your app key when you instantiate SockudoClient.");
        }
        if (string.IsNullOrWhiteSpace(options.Cluster))
        {
            throw new SockudoException("Options must provide a cluster.");
        }

        Key = key;
        Options = options;
        _httpClient = httpClient ?? new HttpClient();
        _prefix = new ProtocolPrefix(options.ProtocolVersion);
        _deduplicator = options.MessageDeduplication ? new MessageDeduplicator(options.MessageDeduplicationCapacity) : null;
        _deltaManager = options.DeltaCompression is not null
            ? new DeltaCompressionManager(options.DeltaCompression, SendEventAsync, _prefix)
            : null;
        User = new UserFacade(this);
        Watchlist = new WatchlistFacade();
    }

    public string Key { get; }
    public SockudoOptions Options { get; }
    public ConnectionState ConnectionState { get; private set; } = ConnectionState.Initialized;
    public string? SocketId { get; private set; }
    public UserFacade User { get; }
    public WatchlistFacade Watchlist { get; }

    public string Bind(string eventName, Action<object?, EventMetadata?> callback) => _dispatcher.Bind(eventName, callback);
    public string BindGlobal(Action<string, object?> callback) => _dispatcher.BindGlobal(callback);
    public void Unbind(string? eventName = null, string? token = null) => _dispatcher.Unbind(eventName, token);
    public SockudoChannel? Channel(string name) => _channels.GetValueOrDefault(name);
    public DeltaStats? GetDeltaStats() => _deltaManager?.GetStats();
    public void ResetDeltaStats() => _deltaManager?.ResetStats();

    public string SocketUrl(SockudoTransport transport)
    {
        var scheme = transport == SockudoTransport.Wss ? "wss" : "ws";
        var host = Options.WsHost ?? $"ws-{Options.Cluster}.sockudo.io";
        var port = transport == SockudoTransport.Wss ? Options.WssPort : Options.WsPort;
        var path = string.IsNullOrEmpty(Options.WsPath) ? $"/app/{Key}" : $"{Options.WsPath}/app/{Key}";
        var query = new Dictionary<string, object>(StringComparer.Ordinal)
        {
            ["protocol"] = _prefix.Version,
            ["client"] = "csharp",
            ["version"] = "2.1.0",
            ["flash"] = false,
        };
        if (Options.ProtocolVersion >= 2)
        {
            Options.ValidateAppendRollupWindow();
            query["format"] = Options.WireFormat;
            query["append_mode"] = Options.AppendMode;
            query["echo_messages"] = Options.EchoMessages;
            if (Options.AppendRollupWindow is not null)
            {
                query["append_rollup_window"] = Options.AppendRollupWindow.Value;
            }
            var token = _authToken ?? Options.TokenAuthentication?.Token;
            if (!string.IsNullOrWhiteSpace(token))
            {
                query["token"] = token;
            }
        }

        var builder = new UriBuilder
        {
            Scheme = scheme,
            Host = host,
            Port = port,
            Path = path,
        };
        return $"{builder.Uri.GetLeftPart(UriPartial.Path)}?{QueryString.Encode(query)}";
    }

    public SockudoChannel Subscribe(string channelName, SubscriptionOptions? subscriptionOptions = null)
    {
        if (!_channels.TryGetValue(channelName, out var channel))
        {
            channel = CreateChannel(channelName);
            _channels[channelName] = channel;
        }

        if (subscriptionOptions is not null)
        {
            channel.Filter = subscriptionOptions.Filter;
            channel.DeltaSettings = subscriptionOptions.Delta;
            channel.EventsFilter = subscriptionOptions.Events;
            channel.Rewind = subscriptionOptions.Rewind;
            channel.AnnotationSubscribe = subscriptionOptions.AnnotationSubscribe;
        }

        channel.SubscribeIfPossible();
        return channel;
    }

    public async Task UnsubscribeAsync(string channelName)
    {
        if (!_channels.TryGetValue(channelName, out var channel))
        {
            return;
        }

        if (channel.SubscriptionPending)
        {
            channel.SubscriptionCancelled = true;
        }
        else if (channel.IsSubscribed)
        {
            _channels.Remove(channelName);
            await channel.UnsubscribeAsync().ConfigureAwait(false);
        }
        else
        {
            _channels.Remove(channelName);
        }

        _channelPositions.Remove(channelName);
        _deltaManager?.ClearChannelState(channelName);
    }

    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        await _socketGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_socket is not null)
            {
                return;
            }

            var transports = TransportSequence();
            if (transports.Count == 0)
            {
                UpdateState(ConnectionState.Failed);
                return;
            }

            _manuallyDisconnected = false;
            _attemptedFallback = false;
            UpdateState(ConnectionState.Connecting);
            await OpenWebSocketAsync(transports[0], cancellationToken).ConfigureAwait(false);
            SetUnavailableTimer();
        }
        finally
        {
            _socketGate.Release();
        }
    }

    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        _manuallyDisconnected = true;
        CancelTimers();

        var socket = _socket;
        _socket = null;
        if (socket is not null)
        {
            try
            {
                _socketCts?.Cancel();
                if (socket.State == WebSocketState.Open || socket.State == WebSocketState.CloseReceived)
                {
                    await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "disconnect", cancellationToken).ConfigureAwait(false);
                }
            }
            catch
            {
            }
            socket.Dispose();
        }

        foreach (var channel in _channels.Values)
        {
            channel.Disconnect();
        }

        SocketId = null;
        User.Cleanup();
        UpdateState(ConnectionState.Disconnected);
    }

    public async Task<bool> SendEventAsync(string eventName, object? data, string? channelName = null, CancellationToken cancellationToken = default)
    {
        var socket = _socket;
        if (socket is null || socket.State != WebSocketState.Open)
        {
            return false;
        }

        var payload = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["event"] = eventName,
            ["data"] = data,
        };
        if (channelName is not null)
        {
            payload["channel"] = channelName;
        }

        var encoded = ProtocolCodec.EncodeEnvelope(payload, Options.WireFormat);
        if (encoded is string text)
        {
            await socket.SendAsync(Encoding.UTF8.GetBytes(text), WebSocketMessageType.Text, true, cancellationToken).ConfigureAwait(false);
            return true;
        }

        await socket.SendAsync((byte[])encoded, WebSocketMessageType.Binary, true, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public async Task<bool> RefreshAuthAsync(CancellationToken cancellationToken = default)
    {
        if (Options.ProtocolVersion < 2)
        {
            throw new UnsupportedFeature("Token auth refresh is only supported on Protocol V2 connections.");
        }

        var token = await ResolveAuthTokenAsync(cancellationToken).ConfigureAwait(false);
        _authToken = token;
        var sent = await SendEventAsync(
            _prefix.Event("auth"),
            new Dictionary<string, object?> { ["token"] = token },
            null,
            cancellationToken).ConfigureAwait(false);
        ScheduleAuthRefreshIfPossible(token);
        return sent;
    }

    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync().ConfigureAwait(false);
        _httpClient.Dispose();
        _socketGate.Dispose();
        _socketCts?.Dispose();
        _authRefreshCts?.Dispose();
    }

    private SockudoChannel CreateChannel(string name)
    {
        if (name.StartsWith("private-encrypted-", StringComparison.Ordinal))
        {
            return new EncryptedChannel(name, this);
        }
        if (name.StartsWith("presence-", StringComparison.Ordinal))
        {
            return new PresenceChannel(name, this);
        }
        if (name.StartsWith("private-", StringComparison.Ordinal))
        {
            return new PrivateChannel(name, this);
        }
        return new SockudoChannel(name, this);
    }

    private async Task OpenWebSocketAsync(SockudoTransport transport, CancellationToken cancellationToken)
    {
        _currentTransport = transport;
        _socketCts?.Cancel();
        _socketCts?.Dispose();
        _socketCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        await EnsureUrlAuthTokenAsync(_socketCts.Token).ConfigureAwait(false);

        var socket = new ClientWebSocket();
        if (Options.ProtocolVersion >= 2)
        {
            socket.Options.KeepAliveInterval = Options.EffectiveActivityTimeout;
            socket.Options.KeepAliveTimeout = Options.EffectivePongTimeout;
        }
        _socket = socket;
        await socket.ConnectAsync(new Uri(SocketUrl(transport)), _socketCts.Token).ConfigureAwait(false);
        if (_authToken is not null)
        {
            ScheduleAuthRefreshIfPossible(_authToken);
        }
        _receiveLoop = ReceiveLoopAsync(socket, _socketCts.Token);
    }

    private async Task EnsureUrlAuthTokenAsync(CancellationToken cancellationToken)
    {
        if (Options.ProtocolVersion < 2 || Options.TokenAuthentication is null)
        {
            return;
        }

        _authToken = await ResolveAuthTokenAsync(cancellationToken).ConfigureAwait(false);
    }

    private void ScheduleAuthRefreshIfPossible(string token)
    {
        CancelAuthRefreshTimer();
        if (Options.ProtocolVersion < 2 || Options.TokenAuthentication?.TokenProvider is null)
        {
            return;
        }

        var refreshAt = TryGetJwtRefreshTime(token);
        if (refreshAt is null)
        {
            return;
        }

        var delay = refreshAt.Value - DateTimeOffset.UtcNow;
        if (delay < TimeSpan.Zero)
        {
            delay = TimeSpan.Zero;
        }

        var cts = new CancellationTokenSource();
        _authRefreshCts = cts;
        _authRefreshLoop = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(delay, cts.Token).ConfigureAwait(false);
                if (!cts.Token.IsCancellationRequested)
                {
                    await RefreshAuthAsync(cts.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception exception)
            {
                _dispatcher.Emit("error", exception);
            }
        });
    }

    private static DateTimeOffset? TryGetJwtRefreshTime(string token)
    {
        var parts = token.Split('.');
        if (parts.Length < 2)
        {
            return null;
        }

        try
        {
            var payload = JsonSupport.Decode(Encoding.UTF8.GetString(Base64UrlDecode(parts[1]))) as Dictionary<string, object?>;
            var exp = ProtocolCodec.CoerceLong(payload?.Get("exp"));
            if (exp is null)
            {
                return null;
            }

            var now = DateTimeOffset.UtcNow;
            var issuedAt = ProtocolCodec.CoerceLong(payload?.Get("iat")) ?? now.ToUnixTimeSeconds();
            var expiresAt = exp.Value;
            if (expiresAt <= issuedAt)
            {
                return now;
            }

            return DateTimeOffset.FromUnixTimeSeconds(issuedAt)
                .AddSeconds((expiresAt - issuedAt) * 0.8);
        }
        catch
        {
            return null;
        }
    }

    private static byte[] Base64UrlDecode(string value)
    {
        var encoded = value.Replace('-', '+').Replace('_', '/');
        encoded = encoded.PadRight(encoded.Length + ((4 - encoded.Length % 4) % 4), '=');
        return Convert.FromBase64String(encoded);
    }

    private async Task<string> ResolveAuthTokenAsync(CancellationToken cancellationToken)
    {
        var options = Options.TokenAuthentication ?? throw new UnsupportedFeature(
            "TokenAuthentication must be configured before token auth can be used.");

        var token = options.TokenProvider is not null
            ? await options.TokenProvider(cancellationToken).ConfigureAwait(false)
            : options.Token;

        if (string.IsNullOrWhiteSpace(token))
        {
            throw new AuthFailure(null, "TokenAuthentication returned an empty token.");
        }
        return token;
    }

    private async Task HandleTokenExpiredAsync(object? data)
    {
        var payload = data as Dictionary<string, object?>;
        var code = ProtocolCodec.CoerceInt(payload?.Get("code")) ?? 40142;
        var reason = payload?.Get("reason") as string ?? (code == 40160 ? "revoked" : "expired");

        _dispatcher.Emit(_prefix.Event("token_expired"), data);

        if (code == 40160)
        {
            _dispatcher.Emit("error", new TokenRevokedException(reason));
            return;
        }

        if (Options.TokenAuthentication?.TokenProvider is not null)
        {
            try
            {
                await RefreshAuthAsync().ConfigureAwait(false);
                return;
            }
            catch (Exception exception)
            {
                _dispatcher.Emit("error", exception);
                return;
            }
        }

        _dispatcher.Emit("error", new TokenExpiredException(reason));
    }

    private async Task ReceiveLoopAsync(ClientWebSocket socket, CancellationToken cancellationToken)
    {
        try
        {
            while (socket.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
            {
                var (payload, messageType) = await ReceiveMessageAsync(socket, cancellationToken).ConfigureAwait(false);
                if (messageType == WebSocketMessageType.Close)
                {
                    break;
                }

                await HandleRawMessageAsync(payload, messageType).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception exception)
        {
            _dispatcher.Emit("error", exception);
        }

        await HandleSocketClosedAsync().ConfigureAwait(false);
    }

    private static async Task<(byte[] Payload, WebSocketMessageType Type)> ReceiveMessageAsync(ClientWebSocket socket, CancellationToken cancellationToken)
    {
        var buffer = new byte[8192];
        using var stream = new MemoryStream();
        WebSocketReceiveResult result;
        do
        {
            result = await socket.ReceiveAsync(buffer, cancellationToken).ConfigureAwait(false);
            if (result.Count > 0)
            {
                stream.Write(buffer, 0, result.Count);
            }
        }
        while (!result.EndOfMessage);

        return (stream.ToArray(), result.MessageType);
    }

    private async Task HandleRawMessageAsync(byte[] payload, WebSocketMessageType messageType)
    {
        object rawMessage = messageType == WebSocketMessageType.Text ? Encoding.UTF8.GetString(payload) : payload;
        try
        {
            var @event = ProtocolCodec.DecodeEvent(rawMessage, Options.WireFormat);

            if (@event.MessageId is not null && _deduplicator is not null)
            {
                if (_deduplicator.IsDuplicate(@event.MessageId))
                {
                    return;
                }
                _deduplicator.Track(@event.MessageId);
            }

            ResetActivityTimer();

            if (Options.ConnectionRecovery && @event.Channel is not null && (@event.Serial is not null || @event.SerialText is not null))
            {
                _channelPositions[@event.Channel] = new RecoveryPosition(
                    @event.Serial is not null ? @event.Serial.Value : @event.SerialText!,
                    @event.StreamId,
                    @event.MessageId
                );
            }

            if (@event.Event == _prefix.Event("connection_established"))
            {
                var payloadData = @event.Data as Dictionary<string, object?> ?? new Dictionary<string, object?>(StringComparer.Ordinal);
                SocketId = payloadData.Get("socket_id") as string ?? throw new SockudoException("Invalid handshake");
                UpdateState(ConnectionState.Connected, new Dictionary<string, object?> { ["socket_id"] = SocketId });

                foreach (var channel in _channels.Values)
                {
                    channel.SubscribeIfPossible();
                }

                if (Options.ConnectionRecovery && _channelPositions.Count > 0)
                {
                    var channelPositions = _channelPositions.ToDictionary(
                        entry => entry.Key,
                        entry => (object?)new Dictionary<string, object?>
                        {
                            ["serial"] = entry.Value.Serial,
                            ["stream_id"] = entry.Value.StreamId,
                            ["last_message_id"] = entry.Value.LastMessageId,
                        }.Where(pair => pair.Value is not null)
                         .ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal),
                        StringComparer.Ordinal
                    );
                    await SendEventAsync(
                        _prefix.Event("resume"),
                        new Dictionary<string, object?> { ["channel_positions"] = channelPositions },
                        null
                    ).ConfigureAwait(false);
                }

                if (Options.DeltaCompression?.Enabled == true && _deltaManager is not null)
                {
                    await _deltaManager.EnableAsync().ConfigureAwait(false);
                }

                await User.HandleConnectedAsync().ConfigureAwait(false);
                return;
            }

            if (@event.Event == _prefix.Event("error"))
            {
                _dispatcher.Emit("error", @event.Data);
                return;
            }

            if (@event.Event == _prefix.Event("ping"))
            {
                await SendEventAsync(_prefix.Event("pong"), new Dictionary<string, object?>()).ConfigureAwait(false);
                return;
            }

            if (@event.Event == _prefix.Event("auth_success"))
            {
                _dispatcher.Emit(@event.Event, @event.Data);
                return;
            }

            if (@event.Event == _prefix.Event("token_expired"))
            {
                await HandleTokenExpiredAsync(@event.Data).ConfigureAwait(false);
                return;
            }

            if (@event.Event == _prefix.Event("signin_success"))
            {
                await User.HandleSignInSuccessAsync(@event.Data).ConfigureAwait(false);
                return;
            }

            if (@event.Event == _prefix.Internal("watchlist_events"))
            {
                Watchlist.Handle(@event.Data);
                return;
            }

            if (@event.Event == _prefix.Event("resume_failed"))
            {
                var payloadData = @event.Data as Dictionary<string, object?>;
                var channelName = payloadData?.Get("channel") as string;
                if (channelName is not null)
                {
                    _channelPositions.Remove(channelName);
                    _deltaManager?.ClearChannelState(channelName);
                    if (_channels.TryGetValue(channelName, out var failedChannel))
                    {
                        failedChannel.ForceResubscribe();
                    }
                }
                _dispatcher.Emit(@event.Event, @event.Data);
                return;
            }

            if (@event.Event == _prefix.Event("resume_success"))
            {
                _dispatcher.Emit(@event.Event, @event.Data);
                return;
            }

            if (@event.Event == _prefix.Event("delta_compression_enabled") && _deltaManager is not null)
            {
                _deltaManager.HandleEnabled(@event.Data);
                _dispatcher.Emit(@event.Event, @event.Data);
                return;
            }

            if (@event.Event == _prefix.Event("delta_cache_sync") && _deltaManager is not null && @event.Channel is not null)
            {
                _deltaManager.HandleCacheSync(@event.Channel, @event.Data);
                return;
            }

            if (@event.Event == _prefix.Event("delta") && _deltaManager is not null && @event.Channel is not null)
            {
                var reconstructed = await _deltaManager.HandleDeltaMessageAsync(@event.Channel, @event.Data).ConfigureAwait(false);
                if (reconstructed is not null)
                {
                    if (_channels.TryGetValue(@event.Channel, out var channel))
                    {
                        channel.Handle(reconstructed);
                    }
                    _dispatcher.Emit(reconstructed.Event, reconstructed.Data, new EventMetadata(reconstructed.UserId));
                }
                return;
            }

            if (@event.Channel is not null && _channels.TryGetValue(@event.Channel, out var subscribedChannel))
            {
                subscribedChannel.Handle(@event);
                if (!_prefix.IsPlatformEvent(@event.Event) &&
                    !_prefix.IsInternalEvent(@event.Event) &&
                    @event.Sequence is not null &&
                    _deltaManager is not null)
                {
                    _deltaManager.HandleFullMessage(@event.Channel, StripDeltaMetadata(@event.RawMessage), @event.Sequence, @event.ConflationKey);
                }
            }

            if (!_prefix.IsInternalEvent(@event.Event))
            {
                _dispatcher.Emit(@event.Event, @event.Data, new EventMetadata(@event.UserId));
            }
        }
        catch (Exception exception)
        {
            _dispatcher.Emit("error", exception);
        }
    }

    private async Task HandleSocketClosedAsync()
    {
        _socket?.Dispose();
        _socket = null;
        CancelActivityTimer();
        CancelAuthRefreshTimer();
        ClearUnavailableTimer();
        SocketId = null;

        foreach (var channel in _channels.Values)
        {
            channel.Disconnect();
        }

        User.Cleanup();

        if (!_manuallyDisconnected)
        {
            await ScheduleRetryAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
        }
    }

    private async Task ScheduleRetryAsync(TimeSpan delay)
    {
        _retryLoop?.DisposeSafe();
        _retryLoop = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(delay).ConfigureAwait(false);
                if (_manuallyDisconnected)
                {
                    return;
                }

                UpdateState(ConnectionState.Connecting);
                var transports = TransportSequence();
                var nextTransport = _currentTransport == SockudoTransport.Ws &&
                                    !_attemptedFallback &&
                                    transports.Contains(SockudoTransport.Wss)
                    ? SockudoTransport.Wss
                    : (transports.FirstOrDefault());
                _attemptedFallback = nextTransport == SockudoTransport.Wss && _currentTransport == SockudoTransport.Ws;
                await OpenWebSocketAsync(nextTransport, CancellationToken.None).ConfigureAwait(false);
                SetUnavailableTimer();
            }
            catch (Exception exception)
            {
                _dispatcher.Emit("error", exception);
            }
        });
    }

    private List<SockudoTransport> TransportSequence()
    {
        var transports = (Options.ForceTls is false
            ? new[] { SockudoTransport.Ws, SockudoTransport.Wss }
            : new[] { SockudoTransport.Wss }).ToList();

        if (Options.EnabledTransports is not null)
        {
            transports = transports.Where(Options.EnabledTransports.Contains).ToList();
        }
        if (Options.DisabledTransports is not null)
        {
            transports = transports.Where(transport => !Options.DisabledTransports.Contains(transport)).ToList();
        }
        return transports;
    }

    private void UpdateState(ConnectionState state, object? metadata = null)
    {
        var previous = ConnectionState;
        ConnectionState = state;
        _dispatcher.Emit("state_change", new StateChange(previous.ToString().ToLowerInvariant(), state.ToString().ToLowerInvariant()));
        _dispatcher.Emit(state.ToString().ToLowerInvariant(), metadata);
    }

    private void CancelActivityTimer()
    {
        _activityLoop?.DisposeSafe();
        _activityLoop = null;
    }

    private void ResetActivityTimer()
    {
        CancelActivityTimer();
        if (Options.ProtocolVersion >= 2)
        {
            return;
        }
        _activityLoop = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(Options.EffectiveActivityTimeout).ConfigureAwait(false);
                await SendEventAsync(_prefix.Event("ping"), new Dictionary<string, object?>()).ConfigureAwait(false);
            }
            catch
            {
            }
        });
    }

    private void SetUnavailableTimer()
    {
        ClearUnavailableTimer();
        _unavailableLoop = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(Options.EffectiveUnavailableTimeout).ConfigureAwait(false);
                UpdateState(ConnectionState.Unavailable);
            }
            catch
            {
            }
        });
    }

    private void ClearUnavailableTimer()
    {
        _unavailableLoop?.DisposeSafe();
        _unavailableLoop = null;
    }

    private void CancelAuthRefreshTimer()
    {
        _authRefreshCts?.Cancel();
        _authRefreshCts?.Dispose();
        _authRefreshCts = null;
        _authRefreshLoop?.DisposeSafe();
        _authRefreshLoop = null;
    }

    private void CancelTimers()
    {
        CancelActivityTimer();
        CancelAuthRefreshTimer();
        ClearUnavailableTimer();
        _retryLoop?.DisposeSafe();
        _retryLoop = null;
    }

    private static string StripDeltaMetadata(string rawMessage) => rawMessage;

    internal async Task<ChannelAuthorizationData> AuthorizeChannelAsync(ChannelAuthorizationRequest request)
    {
        var options = Options.EffectiveChannelAuthorization;
        if (options.CustomHandler is not null)
        {
            return await options.CustomHandler(request).ConfigureAwait(false);
        }

        var parameters = new Dictionary<string, object>(StringComparer.Ordinal);
        if (options.Params is not null)
        {
            foreach (var entry in options.Params)
            {
                parameters[entry.Key] = entry.Value!;
            }
        }
        if (options.ParamsProvider is not null)
        {
            foreach (var entry in options.ParamsProvider())
            {
                parameters[entry.Key] = entry.Value!;
            }
        }
        parameters["socket_id"] = request.SocketId;
        parameters["channel_name"] = request.ChannelName;

        var payload = await PerformAuthRequestAsync(options.Endpoint, options.Headers, options.HeadersProvider, parameters).ConfigureAwait(false);
        var auth = payload.Get("auth") as string;
        if (auth is null)
        {
            throw new AuthFailure(200, "JSON returned from auth endpoint was invalid");
        }
        return new ChannelAuthorizationData(auth, payload.Get("channel_data") as string, payload.Get("shared_secret") as string);
    }

    internal async Task<UserAuthenticationData> AuthenticateUserAsync(UserAuthenticationRequest request)
    {
        var options = Options.EffectiveUserAuthentication;
        if (options.CustomHandler is not null)
        {
            return await options.CustomHandler(request).ConfigureAwait(false);
        }

        var parameters = new Dictionary<string, object>(StringComparer.Ordinal);
        if (options.Params is not null)
        {
            foreach (var entry in options.Params)
            {
                parameters[entry.Key] = entry.Value!;
            }
        }
        if (options.ParamsProvider is not null)
        {
            foreach (var entry in options.ParamsProvider())
            {
                parameters[entry.Key] = entry.Value!;
            }
        }
        parameters["socket_id"] = request.SocketId;

        var payload = await PerformAuthRequestAsync(options.Endpoint, options.Headers, options.HeadersProvider, parameters).ConfigureAwait(false);
        var auth = payload.Get("auth") as string;
        var userData = payload.Get("user_data") as string;
        if (auth is null || userData is null)
        {
            throw new AuthFailure(200, "JSON returned from auth endpoint was invalid");
        }
        return new UserAuthenticationData(auth, userData);
    }

    private async Task<Dictionary<string, object?>> PerformAuthRequestAsync(
        string endpoint,
        IDictionary<string, string>? staticHeaders,
        Func<IDictionary<string, string>>? dynamicHeaders,
        IDictionary<string, object> parameters)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, endpoint);
        request.Content = new FormUrlEncodedContent(parameters.ToDictionary(
            entry => entry.Key,
            entry => entry.Value switch
            {
                bool flag => flag ? "true" : "false",
                Enum value => value.ToString().ToLowerInvariant(),
                _ => entry.Value.ToString() ?? string.Empty,
            },
            StringComparer.Ordinal));

        if (staticHeaders is not null)
        {
            foreach (var header in staticHeaders)
            {
                request.Headers.TryAddWithoutValidation(header.Key, header.Value);
            }
        }
        if (dynamicHeaders is not null)
        {
            foreach (var header in dynamicHeaders())
            {
                request.Headers.TryAddWithoutValidation(header.Key, header.Value);
            }
        }

        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/x-www-form-urlencoded");
        using var response = await _httpClient.SendAsync(request).ConfigureAwait(false);
        if ((int)response.StatusCode >= 400)
        {
            throw new AuthFailure((int)response.StatusCode, $"Could not get auth info from endpoint, status: {(int)response.StatusCode}");
        }

        var payload = JsonSupport.Decode(await response.Content.ReadAsStringAsync().ConfigureAwait(false)) as Dictionary<string, object?>;
        if (payload is null)
        {
            throw new AuthFailure((int)response.StatusCode, "JSON returned from auth endpoint was invalid");
        }
        return payload;
    }

    internal async Task<PresenceHistoryPage> FetchPresenceHistoryAsync(
        string channelName,
        PresenceHistoryParams parameters,
        CancellationToken cancellationToken = default)
    {
        var config = Options.PresenceHistory ?? throw new UnsupportedFeature(
            "PresenceHistory.Endpoint must be configured to use presence.history(). This endpoint should proxy requests to the Sockudo server REST API.");

        var payload = await PerformPresenceHistoryRequestAsync(
            config.Endpoint,
            config.Headers,
            config.HeadersProvider,
            channelName,
            parameters.ToPayload(),
            "history",
            cancellationToken).ConfigureAwait(false);

        return DecodePresenceHistoryPage(
            payload,
            cursor => FetchPresenceHistoryAsync(channelName, parameters with { Cursor = cursor }, cancellationToken));
    }

    internal async Task<PresenceSnapshot> FetchPresenceSnapshotAsync(
        string channelName,
        PresenceSnapshotParams parameters,
        CancellationToken cancellationToken = default)
    {
        var config = Options.PresenceHistory ?? throw new UnsupportedFeature(
            "PresenceHistory.Endpoint must be configured to use presence.snapshot(). This endpoint should proxy requests to the Sockudo server REST API.");

        var payload = await PerformPresenceHistoryRequestAsync(
            config.Endpoint,
            config.Headers,
            config.HeadersProvider,
            channelName,
            parameters.ToPayload(),
            "snapshot",
            cancellationToken).ConfigureAwait(false);

        return DecodePresenceSnapshot(payload);
    }

    internal async Task<ChannelHistoryPageProxy> FetchChannelHistoryAsync(
        string channelName,
        ChannelHistoryParams parameters,
        CancellationToken cancellationToken = default)
    {
        var config = Options.VersionedMessages ?? throw new UnsupportedFeature(
            "VersionedMessages.Endpoint must be configured to use channelHistory(). This endpoint should proxy requests to the Sockudo server REST API.");

        var payload = await PerformPresenceHistoryRequestAsync(
            config.Endpoint,
            config.Headers,
            config.HeadersProvider,
            channelName,
            parameters.ToPayload(),
            "channel_history",
            cancellationToken).ConfigureAwait(false);

        return DecodeChannelHistoryPage(
            payload,
            cursor => FetchChannelHistoryAsync(channelName, parameters with { Cursor = cursor }, cancellationToken));
    }

    internal async Task<Dictionary<string, object?>> FetchLatestMessageAsync(
        string channelName,
        string messageSerial,
        CancellationToken cancellationToken = default)
    {
        var config = Options.VersionedMessages ?? throw new UnsupportedFeature(
            "VersionedMessages.Endpoint must be configured to use getMessage(). This endpoint should proxy requests to the Sockudo server REST API.");

        var payload = await PerformPresenceHistoryRequestAsync(
            config.Endpoint,
            config.Headers,
            config.HeadersProvider,
            channelName,
            new Dictionary<string, object>(),
            "get_message",
            cancellationToken,
            messageSerial).ConfigureAwait(false);

        return payload.Get("item") as Dictionary<string, object?> ?? new Dictionary<string, object?>();
    }

    internal async Task<MessageVersionsPage> FetchMessageVersionsAsync(
        string channelName,
        string messageSerial,
        MessageVersionsParams parameters,
        CancellationToken cancellationToken = default)
    {
        var config = Options.VersionedMessages ?? throw new UnsupportedFeature(
            "VersionedMessages.Endpoint must be configured to use getMessageVersions(). This endpoint should proxy requests to the Sockudo server REST API.");

        var payload = await PerformPresenceHistoryRequestAsync(
            config.Endpoint,
            config.Headers,
            config.HeadersProvider,
            channelName,
            parameters.ToPayload(),
            "get_message_versions",
            cancellationToken,
            messageSerial).ConfigureAwait(false);

        return DecodeMessageVersionsPage(
            payload,
            channelName,
            cursor => FetchMessageVersionsAsync(channelName, messageSerial, parameters with { Cursor = cursor }, cancellationToken));
    }

    internal async Task<PublishAnnotationResponse> PublishAnnotationAsync(
        string channelName,
        string messageSerial,
        PublishAnnotationRequest annotation,
        CancellationToken cancellationToken = default)
    {
        var config = Options.VersionedMessages ?? throw new UnsupportedFeature(
            "VersionedMessages.Endpoint must be configured to use publishAnnotation(). This endpoint should proxy requests to the Sockudo server REST API.");

        var payload = await PerformPresenceHistoryRequestAsync(
            config.Endpoint,
            config.Headers,
            config.HeadersProvider,
            channelName,
            new Dictionary<string, object>(),
            "publish_annotation",
            cancellationToken,
            messageSerial,
            annotation: annotation.ToPayload()).ConfigureAwait(false);

        return new PublishAnnotationResponse(
            payload.Get("annotation") as Dictionary<string, object?> ?? new Dictionary<string, object?>(),
            payload.Get("summary") as Dictionary<string, object?>);
    }

    internal async Task<DeleteAnnotationResponse> DeleteAnnotationAsync(
        string channelName,
        string messageSerial,
        string annotationSerial,
        string? socketId,
        CancellationToken cancellationToken = default)
    {
        var config = Options.VersionedMessages ?? throw new UnsupportedFeature(
            "VersionedMessages.Endpoint must be configured to use deleteAnnotation(). This endpoint should proxy requests to the Sockudo server REST API.");

        var payload = await PerformPresenceHistoryRequestAsync(
            config.Endpoint,
            config.Headers,
            config.HeadersProvider,
            channelName,
            new Dictionary<string, object>(),
            "delete_annotation",
            cancellationToken,
            messageSerial,
            annotationSerial,
            socketId).ConfigureAwait(false);

        return new DeleteAnnotationResponse(
            payload.Get("deleted") as bool? ?? false,
            payload.Get("annotationSerial") as string ?? annotationSerial,
            payload.Get("summary") as Dictionary<string, object?>);
    }

    internal async Task<AnnotationEventsPage> ListAnnotationsAsync(
        string channelName,
        string messageSerial,
        AnnotationEventsParams parameters,
        CancellationToken cancellationToken = default)
    {
        var config = Options.VersionedMessages ?? throw new UnsupportedFeature(
            "VersionedMessages.Endpoint must be configured to use listAnnotations(). This endpoint should proxy requests to the Sockudo server REST API.");

        var payload = await PerformPresenceHistoryRequestAsync(
            config.Endpoint,
            config.Headers,
            config.HeadersProvider,
            channelName,
            parameters.ToPayload(),
            "list_annotations",
            cancellationToken,
            messageSerial).ConfigureAwait(false);

        return DecodeAnnotationEventsPage(
            payload,
            cursor => ListAnnotationsAsync(channelName, messageSerial, parameters with { Cursor = cursor }, cancellationToken));
    }

    internal async Task<VersionedMessageAck> PublishVersionedMessageAsync(
        string channelName,
        string eventName,
        object? data,
        VersionedMessageCreateOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var payload = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["action"] = "publish_create",
            ["channel"] = channelName,
            ["name"] = eventName,
            ["data"] = data,
        };
        AddIfNotNull(payload, "extras", options?.Extras);
        AddIfNotNull(payload, "messageId", options?.MessageId);
        AddIfNotNull(payload, "messageSerial", options?.MessageSerial);
        AddIfNotNull(payload, "clientId", options?.ClientId);
        AddIfNotNull(payload, "socketId", options?.SocketId);
        AddIfNotNull(payload, "opId", options?.OpId);

        var response = await PerformVersionedMutationRequestAsync(payload, cancellationToken).ConfigureAwait(false);
        return DecodeVersionedMessageAck(response, channelName, MutableMessageAction.Create);
    }

    internal async Task<VersionedMessageAck> AppendVersionedMessageAsync(
        string channelName,
        string messageSerial,
        string data,
        VersionedMessageMutationOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var payload = VersionedMutationPayload(
            "message_append",
            channelName,
            messageSerial,
            options,
            MutableMessageAction.Append);
        payload["data"] = data;

        var response = await PerformVersionedMutationRequestAsync(payload, cancellationToken).ConfigureAwait(false);
        return DecodeVersionedMessageAck(response, channelName, MutableMessageAction.Append);
    }

    internal async Task<VersionedMessageAck> UpdateVersionedMessageAsync(
        string channelName,
        string messageSerial,
        VersionedMessageMutationOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var payload = VersionedMutationPayload(
            "message_update",
            channelName,
            messageSerial,
            options,
            MutableMessageAction.Update);

        var response = await PerformVersionedMutationRequestAsync(payload, cancellationToken).ConfigureAwait(false);
        return DecodeVersionedMessageAck(response, channelName, MutableMessageAction.Update);
    }

    internal async Task<VersionedMessageAck> DeleteVersionedMessageAsync(
        string channelName,
        string messageSerial,
        VersionedMessageMutationOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var payload = VersionedMutationPayload(
            "message_delete",
            channelName,
            messageSerial,
            options,
            MutableMessageAction.Delete);

        var response = await PerformVersionedMutationRequestAsync(payload, cancellationToken).ConfigureAwait(false);
        return DecodeVersionedMessageAck(response, channelName, MutableMessageAction.Delete);
    }

    private async Task<Dictionary<string, object?>> PerformVersionedMutationRequestAsync(
        Dictionary<string, object?> payload,
        CancellationToken cancellationToken)
    {
        var config = Options.VersionedMessages ?? throw new UnsupportedFeature(
            "VersionedMessages.Endpoint must be configured to use versioned message mutation helpers. This endpoint should proxy requests to the Sockudo server REST API.");

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));

        using var request = new HttpRequestMessage(HttpMethod.Post, config.Endpoint);
        request.Content = JsonContent.Create(payload);

        if (config.Headers is not null)
        {
            foreach (var header in config.Headers)
            {
                request.Headers.TryAddWithoutValidation(header.Key, header.Value);
            }
        }
        if (config.HeadersProvider is not null)
        {
            foreach (var header in config.HeadersProvider())
            {
                request.Headers.TryAddWithoutValidation(header.Key, header.Value);
            }
        }

        using var response = await _httpClient.SendAsync(request, timeout.Token).ConfigureAwait(false);
        var content = await response.Content.ReadAsStringAsync(timeout.Token).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            throw new SockudoException($"Versioned message request failed ({(int)response.StatusCode}): {content}");
        }

        if (string.IsNullOrWhiteSpace(content))
        {
            return new Dictionary<string, object?>(StringComparer.Ordinal);
        }

        var decoded = JsonSupport.Decode(content) as Dictionary<string, object?>;
        if (decoded is null)
        {
            throw new SockudoException("Versioned message endpoint returned invalid JSON");
        }
        return decoded;
    }

    private static Dictionary<string, object?> VersionedMutationPayload(
        string action,
        string channelName,
        string messageSerial,
        VersionedMessageMutationOptions? options,
        MutableMessageAction mutationAction)
    {
        var payload = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["action"] = action,
            ["channel"] = channelName,
            ["messageSerial"] = messageSerial,
        };

        if (mutationAction == MutableMessageAction.Update)
        {
            AddIfNotNull(payload, "name", options?.Name);
        }
        if (mutationAction is MutableMessageAction.Update or MutableMessageAction.Delete)
        {
            AddIfNotNull(payload, "data", options?.Data);
            AddIfNotNull(payload, "clearFields", options?.ClearFields);
        }

        AddIfNotNull(payload, "extras", options?.Extras);
        AddIfNotNull(payload, "clientId", options?.ClientId);
        AddIfNotNull(payload, "socketId", options?.SocketId);
        AddIfNotNull(payload, "description", options?.Description);
        AddIfNotNull(payload, "metadata", options?.Metadata);
        AddIfNotNull(payload, "opId", options?.OpId);
        return payload;
    }

    private static VersionedMessageAck DecodeVersionedMessageAck(
        Dictionary<string, object?> payload,
        string channelName,
        MutableMessageAction defaultAction)
    {
        var ackPayload = payload;
        if (payload.Get("channels") is Dictionary<string, object?> channels &&
            channels.Get(channelName) is Dictionary<string, object?> channelAck)
        {
            ackPayload = channelAck;
        }

        var historyRaw = FirstValue(ackPayload, "history_serial", "historySerial");
        var deliveryRaw = FirstValue(ackPayload, "delivery_serial", "deliverySerial");
        return new VersionedMessageAck(
            Channel: StringValue(ackPayload, "channel") ?? channelName,
            MessageSerial: StringValue(ackPayload, "message_serial", "messageSerial") ?? string.Empty,
            Action: ParseMutableAction(FirstValue(ackPayload, "action"), defaultAction),
            Accepted: BoolValue(ackPayload, "accepted") ?? true,
            VersionSerial: StringValue(ackPayload, "version_serial", "versionSerial"),
            HistorySerial: ProtocolCodec.CoerceLong(historyRaw),
            DeliverySerial: ProtocolCodec.CoerceLong(deliveryRaw),
            Status: StringValue(ackPayload, "status") ?? "accepted",
            HistorySerialText: ProtocolCodec.CoerceSerialText(historyRaw),
            DeliverySerialText: ProtocolCodec.CoerceSerialText(deliveryRaw));
    }

    private static MutableMessageAction ParseMutableAction(object? value, MutableMessageAction fallback)
    {
        var text = value?.ToString()?.Trim().ToLowerInvariant();
        return text switch
        {
            "message.create" or "create" => MutableMessageAction.Create,
            "message.update" or "update" => MutableMessageAction.Update,
            "message.delete" or "delete" => MutableMessageAction.Delete,
            "message.append" or "append" => MutableMessageAction.Append,
            _ => fallback,
        };
    }

    private static object? FirstValue(Dictionary<string, object?> payload, params string[] keys)
    {
        foreach (var key in keys)
        {
            if (payload.TryGetValue(key, out var value))
            {
                return value;
            }
        }
        return null;
    }

    private static string? StringValue(Dictionary<string, object?> payload, params string[] keys) =>
        FirstValue(payload, keys)?.ToString();

    private static bool? BoolValue(Dictionary<string, object?> payload, params string[] keys) =>
        FirstValue(payload, keys) switch
        {
            bool flag => flag,
            string text when bool.TryParse(text, out var flag) => flag,
            _ => null,
        };

    private static void AddIfNotNull(Dictionary<string, object?> payload, string key, object? value)
    {
        if (value is not null)
        {
            payload[key] = value;
        }
    }

    private async Task<Dictionary<string, object?>> PerformPresenceHistoryRequestAsync(
        string endpoint,
        IDictionary<string, string>? staticHeaders,
        Func<IDictionary<string, string>>? dynamicHeaders,
        string channelName,
        IDictionary<string, object> parameters,
        string action,
        CancellationToken cancellationToken,
        string? messageSerial = null,
        string? annotationSerial = null,
        string? socketId = null,
        IDictionary<string, object?>? annotation = null)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, endpoint);
        request.Content = JsonContent.Create(new Dictionary<string, object?>
        {
            ["channel"] = channelName,
            ["params"] = parameters,
            ["action"] = action,
            ["messageSerial"] = messageSerial,
            ["annotationSerial"] = annotationSerial,
            ["socketId"] = socketId,
            ["annotation"] = annotation,
        });

        if (staticHeaders is not null)
        {
            foreach (var header in staticHeaders)
            {
                request.Headers.TryAddWithoutValidation(header.Key, header.Value);
            }
        }
        if (dynamicHeaders is not null)
        {
            foreach (var header in dynamicHeaders())
            {
                request.Headers.TryAddWithoutValidation(header.Key, header.Value);
            }
        }

        using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
        var content = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            throw new SockudoException($"Presence {action} request failed ({(int)response.StatusCode}): {content}");
        }

        var payload = JsonSupport.Decode(content) as Dictionary<string, object?>;
        if (payload is null)
        {
            throw new SockudoException($"Presence {action} endpoint returned invalid JSON");
        }
        return payload;
    }

    private static PresenceHistoryPage DecodePresenceHistoryPage(
        Dictionary<string, object?> payload,
        Func<string, Task<PresenceHistoryPage>> fetchNext)
    {
        var items = (payload.Get("items") as IEnumerable<object?> ?? Array.Empty<object?>())
            .OfType<Dictionary<string, object?>>()
            .Select(item => new PresenceHistoryItem(
                item.Get("stream_id") as string ?? string.Empty,
                Convert.ToInt64(item.Get("serial") ?? 0),
                Convert.ToInt64(item.Get("published_at_ms") ?? 0),
                item.Get("event") as string ?? string.Empty,
                item.Get("cause") as string ?? string.Empty,
                item.Get("user_id") as string ?? string.Empty,
                item.Get("connection_id") as string,
                item.Get("dead_node_id") as string,
                Convert.ToInt32(item.Get("payload_size_bytes") ?? 0),
                item.Get("presence_event") as Dictionary<string, object?> ?? new Dictionary<string, object?>()))
            .ToArray();

        return new PresenceHistoryPage(
            items,
            payload.Get("direction") as string ?? "oldest_first",
            Convert.ToInt32(payload.Get("limit") ?? 0),
            payload.Get("has_more") as bool? ?? false,
            payload.Get("next_cursor") as string,
            DecodePresenceHistoryBounds(payload.Get("bounds") as Dictionary<string, object?>),
            DecodePresenceHistoryContinuity(payload.Get("continuity") as Dictionary<string, object?>),
            fetchNext);
    }

    private static PresenceSnapshot DecodePresenceSnapshot(Dictionary<string, object?> payload)
    {
        var members = (payload.Get("members") as IEnumerable<object?> ?? Array.Empty<object?>())
            .OfType<Dictionary<string, object?>>()
            .Select(member => new PresenceSnapshotMember(
                member.Get("user_id") as string ?? string.Empty,
                member.Get("last_event") as string ?? string.Empty,
                Convert.ToInt64(member.Get("last_event_serial") ?? 0),
                Convert.ToInt64(member.Get("last_event_at_ms") ?? 0)))
            .ToArray();

        return new PresenceSnapshot(
            payload.Get("channel") as string ?? string.Empty,
            members,
            Convert.ToInt32(payload.Get("member_count") ?? 0),
            Convert.ToInt64(payload.Get("events_replayed") ?? 0),
            payload.Get("snapshot_serial") is null ? null : Convert.ToInt64(payload.Get("snapshot_serial")),
            payload.Get("snapshot_time_ms") is null ? null : Convert.ToInt64(payload.Get("snapshot_time_ms")),
            DecodePresenceHistoryContinuity(payload.Get("continuity") as Dictionary<string, object?>));
    }

    private static ChannelHistoryPageProxy DecodeChannelHistoryPage(
        Dictionary<string, object?> payload,
        Func<string, Task<ChannelHistoryPageProxy>> fetchNext)
    {
        var items = (payload.Get("items") as IEnumerable<object?> ?? Array.Empty<object?>())
            .OfType<Dictionary<string, object?>>()
            .ToArray();

        return new ChannelHistoryPageProxy(
            items,
            payload.Get("direction") as string ?? "oldest_first",
            Convert.ToInt32(payload.Get("limit") ?? 0),
            payload.Get("has_more") as bool? ?? false,
            payload.Get("next_cursor") as string,
            payload.Get("bounds") as Dictionary<string, object?> ?? new Dictionary<string, object?>(),
            payload.Get("continuity") as Dictionary<string, object?> ?? new Dictionary<string, object?>(),
            fetchNext);
    }

    private static MessageVersionsPage DecodeMessageVersionsPage(
        Dictionary<string, object?> payload,
        string channelName,
        Func<string, Task<MessageVersionsPage>> fetchNext)
    {
        var items = (payload.Get("items") as IEnumerable<object?> ?? Array.Empty<object?>())
            .OfType<Dictionary<string, object?>>()
            .ToArray();

        return new MessageVersionsPage(
            payload.Get("channel") as string ?? channelName,
            items,
            payload.Get("direction") as string ?? "oldest_first",
            Convert.ToInt32(payload.Get("limit") ?? 0),
            payload.Get("has_more") as bool? ?? false,
            payload.Get("next_cursor") as string,
            fetchNext);
    }

    private static AnnotationEventsPage DecodeAnnotationEventsPage(
        Dictionary<string, object?> payload,
        Func<string, Task<AnnotationEventsPage>> fetchNext)
    {
        var items = (payload.Get("items") as IEnumerable<object?> ?? Array.Empty<object?>())
            .OfType<Dictionary<string, object?>>()
            .ToArray();

        return new AnnotationEventsPage(
            items,
            payload.Get("direction") as string ?? "oldest_first",
            Convert.ToInt32(payload.Get("limit") ?? 0),
            payload.Get("has_more") as bool? ?? false,
            payload.Get("next_cursor") as string,
            fetchNext);
    }

    private static PresenceHistoryBounds DecodePresenceHistoryBounds(Dictionary<string, object?>? payload)
    {
        payload ??= new Dictionary<string, object?>();
        return new PresenceHistoryBounds(
            payload.Get("start_serial") is null ? null : Convert.ToInt64(payload.Get("start_serial")),
            payload.Get("end_serial") is null ? null : Convert.ToInt64(payload.Get("end_serial")),
            payload.Get("start_time_ms") is null ? null : Convert.ToInt64(payload.Get("start_time_ms")),
            payload.Get("end_time_ms") is null ? null : Convert.ToInt64(payload.Get("end_time_ms")));
    }

    private static PresenceHistoryContinuity DecodePresenceHistoryContinuity(Dictionary<string, object?>? payload)
    {
        payload ??= new Dictionary<string, object?>();
        return new PresenceHistoryContinuity(
            payload.Get("stream_id") as string,
            payload.Get("oldest_available_serial") is null ? null : Convert.ToInt64(payload.Get("oldest_available_serial")),
            payload.Get("newest_available_serial") is null ? null : Convert.ToInt64(payload.Get("newest_available_serial")),
            payload.Get("oldest_available_published_at_ms") is null ? null : Convert.ToInt64(payload.Get("oldest_available_published_at_ms")),
            payload.Get("newest_available_published_at_ms") is null ? null : Convert.ToInt64(payload.Get("newest_available_published_at_ms")),
            Convert.ToInt64(payload.Get("retained_events") ?? 0),
            Convert.ToInt64(payload.Get("retained_bytes") ?? 0),
            payload.Get("degraded") as bool? ?? false,
            payload.Get("complete") as bool? ?? false,
            payload.Get("truncated_by_retention") as bool? ?? false);
    }

    public sealed class UserFacade
    {
        private readonly SockudoClient _client;
        private readonly EventDispatcher _dispatcher = new();
        private SockudoChannel? _serverChannel;

        internal UserFacade(SockudoClient client)
        {
            _client = client;
        }

        public Dictionary<string, object?>? UserData { get; private set; }
        public string? UserId => UserData?.Get("id") as string;

        public string Bind(string eventName, Action<object?, EventMetadata?> callback) => _dispatcher.Bind(eventName, callback);

        public Task SignInAsync() => AttemptSignInAsync(requested: true);

        internal Task HandleConnectedAsync() => AttemptSignInAsync(requested: false);

        internal async Task HandleSignInSuccessAsync(object? data)
        {
            var payload = data as Dictionary<string, object?>;
            var userData = payload?.Get("user_data") as string;
            if (userData is null)
            {
                Cleanup();
                return;
            }

            var parsed = JsonSupport.Decode(userData) as Dictionary<string, object?>;
            if (parsed?.Get("id") is not string userId)
            {
                Cleanup();
                return;
            }

            UserData = parsed;
            await SubscribeServerChannelAsync(userId).ConfigureAwait(false);
        }

        internal void Cleanup()
        {
            UserData = null;
            _serverChannel?.Unbind();
            _serverChannel?.Disconnect();
            _serverChannel = null;
        }

        private async Task AttemptSignInAsync(bool requested)
        {
            if (requested)
            {
                IsSignInRequested = true;
            }
            if (!IsSignInRequested || _client.ConnectionState != ConnectionState.Connected || _client.SocketId is null)
            {
                return;
            }

            try
            {
                var auth = await _client.AuthenticateUserAsync(new UserAuthenticationRequest(_client.SocketId)).ConfigureAwait(false);
                await _client.SendEventAsync(_client._prefix.Event("signin"), new Dictionary<string, object?>
                {
                    ["auth"] = auth.Auth,
                    ["user_data"] = auth.UserData,
                }).ConfigureAwait(false);
            }
            catch
            {
                Cleanup();
            }
        }

        private async Task SubscribeServerChannelAsync(string userId)
        {
            var channel = new SockudoChannel($"#server-to-user-{userId}", _client);
            channel.BindGlobal((eventName, payload) =>
            {
                if (!_client._prefix.IsInternalEvent(eventName) && !_client._prefix.IsPlatformEvent(eventName))
                {
                    _dispatcher.Emit(eventName, payload);
                }
            });
            _serverChannel = channel;
            _client._channels[channel.Name] = channel;
            channel.SubscribeIfPossible();
            await Task.CompletedTask;
        }

        public bool IsSignInRequested { get; private set; }
    }

    public sealed class WatchlistFacade
    {
        private readonly EventDispatcher _dispatcher = new();

        public string Bind(string eventName, Action<object?, EventMetadata?> callback) => _dispatcher.Bind(eventName, callback);

        internal void Handle(object? data)
        {
            if (data is not Dictionary<string, object?> payload ||
                payload.Get("events") is not List<object?> events)
            {
                return;
            }

            foreach (var entry in events.OfType<Dictionary<string, object?>>())
            {
                if (entry.Get("name") is string eventName)
                {
                    _dispatcher.Emit(eventName, entry);
                }
            }
        }
    }
}

public class SockudoChannel
{
    private readonly EventDispatcher _dispatcher = new();

    internal SockudoChannel(string name, SockudoClient client)
    {
        Name = name;
        Client = client;
    }

    public string Name { get; }
    public SockudoClient Client { get; }
    public FilterNode? Filter { get; internal set; }
    public ChannelDeltaSettings? DeltaSettings { get; internal set; }
    public IReadOnlyList<string>? EventsFilter { get; internal set; }
    public SubscriptionRewind? Rewind { get; internal set; }
    public bool AnnotationSubscribe { get; internal set; }
    public bool IsSubscribed { get; internal set; }
    public bool SubscriptionPending { get; internal set; }
    public bool SubscriptionCancelled { get; internal set; }
    public int? SubscriptionCount { get; private set; }
    public long? AttachSerial { get; private set; }

    public string Bind(string eventName, Action<object?, EventMetadata?> callback) => _dispatcher.Bind(eventName, callback);
    public string BindGlobal(Action<string, object?> callback) => _dispatcher.BindGlobal(callback);
    public void Unbind(string? eventName = null, string? token = null) => _dispatcher.Unbind(eventName, token);
    protected void Emit(string eventName, object? data, EventMetadata? metadata = null) => _dispatcher.Emit(eventName, data, metadata);

    public virtual async Task<bool> TriggerAsync(string eventName, object? data, CancellationToken cancellationToken = default)
    {
        if (!eventName.StartsWith("client-", StringComparison.Ordinal))
        {
            throw new BadEventName($"Event '{eventName}' does not start with 'client-'");
        }
        return await Client.SendEventAsync(eventName, data, Name, cancellationToken).ConfigureAwait(false);
    }

    public virtual Task<ChannelAuthorizationData> AuthorizeAsync(string socketId) => Task.FromResult(new ChannelAuthorizationData(string.Empty));

    public void SubscribeIfPossible()
    {
        if (SubscriptionPending && SubscriptionCancelled)
        {
            SubscriptionCancelled = false;
        }
        else if (!SubscriptionPending && Client.ConnectionState == ConnectionState.Connected)
        {
            _ = SubscribeAsync();
        }
    }

    public async Task SubscribeAsync()
    {
        if (IsSubscribed)
        {
            return;
        }

        SubscriptionPending = true;
        SubscriptionCancelled = false;

        try
        {
            var auth = await AuthorizeAsync(Client.SocketId ?? string.Empty).ConfigureAwait(false);
            var payload = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["auth"] = auth.Auth,
                ["channel"] = Name,
            };
            if (auth.ChannelData is not null)
            {
                payload["channel_data"] = auth.ChannelData;
            }
            if (Filter is not null)
            {
                payload["tags_filter"] = Filter;
            }
            if (DeltaSettings is not null)
            {
                payload["delta"] = DeltaSettings.SubscriptionValue();
            }
            if (EventsFilter is not null)
            {
                payload["events"] = EventsFilter;
            }
            if (Rewind is not null)
            {
                payload["rewind"] = Rewind.SubscriptionValue();
            }
            if (AnnotationSubscribe)
            {
                payload["modes"] = new[] { "SUBSCRIBE", "ANNOTATION_SUBSCRIBE" };
            }

            await Client.SendEventAsync(ClientPrefix().Event("subscribe"), payload).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            SubscriptionPending = false;
            Emit(ClientPrefix().Event("subscription_error"), new Dictionary<string, object?> { ["type"] = "AuthError", ["error"] = exception.Message });
        }
    }

    public async Task UnsubscribeAsync()
    {
        IsSubscribed = false;
        AttachSerial = null;
        await Client.SendEventAsync(ClientPrefix().Event("unsubscribe"), new Dictionary<string, object?> { ["channel"] = Name }).ConfigureAwait(false);
    }

    internal virtual void Disconnect()
    {
        IsSubscribed = false;
        SubscriptionPending = false;
        AttachSerial = null;
    }

    internal virtual void Handle(SockudoEvent @event)
    {
        var prefix = ClientPrefix();
        if (@event.Event == prefix.Internal("subscription_succeeded"))
        {
            SubscriptionPending = false;
            IsSubscribed = true;
            RememberAttachSerial(@event.Data);
            if (SubscriptionCancelled)
            {
                _ = Client.UnsubscribeAsync(Name);
            }
            else
            {
                Emit(prefix.Event("subscription_succeeded"), @event.Data);
            }
            return;
        }

        if (@event.Event == prefix.Internal("subscription_count"))
        {
            if (@event.Data is Dictionary<string, object?> payload)
            {
                SubscriptionCount = ProtocolCodec.CoerceInt(payload.Get("subscription_count"));
            }
            Emit(prefix.Event("subscription_count"), @event.Data);
            return;
        }

        if (@event.Event == prefix.Internal("message") &&
            @event.Data is Dictionary<string, object?> messagePayload &&
            messagePayload.Get("action") as string == "message.summary")
        {
            Emit("message.summary", messagePayload, new EventMetadata(@event.UserId));
            return;
        }

        if (@event.Event == prefix.Internal("annotation") &&
            @event.Data is Dictionary<string, object?> annotationPayload &&
            annotationPayload.Get("action") is string action)
        {
            Emit(action, annotationPayload, new EventMetadata(@event.UserId));
            return;
        }

        if (!prefix.IsInternalEvent(@event.Event))
        {
            Emit(@event.Event, @event.Data, new EventMetadata(@event.UserId));
        }
    }

    internal void ForceResubscribe()
    {
        IsSubscribed = false;
        SubscriptionPending = false;
        SubscribeIfPossible();
    }

    protected void RememberAttachSerial(object? data)
    {
        if (data is Dictionary<string, object?> payload)
        {
            AttachSerial = ProtocolCodec.CoerceLong(payload.Get("attach_serial")) ?? AttachSerial;
        }
    }

    internal ProtocolPrefix ClientPrefix() => new(Client.Options.ProtocolVersion);

    public Task<ChannelHistoryPageProxy> ChannelHistoryAsync(
        ChannelHistoryParams? parameters = null,
        CancellationToken cancellationToken = default) =>
        Client.FetchChannelHistoryAsync(Name, parameters ?? new ChannelHistoryParams(), cancellationToken);

    public Task<Dictionary<string, object?>> GetMessageAsync(
        string messageSerial,
        CancellationToken cancellationToken = default) =>
        Client.FetchLatestMessageAsync(Name, messageSerial, cancellationToken);

    public Task<MessageVersionsPage> GetMessageVersionsAsync(
        string messageSerial,
        MessageVersionsParams? parameters = null,
        CancellationToken cancellationToken = default) =>
        Client.FetchMessageVersionsAsync(Name, messageSerial, parameters ?? new MessageVersionsParams(), cancellationToken);

    public Task<VersionedMessageAck> CreateVersionedMessageAsync(
        string eventName,
        object? data,
        VersionedMessageCreateOptions? options = null,
        CancellationToken cancellationToken = default) =>
        Client.PublishVersionedMessageAsync(Name, eventName, data, options, cancellationToken);

    public Task<VersionedMessageAck> AppendVersionedMessageAsync(
        string messageSerial,
        string data,
        VersionedMessageMutationOptions? options = null,
        CancellationToken cancellationToken = default) =>
        Client.AppendVersionedMessageAsync(Name, messageSerial, data, options, cancellationToken);

    public Task<VersionedMessageAck> UpdateVersionedMessageAsync(
        string messageSerial,
        VersionedMessageMutationOptions? options = null,
        CancellationToken cancellationToken = default) =>
        Client.UpdateVersionedMessageAsync(Name, messageSerial, options, cancellationToken);

    public Task<VersionedMessageAck> DeleteVersionedMessageAsync(
        string messageSerial,
        VersionedMessageMutationOptions? options = null,
        CancellationToken cancellationToken = default) =>
        Client.DeleteVersionedMessageAsync(Name, messageSerial, options, cancellationToken);

    public Task<PublishAnnotationResponse> PublishAnnotationAsync(
        string messageSerial,
        PublishAnnotationRequest annotation,
        CancellationToken cancellationToken = default) =>
        Client.PublishAnnotationAsync(Name, messageSerial, annotation, cancellationToken);

    public Task<DeleteAnnotationResponse> DeleteAnnotationAsync(
        string messageSerial,
        string annotationSerial,
        string? socketId = null,
        CancellationToken cancellationToken = default) =>
        Client.DeleteAnnotationAsync(Name, messageSerial, annotationSerial, socketId, cancellationToken);

    public Task<AnnotationEventsPage> ListAnnotationsAsync(
        string messageSerial,
        AnnotationEventsParams? parameters = null,
        CancellationToken cancellationToken = default) =>
        Client.ListAnnotationsAsync(Name, messageSerial, parameters ?? new AnnotationEventsParams(), cancellationToken);
}

public class PrivateChannel : SockudoChannel
{
    internal PrivateChannel(string name, SockudoClient client) : base(name, client)
    {
    }

    public override Task<ChannelAuthorizationData> AuthorizeAsync(string socketId) => Client.AuthorizeChannelAsync(new ChannelAuthorizationRequest(socketId, Name));
}

public sealed class PresenceChannel : PrivateChannel
{
    public PresenceChannel(string name, SockudoClient client) : base(name, client)
    {
        Members = new PresenceMembers();
    }

    public PresenceMembers Members { get; }

    public override async Task<ChannelAuthorizationData> AuthorizeAsync(string socketId)
    {
        var response = await base.AuthorizeAsync(socketId).ConfigureAwait(false);
        if (response.ChannelData is not null &&
            JsonSupport.Decode(response.ChannelData) is Dictionary<string, object?> payload &&
            payload.Get("user_id") is string userId)
        {
            Members.RememberMyId(userId);
            return response;
        }

        if (Client.User.UserId is not null)
        {
            Members.RememberMyId(Client.User.UserId);
            return response;
        }

        throw new AuthFailure(null, $"Invalid auth response for presence channel '{Name}'");
    }

    internal override void Handle(SockudoEvent @event)
    {
        var prefix = ClientPrefix();
        if (@event.Event == prefix.Internal("subscription_succeeded"))
        {
            SubscriptionPending = false;
            IsSubscribed = true;
            RememberAttachSerial(@event.Data);
            Members.ApplySubscriptionData(@event.Data as Dictionary<string, object?> ?? new Dictionary<string, object?>());
            Emit(prefix.Event("subscription_succeeded"), Members);
            return;
        }

        if (@event.Event == prefix.Internal("member_added") && @event.Data is Dictionary<string, object?> addedPayload)
        {
            var member = Members.Add(addedPayload);
            if (member is not null)
            {
                Emit(prefix.Event("member_added"), member);
            }
            return;
        }

        if (@event.Event == prefix.Internal("member_removed") && @event.Data is Dictionary<string, object?> removedPayload)
        {
            var member = Members.Remove(removedPayload);
            if (member is not null)
            {
                Emit(prefix.Event("member_removed"), member);
            }
            return;
        }

        if (@event.Event == prefix.Internal("presence_update") && @event.Data is Dictionary<string, object?> updatedPayload)
        {
            var member = Members.Update(updatedPayload);
            if (member is not null)
            {
                Emit(prefix.Event("presence_update"), member);
            }
            return;
        }

        base.Handle(@event);
    }

    internal override void Disconnect()
    {
        Members.Reset();
        base.Disconnect();
    }

    public Task<PresenceHistoryPage> HistoryAsync(
        PresenceHistoryParams? parameters = null,
        CancellationToken cancellationToken = default) =>
        Client.FetchPresenceHistoryAsync(Name, parameters ?? new PresenceHistoryParams(), cancellationToken);

    public Task<PresenceSnapshot> SnapshotAsync(
        PresenceSnapshotParams? parameters = null,
        CancellationToken cancellationToken = default) =>
        Client.FetchPresenceSnapshotAsync(Name, parameters ?? new PresenceSnapshotParams(), cancellationToken);

    public Task<bool> UpdateAsync(object? data, CancellationToken cancellationToken = default)
    {
        if (Client.Options.ProtocolVersion < 2)
        {
            throw new UnsupportedFeature("Presence update is only supported on Protocol V2 presence channels.");
        }

        return Client.SendEventAsync(
            ClientPrefix().Event("presence_update"),
            new Dictionary<string, object?>
            {
                ["channel"] = Name,
                ["data"] = data,
            },
            null,
            cancellationToken);
    }
}

public sealed class EncryptedChannel : PrivateChannel
{
    private byte[]? _sharedSecret;

    internal EncryptedChannel(string name, SockudoClient client) : base(name, client)
    {
    }

    public override async Task<ChannelAuthorizationData> AuthorizeAsync(string socketId)
    {
        var response = await base.AuthorizeAsync(socketId).ConfigureAwait(false);
        if (response.SharedSecret is null)
        {
            throw new AuthFailure(null, $"No shared_secret key in auth payload for encrypted channel: {Name}");
        }
        _sharedSecret = Convert.FromBase64String(response.SharedSecret);
        return new ChannelAuthorizationData(response.Auth, response.ChannelData);
    }

    public object? Decrypt(IDictionary<string, object?> payload)
    {
        if (_sharedSecret is null)
        {
            return null;
        }
        if (payload.Get("ciphertext") is not string ciphertext || payload.Get("nonce") is not string nonce)
        {
            return null;
        }

        var combined = Convert.FromBase64String(ciphertext);
        var message = SecretBox.Open(combined, Convert.FromBase64String(nonce), _sharedSecret);
        return JsonSupport.Decode(Encoding.UTF8.GetString(message));
    }

    public override Task<bool> TriggerAsync(string eventName, object? data, CancellationToken cancellationToken = default)
    {
        throw new UnsupportedFeature("Client events are not currently supported for encrypted channels");
    }

    internal override void Handle(SockudoEvent @event)
    {
        var prefix = ClientPrefix();
        if (prefix.IsInternalEvent(@event.Event) || prefix.IsPlatformEvent(@event.Event))
        {
            base.Handle(@event);
            return;
        }

        if (_sharedSecret is null || @event.Data is not Dictionary<string, object?> payload)
        {
            return;
        }

        if (payload.Get("ciphertext") is not string ciphertext || payload.Get("nonce") is not string nonce)
        {
            return;
        }

        try
        {
            var parsed = Decrypt(payload);
            if (parsed is not null)
            {
                Emit(@event.Event, parsed, new EventMetadata(@event.UserId));
            }
        }
        catch
        {
        }
    }
}

public sealed class PresenceMembers
{
    private readonly Dictionary<string, object?> _members = new(StringComparer.Ordinal);

    public int Count { get; private set; }
    public string? MyId { get; private set; }
    public PresenceMember? Me { get; private set; }

    public PresenceMember? Member(string memberId) => _members.TryGetValue(memberId, out var info) ? new PresenceMember(memberId, info) : null;

    internal void RememberMyId(string memberId)
    {
        MyId = memberId;
        Me = Member(memberId);
    }

    internal void ApplySubscriptionData(Dictionary<string, object?> data)
    {
        var presence = data.Get("presence") as Dictionary<string, object?>;
        if (presence?.Get("hash") is not Dictionary<string, object?> hash)
        {
            return;
        }

        _members.Clear();
        foreach (var entry in hash)
        {
            _members[entry.Key] = entry.Value;
        }
        Count = ProtocolCodec.CoerceInt(presence?.Get("count")) ?? _members.Count;
        Me = MyId is not null ? Member(MyId) : null;
    }

    internal PresenceMember? Add(Dictionary<string, object?> data)
    {
        if (data.Get("user_id") is not string userId)
        {
            return null;
        }
        if (!_members.ContainsKey(userId))
        {
            Count += 1;
        }
        _members[userId] = data.Get("user_info");
        Me = MyId is not null ? Member(MyId) : null;
        return new PresenceMember(userId, _members[userId]);
    }

    internal PresenceMember? Remove(Dictionary<string, object?> data)
    {
        if (data.Get("user_id") is not string userId || !_members.TryGetValue(userId, out var info))
        {
            return null;
        }
        _members.Remove(userId);
        Count = Math.Max(0, Count - 1);
        Me = MyId is not null ? Member(MyId) : null;
        return new PresenceMember(userId, info);
    }

    internal PresenceMember? Update(Dictionary<string, object?> data)
    {
        if (data.Get("user_id") is not string userId || !_members.ContainsKey(userId))
        {
            return null;
        }

        _members[userId] = data.Get("user_info");
        Count = Math.Max(Count, _members.Count);
        Me = MyId is not null ? Member(MyId) : null;
        return new PresenceMember(userId, _members[userId]);
    }

    internal void Reset()
    {
        _members.Clear();
        Count = 0;
        MyId = null;
        Me = null;
    }
}

internal sealed class DeltaCompressionManager
{
    private readonly DeltaOptions _options;
    private readonly Func<string, object?, string?, CancellationToken, Task<bool>> _sendEvent;
    private readonly ProtocolPrefix _prefix;
    private bool _enabled;
    private DeltaAlgorithm _defaultAlgorithm = DeltaAlgorithm.Fossil;
    private DeltaStats _stats = new(0, 0, 0, 0, 0, 0);
    private readonly Dictionary<string, ChannelDeltaState> _channelStates = new(StringComparer.Ordinal);

    public DeltaCompressionManager(
        DeltaOptions options,
        Func<string, object?, string?, CancellationToken, Task<bool>> sendEvent,
        ProtocolPrefix prefix)
    {
        _options = options;
        _sendEvent = sendEvent;
        _prefix = prefix;
    }

    public async Task EnableAsync()
    {
        if (_enabled)
        {
            return;
        }

        await _sendEvent(_prefix.Event("enable_delta_compression"), new Dictionary<string, object?>
        {
            ["algorithms"] = _options.EffectiveAlgorithms.Select(algorithm => algorithm.ToString().ToLowerInvariant()).ToList(),
        }, null, CancellationToken.None).ConfigureAwait(false);
    }

    public void HandleEnabled(object? data)
    {
        var payload = data as Dictionary<string, object?>;
        _enabled = (payload?.Get("enabled") as bool?) ?? true;
        if (payload?.Get("algorithm") is string algorithm &&
            Enum.TryParse<DeltaAlgorithm>(algorithm, true, out var parsed))
        {
            _defaultAlgorithm = parsed;
        }
    }

    public void HandleCacheSync(string channel, object? data)
    {
        var payload = data as Dictionary<string, object?> ?? new Dictionary<string, object?>();
        var state = new ChannelDeltaState();
        if (payload.Get("states") is Dictionary<string, object?> states)
        {
            foreach (var entry in states)
            {
                if (entry.Value is Dictionary<string, object?> statePayload &&
                    ProtocolCodec.CoerceInt(statePayload.Get("seq")) is int seq &&
                    statePayload.Get("message") is string message)
                {
                    state.UpdateConflationCache(entry.Key, message, seq);
                }
            }
        }
        if (payload.Get("conflation_key") is string conflationKey)
        {
            state.ConflationKey = conflationKey;
        }
        _channelStates[channel] = state;
    }

    public async Task<SockudoEvent?> HandleDeltaMessageAsync(string channel, object? data)
    {
        var payload = data as Dictionary<string, object?>;
        if (payload?.Get("event") is not string eventName || payload.Get("delta") is not string deltaPayload)
        {
            return null;
        }

        var algorithmName = payload.Get("algorithm") as string ?? _defaultAlgorithm.ToString().ToLowerInvariant();
        var sequence = ProtocolCodec.CoerceInt(payload.Get("seq"));
        var conflationKey = payload.Get("conflation_key") as string;

        if (!_channelStates.TryGetValue(channel, out var state))
        {
            await _sendEvent(_prefix.Event("delta_sync_error"), new Dictionary<string, object?> { ["channel"] = channel }, null, CancellationToken.None).ConfigureAwait(false);
            return null;
        }

        try
        {
            var deltaBytes = Convert.FromBase64String(deltaPayload);
            var baseMessage = state.GetBaseMessage(conflationKey, ProtocolCodec.CoerceInt(payload.Get("base_index")));
            if (baseMessage is null)
            {
                throw new SockudoException($"No base message for channel {channel}");
            }

            if (algorithmName.Equals("xdelta3", StringComparison.OrdinalIgnoreCase))
            {
                using var source = new MemoryStream(Encoding.UTF8.GetBytes(baseMessage));
                using var delta = new MemoryStream(deltaBytes);
                using var output = new MemoryStream();
                using var decoder = new VcDecoder(source, delta, output);
                decoder.Decode(out _);
                var reconstructedXdelta = Encoding.UTF8.GetString(output.ToArray());
                var parsedXdelta = JsonSupport.Decode(reconstructedXdelta) as Dictionary<string, object?>;
                var eventDataXdelta = parsedXdelta?.Get("data") ?? parsedXdelta;
                HandleFullMessage(channel, reconstructedXdelta, sequence, conflationKey);
                IncrementStats(deltaBytes.Length, reconstructedXdelta.Length, isDelta: true);
                return new SockudoEvent(eventName, channel, eventDataXdelta, null, null, null, reconstructedXdelta, sequence, conflationKey);
            }

            var reconstructed = Encoding.UTF8.GetString(FossilDelta.Apply(Encoding.UTF8.GetBytes(baseMessage), deltaBytes));
            var parsed = JsonSupport.Decode(reconstructed) as Dictionary<string, object?>;
            var eventData = parsed?.Get("data") ?? parsed;
            HandleFullMessage(channel, reconstructed, sequence, conflationKey);
            IncrementStats(deltaBytes.Length, reconstructed.Length, isDelta: true);
            return new SockudoEvent(eventName, channel, eventData, null, null, null, reconstructed, sequence, conflationKey);
        }
        catch (Exception exception)
        {
            IncrementError(exception);
            return null;
        }
    }

    public void HandleFullMessage(string channel, string rawMessage, int? sequence, string? conflationKey)
    {
        var state = _channelStates.GetValueOrDefault(channel);
        if (state is null)
        {
            state = new ChannelDeltaState();
            _channelStates[channel] = state;
        }

        if (conflationKey is not null && sequence is not null)
        {
            state.ConflationKey ??= "enabled";
            state.UpdateConflationCache(conflationKey, rawMessage, sequence.Value);
        }
        else
        {
            state.BaseMessage = rawMessage;
        }

        IncrementStats(rawMessage.Length, rawMessage.Length, isDelta: false);
    }

    public DeltaStats GetStats() => _stats;
    public void ResetStats() => _stats = new DeltaStats(0, 0, 0, 0, 0, 0);
    public void ClearChannelState(string channel) => _channelStates.Remove(channel);

    private void IncrementStats(int compressedBytes, int fullBytes, bool isDelta)
    {
        _stats = _stats with
        {
            TotalMessages = _stats.TotalMessages + 1,
            DeltaMessages = _stats.DeltaMessages + (isDelta ? 1 : 0),
            FullMessages = _stats.FullMessages + (isDelta ? 0 : 1),
            TotalBytesWithoutCompression = _stats.TotalBytesWithoutCompression + fullBytes,
            TotalBytesWithCompression = _stats.TotalBytesWithCompression + compressedBytes,
        };
        _options.OnStats?.Invoke(_stats);
    }

    private void IncrementError(Exception exception)
    {
        _stats = _stats with { Errors = _stats.Errors + 1 };
        _options.OnError?.Invoke(exception);
    }

    private sealed class ChannelDeltaState
    {
        public string? ConflationKey { get; set; }
        public string? BaseMessage { get; set; }
        private readonly Dictionary<string, List<(int Sequence, string Message)>> _conflationCaches = new(StringComparer.Ordinal);

        public string? GetBaseMessage(string? key, int? baseIndex)
        {
            if (ConflationKey is null)
            {
                return BaseMessage;
            }

            if (!_conflationCaches.TryGetValue(key ?? string.Empty, out var cache) || cache.Count == 0)
            {
                return null;
            }

            if (baseIndex is null || baseIndex < 0 || baseIndex >= cache.Count)
            {
                return cache[^1].Message;
            }

            return cache[baseIndex.Value].Message;
        }

        public void UpdateConflationCache(string key, string message, int sequence)
        {
            var bucketKey = key ?? string.Empty;
            if (!_conflationCaches.TryGetValue(bucketKey, out var cache))
            {
                cache = new List<(int Sequence, string Message)>();
                _conflationCaches[bucketKey] = cache;
            }

            cache.RemoveAll(entry => entry.Sequence == sequence);
            cache.Add((sequence, message));
            cache.Sort((left, right) => left.Sequence.CompareTo(right.Sequence));
        }
    }
}

internal static class TaskExtensions
{
    public static void DisposeSafe(this Task task)
    {
    }
}
