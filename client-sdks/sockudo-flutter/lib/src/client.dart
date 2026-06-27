import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:http/http.dart' as http;
import 'package:sodium/sodium.dart' as sodium;
import 'package:web_socket_channel/io.dart';
import 'package:web_socket_channel/status.dart' as ws_status;

import 'auth.dart';
import 'delta_compression.dart';
import 'filter.dart';
import 'message_dedup.dart';
import 'models.dart';
import 'protocol_codec.dart';
import 'protocol_prefix.dart';
import 'support.dart';

int _nextGeneratedMessageId = 0;

String _generatedMessageId() {
  final next = ++_nextGeneratedMessageId;
  return 'flutter-${DateTime.now().microsecondsSinceEpoch}-$next';
}

Map<Object?, Object?>? _decodeJwtPayload(String token) {
  final parts = token.split('.');
  if (parts.length < 2) {
    return null;
  }
  var payload = parts[1];
  final padding = payload.length % 4;
  if (padding != 0) {
    payload = payload.padRight(payload.length + 4 - padding, '=');
  }
  try {
    final decoded = utf8.decode(base64Url.decode(payload));
    final value = jsonDecode(decoded);
    return value is Map<Object?, Object?> ? value : null;
  } catch (_) {
    return null;
  }
}

double? _numericDateSeconds(Object? value) {
  if (value is num && value.isFinite) {
    return value.toDouble();
  }
  if (value is String) {
    final parsed = double.tryParse(value);
    if (parsed != null && parsed.isFinite) {
      return parsed;
    }
  }
  return null;
}

class SockudoOptions {
  const SockudoOptions({
    required this.cluster,
    this.protocolVersion = 7,
    this.token,
    this.authCallback,
    this.activityTimeout = const Duration(seconds: 120),
    this.forceTls,
    this.enabledTransports,
    this.disabledTransports,
    this.wsHost,
    this.wsPort = 80,
    this.wssPort = 443,
    this.wsPath = '',
    this.httpHost,
    this.httpPort = 80,
    this.httpsPort = 443,
    this.httpPath = '/sockudo',
    this.pongTimeout = const Duration(seconds: 30),
    this.unavailableTimeout = const Duration(seconds: 10),
    this.enableStats = false,
    this.statsHost = 'stats.sockudo.com',
    this.timelineParams = const <String, AuthValue>{},
    this.deltaCompression,
    this.messageDeduplication = true,
    this.messageDeduplicationCapacity = 1000,
    this.connectionRecovery = false,
    this.echoMessages = true,
    this.wireFormat = SockudoWireFormat.json,
    this.appendMode = SockudoAppendMode.delta,
    this.appendRollupWindow,
    this.channelAuthorization = const ChannelAuthorizationOptions(),
    this.userAuthentication = const UserAuthenticationOptions(),
    this.presenceHistory,
    this.versionedMessages,
  });

  final String cluster;
  final int protocolVersion;
  final String? token;
  final AuthTokenCallback? authCallback;
  final Duration activityTimeout;
  final bool? forceTls;
  final List<SockudoTransport>? enabledTransports;
  final List<SockudoTransport>? disabledTransports;
  final String? wsHost;
  final int wsPort;
  final int wssPort;
  final String wsPath;
  final String? httpHost;
  final int httpPort;
  final int httpsPort;
  final String httpPath;
  final Duration pongTimeout;
  final Duration unavailableTimeout;
  final bool enableStats;
  final String statsHost;
  final Map<String, AuthValue> timelineParams;
  final DeltaOptions? deltaCompression;
  final bool messageDeduplication;
  final int messageDeduplicationCapacity;
  final bool connectionRecovery;
  final bool echoMessages;
  final SockudoWireFormat wireFormat;
  final SockudoAppendMode appendMode;
  final int? appendRollupWindow;
  final ChannelAuthorizationOptions channelAuthorization;
  final UserAuthenticationOptions userAuthentication;
  final PresenceHistoryOptions? presenceHistory;
  final VersionedMessagesOptions? versionedMessages;
}

class SockudoClient {
  SockudoClient(this.key, this.options, {http.Client? httpClient})
    : _httpClient = httpClient ?? http.Client(),
      _config = ResolvedConfiguration(options, httpClient ?? http.Client()),
      _p = ProtocolPrefix(options.protocolVersion) {
    if (key.isEmpty) {
      throw const SockudoException(
        'You must pass your app key when you instantiate SockudoClient.',
      );
    }
    if (options.cluster.isEmpty) {
      throw const SockudoException('Options must provide a cluster.');
    }
    final appendRollupWindow = options.appendRollupWindow;
    if (appendRollupWindow != null &&
        !allowedAppendRollupWindows.contains(appendRollupWindow)) {
      throw SockudoException(
        'appendRollupWindow must be one of '
        '${allowedAppendRollupWindows.join(', ')}.',
      );
    }

    _deltaManager = options.deltaCompression == null
        ? null
        : DeltaCompressionManager(
            options.deltaCompression!,
            (event, data) => _sendEvent(event, data, null),
            prefix: _p,
          );
    _deduplicator = options.messageDeduplication
        ? MessageDeduplicator(capacity: options.messageDeduplicationCapacity)
        : null;
    user._attach(this);
    watchlist._attach(this);
  }

  final String key;
  final SockudoOptions options;
  final http.Client _httpClient;
  final ResolvedConfiguration _config;
  final ProtocolPrefix _p;
  final EventDispatcher _dispatcher = EventDispatcher();
  final Map<String, SockudoChannel> _channels = <String, SockudoChannel>{};
  Future<sodium.Sodium>? _sodium;
  late final DeltaCompressionManager? _deltaManager;
  late final MessageDeduplicator? _deduplicator;
  final Map<String, RecoveryPosition> _channelPositions =
      <String, RecoveryPosition>{};

  IOWebSocketChannel? _webSocket;
  StreamSubscription<Object?>? _socketSubscription;
  Timer? _activityTimer;
  Timer? _unavailableTimer;
  Timer? _retryTimer;
  SockudoTransport? _currentTransport;
  bool _attemptedFallback = false;
  bool _manuallyDisconnected = false;
  Future<void>? _authRefreshFuture;
  Timer? _authRefreshTimer;
  int _openAttempt = 0;

  final UserFacade user = UserFacade();
  final WatchlistFacade watchlist = WatchlistFacade();

  ConnectionState connectionState = ConnectionState.initialized;
  String? socketId;

  bool get shouldUseTls => _config.useTls;
  List<SockudoChannel> get channels => _channels.values.toList(growable: false);

  EventBindingToken on(String eventName, EventCallback callback) =>
      _dispatcher.bind(eventName, callback);

  EventBindingToken bind(String eventName, EventCallback callback) =>
      on(eventName, callback);

  EventBindingToken onGlobal(GlobalEventCallback callback) =>
      _dispatcher.bindGlobal(callback);

  EventBindingToken bindGlobal(GlobalEventCallback callback) =>
      onGlobal(callback);

  void off({String? eventName, EventBindingToken? token}) {
    _dispatcher.unbind(eventName: eventName, token: token);
  }

  void unbind({String? eventName, EventBindingToken? token}) =>
      off(eventName: eventName, token: token);

  void unbindAll() => _dispatcher.unbind();

  SockudoChannel? channel(String name) => _channels[name];

  SockudoChannel subscribe(String channelName, {SubscriptionOptions? options}) {
    final channel = _channels.putIfAbsent(
      channelName,
      () => _createChannel(channelName),
    );
    if (options != null) {
      channel.filter = options.filter;
      channel.deltaSettings = options.delta;
      channel.eventsFilter = options.events;
      channel.rewind = options.rewind;
      channel.annotationSubscribe = options.annotationSubscribe;
    }
    channel._subscribeIfPossible();
    return channel;
  }

  SockudoChannel subscribeFiltered(String channelName, FilterNode filter) =>
      subscribe(channelName, options: SubscriptionOptions(filter: filter));

  void unsubscribe(String channelName) {
    final channel = _channels[channelName];
    if (channel == null) {
      return;
    }

    if (channel.subscriptionPending) {
      channel.subscriptionCancelled = true;
      return;
    }

    _channels.remove(channelName);
    if (channel.isSubscribed) {
      channel._unsubscribe();
    }
    _channelPositions.remove(channelName);
    _deltaManager?.clearChannelState(channelName);
  }

  void connect() {
    if (_webSocket != null) {
      return;
    }
    final transports = _transportSequence();
    if (transports.isEmpty) {
      _updateState(ConnectionState.failed);
      return;
    }
    _manuallyDisconnected = false;
    _attemptedFallback = false;
    _updateState(ConnectionState.connecting);
    _openWebSocket(transports.first);
    _setUnavailableTimer();
  }

  void disconnect() {
    _manuallyDisconnected = true;
    _openAttempt += 1;
    _cancelAuthRefreshTimer();
    _invalidateTimers();
    _socketSubscription?.cancel();
    _socketSubscription = null;
    _webSocket?.sink.close(ws_status.normalClosure);
    _webSocket = null;
    _currentTransport = null;
    for (final channel in _channels.values) {
      channel._disconnect();
    }
    _deduplicator?.clear();
    _updateState(ConnectionState.disconnected);
  }

  void signIn() => user.signIn();

  DeltaStats? getDeltaStats() => _deltaManager?.getStats();

  void resetDeltaStats() => _deltaManager?.resetStats();

  RecoveryPosition? getRecoveryPosition(String channelName) =>
      _channelPositions[channelName];

  Map<String, RecoveryPosition> getRecoveryPositions() =>
      Map<String, RecoveryPosition>.from(_channelPositions);

  void setRecoveryPosition(String channelName, RecoveryPosition? position) {
    if (position == null) {
      _channelPositions.remove(channelName);
    } else {
      _channelPositions[channelName] = position;
    }
  }

  void setRecoveryPositions(Map<String, RecoveryPosition> positions) {
    _channelPositions
      ..clear()
      ..addAll(positions);
  }

  void close() {
    disconnect();
    _httpClient.close();
  }

  bool _sendEvent(String name, Object? data, String? channelName) {
    final webSocket = _webSocket;
    if (webSocket == null) {
      return false;
    }
    final payload = <String, Object?>{'event': name, 'data': data};
    if (channelName != null) {
      payload['channel'] = channelName;
    }
    webSocket.sink.add(
      SockudoProtocolCodec.encodeEnvelope(payload, options.wireFormat),
    );
    return true;
  }

  Future<sodium.Sodium> _sodiumInstance() {
    final existing = _sodium;
    if (existing != null) {
      return existing;
    }
    final created = Future<sodium.Sodium>.value(sodium.SodiumInit.init());
    _sodium = created;
    return created;
  }

  void _subscribeAll() {
    for (final channel in _channels.values) {
      channel._subscribeIfPossible();
    }
  }

  SockudoChannel _createChannel(String name) {
    if (name.startsWith('private-encrypted-')) {
      return EncryptedChannel(name: name, client: this);
    }
    if (name.startsWith('presence-')) {
      return PresenceChannel(name: name, client: this);
    }
    if (name.startsWith('private-')) {
      return PrivateChannel(name: name, client: this);
    }
    return SockudoChannel(name: name, client: this);
  }

  void _openWebSocket(SockudoTransport transport) {
    _currentTransport = transport;
    final attempt = ++_openAttempt;
    unawaited(_openWebSocketWithToken(transport, attempt));
  }

  Future<void> _openWebSocketWithToken(
    SockudoTransport transport,
    int attempt,
  ) async {
    final String? token;
    try {
      token = await _resolveInitialCapabilityToken();
    } catch (error) {
      if (!_manuallyDisconnected && attempt == _openAttempt) {
        _dispatcher.emit('error', error);
        _scheduleRetry(const Duration(seconds: 1));
      }
      return;
    }
    if (_manuallyDisconnected ||
        attempt != _openAttempt ||
        _webSocket != null) {
      return;
    }

    final uri = _socketUri(transport, token: token);
    final channel = IOWebSocketChannel.connect(
      uri,
      pingInterval: options.protocolVersion >= 2
          ? _config.activityTimeout
          : null,
    );
    _webSocket = channel;
    _socketSubscription = channel.stream.listen(
      _handleRawMessage,
      onError: (Object error, StackTrace stackTrace) {
        _dispatcher.emit('error', error);
        _handleSocketClosed(1006, error.toString());
      },
      onDone: () {
        _handleSocketClosed(channel.closeCode ?? 1000, channel.closeReason);
      },
    );
  }

  Future<String?> _resolveInitialCapabilityToken() async {
    if (options.protocolVersion != 2) {
      return null;
    }
    final initialToken = options.token;
    if (initialToken != null && initialToken.isNotEmpty) {
      _scheduleCapabilityTokenRefresh(initialToken);
      return initialToken;
    }
    final callback = options.authCallback;
    final token = callback == null ? null : await callback();
    if (token == null || token.isEmpty) {
      return null;
    }
    _scheduleCapabilityTokenRefresh(token);
    return token;
  }

  Future<void> _refreshCapabilityToken() async {
    if (options.protocolVersion != 2) {
      return;
    }
    final callback = options.authCallback;
    if (callback == null) {
      return;
    }
    final existing = _authRefreshFuture;
    if (existing != null) {
      await existing;
      return;
    }

    final future = () async {
      final token = await callback();
      if (token.isEmpty) {
        throw const SockudoException('authCallback returned an empty token');
      }
      final sent = _sendEvent(_p.event('auth'), <String, Object?>{
        'token': token,
      }, null);
      if (!sent) {
        throw const SockudoException('Unable to refresh auth token');
      }
      _scheduleCapabilityTokenRefresh(token);
    }();
    _authRefreshFuture = future;
    try {
      await future;
    } finally {
      if (identical(_authRefreshFuture, future)) {
        _authRefreshFuture = null;
      }
    }
  }

  void _scheduleCapabilityTokenRefresh(String token) {
    _cancelAuthRefreshTimer();
    if (options.protocolVersion != 2 || options.authCallback == null) {
      return;
    }

    final delay = _capabilityTokenRefreshDelay(token);
    if (delay == null) {
      return;
    }
    _authRefreshTimer = Timer(delay, () {
      _authRefreshTimer = null;
      if (_manuallyDisconnected || _webSocket == null) {
        return;
      }
      unawaited(
        _refreshCapabilityToken().catchError((Object error) {
          _dispatcher.emit('error', error);
        }),
      );
    });
  }

  Duration? _capabilityTokenRefreshDelay(String token) {
    final claims = _decodeJwtPayload(token);
    if (claims == null) {
      return null;
    }
    final exp = _numericDateSeconds(claims['exp']);
    if (exp == null) {
      return null;
    }
    final now = DateTime.now().millisecondsSinceEpoch / 1000.0;
    final iat = _numericDateSeconds(claims['iat']);
    final start = iat ?? now;
    final refreshAt = start + ((exp - start) * 0.8);
    final delayMs = ((refreshAt - now) * 1000).round();
    if (delayMs <= 0) {
      return Duration.zero;
    }
    return Duration(milliseconds: delayMs);
  }

  void _cancelAuthRefreshTimer() {
    _authRefreshTimer?.cancel();
    _authRefreshTimer = null;
  }

  void _handleRawMessage(Object? rawMessage) {
    try {
      final event = _decodeEvent(rawMessage);
      final message = event.rawMessage;
      _resetActivityTimer();

      // Message deduplication: skip already-processed messages.
      if (_deduplicator != null && event.messageId != null) {
        final messageId = event.messageId!;
        if (_deduplicator.isDuplicate(messageId)) return;
        _deduplicator.track(messageId);
      }

      // Track serial per channel for connection recovery
      if (options.connectionRecovery &&
          event.channel != null &&
          event.serial != null) {
        _channelPositions[event.channel!] = RecoveryPosition(
          streamId: event.streamId,
          serial: event.serial!,
          lastMessageId: event.messageId,
        );
      }

      final eventName = event.event;
      if (eventName == _p.event('connection_established')) {
        final payload = event.data as Map<Object?, Object?>?;
        final newSocketId = payload?['socket_id'] as String?;
        if (newSocketId == null) {
          throw const SockudoException('Invalid handshake');
        }
        socketId = newSocketId;
        _clearUnavailableTimer();
        _updateState(
          ConnectionState.connected,
          metadata: <String, Object?>{'socket_id': newSocketId},
        );
        _subscribeAll();
        if (options.connectionRecovery && _channelPositions.isNotEmpty) {
          _sendEvent(
            _p.event('resume'),
            jsonEncode(<String, Object?>{
              'channel_positions': _channelPositions.map(
                (channel, position) => MapEntry(channel, position.toJson()),
              ),
            }),
            null,
          );
        }
        if (options.deltaCompression?.enabled == true) {
          _deltaManager?.enable();
        }
        user._handleConnected();
      } else if (eventName == _p.event('error')) {
        _dispatcher.emit('error', event.data);
      } else if (eventName == _p.event('auth_success')) {
        _dispatcher.emit(eventName, event.data);
      } else if (eventName == _p.event('token_expired')) {
        _dispatcher.emit(eventName, event.data);
        if (_tokenExpiredCode(event.data) == 40142) {
          unawaited(
            _refreshCapabilityToken().catchError((Object error) {
              _dispatcher.emit('error', error);
            }),
          );
        }
      } else if (eventName == _p.event('ping')) {
        _sendEvent(_p.event('pong'), const <String, Object?>{}, null);
      } else if (eventName == _p.event('pong')) {
        return;
      } else if (eventName == _p.event('signin_success')) {
        user._handleSignInSuccess(event.data);
      } else if (eventName == _p.internal('watchlist_events')) {
        watchlist._handle(event.data);
      } else if (eventName == _p.event('delta_compression_enabled')) {
        _deltaManager?.handleEnabled(event.data);
        _dispatcher.emit(eventName, event.data);
      } else if (eventName == _p.event('delta_cache_sync')) {
        final channelName = event.channel;
        if (channelName != null) {
          _deltaManager?.handleCacheSync(channelName, event.data);
        }
      } else if (eventName == _p.event('delta')) {
        final channelName = event.channel;
        if (channelName != null) {
          final reconstructed = _deltaManager?.handleDeltaMessage(
            channelName,
            event.data,
          );
          if (reconstructed != null) {
            _channels[channelName]?._handle(reconstructed);
            _dispatcher.emit(reconstructed.event, reconstructed.data);
          }
        }
      } else if (eventName == _p.event('resume_success')) {
        final data = _decodeResumeSuccessData(event.data);
        SockudoLogger.debug('Connection recovery succeeded: $data');
        _dispatcher.emit(eventName, data);
      } else if (eventName == _p.event('resume_failed')) {
        final failData = _decodeResumeFailedData(event.data);
        final failedChannelName = failData.channel;
        if (failedChannelName.isNotEmpty) {
          _channelPositions.remove(failedChannelName);
          SockudoLogger.warn(
            'Connection recovery failed for channel: $failedChannelName',
          );
          final failedChannel = _channels[failedChannelName];
          if (failedChannel != null) {
            unawaited(failedChannel._subscribe());
          }
        }
        _dispatcher.emit(eventName, failData);
      } else {
        final normalizedEvent = eventName == _p.event('rewind_complete')
            ? SockudoEvent(
                event: event.event,
                rawMessage: event.rawMessage,
                channel: event.channel,
                data: _decodeRewindCompleteData(event.data),
                userId: event.userId,
                streamId: event.streamId,
                messageId: event.messageId,
                sequence: event.sequence,
                conflationKey: event.conflationKey,
                serial: event.serial,
                extras: event.extras,
              )
            : event;
        final channelName = normalizedEvent.channel;
        if (channelName != null) {
          _channels[channelName]?._handle(normalizedEvent);
          if (!_p.isPlatformEvent(eventName) &&
              !_p.isInternalEvent(eventName) &&
              normalizedEvent.sequence != null) {
            final sequence = wireSerialAsInt(normalizedEvent.sequence);
            if (sequence != null) {
              _deltaManager?.handleFullMessage(
                channelName,
                _stripDeltaMetadata(message),
                sequence,
                normalizedEvent.conflationKey,
              );
            }
          }
        }
        if (!_p.isInternalEvent(eventName)) {
          _dispatcher.emit(
            eventName,
            normalizedEvent.data,
            metadata: EventMetadata(userId: normalizedEvent.userId),
          );
        }
      }
    } catch (error) {
      if (error is SockudoException &&
          error.message == 'Unable to decode event envelope') {
        SockudoLogger.debug('Ignoring socket payload without an event name');
        return;
      }
      _dispatcher.emit('error', error);
    }
  }

  SockudoEvent _decodeEvent(Object? rawMessage) {
    return SockudoProtocolCodec.decodeEvent(rawMessage, options.wireFormat);
  }

  ResumeSuccessData _decodeResumeSuccessData(Object? raw) {
    final payload = raw is Map ? raw : const <Object?, Object?>{};
    final recoveredRaw = payload['recovered'];
    final failedRaw = payload['failed'];
    return ResumeSuccessData(
      recovered: recoveredRaw is List
          ? recoveredRaw
                .whereType<Map>()
                .map(
                  (item) => ResumeRecoveredChannel(
                    channel: item['channel'] as String? ?? '',
                    source: item['source'] as String? ?? '',
                    replayed: (item['replayed'] as num?)?.toInt() ?? 0,
                  ),
                )
                .toList(growable: false)
          : const <ResumeRecoveredChannel>[],
      failed: failedRaw is List
          ? failedRaw.map(_decodeResumeFailedData).toList(growable: false)
          : const <ResumeFailedChannel>[],
    );
  }

  ResumeFailedChannel _decodeResumeFailedData(Object? raw) {
    final payload = raw is Map ? raw : const <Object?, Object?>{};
    return ResumeFailedChannel(
      channel: payload['channel'] as String? ?? '',
      code: payload['code'] as String? ?? '',
      reason: payload['reason'] as String? ?? '',
      expectedStreamId: payload['expected_stream_id'] as String?,
      currentStreamId: payload['current_stream_id'] as String?,
      oldestAvailableSerial: (payload['oldest_available_serial'] as num?)
          ?.toInt(),
      newestAvailableSerial: (payload['newest_available_serial'] as num?)
          ?.toInt(),
    );
  }

  RewindCompleteData _decodeRewindCompleteData(Object? raw) {
    final payload = raw is Map ? raw : const <Object?, Object?>{};
    return RewindCompleteData(
      historicalCount: (payload['historical_count'] as num?)?.toInt() ?? 0,
      liveCount: (payload['live_count'] as num?)?.toInt() ?? 0,
      complete: payload['complete'] as bool? ?? false,
      truncatedByRetention: payload['truncated_by_retention'] as bool? ?? false,
      truncatedByLimit: payload['truncated_by_limit'] as bool? ?? false,
    );
  }

  int? _tokenExpiredCode(Object? raw) {
    final payload = raw is Map ? raw : const <Object?, Object?>{};
    return (payload['code'] as num?)?.toInt();
  }

  String _stripDeltaMetadata(String rawMessage) {
    return rawMessage
        .replaceAll(RegExp(r',"__delta_seq":\d+'), '')
        .replaceAll(RegExp(r'"__delta_seq":\d+,'), '')
        .replaceAll(RegExp(r',"__conflation_key":"[^"]*"'), '')
        .replaceAll(RegExp(r'"__conflation_key":"[^"]*",'), '');
  }

  void _handleSocketClosed(int code, String? reason) {
    _activityTimer?.cancel();
    _cancelAuthRefreshTimer();
    _clearUnavailableTimer();
    _socketSubscription?.cancel();
    _socketSubscription = null;
    _webSocket = null;
    for (final channel in _channels.values) {
      channel._disconnect();
    }

    switch (_closeAction(code)) {
      case _CloseAction.tlsOnly:
        _config.useTls = true;
        _scheduleRetry(Duration.zero);
      case _CloseAction.backoff:
        _scheduleRetry(const Duration(seconds: 1));
      case _CloseAction.retry:
        _scheduleRetry(Duration.zero);
      case _CloseAction.refused:
        _updateState(ConnectionState.disconnected);
      case null:
        if (!_manuallyDisconnected) {
          _scheduleRetry(const Duration(seconds: 1));
        }
    }

    if (reason != null && reason.isNotEmpty) {
      _dispatcher.emit(
        'error',
        const SockudoException('Connection unavailable'),
      );
      SockudoLogger.warn('Socket closed $code $reason');
    }
  }

  _CloseAction? _closeAction(int code) {
    if (code < 4000) {
      return (code >= 1002 && code <= 1004) ? _CloseAction.backoff : null;
    }
    if (code == 4000) {
      return _CloseAction.tlsOnly;
    }
    if (code < 4100) {
      return _CloseAction.refused;
    }
    if (code < 4200) {
      return _CloseAction.backoff;
    }
    if (code < 4300) {
      return _CloseAction.retry;
    }
    return _CloseAction.refused;
  }

  Uri _socketUri(SockudoTransport transport, {String? token}) {
    final isTls = transport == SockudoTransport.wss;
    final queryParameters = <String, String>{
      'protocol': '${options.protocolVersion}',
      'client': 'flutter',
      'version': '2.0.0',
      'flash': 'false',
    };
    if (options.protocolVersion == 2) {
      queryParameters['format'] = options.wireFormat.queryValue;
      queryParameters['append_mode'] = options.appendMode.queryValue;
      if (options.appendRollupWindow != null) {
        queryParameters['append_rollup_window'] =
            '${options.appendRollupWindow}';
      }
      if (token != null) {
        queryParameters['token'] = token;
      }
    }
    return Uri(
      scheme: isTls ? 'wss' : 'ws',
      host: _config.wsHost,
      port: isTls ? _config.wssPort : _config.wsPort,
      path: '${_config.wsPath}/app/$key',
      queryParameters: queryParameters,
    );
  }

  List<SockudoTransport> _transportSequence() {
    var transports = _config.useTls
        ? <SockudoTransport>[SockudoTransport.wss]
        : <SockudoTransport>[SockudoTransport.ws, SockudoTransport.wss];
    if (_config.enabledTransports != null) {
      transports = transports
          .where(_config.enabledTransports!.contains)
          .toList(growable: false);
    }
    if (_config.disabledTransports != null) {
      transports = transports
          .where(
            (transport) => !_config.disabledTransports!.contains(transport),
          )
          .toList(growable: false);
    }
    return transports;
  }

  void _sendPing() {
    if (options.protocolVersion >= 2) {
      return;
    }
    _sendEvent(_p.event('ping'), const <String, Object?>{}, null);
    _activityTimer?.cancel();
    _activityTimer = Timer(_config.pongTimeout, () {
      _scheduleRetry(Duration.zero);
    });
  }

  void _resetActivityTimer() {
    _activityTimer?.cancel();
    if (options.protocolVersion >= 2) {
      _activityTimer = null;
      return;
    }
    _activityTimer = Timer(_config.activityTimeout, _sendPing);
  }

  void _setUnavailableTimer() {
    _clearUnavailableTimer();
    _unavailableTimer = Timer(_config.unavailableTimeout, () {
      _updateState(ConnectionState.unavailable);
    });
  }

  void _clearUnavailableTimer() {
    _unavailableTimer?.cancel();
    _unavailableTimer = null;
  }

  void _scheduleRetry(Duration after) {
    if (_manuallyDisconnected) {
      return;
    }
    _retryTimer?.cancel();
    _retryTimer = Timer(after, () {
      _socketSubscription?.cancel();
      _socketSubscription = null;
      _webSocket = null;
      _updateState(ConnectionState.connecting);
      final transports = _transportSequence();
      if (_currentTransport == SockudoTransport.ws &&
          !_attemptedFallback &&
          transports.contains(SockudoTransport.wss)) {
        _attemptedFallback = true;
        _openWebSocket(SockudoTransport.wss);
      } else {
        _attemptedFallback = false;
        _openWebSocket(
          transports.isEmpty ? SockudoTransport.wss : transports.first,
        );
      }
      _setUnavailableTimer();
    });
  }

  void _invalidateTimers() {
    _activityTimer?.cancel();
    _activityTimer = null;
    _cancelAuthRefreshTimer();
    _clearUnavailableTimer();
    _retryTimer?.cancel();
    _retryTimer = null;
  }

  void _updateState(ConnectionState state, {Map<String, Object?>? metadata}) {
    final previous = connectionState;
    connectionState = state;
    _dispatcher.emit('state_change', <String, String>{
      'previous': previous.name,
      'current': state.name,
    });
    _dispatcher.emit(state.name, metadata);
  }
}

class SockudoChannel {
  SockudoChannel({required this.name, required this.client})
    : _dispatcher = EventDispatcher(
        failThrough: (eventName, _) =>
            SockudoLogger.debug('No callbacks on $name for $eventName'),
      );

  final String name;
  final SockudoClient client;
  final EventDispatcher _dispatcher;

  bool isSubscribed = false;
  bool subscriptionPending = false;
  bool subscriptionCancelled = false;
  int? subscriptionCount;
  Object? attachSerial;
  FilterNode? filter;
  ChannelDeltaSettings? deltaSettings;
  List<String>? eventsFilter;
  SubscriptionRewind? rewind;
  bool annotationSubscribe = false;

  EventBindingToken on(String eventName, EventCallback callback) =>
      _dispatcher.bind(eventName, callback);

  EventBindingToken bind(String eventName, EventCallback callback) =>
      on(eventName, callback);

  EventBindingToken onGlobal(GlobalEventCallback callback) =>
      _dispatcher.bindGlobal(callback);

  EventBindingToken bindGlobal(GlobalEventCallback callback) =>
      onGlobal(callback);

  void off({String? eventName, EventBindingToken? token}) {
    _dispatcher.unbind(eventName: eventName, token: token);
  }

  void unbind({String? eventName, EventBindingToken? token}) =>
      off(eventName: eventName, token: token);

  void unbindAll() => _dispatcher.unbind();

  bool trigger(String event, Object data) {
    if (!event.startsWith('client-')) {
      throw SockudoException("Event '$event' does not start with 'client-'");
    }
    if (!isSubscribed) {
      SockudoLogger.warn(
        'Client event triggered before channel subscription succeeded',
      );
    }
    return client._sendEvent(event, data, name);
  }

  Future<ChannelAuthorizationData> _authorize(String socketId) async =>
      const ChannelAuthorizationData(auth: '');

  void _subscribeIfPossible() {
    if (subscriptionPending && subscriptionCancelled) {
      subscriptionCancelled = false;
      return;
    }

    if (!subscriptionPending &&
        client.connectionState == ConnectionState.connected) {
      unawaited(_subscribe());
    }
  }

  Future<void> _subscribe() async {
    if (isSubscribed) {
      return;
    }
    subscriptionPending = true;
    subscriptionCancelled = false;

    try {
      final auth = await _authorize(client.socketId ?? '');
      final payload = <String, Object?>{'auth': auth.auth, 'channel': name};
      if (auth.channelData != null) {
        payload['channel_data'] = auth.channelData;
      }
      if (filter != null) {
        payload['tags_filter'] = filter!.toJson();
      }
      if (deltaSettings != null) {
        payload['delta'] = deltaSettings!.toSubscriptionValue();
      }
      if (eventsFilter != null) {
        payload['events'] = eventsFilter;
      }
      if (rewind != null) {
        payload['rewind'] = rewind!.toSubscriptionValue();
      }
      if (annotationSubscribe) {
        payload['modes'] = const <String>['SUBSCRIBE', 'ANNOTATION_SUBSCRIBE'];
      }
      client._sendEvent(client._p.event('subscribe'), payload, null);
    } catch (error) {
      subscriptionPending = false;
      _dispatcher.emit(client._p.event('subscription_error'), <String, Object?>{
        'type': 'AuthError',
        'error': error.toString(),
      });
    }
  }

  void _unsubscribe() {
    isSubscribed = false;
    client._sendEvent(client._p.event('unsubscribe'), <String, Object?>{
      'channel': name,
    }, null);
  }

  void _disconnect() {
    isSubscribed = false;
    subscriptionPending = false;
    attachSerial = null;
  }

  void _handle(SockudoEvent event) {
    final p = client._p;
    if (event.event == p.internal('subscription_succeeded')) {
      subscriptionPending = false;
      isSubscribed = true;
      final data = event.data is Map<Object?, Object?>
          ? event.data as Map<Object?, Object?>
          : null;
      attachSerial = normalizeWireSerial(data?['attach_serial']);
      if (subscriptionCancelled) {
        client.unsubscribe(name);
      } else {
        _dispatcher.emit(p.event('subscription_succeeded'), event.data);
      }
    } else if (event.event == p.internal('subscription_count')) {
      final data = event.data is Map<Object?, Object?>
          ? event.data as Map<Object?, Object?>
          : null;
      subscriptionCount = (data?['subscription_count'] as num?)?.toInt();
      _dispatcher.emit(p.event('subscription_count'), event.data);
    } else if (event.event == p.internal('message')) {
      final data = event.data is Map<Object?, Object?>
          ? event.data as Map<Object?, Object?>
          : null;
      if (data?['action'] == 'message.summary') {
        _dispatcher.emit('message.summary', event.data);
      }
    } else if (event.event == p.internal('annotation')) {
      final data = event.data is Map<Object?, Object?>
          ? event.data as Map<Object?, Object?>
          : null;
      final action = data?['action'] as String?;
      if (action != null) {
        _dispatcher.emit(action, event.data);
      }
    } else if (p.isInternalEvent(event.event)) {
      _dispatcher.emit(event.event, event.data);
    } else if (!p.isInternalEvent(event.event)) {
      _dispatcher.emit(
        event.event,
        event.data,
        metadata: EventMetadata(userId: event.userId),
      );
    }
  }

  Future<MessageAck> createMessage([
    VersionedMessageCreateRequest request =
        const VersionedMessageCreateRequest(),
  ]) {
    return client._config.createMessage(
      name,
      request,
      defaultName: client._p.event('message.create'),
    );
  }

  Future<MessageAck> appendMessage(
    String messageSerial,
    String data, [
    VersionedMessageMutation mutation = const VersionedMessageMutation(),
  ]) {
    return client._config.appendMessage(name, messageSerial, data, mutation);
  }

  Future<MessageAck> updateMessage(
    String messageSerial, [
    VersionedMessageMutation mutation = const VersionedMessageMutation(),
  ]) {
    return client._config.updateMessage(name, messageSerial, mutation);
  }

  Future<MessageAck> deleteMessage(
    String messageSerial, [
    VersionedMessageMutation mutation = const VersionedMessageMutation(),
  ]) {
    return client._config.deleteMessage(name, messageSerial, mutation);
  }

  Future<PublishAnnotationResponse> publishAnnotation(
    String messageSerial,
    PublishAnnotationRequest annotation,
  ) {
    return client._config.publishAnnotation(name, messageSerial, annotation);
  }

  Future<DeleteAnnotationResponse> deleteAnnotation(
    String messageSerial,
    String annotationSerial, {
    String? socketId,
  }) {
    return client._config.deleteAnnotation(
      name,
      messageSerial,
      annotationSerial,
      socketId: socketId,
    );
  }

  Future<AnnotationEventsPage> listAnnotations(
    String messageSerial, [
    AnnotationEventsParams params = const AnnotationEventsParams(),
  ]) {
    return client._config.listAnnotations(name, messageSerial, params);
  }
}

class PrivateChannel extends SockudoChannel {
  PrivateChannel({required super.name, required super.client});

  @override
  Future<ChannelAuthorizationData> _authorize(String socketId) {
    return client._config.channelAuthorizer(
      ChannelAuthorizationRequest(socketId: socketId, channelName: name),
    );
  }
}

class PresenceMembers {
  final Map<String, Object?> _members = <String, Object?>{};

  int count = 0;
  String? myId;
  PresenceMember? me;

  PresenceMember? member(String id) {
    if (!_members.containsKey(id)) {
      return null;
    }
    return PresenceMember(id: id, info: _members[id]);
  }

  void forEach(void Function(PresenceMember member) body) {
    _members.forEach((id, info) => body(PresenceMember(id: id, info: info)));
  }

  void _rememberMyId(String id) {
    myId = id;
  }

  void _applySubscriptionData(Map<Object?, Object?> data) {
    final presence = data['presence'] is Map<Object?, Object?>
        ? data['presence'] as Map<Object?, Object?>
        : null;
    final hash = presence?['hash'] is Map<Object?, Object?>
        ? presence!['hash'] as Map<Object?, Object?>
        : const <Object?, Object?>{};
    _members
      ..clear()
      ..addAll(hash.map((key, value) => MapEntry('$key', value)));
    count = (presence?['count'] as num?)?.toInt() ?? _members.length;
    me = myId == null ? null : member(myId!);
  }

  PresenceMember? _add(Map<Object?, Object?> data) {
    final userId = data['user_id'] as String?;
    if (userId == null) {
      return null;
    }
    final info = data['user_info'];
    if (!_members.containsKey(userId)) {
      count += 1;
    }
    _members[userId] = info;
    return PresenceMember(id: userId, info: info);
  }

  PresenceMember? _remove(Map<Object?, Object?> data) {
    final userId = data['user_id'] as String?;
    if (userId == null || !_members.containsKey(userId)) {
      return null;
    }
    final info = _members.remove(userId);
    count = count > 0 ? count - 1 : 0;
    return PresenceMember(id: userId, info: info);
  }

  PresenceMember? _update(Map<Object?, Object?> data) {
    final userId = data['user_id'] as String?;
    if (userId == null || !_members.containsKey(userId)) {
      return null;
    }
    final info = data['user_info'];
    _members[userId] = info;
    final member = PresenceMember(id: userId, info: info);
    if (myId == userId) {
      me = member;
    }
    return member;
  }

  void _reset() {
    _members.clear();
    count = 0;
    myId = null;
    me = null;
  }
}

class PresenceChannel extends PrivateChannel {
  PresenceChannel({required super.name, required super.client});

  final PresenceMembers members = PresenceMembers();

  bool update(Object? data) {
    if (client.options.protocolVersion != 2) {
      throw const SockudoException('presence_update requires Protocol V2');
    }
    if (!isSubscribed) {
      SockudoLogger.warn(
        'Presence update sent before channel subscription succeeded',
      );
    }
    return client._sendEvent(
      client._p.event('presence_update'),
      <String, Object?>{'channel': name, 'data': data},
      null,
    );
  }

  @override
  Future<ChannelAuthorizationData> _authorize(String socketId) async {
    final auth = await super._authorize(socketId);
    final channelData = auth.channelData;
    if (channelData != null) {
      final parsed = JsonSupport.decode(channelData) as Map<Object?, Object?>;
      final userId = parsed['user_id'] as String?;
      if (userId != null) {
        members._rememberMyId(userId);
        return auth;
      }
    }

    if (client.user.userId != null) {
      members._rememberMyId(client.user.userId!);
      return auth;
    }

    throw SockudoException(
      "Invalid auth response for presence channel '$name'",
    );
  }

  @override
  void _handle(SockudoEvent event) {
    final p = client._p;
    if (event.event == p.internal('subscription_succeeded')) {
      subscriptionPending = false;
      isSubscribed = true;
      if (subscriptionCancelled) {
        client.unsubscribe(name);
      } else {
        final data = event.data is Map<Object?, Object?>
            ? event.data as Map<Object?, Object?>
            : <Object?, Object?>{};
        attachSerial = normalizeWireSerial(data['attach_serial']);
        members._applySubscriptionData(data);
        _dispatcher.emit(p.event('subscription_succeeded'), members);
      }
    } else if (event.event == p.internal('subscription_count')) {
      super._handle(event);
    } else if (event.event == p.internal('member_added')) {
      final data = event.data is Map<Object?, Object?>
          ? event.data as Map<Object?, Object?>
          : <Object?, Object?>{};
      final member = members._add(data);
      if (member != null) {
        _dispatcher.emit(p.event('member_added'), member);
      }
    } else if (event.event == p.internal('member_removed')) {
      final data = event.data is Map<Object?, Object?>
          ? event.data as Map<Object?, Object?>
          : <Object?, Object?>{};
      final member = members._remove(data);
      if (member != null) {
        _dispatcher.emit(p.event('member_removed'), member);
      }
    } else if (event.event == p.internal('presence_update')) {
      final data = event.data is Map<Object?, Object?>
          ? event.data as Map<Object?, Object?>
          : <Object?, Object?>{};
      final member = members._update(data);
      if (member != null) {
        _dispatcher.emit(p.event('presence_update'), member);
      }
    } else if (p.isInternalEvent(event.event)) {
      _dispatcher.emit(event.event, event.data);
    } else if (!p.isInternalEvent(event.event)) {
      _dispatcher.emit(
        event.event,
        event.data,
        metadata: EventMetadata(userId: event.userId),
      );
    }
  }

  @override
  void _disconnect() {
    members._reset();
    super._disconnect();
  }

  Future<PresenceHistoryPage> history([
    PresenceHistoryParams params = const PresenceHistoryParams(),
  ]) {
    return client._config.fetchPresenceHistory(name, params);
  }

  Future<PresenceSnapshot> snapshot([
    PresenceSnapshotParams params = const PresenceSnapshotParams(),
  ]) {
    return client._config.fetchPresenceSnapshot(name, params);
  }

  Future<ChannelHistoryPageProxy> channelHistory([
    ChannelHistoryParams params = const ChannelHistoryParams(),
  ]) {
    return client._config.fetchChannelHistory(name, params);
  }

  Future<Map<Object?, Object?>> getMessage(String messageSerial) {
    return client._config.fetchLatestMessage(name, messageSerial);
  }

  Future<MessageVersionsPage> getMessageVersions(
    String messageSerial, [
    MessageVersionsParams params = const MessageVersionsParams(),
  ]) {
    return client._config.fetchMessageVersions(name, messageSerial, params);
  }
}

class EncryptedChannel extends PrivateChannel {
  EncryptedChannel({required super.name, required super.client});

  Uint8List? _sharedSecret;

  @override
  Future<ChannelAuthorizationData> _authorize(String socketId) async {
    final auth = await super._authorize(socketId);
    if (auth.sharedSecret == null) {
      throw SockudoException(
        'No shared_secret key in auth payload for encrypted channel: $name',
      );
    }
    _sharedSecret = base64Decode(auth.sharedSecret!);
    return ChannelAuthorizationData(
      auth: auth.auth,
      channelData: auth.channelData,
    );
  }

  @override
  bool trigger(String event, Object data) {
    throw const SockudoException(
      'Client events are not currently supported for encrypted channels',
    );
  }

  @override
  void _handle(SockudoEvent event) {
    final p = client._p;
    if (p.isInternalEvent(event.event) || p.isPlatformEvent(event.event)) {
      super._handle(event);
      return;
    }

    final secret = _sharedSecret;
    if (secret == null) {
      SockudoLogger.debug(
        'Received encrypted event before key has been retrieved from the auth endpoint',
      );
      return;
    }

    final payload = event.data as Map<Object?, Object?>?;
    final cipherText = payload?['ciphertext'] as String?;
    final nonce = payload?['nonce'] as String?;
    if (cipherText == null || nonce == null) {
      SockudoLogger.error('Unexpected format for encrypted event on $name');
      return;
    }

    unawaited(() async {
      final sodiumInstance = await client._sodiumInstance();
      final decrypted = _openCipherText(
        sodiumInstance,
        cipherText: cipherText,
        nonce: nonce,
        keyBytes: secret,
      );
      if (decrypted != null) {
        _emitDecrypted(event.event, decrypted);
        return;
      }

      try {
        await _authorize(client.socketId ?? '');
      } catch (error) {
        SockudoLogger.error('Failed to fetch new encryption key: $error');
        return;
      }

      final refreshed = _sharedSecret;
      if (refreshed == null) {
        return;
      }
      final retried = _openCipherText(
        sodiumInstance,
        cipherText: cipherText,
        nonce: nonce,
        keyBytes: refreshed,
      );
      if (retried == null) {
        SockudoLogger.error(
          'Failed to decrypt event with new key. Dropping encrypted event',
        );
        return;
      }
      _emitDecrypted(event.event, retried);
    }());
  }

  Uint8List? _openCipherText(
    sodium.Sodium sodiumInstance, {
    required String cipherText,
    required String nonce,
    required Uint8List keyBytes,
  }) {
    final key = sodium.SecureKey.fromList(sodiumInstance, keyBytes);
    try {
      return sodiumInstance.crypto.secretBox.openEasy(
        cipherText: base64Decode(cipherText),
        nonce: base64Decode(nonce),
        key: key,
      );
    } catch (_) {
      return null;
    } finally {
      key.dispose();
    }
  }

  void _emitDecrypted(String eventName, Uint8List bytes) {
    final raw = utf8.decode(bytes);
    Object? payload;
    try {
      payload = JsonSupport.decode(raw);
    } catch (_) {
      payload = raw;
    }
    _dispatcher.emit(eventName, payload);
  }
}

class UserFacade {
  SockudoClient? _client;
  final EventDispatcher _dispatcher = EventDispatcher(
    failThrough: (eventName, _) =>
        SockudoLogger.debug('No callbacks on user for $eventName'),
  );

  bool isSignInRequested = false;
  Map<Object?, Object?>? userData;

  String? get userId => userData?['id'] as String?;

  void _attach(SockudoClient client) {
    _client = client;
  }

  EventBindingToken on(String eventName, EventCallback callback) =>
      _dispatcher.bind(eventName, callback);

  void signIn() {
    isSignInRequested = true;
    _attemptSignIn();
  }

  void _handleConnected() => _attemptSignIn();

  void _handleSignInSuccess(Object? data) {
    final payload = data as Map<Object?, Object?>?;
    final userDataString = payload?['user_data'] as String?;
    if (userDataString == null) {
      _cleanup();
      return;
    }
    final parsed = JsonSupport.decode(userDataString) as Map<Object?, Object?>;
    final id = parsed['id'] as String?;
    if (id == null || id.isEmpty) {
      _cleanup();
      return;
    }
    userData = parsed;
    _subscribeServerChannel(id);
  }

  void _attemptSignIn() {
    final client = _client;
    final currentSocketId = client?.socketId;
    if (client == null ||
        !isSignInRequested ||
        client.connectionState != ConnectionState.connected ||
        currentSocketId == null) {
      return;
    }

    unawaited(() async {
      try {
        final auth = await client._config.userAuthenticator(
          UserAuthenticationRequest(socketId: currentSocketId),
        );
        client._sendEvent(client._p.event('signin'), <String, Object?>{
          'auth': auth.auth,
          'user_data': auth.userData,
        }, null);
      } catch (_) {
        _cleanup();
      }
    }());
  }

  void _subscribeServerChannel(String userId) {
    final client = _client;
    if (client == null) {
      return;
    }
    final channel = SockudoChannel(
      name: '#server-to-user-$userId',
      client: client,
    );
    channel.onGlobal((eventName, data) {
      final p = client._p;
      if (!p.isInternalEvent(eventName) && !p.isPlatformEvent(eventName)) {
        _dispatcher.emit(eventName, data);
      }
    });
    channel._subscribeIfPossible();
  }

  void _cleanup() {
    userData = null;
  }
}

class WatchlistFacade {
  final EventDispatcher _dispatcher = EventDispatcher(
    failThrough: (eventName, _) =>
        SockudoLogger.debug('No callbacks on watchlist for $eventName'),
  );

  EventBindingToken on(String eventName, EventCallback callback) =>
      _dispatcher.bind(eventName, callback);

  void _attach(SockudoClient client) {}

  void _handle(Object? data) {
    final payload = data as Map<Object?, Object?>?;
    final events = payload?['events'] as List<Object?>?;
    if (events == null) {
      return;
    }
    for (final event in events.whereType<Map<Object?, Object?>>()) {
      final name = event['name'] as String?;
      if (name != null) {
        _dispatcher.emit(name, event);
      }
    }
  }
}

class ResolvedConfiguration {
  ResolvedConfiguration(this.options, http.Client httpClient)
    : cluster = options.cluster,
      wsHost = options.wsHost ?? 'ws-${options.cluster}.sockudo.com',
      wsPort = options.wsPort,
      wssPort = options.wssPort,
      wsPath = options.wsPath,
      httpHost = options.httpHost ?? 'sockjs-${options.cluster}.sockudo.com',
      httpPort = options.httpPort,
      httpsPort = options.httpsPort,
      httpPath = options.httpPath,
      pongTimeout = options.pongTimeout,
      unavailableTimeout = options.unavailableTimeout,
      enableStats = options.enableStats,
      statsHost = options.statsHost,
      timelineParams = options.timelineParams,
      enabledTransports = options.enabledTransports,
      disabledTransports = options.disabledTransports,
      _httpClient = httpClient,
      useTls = options.forceTls != false,
      activityTimeout = options.activityTimeout;

  final SockudoOptions options;
  final String cluster;
  final String wsHost;
  final int wsPort;
  final int wssPort;
  final String wsPath;
  final String httpHost;
  final int httpPort;
  final int httpsPort;
  final String httpPath;
  final Duration pongTimeout;
  final Duration unavailableTimeout;
  final bool enableStats;
  final String statsHost;
  final Map<String, AuthValue> timelineParams;
  final List<SockudoTransport>? enabledTransports;
  final List<SockudoTransport>? disabledTransports;
  final http.Client _httpClient;

  bool useTls;
  Duration activityTimeout;

  ChannelAuthorizationHandler get channelAuthorizer =>
      options.channelAuthorization.customHandler ?? _defaultChannelAuthorizer;

  UserAuthenticationHandler get userAuthenticator =>
      options.userAuthentication.customHandler ?? _defaultUserAuthenticator;

  Future<ChannelAuthorizationData> _defaultChannelAuthorizer(
    ChannelAuthorizationRequest request,
  ) {
    return _performAuthRequest<ChannelAuthorizationData>(
      endpoint: options.channelAuthorization.endpoint,
      headers: <String, String>{
        ...options.channelAuthorization.headers,
        ...?options.channelAuthorization.headersProvider?.call(),
      },
      params: <String, AuthValue>{
        ...options.channelAuthorization.params,
        ...?options.channelAuthorization.paramsProvider?.call(),
        'socket_id': AuthString(request.socketId),
        'channel_name': AuthString(request.channelName),
      },
      parse: (Map<Object?, Object?> json) {
        final auth = json['auth'] as String?;
        if (auth == null) {
          throw const SockudoException(
            'JSON returned from auth endpoint was invalid',
            statusCode: 200,
          );
        }
        return ChannelAuthorizationData(
          auth: auth,
          channelData: json['channel_data'] as String?,
          sharedSecret: json['shared_secret'] as String?,
        );
      },
    );
  }

  Future<UserAuthenticationData> _defaultUserAuthenticator(
    UserAuthenticationRequest request,
  ) {
    return _performAuthRequest<UserAuthenticationData>(
      endpoint: options.userAuthentication.endpoint,
      headers: <String, String>{
        ...options.userAuthentication.headers,
        ...?options.userAuthentication.headersProvider?.call(),
      },
      params: <String, AuthValue>{
        ...options.userAuthentication.params,
        ...?options.userAuthentication.paramsProvider?.call(),
        'socket_id': AuthString(request.socketId),
      },
      parse: (Map<Object?, Object?> json) {
        final auth = json['auth'] as String?;
        final userData = json['user_data'] as String?;
        if (auth == null || userData == null) {
          throw const SockudoException(
            'JSON returned from auth endpoint was invalid',
            statusCode: 200,
          );
        }
        return UserAuthenticationData(auth: auth, userData: userData);
      },
    );
  }

  Future<T> _performAuthRequest<T>({
    required String endpoint,
    required Map<String, String> headers,
    required Map<String, AuthValue> params,
    required T Function(Map<Object?, Object?> json) parse,
  }) async {
    final response = await _httpClient.post(
      Uri.parse(endpoint),
      headers: <String, String>{
        'Content-Type': 'application/x-www-form-urlencoded',
        ...headers,
      },
      body: encodeQuery(
        params.map(
          (key, value) => MapEntry<String, Object?>(key, value.stringValue),
        ),
      ),
    );

    if (response.statusCode != 200) {
      throw SockudoException(
        'Could not get auth info from endpoint, status: ${response.statusCode}',
        statusCode: response.statusCode,
      );
    }

    final parsed = JsonSupport.decode(response.body) as Map<Object?, Object?>;
    return parse(parsed);
  }

  Future<PresenceHistoryPage> fetchPresenceHistory(
    String channelName,
    PresenceHistoryParams params,
  ) async {
    final config = options.presenceHistory;
    if (config == null) {
      throw const SockudoException(
        'presenceHistory.endpoint must be configured to use presence.history(). This endpoint should proxy requests to the Sockudo server REST API.',
      );
    }

    final payload = await _performPresenceHistoryRequest(
      endpoint: config.endpoint,
      headers: <String, String>{
        ...config.headers,
        ...?config.headersProvider?.call(),
      },
      channelName: channelName,
      params: params.toJson(),
      action: 'history',
    );

    return _decodePresenceHistoryPage(
      payload,
      (cursor) => fetchPresenceHistory(
        channelName,
        PresenceHistoryParams(
          direction: params.direction,
          limit: params.limit,
          cursor: cursor,
          startSerial: params.startSerial,
          endSerial: params.endSerial,
          startTimeMs: params.startTimeMs,
          endTimeMs: params.endTimeMs,
          start: params.start,
          end: params.end,
        ),
      ),
    );
  }

  Future<PresenceSnapshot> fetchPresenceSnapshot(
    String channelName,
    PresenceSnapshotParams params,
  ) async {
    final config = options.presenceHistory;
    if (config == null) {
      throw const SockudoException(
        'presenceHistory.endpoint must be configured to use presence.snapshot(). This endpoint should proxy requests to the Sockudo server REST API.',
      );
    }

    final payload = await _performPresenceHistoryRequest(
      endpoint: config.endpoint,
      headers: <String, String>{
        ...config.headers,
        ...?config.headersProvider?.call(),
      },
      channelName: channelName,
      params: params.toJson(),
      action: 'snapshot',
    );

    return _decodePresenceSnapshot(payload);
  }

  Future<ChannelHistoryPageProxy> fetchChannelHistory(
    String channelName,
    ChannelHistoryParams params,
  ) async {
    final config = options.versionedMessages;
    if (config == null) {
      throw const SockudoException(
        'versionedMessages.endpoint must be configured to use channelHistory(). This endpoint should proxy requests to the Sockudo server REST API.',
      );
    }

    final payload = await _performPresenceHistoryRequest(
      endpoint: config.endpoint,
      headers: <String, String>{
        ...config.headers,
        ...?config.headersProvider?.call(),
      },
      channelName: channelName,
      params: params.toJson(),
      action: 'channel_history',
    );

    return _decodeChannelHistoryPage(
      payload,
      (cursor) => fetchChannelHistory(
        channelName,
        ChannelHistoryParams(
          direction: params.direction,
          limit: params.limit,
          cursor: cursor,
          startSerial: params.startSerial,
          endSerial: params.endSerial,
          startTimeMs: params.startTimeMs,
          endTimeMs: params.endTimeMs,
          untilAttach: params.untilAttach,
        ),
      ),
    );
  }

  Future<Map<Object?, Object?>> fetchLatestMessage(
    String channelName,
    String messageSerial,
  ) async {
    final config = options.versionedMessages;
    if (config == null) {
      throw const SockudoException(
        'versionedMessages.endpoint must be configured to use getMessage(). This endpoint should proxy requests to the Sockudo server REST API.',
      );
    }

    final payload = await _performPresenceHistoryRequest(
      endpoint: config.endpoint,
      headers: <String, String>{
        ...config.headers,
        ...?config.headersProvider?.call(),
      },
      channelName: channelName,
      params: const <String, Object>{},
      action: 'get_message',
      messageSerial: messageSerial,
    );

    return payload['item'] as Map<Object?, Object?>? ??
        const <Object?, Object?>{};
  }

  Future<MessageVersionsPage> fetchMessageVersions(
    String channelName,
    String messageSerial,
    MessageVersionsParams params,
  ) async {
    final config = options.versionedMessages;
    if (config == null) {
      throw const SockudoException(
        'versionedMessages.endpoint must be configured to use getMessageVersions(). This endpoint should proxy requests to the Sockudo server REST API.',
      );
    }

    final payload = await _performPresenceHistoryRequest(
      endpoint: config.endpoint,
      headers: <String, String>{
        ...config.headers,
        ...?config.headersProvider?.call(),
      },
      channelName: channelName,
      params: params.toJson(),
      action: 'get_message_versions',
      messageSerial: messageSerial,
    );

    return _decodeMessageVersionsPage(
      payload,
      channelName,
      (cursor) => fetchMessageVersions(
        channelName,
        messageSerial,
        MessageVersionsParams(
          direction: params.direction,
          limit: params.limit,
          cursor: cursor,
        ),
      ),
    );
  }

  Future<MessageAck> createMessage(
    String channelName,
    VersionedMessageCreateRequest request, {
    required String defaultName,
  }) async {
    final config = options.versionedMessages;
    if (config == null) {
      throw const SockudoException(
        'versionedMessages.endpoint must be configured to use createMessage(). This endpoint should proxy requests to the Sockudo server REST API.',
      );
    }

    final fields = request.toJson(
      defaultName: defaultName,
      defaultMessageId: _generatedMessageId(),
    );
    final payload = await _performPresenceHistoryRequest(
      endpoint: config.endpoint,
      headers: <String, String>{
        ...config.headers,
        ...?config.headersProvider?.call(),
      },
      channelName: channelName,
      params: const <String, Object>{},
      action: 'publish_create',
      extraFields: fields,
      timeout: config.timeout,
    );

    return _decodeMessageAck(
      payload,
      fallbackMessageSerial: request.messageSerial,
    );
  }

  Future<MessageAck> appendMessage(
    String channelName,
    String messageSerial,
    String data,
    VersionedMessageMutation mutation,
  ) {
    return _mutateMessage(
      channelName,
      messageSerial,
      action: 'message_append',
      mutation: mutation,
      dataOverride: data,
    );
  }

  Future<MessageAck> updateMessage(
    String channelName,
    String messageSerial,
    VersionedMessageMutation mutation,
  ) {
    return _mutateMessage(
      channelName,
      messageSerial,
      action: 'message_update',
      mutation: mutation,
    );
  }

  Future<MessageAck> deleteMessage(
    String channelName,
    String messageSerial,
    VersionedMessageMutation mutation,
  ) {
    return _mutateMessage(
      channelName,
      messageSerial,
      action: 'message_delete',
      mutation: mutation,
    );
  }

  Future<MessageAck> _mutateMessage(
    String channelName,
    String messageSerial, {
    required String action,
    required VersionedMessageMutation mutation,
    Object? dataOverride,
  }) async {
    final config = options.versionedMessages;
    if (config == null) {
      throw const SockudoException(
        'versionedMessages.endpoint must be configured to use versioned message mutations. This endpoint should proxy requests to the Sockudo server REST API.',
      );
    }

    final payload = await _performPresenceHistoryRequest(
      endpoint: config.endpoint,
      headers: <String, String>{
        ...config.headers,
        ...?config.headersProvider?.call(),
      },
      channelName: channelName,
      params: const <String, Object>{},
      action: action,
      messageSerial: messageSerial,
      extraFields: mutation.toJson(dataOverride: dataOverride),
      timeout: config.timeout,
    );

    return _decodeMessageAck(payload, fallbackMessageSerial: messageSerial);
  }

  Future<PublishAnnotationResponse> publishAnnotation(
    String channelName,
    String messageSerial,
    PublishAnnotationRequest annotation,
  ) async {
    final config = options.versionedMessages;
    if (config == null) {
      throw const SockudoException(
        'versionedMessages.endpoint must be configured to use publishAnnotation(). This endpoint should proxy requests to the Sockudo server REST API.',
      );
    }

    final payload = await _performPresenceHistoryRequest(
      endpoint: config.endpoint,
      headers: <String, String>{
        ...config.headers,
        ...?config.headersProvider?.call(),
      },
      channelName: channelName,
      params: const <String, Object>{},
      action: 'publish_annotation',
      messageSerial: messageSerial,
      annotation: annotation.toJson(),
    );

    return PublishAnnotationResponse(
      annotationSerial: payload['annotationSerial'] as String? ?? '',
    );
  }

  Future<DeleteAnnotationResponse> deleteAnnotation(
    String channelName,
    String messageSerial,
    String annotationSerial, {
    String? socketId,
  }) async {
    final config = options.versionedMessages;
    if (config == null) {
      throw const SockudoException(
        'versionedMessages.endpoint must be configured to use deleteAnnotation(). This endpoint should proxy requests to the Sockudo server REST API.',
      );
    }

    final payload = await _performPresenceHistoryRequest(
      endpoint: config.endpoint,
      headers: <String, String>{
        ...config.headers,
        ...?config.headersProvider?.call(),
      },
      channelName: channelName,
      params: const <String, Object>{},
      action: 'delete_annotation',
      messageSerial: messageSerial,
      annotationSerial: annotationSerial,
      socketId: socketId,
    );

    return DeleteAnnotationResponse(
      annotationSerial: payload['annotationSerial'] as String? ?? '',
      deletedAnnotationSerial:
          payload['deletedAnnotationSerial'] as String? ?? '',
    );
  }

  Future<AnnotationEventsPage> listAnnotations(
    String channelName,
    String messageSerial,
    AnnotationEventsParams params,
  ) async {
    final config = options.versionedMessages;
    if (config == null) {
      throw const SockudoException(
        'versionedMessages.endpoint must be configured to use listAnnotations(). This endpoint should proxy requests to the Sockudo server REST API.',
      );
    }

    final payload = await _performPresenceHistoryRequest(
      endpoint: config.endpoint,
      headers: <String, String>{
        ...config.headers,
        ...?config.headersProvider?.call(),
      },
      channelName: channelName,
      params: params.toJson(),
      action: 'list_annotations',
      messageSerial: messageSerial,
    );

    return _decodeAnnotationEventsPage(
      payload,
      channelName,
      messageSerial,
      (cursor) => listAnnotations(
        channelName,
        messageSerial,
        AnnotationEventsParams(
          type: params.type,
          limit: params.limit,
          fromSerial: cursor,
          socketId: params.socketId,
        ),
      ),
    );
  }

  Future<Map<Object?, Object?>> _performPresenceHistoryRequest({
    required String endpoint,
    required Map<String, String> headers,
    required String channelName,
    required Map<String, Object> params,
    required String action,
    String? messageSerial,
    String? annotationSerial,
    String? socketId,
    Map<String, Object?>? annotation,
    Map<String, Object?> extraFields = const <String, Object?>{},
    Duration? timeout,
  }) async {
    final body = <String, Object?>{
      ...extraFields,
      'channel': channelName,
      'params': params,
      'action': action,
      'messageSerial': messageSerial,
      'annotationSerial': annotationSerial,
      'socketId': socketId,
      'annotation': annotation,
    }..removeWhere((_, value) => value == null);
    final request = _httpClient.post(
      Uri.parse(endpoint),
      headers: <String, String>{'Content-Type': 'application/json', ...headers},
      body: jsonEncode(body),
    );
    final response = await (timeout == null
        ? request
        : request.timeout(timeout));

    if (response.statusCode < 200 || response.statusCode >= 300) {
      throw SockudoException(
        'Presence $action request failed (${response.statusCode}): ${response.body}',
        statusCode: response.statusCode,
      );
    }

    final parsed =
        jsonDecode(preserveUnsafeJsonSerials(response.body))
            as Map<Object?, Object?>;
    return parsed;
  }

  MessageAck _decodeMessageAck(
    Map<Object?, Object?> payload, {
    String? fallbackMessageSerial,
  }) {
    final channels = payload['channels'];
    final ack = channels is Map<Object?, Object?> && channels.isNotEmpty
        ? channels.values.first
        : payload;
    final data = ack is Map<Object?, Object?>
        ? ack
        : const <Object?, Object?>{};
    final messageSerial =
        data['messageSerial'] as String? ??
        data['message_serial'] as String? ??
        fallbackMessageSerial;
    final historySerial = normalizeWireSerial(
      data['historySerial'] ?? data['history_serial'],
    );
    if (messageSerial == null || historySerial == null) {
      throw const SockudoException(
        'Versioned message acknowledgement missing messageSerial or historySerial',
      );
    }
    return MessageAck(
      messageSerial: messageSerial,
      historySerial: historySerial,
      deliverySerial: normalizeWireSerial(
        data['deliverySerial'] ?? data['delivery_serial'],
      ),
      versionSerial:
          data['versionSerial'] as String? ?? data['version_serial'] as String?,
      status: data['status'] as String?,
    );
  }

  ChannelHistoryPageProxy _decodeChannelHistoryPage(
    Map<Object?, Object?> payload,
    Future<ChannelHistoryPageProxy> Function(String cursor) fetchNext,
  ) {
    final items = (payload['items'] as List<Object?>? ?? const <Object?>[])
        .whereType<Map<Object?, Object?>>()
        .toList(growable: false);

    return ChannelHistoryPageProxy(
      items: items,
      direction: payload['direction'] as String? ?? 'oldest_first',
      limit: (payload['limit'] as num?)?.toInt() ?? 0,
      hasMore: payload['has_more'] as bool? ?? false,
      nextCursor: payload['next_cursor'] as String?,
      bounds:
          payload['bounds'] as Map<Object?, Object?>? ??
          const <Object?, Object?>{},
      continuity:
          payload['continuity'] as Map<Object?, Object?>? ??
          const <Object?, Object?>{},
      fetchNext: fetchNext,
    );
  }

  MessageVersionsPage _decodeMessageVersionsPage(
    Map<Object?, Object?> payload,
    String channelName,
    Future<MessageVersionsPage> Function(String cursor) fetchNext,
  ) {
    final items = (payload['items'] as List<Object?>? ?? const <Object?>[])
        .whereType<Map<Object?, Object?>>()
        .toList(growable: false);

    return MessageVersionsPage(
      channel: payload['channel'] as String? ?? channelName,
      items: items,
      direction: payload['direction'] as String? ?? 'oldest_first',
      limit: (payload['limit'] as num?)?.toInt() ?? 0,
      hasMore: payload['has_more'] as bool? ?? false,
      nextCursor: payload['next_cursor'] as String?,
      fetchNext: fetchNext,
    );
  }

  AnnotationEventsPage _decodeAnnotationEventsPage(
    Map<Object?, Object?> payload,
    String channelName,
    String messageSerial,
    Future<AnnotationEventsPage> Function(String cursor) fetchNext,
  ) {
    final items = (payload['items'] as List<Object?>? ?? const <Object?>[])
        .whereType<Map<Object?, Object?>>()
        .toList(growable: false);

    return AnnotationEventsPage(
      channel: payload['channel'] as String? ?? channelName,
      messageSerial: payload['messageSerial'] as String? ?? messageSerial,
      limit: (payload['limit'] as num?)?.toInt() ?? 0,
      hasMore: payload['hasMore'] as bool? ?? false,
      nextCursor: payload['nextCursor'] as String?,
      items: items,
      fetchNext: fetchNext,
    );
  }

  PresenceHistoryPage _decodePresenceHistoryPage(
    Map<Object?, Object?> payload,
    Future<PresenceHistoryPage> Function(String cursor) fetchNext,
  ) {
    final items = (payload['items'] as List<Object?>? ?? const <Object?>[])
        .whereType<Map<Object?, Object?>>()
        .map(
          (item) => PresenceHistoryItem(
            streamId: item['stream_id'] as String? ?? '',
            serial: (item['serial'] as num?)?.toInt() ?? 0,
            publishedAtMs: (item['published_at_ms'] as num?)?.toInt() ?? 0,
            event: item['event'] as String? ?? '',
            cause: item['cause'] as String? ?? '',
            userId: item['user_id'] as String? ?? '',
            connectionId: item['connection_id'] as String?,
            deadNodeId: item['dead_node_id'] as String?,
            payloadSizeBytes:
                (item['payload_size_bytes'] as num?)?.toInt() ?? 0,
            presenceEvent:
                item['presence_event'] as Map<Object?, Object?>? ??
                const <Object?, Object?>{},
          ),
        )
        .toList(growable: false);

    return PresenceHistoryPage(
      items: items,
      direction: payload['direction'] as String? ?? 'oldest_first',
      limit: (payload['limit'] as num?)?.toInt() ?? 0,
      hasMore: payload['has_more'] as bool? ?? false,
      nextCursor: payload['next_cursor'] as String?,
      bounds: _decodePresenceHistoryBounds(
        payload['bounds'] as Map<Object?, Object?>?,
      ),
      continuity: _decodePresenceHistoryContinuity(
        payload['continuity'] as Map<Object?, Object?>?,
      ),
      fetchNext: fetchNext,
    );
  }

  PresenceSnapshot _decodePresenceSnapshot(Map<Object?, Object?> payload) {
    final members = (payload['members'] as List<Object?>? ?? const <Object?>[])
        .whereType<Map<Object?, Object?>>()
        .map(
          (member) => PresenceSnapshotMember(
            userId: member['user_id'] as String? ?? '',
            lastEvent: member['last_event'] as String? ?? '',
            lastEventSerial:
                (member['last_event_serial'] as num?)?.toInt() ?? 0,
            lastEventAtMs: (member['last_event_at_ms'] as num?)?.toInt() ?? 0,
          ),
        )
        .toList(growable: false);

    return PresenceSnapshot(
      channel: payload['channel'] as String? ?? '',
      members: members,
      memberCount: (payload['member_count'] as num?)?.toInt() ?? 0,
      eventsReplayed: (payload['events_replayed'] as num?)?.toInt() ?? 0,
      snapshotSerial: (payload['snapshot_serial'] as num?)?.toInt(),
      snapshotTimeMs: (payload['snapshot_time_ms'] as num?)?.toInt(),
      continuity: _decodePresenceHistoryContinuity(
        payload['continuity'] as Map<Object?, Object?>?,
      ),
    );
  }

  PresenceHistoryBounds _decodePresenceHistoryBounds(
    Map<Object?, Object?>? payload,
  ) {
    return PresenceHistoryBounds(
      startSerial: (payload?['start_serial'] as num?)?.toInt(),
      endSerial: (payload?['end_serial'] as num?)?.toInt(),
      startTimeMs: (payload?['start_time_ms'] as num?)?.toInt(),
      endTimeMs: (payload?['end_time_ms'] as num?)?.toInt(),
    );
  }

  PresenceHistoryContinuity _decodePresenceHistoryContinuity(
    Map<Object?, Object?>? payload,
  ) {
    return PresenceHistoryContinuity(
      streamId: payload?['stream_id'] as String?,
      oldestAvailableSerial: (payload?['oldest_available_serial'] as num?)
          ?.toInt(),
      newestAvailableSerial: (payload?['newest_available_serial'] as num?)
          ?.toInt(),
      oldestAvailablePublishedAtMs:
          (payload?['oldest_available_published_at_ms'] as num?)?.toInt(),
      newestAvailablePublishedAtMs:
          (payload?['newest_available_published_at_ms'] as num?)?.toInt(),
      retainedEvents: (payload?['retained_events'] as num?)?.toInt() ?? 0,
      retainedBytes: (payload?['retained_bytes'] as num?)?.toInt() ?? 0,
      degraded: payload?['degraded'] as bool? ?? false,
      complete: payload?['complete'] as bool? ?? false,
      truncatedByRetention:
          payload?['truncated_by_retention'] as bool? ?? false,
    );
  }
}

enum _CloseAction { tlsOnly, refused, backoff, retry }
