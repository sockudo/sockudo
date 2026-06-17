import 'models.dart';

typedef ChannelAuthorizationHandler =
    Future<ChannelAuthorizationData> Function(
      ChannelAuthorizationRequest request,
    );
typedef UserAuthenticationHandler =
    Future<UserAuthenticationData> Function(UserAuthenticationRequest request);

class ChannelAuthorizationRequest {
  const ChannelAuthorizationRequest({
    required this.socketId,
    required this.channelName,
  });

  final String socketId;
  final String channelName;
}

class UserAuthenticationRequest {
  const UserAuthenticationRequest({required this.socketId});

  final String socketId;
}

class ChannelAuthorizationData {
  const ChannelAuthorizationData({
    required this.auth,
    this.channelData,
    this.sharedSecret,
  });

  final String auth;
  final String? channelData;
  final String? sharedSecret;
}

class UserAuthenticationData {
  const UserAuthenticationData({required this.auth, required this.userData});

  final String auth;
  final String userData;
}

class ChannelAuthorizationOptions {
  const ChannelAuthorizationOptions({
    this.endpoint = '/sockudo/auth',
    this.headers = const <String, String>{},
    this.params = const <String, AuthValue>{},
    this.headersProvider,
    this.paramsProvider,
    this.customHandler,
  });

  final String endpoint;
  final Map<String, String> headers;
  final Map<String, AuthValue> params;
  final Map<String, String> Function()? headersProvider;
  final Map<String, AuthValue> Function()? paramsProvider;
  final ChannelAuthorizationHandler? customHandler;
}

class UserAuthenticationOptions {
  const UserAuthenticationOptions({
    this.endpoint = '/sockudo/user-auth',
    this.headers = const <String, String>{},
    this.params = const <String, AuthValue>{},
    this.headersProvider,
    this.paramsProvider,
    this.customHandler,
  });

  final String endpoint;
  final Map<String, String> headers;
  final Map<String, AuthValue> params;
  final Map<String, String> Function()? headersProvider;
  final Map<String, AuthValue> Function()? paramsProvider;
  final UserAuthenticationHandler? customHandler;
}

class PresenceHistoryOptions {
  const PresenceHistoryOptions({
    required this.endpoint,
    this.headers = const <String, String>{},
    this.headersProvider,
  });

  final String endpoint;
  final Map<String, String> headers;
  final Map<String, String> Function()? headersProvider;
}

class VersionedMessagesOptions {
  const VersionedMessagesOptions({
    required this.endpoint,
    this.headers = const <String, String>{},
    this.headersProvider,
  });

  final String endpoint;
  final Map<String, String> headers;
  final Map<String, String> Function()? headersProvider;
}
