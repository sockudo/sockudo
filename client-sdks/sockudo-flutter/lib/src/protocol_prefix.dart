/// Protocol version prefix helpers.
///
/// Protocol v1 uses the original Pusher event prefix ("pusher:" / "pusher_internal:").
/// Protocol v2 (Sockudo-native) uses "sockudo:" / "sockudo_internal:".
class ProtocolPrefix {
  ProtocolPrefix(int version) : _version = version {
    if (version == 2) {
      _eventPrefix = 'sockudo:';
      _internalPrefix = 'sockudo_internal:';
    } else {
      _eventPrefix = 'pusher:';
      _internalPrefix = 'pusher_internal:';
    }
  }

  final int _version;
  late final String _eventPrefix;
  late final String _internalPrefix;

  int get version => _version;
  String get eventPrefix => _eventPrefix;
  String get internalPrefix => _internalPrefix;

  String event(String name) => '$_eventPrefix$name';
  String internal(String name) => '$_internalPrefix$name';

  bool isInternalEvent(String name) => name.startsWith(_internalPrefix);
  bool isPlatformEvent(String name) => name.startsWith(_eventPrefix);
}
