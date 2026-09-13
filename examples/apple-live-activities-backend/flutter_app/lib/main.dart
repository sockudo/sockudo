// Sockudo Rides (Flutter): Live Activity token upload and proxy publishing with
// `sockudo_flutter`. ActivityKit itself is reached through a small native bridge
// (ios/Runner/LiveActivityBridge.swift), as the SDK README recommends.
import 'dart:async';
import 'dart:convert';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:http/http.dart' as http;
import 'package:sockudo_flutter/sockudo_flutter.dart';

const backendUrl = String.fromEnvironment(
  'SOCKUDO_BACKEND_URL',
  defaultValue: 'http://127.0.0.1:8787',
);
const userId = 'flutter-user';

void main() => runApp(const RidesApp());

class TrackedActivity {
  TrackedActivity({required this.id, required this.rideId});
  final String id;
  final String rideId;
  String? token;
  String status = 'requested';
  int etaMinutes = 0;
  String lifecycle = 'active';
}

class RideStore extends ChangeNotifier {
  RideStore() {
    _events.receiveBroadcastStream().listen(_onEvent);
    _mirrorTimer = Timer.periodic(const Duration(seconds: 2), (_) => _mirror());
    log('backend $backendUrl');
  }

  static const _methods = MethodChannel('sockudo.rides/activitykit');
  static const _events = EventChannel('sockudo.rides/activitykit/events');

  // The SDK's push helper points at the backend's authenticated proxy surface.
  final SockudoPushRegistration push = SockudoPushRegistration(
    const PushRegistrationOptions(endpoint: '$backendUrl/push'),
  );

  String? pushToStartToken;
  final List<TrackedActivity> activities = <TrackedActivity>[];
  final List<String> lines = <String>[];
  final Set<String> _mirrored = <String>{};
  late final Timer _mirrorTimer;
  int _localCounter = 0;

  void log(String line) {
    final stamp = TimeOfDay.now();
    lines.insert(
      0,
      '${stamp.hour.toString().padLeft(2, '0')}:${stamp.minute.toString().padLeft(2, '0')} $line',
    );
    if (lines.length > 80) lines.removeLast();
    notifyListeners();
  }

  Future<void> _onEvent(Object? raw) async {
    final event = (raw as Map).cast<String, Object?>();
    switch (event['type']) {
      case 'pushToStartToken':
        pushToStartToken = event['token'] as String;
        log('push-to-start token ${pushToStartToken!.substring(0, 12)}…');
        // sockudo_flutter's typed token update -> backend upload.
        final update = ApnsLiveActivityTokenUpdate.pushToStart(
          pushToStartToken!,
        );
        await _uploadToken(update.toJson());
      case 'activityToken':
        final activity = _track(event);
        activity.token = event['token'] as String;
        log(
          '${activity.rideId} update token ${activity.token!.substring(0, 12)}…',
        );
        final update = ApnsLiveActivityTokenUpdate.activity(
          activityId: activity.id,
          token: activity.token!,
        );
        await _uploadToken(<String, Object?>{
          ...update.toJson(),
          'rideId': activity.rideId,
        });
      case 'error':
        log('ActivityKit error: ${event['message']}');
      case 'state':
        final activity = _track(event);
        activity.status = event['status'] as String;
        activity.etaMinutes = event['etaMinutes'] as int;
        activity.lifecycle = event['lifecycle'] as String;
        log(
          '${activity.rideId} → ${activity.status} eta ${activity.etaMinutes} (${activity.lifecycle})',
        );
    }
    notifyListeners();
  }

  TrackedActivity _track(Map<String, Object?> event) {
    final id = event['activityId'] as String;
    return activities.firstWhere(
      (a) => a.id == id,
      orElse: () {
        final created = TrackedActivity(
          id: id,
          rideId: event['rideId'] as String,
        );
        activities.add(created);
        log('tracking activity ${id.substring(0, 8)} ride ${created.rideId}');
        return created;
      },
    );
  }

  Future<void> _uploadToken(Map<String, Object?> body) async {
    final response = await http.post(
      Uri.parse('$backendUrl/liveActivities/tokens'),
      headers: const <String, String>{'Content-Type': 'application/json'},
      body: jsonEncode(<String, Object?>{...body, 'userId': userId}),
    );
    log('POST /liveActivities/tokens → ${response.statusCode}');
  }

  Future<void> startLocally() async {
    _localCounter += 1;
    final rideId = 'flutter-$_localCounter';
    await _methods.invokeMethod<void>('start', <String, Object?>{
      'rideId': rideId,
      'status': 'requested',
      'etaMinutes': 9,
    });
    log('requested local activity for $rideId');
  }

  /// Publishes an update for the newest activity through the proxy using the
  /// sockudo_flutter request builder. The proxy signs the Sockudo request.
  Future<void> advance() async {
    final activity = activities.lastWhere(
      (a) => a.token != null && a.lifecycle == 'active',
      orElse: () => throw StateError('no active activity with a token'),
    );
    final now = DateTime.now().millisecondsSinceEpoch ~/ 1000;
    final next = activity.status == 'requested'
        ? (status: 'driverAssigned', eta: 6)
        : activity.status == 'driverAssigned'
        ? (status: 'arriving', eta: 2)
        : (status: 'completed', eta: 0);
    final isEnd = next.status == 'completed';
    try {
      final accepted = await push.publishLiveActivity(
        ApnsLiveActivityPublishRequest(
          publishId: '${activity.rideId}-${next.status}-$now',
          recipient: ApnsLiveActivityTokenRecipient(activity.token!),
          liveActivity: ApnsLiveActivityPayload(
            event: isEnd
                ? ApnsLiveActivityEvent.end
                : ApnsLiveActivityEvent.update,
            timestamp: now,
            contentState: <String, Object?>{
              'status': next.status,
              'etaMinutes': next.eta,
            },
            staleDate: isEnd ? null : now + 300,
            dismissalDate: isEnd ? now + 600 : null,
            relevanceScore: 0.9,
            priority: isEnd
                ? ApnsLiveActivityPriority.immediate
                : ApnsLiveActivityPriority.conservePower,
          ),
        ),
      );
      log('published ${accepted['publishId']} (${accepted['status']})');
    } on ArgumentError catch (error) {
      log('rejected client-side: ${error.message}');
    } on Exception catch (error) {
      log('publish failed: $error');
    }
  }

  Future<void> endAll() => _methods.invokeMethod<void>('endAll');

  /// The Simulator cannot receive `liveactivity` pushes from a mock APNs, so the
  /// app applies the payload Sockudo last accepted for each ride onto the activity.
  Future<void> _mirror() async {
    for (final activity in activities.where((a) => a.lifecycle == 'active')) {
      try {
        final response = await http.get(
          Uri.parse('$backendUrl/rides/${activity.rideId}/latest'),
        );
        final latest = (jsonDecode(response.body) as Map)
            .cast<String, Object?>();
        final publishId = latest['publishId'] as String?;
        if (publishId == null || _mirrored.contains(publishId)) continue;
        final status = await push.getPublishStatus(publishId);
        final counters = (status['counters'] as Map).cast<String, Object?>();
        if ((counters['succeeded'] as int? ?? 0) < 1) {
          continue; // wait for APNs acceptance
        }
        _mirrored.add(publishId);
        final state = (latest['contentState'] as Map).cast<String, Object?>();
        await _methods.invokeMethod<void>('apply', <String, Object?>{
          'activityId': activity.id,
          'status': state['status'],
          'etaMinutes': state['etaMinutes'],
          'end': latest['event'] == 'end',
        });
        log('mirrored $publishId after APNs accepted it');
      } catch (_) {
        // backend not reachable yet
      }
    }
  }

  @override
  void dispose() {
    _mirrorTimer.cancel();
    super.dispose();
  }
}

class RidesApp extends StatefulWidget {
  const RidesApp({super.key});
  @override
  State<RidesApp> createState() => _RidesAppState();
}

class _RidesAppState extends State<RidesApp> {
  final RideStore store = RideStore();

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Sockudo Rides',
      theme: ThemeData(colorSchemeSeed: Colors.indigo, useMaterial3: true),
      home: AnimatedBuilder(
        animation: store,
        builder: (context, _) => Scaffold(
          appBar: AppBar(title: const Text('Sockudo Rides · Flutter')),
          body: ListView(
            padding: const EdgeInsets.all(12),
            children: <Widget>[
              _section('Push-to-start token', <Widget>[
                Text(
                  store.pushToStartToken == null
                      ? 'waiting for ActivityKit…'
                      : '${store.pushToStartToken!.substring(0, 24)}…',
                  style: const TextStyle(fontFamily: 'Menlo', fontSize: 12),
                ),
              ]),
              _section('Activities', <Widget>[
                if (store.activities.isEmpty) const Text('none'),
                for (final a in store.activities)
                  ListTile(
                    dense: true,
                    title: Text(a.rideId),
                    subtitle: Text(
                      '${a.status} · ETA ${a.etaMinutes} min · ${a.lifecycle}\n${a.token == null ? 'no update token yet' : '${a.token!.substring(0, 24)}…'}',
                      style: const TextStyle(fontFamily: 'Menlo', fontSize: 11),
                    ),
                  ),
              ]),
              _section('Log', <Widget>[
                for (final line in store.lines)
                  Text(
                    line,
                    style: const TextStyle(fontFamily: 'Menlo', fontSize: 11),
                  ),
              ]),
            ],
          ),
          bottomNavigationBar: SafeArea(
            child: Padding(
              padding: const EdgeInsets.all(8),
              child: Wrap(
                alignment: WrapAlignment.spaceEvenly,
                spacing: 8,
                children: <Widget>[
                  FilledButton(
                    onPressed: store.startLocally,
                    child: const Text('Start locally'),
                  ),
                  FilledButton.tonal(
                    onPressed: store.advance,
                    child: const Text('Advance via Sockudo'),
                  ),
                  TextButton(
                    onPressed: store.endAll,
                    child: const Text('End all'),
                  ),
                ],
              ),
            ),
          ),
        ),
      ),
    );
  }

  Widget _section(String title, List<Widget> children) => Card(
    child: Padding(
      padding: const EdgeInsets.all(12),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: <Widget>[
          Text(title, style: Theme.of(context).textTheme.titleMedium),
          const SizedBox(height: 6),
          ...children,
        ],
      ),
    ),
  );
}
