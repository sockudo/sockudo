// This is a generated file - do not edit.
//
// Generated from wire.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports

import 'dart:core' as $core;

import 'package:fixnum/fixnum.dart' as $fixnum;
import 'package:protobuf/protobuf.dart' as $pb;

export 'package:protobuf/protobuf.dart' show GeneratedMessageGenericExtensions;

class ProtoPusherMessage extends $pb.GeneratedMessage {
  factory ProtoPusherMessage({
    $core.String? event,
    $core.String? channel,
    ProtoMessageData? data,
    $core.String? name,
    $core.String? userId,
    $core.Iterable<$core.MapEntry<$core.String, $core.String>>? tags,
    $fixnum.Int64? sequence,
    $core.String? conflationKey,
    $core.String? messageId,
    $fixnum.Int64? serial,
    $core.String? idempotencyKey,
    ProtoMessageExtras? extras,
    $fixnum.Int64? deltaSequence,
    $core.String? deltaConflationKey,
    $core.String? streamId,
  }) {
    final result = create();
    if (event != null) result.event = event;
    if (channel != null) result.channel = channel;
    if (data != null) result.data = data;
    if (name != null) result.name = name;
    if (userId != null) result.userId = userId;
    if (tags != null) result.tags.addEntries(tags);
    if (sequence != null) result.sequence = sequence;
    if (conflationKey != null) result.conflationKey = conflationKey;
    if (messageId != null) result.messageId = messageId;
    if (serial != null) result.serial = serial;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    if (extras != null) result.extras = extras;
    if (deltaSequence != null) result.deltaSequence = deltaSequence;
    if (deltaConflationKey != null)
      result.deltaConflationKey = deltaConflationKey;
    if (streamId != null) result.streamId = streamId;
    return result;
  }

  ProtoPusherMessage._();

  factory ProtoPusherMessage.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory ProtoPusherMessage.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ProtoPusherMessage',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'sockudo'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'event')
    ..aOS(2, _omitFieldNames ? '' : 'channel')
    ..aOM<ProtoMessageData>(3, _omitFieldNames ? '' : 'data',
        subBuilder: ProtoMessageData.create)
    ..aOS(4, _omitFieldNames ? '' : 'name')
    ..aOS(5, _omitFieldNames ? '' : 'userId')
    ..m<$core.String, $core.String>(6, _omitFieldNames ? '' : 'tags',
        entryClassName: 'ProtoPusherMessage.TagsEntry',
        keyFieldType: $pb.PbFieldType.OS,
        valueFieldType: $pb.PbFieldType.OS,
        packageName: const $pb.PackageName('sockudo'))
    ..a<$fixnum.Int64>(
        7, _omitFieldNames ? '' : 'sequence', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOS(8, _omitFieldNames ? '' : 'conflationKey')
    ..aOS(9, _omitFieldNames ? '' : 'messageId')
    ..a<$fixnum.Int64>(10, _omitFieldNames ? '' : 'serial', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOS(11, _omitFieldNames ? '' : 'idempotencyKey')
    ..aOM<ProtoMessageExtras>(12, _omitFieldNames ? '' : 'extras',
        subBuilder: ProtoMessageExtras.create)
    ..a<$fixnum.Int64>(
        13, _omitFieldNames ? '' : 'deltaSequence', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOS(14, _omitFieldNames ? '' : 'deltaConflationKey')
    ..aOS(15, _omitFieldNames ? '' : 'streamId')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProtoPusherMessage clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProtoPusherMessage copyWith(void Function(ProtoPusherMessage) updates) =>
      super.copyWith((message) => updates(message as ProtoPusherMessage))
          as ProtoPusherMessage;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static ProtoPusherMessage create() => ProtoPusherMessage._();
  @$core.override
  ProtoPusherMessage createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static ProtoPusherMessage getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ProtoPusherMessage>(create);
  static ProtoPusherMessage? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get event => $_getSZ(0);
  @$pb.TagNumber(1)
  set event($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasEvent() => $_has(0);
  @$pb.TagNumber(1)
  void clearEvent() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get channel => $_getSZ(1);
  @$pb.TagNumber(2)
  set channel($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasChannel() => $_has(1);
  @$pb.TagNumber(2)
  void clearChannel() => $_clearField(2);

  @$pb.TagNumber(3)
  ProtoMessageData get data => $_getN(2);
  @$pb.TagNumber(3)
  set data(ProtoMessageData value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasData() => $_has(2);
  @$pb.TagNumber(3)
  void clearData() => $_clearField(3);
  @$pb.TagNumber(3)
  ProtoMessageData ensureData() => $_ensure(2);

  @$pb.TagNumber(4)
  $core.String get name => $_getSZ(3);
  @$pb.TagNumber(4)
  set name($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasName() => $_has(3);
  @$pb.TagNumber(4)
  void clearName() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.String get userId => $_getSZ(4);
  @$pb.TagNumber(5)
  set userId($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasUserId() => $_has(4);
  @$pb.TagNumber(5)
  void clearUserId() => $_clearField(5);

  @$pb.TagNumber(6)
  $pb.PbMap<$core.String, $core.String> get tags => $_getMap(5);

  @$pb.TagNumber(7)
  $fixnum.Int64 get sequence => $_getI64(6);
  @$pb.TagNumber(7)
  set sequence($fixnum.Int64 value) => $_setInt64(6, value);
  @$pb.TagNumber(7)
  $core.bool hasSequence() => $_has(6);
  @$pb.TagNumber(7)
  void clearSequence() => $_clearField(7);

  @$pb.TagNumber(8)
  $core.String get conflationKey => $_getSZ(7);
  @$pb.TagNumber(8)
  set conflationKey($core.String value) => $_setString(7, value);
  @$pb.TagNumber(8)
  $core.bool hasConflationKey() => $_has(7);
  @$pb.TagNumber(8)
  void clearConflationKey() => $_clearField(8);

  @$pb.TagNumber(9)
  $core.String get messageId => $_getSZ(8);
  @$pb.TagNumber(9)
  set messageId($core.String value) => $_setString(8, value);
  @$pb.TagNumber(9)
  $core.bool hasMessageId() => $_has(8);
  @$pb.TagNumber(9)
  void clearMessageId() => $_clearField(9);

  @$pb.TagNumber(10)
  $fixnum.Int64 get serial => $_getI64(9);
  @$pb.TagNumber(10)
  set serial($fixnum.Int64 value) => $_setInt64(9, value);
  @$pb.TagNumber(10)
  $core.bool hasSerial() => $_has(9);
  @$pb.TagNumber(10)
  void clearSerial() => $_clearField(10);

  @$pb.TagNumber(11)
  $core.String get idempotencyKey => $_getSZ(10);
  @$pb.TagNumber(11)
  set idempotencyKey($core.String value) => $_setString(10, value);
  @$pb.TagNumber(11)
  $core.bool hasIdempotencyKey() => $_has(10);
  @$pb.TagNumber(11)
  void clearIdempotencyKey() => $_clearField(11);

  @$pb.TagNumber(12)
  ProtoMessageExtras get extras => $_getN(11);
  @$pb.TagNumber(12)
  set extras(ProtoMessageExtras value) => $_setField(12, value);
  @$pb.TagNumber(12)
  $core.bool hasExtras() => $_has(11);
  @$pb.TagNumber(12)
  void clearExtras() => $_clearField(12);
  @$pb.TagNumber(12)
  ProtoMessageExtras ensureExtras() => $_ensure(11);

  @$pb.TagNumber(13)
  $fixnum.Int64 get deltaSequence => $_getI64(12);
  @$pb.TagNumber(13)
  set deltaSequence($fixnum.Int64 value) => $_setInt64(12, value);
  @$pb.TagNumber(13)
  $core.bool hasDeltaSequence() => $_has(12);
  @$pb.TagNumber(13)
  void clearDeltaSequence() => $_clearField(13);

  @$pb.TagNumber(14)
  $core.String get deltaConflationKey => $_getSZ(13);
  @$pb.TagNumber(14)
  set deltaConflationKey($core.String value) => $_setString(13, value);
  @$pb.TagNumber(14)
  $core.bool hasDeltaConflationKey() => $_has(13);
  @$pb.TagNumber(14)
  void clearDeltaConflationKey() => $_clearField(14);

  @$pb.TagNumber(15)
  $core.String get streamId => $_getSZ(14);
  @$pb.TagNumber(15)
  set streamId($core.String value) => $_setString(14, value);
  @$pb.TagNumber(15)
  $core.bool hasStreamId() => $_has(14);
  @$pb.TagNumber(15)
  void clearStreamId() => $_clearField(15);
}

enum ProtoMessageData_Kind {
  stringValue,
  structured,
  jsonValue,
  binaryValue,
  notSet
}

class ProtoMessageData extends $pb.GeneratedMessage {
  factory ProtoMessageData({
    $core.String? stringValue,
    ProtoStructuredData? structured,
    $core.String? jsonValue,
    $core.List<$core.int>? binaryValue,
  }) {
    final result = create();
    if (stringValue != null) result.stringValue = stringValue;
    if (structured != null) result.structured = structured;
    if (jsonValue != null) result.jsonValue = jsonValue;
    if (binaryValue != null) result.binaryValue = binaryValue;
    return result;
  }

  ProtoMessageData._();

  factory ProtoMessageData.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory ProtoMessageData.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, ProtoMessageData_Kind>
      _ProtoMessageData_KindByTag = {
    1: ProtoMessageData_Kind.stringValue,
    2: ProtoMessageData_Kind.structured,
    3: ProtoMessageData_Kind.jsonValue,
    4: ProtoMessageData_Kind.binaryValue,
    0: ProtoMessageData_Kind.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ProtoMessageData',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'sockudo'),
      createEmptyInstance: create)
    ..oo(0, [1, 2, 3, 4])
    ..aOS(1, _omitFieldNames ? '' : 'stringValue')
    ..aOM<ProtoStructuredData>(2, _omitFieldNames ? '' : 'structured',
        subBuilder: ProtoStructuredData.create)
    ..aOS(3, _omitFieldNames ? '' : 'jsonValue')
    ..a<$core.List<$core.int>>(
        4, _omitFieldNames ? '' : 'binaryValue', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProtoMessageData clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProtoMessageData copyWith(void Function(ProtoMessageData) updates) =>
      super.copyWith((message) => updates(message as ProtoMessageData))
          as ProtoMessageData;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static ProtoMessageData create() => ProtoMessageData._();
  @$core.override
  ProtoMessageData createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static ProtoMessageData getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ProtoMessageData>(create);
  static ProtoMessageData? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  ProtoMessageData_Kind whichKind() =>
      _ProtoMessageData_KindByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  void clearKind() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  $core.String get stringValue => $_getSZ(0);
  @$pb.TagNumber(1)
  set stringValue($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasStringValue() => $_has(0);
  @$pb.TagNumber(1)
  void clearStringValue() => $_clearField(1);

  @$pb.TagNumber(2)
  ProtoStructuredData get structured => $_getN(1);
  @$pb.TagNumber(2)
  set structured(ProtoStructuredData value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasStructured() => $_has(1);
  @$pb.TagNumber(2)
  void clearStructured() => $_clearField(2);
  @$pb.TagNumber(2)
  ProtoStructuredData ensureStructured() => $_ensure(1);

  @$pb.TagNumber(3)
  $core.String get jsonValue => $_getSZ(2);
  @$pb.TagNumber(3)
  set jsonValue($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasJsonValue() => $_has(2);
  @$pb.TagNumber(3)
  void clearJsonValue() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.List<$core.int> get binaryValue => $_getN(3);
  @$pb.TagNumber(4)
  set binaryValue($core.List<$core.int> value) => $_setBytes(3, value);
  @$pb.TagNumber(4)
  $core.bool hasBinaryValue() => $_has(3);
  @$pb.TagNumber(4)
  void clearBinaryValue() => $_clearField(4);
}

class ProtoStructuredData extends $pb.GeneratedMessage {
  factory ProtoStructuredData({
    $core.String? channelData,
    $core.String? channel,
    $core.String? userData,
    $core.Iterable<$core.MapEntry<$core.String, $core.String>>? extra,
  }) {
    final result = create();
    if (channelData != null) result.channelData = channelData;
    if (channel != null) result.channel = channel;
    if (userData != null) result.userData = userData;
    if (extra != null) result.extra.addEntries(extra);
    return result;
  }

  ProtoStructuredData._();

  factory ProtoStructuredData.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory ProtoStructuredData.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ProtoStructuredData',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'sockudo'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'channelData')
    ..aOS(2, _omitFieldNames ? '' : 'channel')
    ..aOS(3, _omitFieldNames ? '' : 'userData')
    ..m<$core.String, $core.String>(4, _omitFieldNames ? '' : 'extra',
        entryClassName: 'ProtoStructuredData.ExtraEntry',
        keyFieldType: $pb.PbFieldType.OS,
        valueFieldType: $pb.PbFieldType.OS,
        packageName: const $pb.PackageName('sockudo'))
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProtoStructuredData clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProtoStructuredData copyWith(void Function(ProtoStructuredData) updates) =>
      super.copyWith((message) => updates(message as ProtoStructuredData))
          as ProtoStructuredData;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static ProtoStructuredData create() => ProtoStructuredData._();
  @$core.override
  ProtoStructuredData createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static ProtoStructuredData getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ProtoStructuredData>(create);
  static ProtoStructuredData? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get channelData => $_getSZ(0);
  @$pb.TagNumber(1)
  set channelData($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasChannelData() => $_has(0);
  @$pb.TagNumber(1)
  void clearChannelData() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get channel => $_getSZ(1);
  @$pb.TagNumber(2)
  set channel($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasChannel() => $_has(1);
  @$pb.TagNumber(2)
  void clearChannel() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get userData => $_getSZ(2);
  @$pb.TagNumber(3)
  set userData($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasUserData() => $_has(2);
  @$pb.TagNumber(3)
  void clearUserData() => $_clearField(3);

  @$pb.TagNumber(4)
  $pb.PbMap<$core.String, $core.String> get extra => $_getMap(3);
}

class ProtoMessageExtras extends $pb.GeneratedMessage {
  factory ProtoMessageExtras({
    $core.Iterable<$core.MapEntry<$core.String, ProtoExtrasValue>>? headers,
    $core.bool? ephemeral,
    $core.String? idempotencyKey,
    $core.bool? echo,
    ProtoAiExtras? ai,
  }) {
    final result = create();
    if (headers != null) result.headers.addEntries(headers);
    if (ephemeral != null) result.ephemeral = ephemeral;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    if (echo != null) result.echo = echo;
    if (ai != null) result.ai = ai;
    return result;
  }

  ProtoMessageExtras._();

  factory ProtoMessageExtras.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory ProtoMessageExtras.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ProtoMessageExtras',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'sockudo'),
      createEmptyInstance: create)
    ..m<$core.String, ProtoExtrasValue>(1, _omitFieldNames ? '' : 'headers',
        entryClassName: 'ProtoMessageExtras.HeadersEntry',
        keyFieldType: $pb.PbFieldType.OS,
        valueFieldType: $pb.PbFieldType.OM,
        valueCreator: ProtoExtrasValue.create,
        valueDefaultOrMaker: ProtoExtrasValue.getDefault,
        packageName: const $pb.PackageName('sockudo'))
    ..aOB(2, _omitFieldNames ? '' : 'ephemeral')
    ..aOS(3, _omitFieldNames ? '' : 'idempotencyKey')
    ..aOB(4, _omitFieldNames ? '' : 'echo')
    ..aOM<ProtoAiExtras>(5, _omitFieldNames ? '' : 'ai',
        subBuilder: ProtoAiExtras.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProtoMessageExtras clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProtoMessageExtras copyWith(void Function(ProtoMessageExtras) updates) =>
      super.copyWith((message) => updates(message as ProtoMessageExtras))
          as ProtoMessageExtras;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static ProtoMessageExtras create() => ProtoMessageExtras._();
  @$core.override
  ProtoMessageExtras createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static ProtoMessageExtras getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ProtoMessageExtras>(create);
  static ProtoMessageExtras? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbMap<$core.String, ProtoExtrasValue> get headers => $_getMap(0);

  @$pb.TagNumber(2)
  $core.bool get ephemeral => $_getBF(1);
  @$pb.TagNumber(2)
  set ephemeral($core.bool value) => $_setBool(1, value);
  @$pb.TagNumber(2)
  $core.bool hasEphemeral() => $_has(1);
  @$pb.TagNumber(2)
  void clearEphemeral() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get idempotencyKey => $_getSZ(2);
  @$pb.TagNumber(3)
  set idempotencyKey($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasIdempotencyKey() => $_has(2);
  @$pb.TagNumber(3)
  void clearIdempotencyKey() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.bool get echo => $_getBF(3);
  @$pb.TagNumber(4)
  set echo($core.bool value) => $_setBool(3, value);
  @$pb.TagNumber(4)
  $core.bool hasEcho() => $_has(3);
  @$pb.TagNumber(4)
  void clearEcho() => $_clearField(4);

  @$pb.TagNumber(5)
  ProtoAiExtras get ai => $_getN(4);
  @$pb.TagNumber(5)
  set ai(ProtoAiExtras value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasAi() => $_has(4);
  @$pb.TagNumber(5)
  void clearAi() => $_clearField(5);
  @$pb.TagNumber(5)
  ProtoAiExtras ensureAi() => $_ensure(4);
}

class ProtoAiExtras extends $pb.GeneratedMessage {
  factory ProtoAiExtras({
    $core.Iterable<$core.MapEntry<$core.String, $core.String>>? transport,
    $core.Iterable<$core.MapEntry<$core.String, $core.String>>? codec,
  }) {
    final result = create();
    if (transport != null) result.transport.addEntries(transport);
    if (codec != null) result.codec.addEntries(codec);
    return result;
  }

  ProtoAiExtras._();

  factory ProtoAiExtras.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory ProtoAiExtras.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ProtoAiExtras',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'sockudo'),
      createEmptyInstance: create)
    ..m<$core.String, $core.String>(1, _omitFieldNames ? '' : 'transport',
        entryClassName: 'ProtoAiExtras.TransportEntry',
        keyFieldType: $pb.PbFieldType.OS,
        valueFieldType: $pb.PbFieldType.OS,
        packageName: const $pb.PackageName('sockudo'))
    ..m<$core.String, $core.String>(2, _omitFieldNames ? '' : 'codec',
        entryClassName: 'ProtoAiExtras.CodecEntry',
        keyFieldType: $pb.PbFieldType.OS,
        valueFieldType: $pb.PbFieldType.OS,
        packageName: const $pb.PackageName('sockudo'))
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProtoAiExtras clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProtoAiExtras copyWith(void Function(ProtoAiExtras) updates) =>
      super.copyWith((message) => updates(message as ProtoAiExtras))
          as ProtoAiExtras;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static ProtoAiExtras create() => ProtoAiExtras._();
  @$core.override
  ProtoAiExtras createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static ProtoAiExtras getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ProtoAiExtras>(create);
  static ProtoAiExtras? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbMap<$core.String, $core.String> get transport => $_getMap(0);

  @$pb.TagNumber(2)
  $pb.PbMap<$core.String, $core.String> get codec => $_getMap(1);
}

enum ProtoExtrasValue_Kind { stringValue, numberValue, boolValue, notSet }

class ProtoExtrasValue extends $pb.GeneratedMessage {
  factory ProtoExtrasValue({
    $core.String? stringValue,
    $core.double? numberValue,
    $core.bool? boolValue,
  }) {
    final result = create();
    if (stringValue != null) result.stringValue = stringValue;
    if (numberValue != null) result.numberValue = numberValue;
    if (boolValue != null) result.boolValue = boolValue;
    return result;
  }

  ProtoExtrasValue._();

  factory ProtoExtrasValue.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory ProtoExtrasValue.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, ProtoExtrasValue_Kind>
      _ProtoExtrasValue_KindByTag = {
    1: ProtoExtrasValue_Kind.stringValue,
    2: ProtoExtrasValue_Kind.numberValue,
    3: ProtoExtrasValue_Kind.boolValue,
    0: ProtoExtrasValue_Kind.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ProtoExtrasValue',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'sockudo'),
      createEmptyInstance: create)
    ..oo(0, [1, 2, 3])
    ..aOS(1, _omitFieldNames ? '' : 'stringValue')
    ..aD(2, _omitFieldNames ? '' : 'numberValue')
    ..aOB(3, _omitFieldNames ? '' : 'boolValue')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProtoExtrasValue clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProtoExtrasValue copyWith(void Function(ProtoExtrasValue) updates) =>
      super.copyWith((message) => updates(message as ProtoExtrasValue))
          as ProtoExtrasValue;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static ProtoExtrasValue create() => ProtoExtrasValue._();
  @$core.override
  ProtoExtrasValue createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static ProtoExtrasValue getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ProtoExtrasValue>(create);
  static ProtoExtrasValue? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  ProtoExtrasValue_Kind whichKind() =>
      _ProtoExtrasValue_KindByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  void clearKind() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  $core.String get stringValue => $_getSZ(0);
  @$pb.TagNumber(1)
  set stringValue($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasStringValue() => $_has(0);
  @$pb.TagNumber(1)
  void clearStringValue() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.double get numberValue => $_getN(1);
  @$pb.TagNumber(2)
  set numberValue($core.double value) => $_setDouble(1, value);
  @$pb.TagNumber(2)
  $core.bool hasNumberValue() => $_has(1);
  @$pb.TagNumber(2)
  void clearNumberValue() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.bool get boolValue => $_getBF(2);
  @$pb.TagNumber(3)
  set boolValue($core.bool value) => $_setBool(2, value);
  @$pb.TagNumber(3)
  $core.bool hasBoolValue() => $_has(2);
  @$pb.TagNumber(3)
  void clearBoolValue() => $_clearField(3);
}

const $core.bool _omitFieldNames =
    $core.bool.fromEnvironment('protobuf.omit_field_names');
const $core.bool _omitMessageNames =
    $core.bool.fromEnvironment('protobuf.omit_message_names');
