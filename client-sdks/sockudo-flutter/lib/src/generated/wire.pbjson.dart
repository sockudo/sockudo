// This is a generated file - do not edit.
//
// Generated from wire.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports
// ignore_for_file: unused_import

import 'dart:convert' as $convert;
import 'dart:core' as $core;
import 'dart:typed_data' as $typed_data;

@$core.Deprecated('Use protoPusherMessageDescriptor instead')
const ProtoPusherMessage$json = {
  '1': 'ProtoPusherMessage',
  '2': [
    {'1': 'event', '3': 1, '4': 1, '5': 9, '10': 'event'},
    {'1': 'channel', '3': 2, '4': 1, '5': 9, '10': 'channel'},
    {
      '1': 'data',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.sockudo.ProtoMessageData',
      '10': 'data'
    },
    {'1': 'name', '3': 4, '4': 1, '5': 9, '10': 'name'},
    {'1': 'user_id', '3': 5, '4': 1, '5': 9, '10': 'userId'},
    {
      '1': 'tags',
      '3': 6,
      '4': 3,
      '5': 11,
      '6': '.sockudo.ProtoPusherMessage.TagsEntry',
      '10': 'tags'
    },
    {'1': 'sequence', '3': 7, '4': 1, '5': 4, '10': 'sequence'},
    {'1': 'conflation_key', '3': 8, '4': 1, '5': 9, '10': 'conflationKey'},
    {'1': 'message_id', '3': 9, '4': 1, '5': 9, '10': 'messageId'},
    {'1': 'serial', '3': 10, '4': 1, '5': 4, '10': 'serial'},
    {'1': 'idempotency_key', '3': 11, '4': 1, '5': 9, '10': 'idempotencyKey'},
    {
      '1': 'extras',
      '3': 12,
      '4': 1,
      '5': 11,
      '6': '.sockudo.ProtoMessageExtras',
      '10': 'extras'
    },
    {'1': 'delta_sequence', '3': 13, '4': 1, '5': 4, '10': 'deltaSequence'},
    {
      '1': 'delta_conflation_key',
      '3': 14,
      '4': 1,
      '5': 9,
      '10': 'deltaConflationKey'
    },
    {'1': 'stream_id', '3': 15, '4': 1, '5': 9, '10': 'streamId'},
  ],
  '3': [ProtoPusherMessage_TagsEntry$json],
};

@$core.Deprecated('Use protoPusherMessageDescriptor instead')
const ProtoPusherMessage_TagsEntry$json = {
  '1': 'TagsEntry',
  '2': [
    {'1': 'key', '3': 1, '4': 1, '5': 9, '10': 'key'},
    {'1': 'value', '3': 2, '4': 1, '5': 9, '10': 'value'},
  ],
  '7': {'7': true},
};

/// Descriptor for `ProtoPusherMessage`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List protoPusherMessageDescriptor = $convert.base64Decode(
    'ChJQcm90b1B1c2hlck1lc3NhZ2USFAoFZXZlbnQYASABKAlSBWV2ZW50EhgKB2NoYW5uZWwYAi'
    'ABKAlSB2NoYW5uZWwSLQoEZGF0YRgDIAEoCzIZLnNvY2t1ZG8uUHJvdG9NZXNzYWdlRGF0YVIE'
    'ZGF0YRISCgRuYW1lGAQgASgJUgRuYW1lEhcKB3VzZXJfaWQYBSABKAlSBnVzZXJJZBI5CgR0YW'
    'dzGAYgAygLMiUuc29ja3Vkby5Qcm90b1B1c2hlck1lc3NhZ2UuVGFnc0VudHJ5UgR0YWdzEhoK'
    'CHNlcXVlbmNlGAcgASgEUghzZXF1ZW5jZRIlCg5jb25mbGF0aW9uX2tleRgIIAEoCVINY29uZm'
    'xhdGlvbktleRIdCgptZXNzYWdlX2lkGAkgASgJUgltZXNzYWdlSWQSFgoGc2VyaWFsGAogASgE'
    'UgZzZXJpYWwSJwoPaWRlbXBvdGVuY3lfa2V5GAsgASgJUg5pZGVtcG90ZW5jeUtleRIzCgZleH'
    'RyYXMYDCABKAsyGy5zb2NrdWRvLlByb3RvTWVzc2FnZUV4dHJhc1IGZXh0cmFzEiUKDmRlbHRh'
    'X3NlcXVlbmNlGA0gASgEUg1kZWx0YVNlcXVlbmNlEjAKFGRlbHRhX2NvbmZsYXRpb25fa2V5GA'
    '4gASgJUhJkZWx0YUNvbmZsYXRpb25LZXkSGwoJc3RyZWFtX2lkGA8gASgJUghzdHJlYW1JZBo3'
    'CglUYWdzRW50cnkSEAoDa2V5GAEgASgJUgNrZXkSFAoFdmFsdWUYAiABKAlSBXZhbHVlOgI4AQ'
    '==');

@$core.Deprecated('Use protoMessageDataDescriptor instead')
const ProtoMessageData$json = {
  '1': 'ProtoMessageData',
  '2': [
    {'1': 'string_value', '3': 1, '4': 1, '5': 9, '9': 0, '10': 'stringValue'},
    {
      '1': 'structured',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.sockudo.ProtoStructuredData',
      '9': 0,
      '10': 'structured'
    },
    {'1': 'json_value', '3': 3, '4': 1, '5': 9, '9': 0, '10': 'jsonValue'},
    {'1': 'binary_value', '3': 4, '4': 1, '5': 12, '9': 0, '10': 'binaryValue'},
  ],
  '8': [
    {'1': 'kind'},
  ],
};

/// Descriptor for `ProtoMessageData`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List protoMessageDataDescriptor = $convert.base64Decode(
    'ChBQcm90b01lc3NhZ2VEYXRhEiMKDHN0cmluZ192YWx1ZRgBIAEoCUgAUgtzdHJpbmdWYWx1ZR'
    'I+CgpzdHJ1Y3R1cmVkGAIgASgLMhwuc29ja3Vkby5Qcm90b1N0cnVjdHVyZWREYXRhSABSCnN0'
    'cnVjdHVyZWQSHwoKanNvbl92YWx1ZRgDIAEoCUgAUglqc29uVmFsdWUSIwoMYmluYXJ5X3ZhbH'
    'VlGAQgASgMSABSC2JpbmFyeVZhbHVlQgYKBGtpbmQ=');

@$core.Deprecated('Use protoStructuredDataDescriptor instead')
const ProtoStructuredData$json = {
  '1': 'ProtoStructuredData',
  '2': [
    {'1': 'channel_data', '3': 1, '4': 1, '5': 9, '10': 'channelData'},
    {'1': 'channel', '3': 2, '4': 1, '5': 9, '10': 'channel'},
    {'1': 'user_data', '3': 3, '4': 1, '5': 9, '10': 'userData'},
    {
      '1': 'extra',
      '3': 4,
      '4': 3,
      '5': 11,
      '6': '.sockudo.ProtoStructuredData.ExtraEntry',
      '10': 'extra'
    },
  ],
  '3': [ProtoStructuredData_ExtraEntry$json],
};

@$core.Deprecated('Use protoStructuredDataDescriptor instead')
const ProtoStructuredData_ExtraEntry$json = {
  '1': 'ExtraEntry',
  '2': [
    {'1': 'key', '3': 1, '4': 1, '5': 9, '10': 'key'},
    {'1': 'value', '3': 2, '4': 1, '5': 9, '10': 'value'},
  ],
  '7': {'7': true},
};

/// Descriptor for `ProtoStructuredData`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List protoStructuredDataDescriptor = $convert.base64Decode(
    'ChNQcm90b1N0cnVjdHVyZWREYXRhEiEKDGNoYW5uZWxfZGF0YRgBIAEoCVILY2hhbm5lbERhdG'
    'ESGAoHY2hhbm5lbBgCIAEoCVIHY2hhbm5lbBIbCgl1c2VyX2RhdGEYAyABKAlSCHVzZXJEYXRh'
    'Ej0KBWV4dHJhGAQgAygLMicuc29ja3Vkby5Qcm90b1N0cnVjdHVyZWREYXRhLkV4dHJhRW50cn'
    'lSBWV4dHJhGjgKCkV4dHJhRW50cnkSEAoDa2V5GAEgASgJUgNrZXkSFAoFdmFsdWUYAiABKAlS'
    'BXZhbHVlOgI4AQ==');

@$core.Deprecated('Use protoMessageExtrasDescriptor instead')
const ProtoMessageExtras$json = {
  '1': 'ProtoMessageExtras',
  '2': [
    {
      '1': 'headers',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.sockudo.ProtoMessageExtras.HeadersEntry',
      '10': 'headers'
    },
    {
      '1': 'ephemeral',
      '3': 2,
      '4': 1,
      '5': 8,
      '9': 0,
      '10': 'ephemeral',
      '17': true
    },
    {
      '1': 'idempotency_key',
      '3': 3,
      '4': 1,
      '5': 9,
      '9': 1,
      '10': 'idempotencyKey',
      '17': true
    },
    {'1': 'echo', '3': 4, '4': 1, '5': 8, '9': 2, '10': 'echo', '17': true},
    {
      '1': 'ai',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.sockudo.ProtoAiExtras',
      '10': 'ai'
    },
  ],
  '3': [ProtoMessageExtras_HeadersEntry$json],
  '8': [
    {'1': '_ephemeral'},
    {'1': '_idempotency_key'},
    {'1': '_echo'},
  ],
};

@$core.Deprecated('Use protoMessageExtrasDescriptor instead')
const ProtoMessageExtras_HeadersEntry$json = {
  '1': 'HeadersEntry',
  '2': [
    {'1': 'key', '3': 1, '4': 1, '5': 9, '10': 'key'},
    {
      '1': 'value',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.sockudo.ProtoExtrasValue',
      '10': 'value'
    },
  ],
  '7': {'7': true},
};

/// Descriptor for `ProtoMessageExtras`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List protoMessageExtrasDescriptor = $convert.base64Decode(
    'ChJQcm90b01lc3NhZ2VFeHRyYXMSQgoHaGVhZGVycxgBIAMoCzIoLnNvY2t1ZG8uUHJvdG9NZX'
    'NzYWdlRXh0cmFzLkhlYWRlcnNFbnRyeVIHaGVhZGVycxIhCgllcGhlbWVyYWwYAiABKAhIAFIJ'
    'ZXBoZW1lcmFsiAEBEiwKD2lkZW1wb3RlbmN5X2tleRgDIAEoCUgBUg5pZGVtcG90ZW5jeUtleY'
    'gBARIXCgRlY2hvGAQgASgISAJSBGVjaG+IAQESJgoCYWkYBSABKAsyFi5zb2NrdWRvLlByb3Rv'
    'QWlFeHRyYXNSAmFpGlUKDEhlYWRlcnNFbnRyeRIQCgNrZXkYASABKAlSA2tleRIvCgV2YWx1ZR'
    'gCIAEoCzIZLnNvY2t1ZG8uUHJvdG9FeHRyYXNWYWx1ZVIFdmFsdWU6AjgBQgwKCl9lcGhlbWVy'
    'YWxCEgoQX2lkZW1wb3RlbmN5X2tleUIHCgVfZWNobw==');

@$core.Deprecated('Use protoAiExtrasDescriptor instead')
const ProtoAiExtras$json = {
  '1': 'ProtoAiExtras',
  '2': [
    {
      '1': 'transport',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.sockudo.ProtoAiExtras.TransportEntry',
      '10': 'transport'
    },
    {
      '1': 'codec',
      '3': 2,
      '4': 3,
      '5': 11,
      '6': '.sockudo.ProtoAiExtras.CodecEntry',
      '10': 'codec'
    },
  ],
  '3': [ProtoAiExtras_TransportEntry$json, ProtoAiExtras_CodecEntry$json],
};

@$core.Deprecated('Use protoAiExtrasDescriptor instead')
const ProtoAiExtras_TransportEntry$json = {
  '1': 'TransportEntry',
  '2': [
    {'1': 'key', '3': 1, '4': 1, '5': 9, '10': 'key'},
    {'1': 'value', '3': 2, '4': 1, '5': 9, '10': 'value'},
  ],
  '7': {'7': true},
};

@$core.Deprecated('Use protoAiExtrasDescriptor instead')
const ProtoAiExtras_CodecEntry$json = {
  '1': 'CodecEntry',
  '2': [
    {'1': 'key', '3': 1, '4': 1, '5': 9, '10': 'key'},
    {'1': 'value', '3': 2, '4': 1, '5': 9, '10': 'value'},
  ],
  '7': {'7': true},
};

/// Descriptor for `ProtoAiExtras`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List protoAiExtrasDescriptor = $convert.base64Decode(
    'Cg1Qcm90b0FpRXh0cmFzEkMKCXRyYW5zcG9ydBgBIAMoCzIlLnNvY2t1ZG8uUHJvdG9BaUV4dH'
    'Jhcy5UcmFuc3BvcnRFbnRyeVIJdHJhbnNwb3J0EjcKBWNvZGVjGAIgAygLMiEuc29ja3Vkby5Q'
    'cm90b0FpRXh0cmFzLkNvZGVjRW50cnlSBWNvZGVjGjwKDlRyYW5zcG9ydEVudHJ5EhAKA2tleR'
    'gBIAEoCVIDa2V5EhQKBXZhbHVlGAIgASgJUgV2YWx1ZToCOAEaOAoKQ29kZWNFbnRyeRIQCgNr'
    'ZXkYASABKAlSA2tleRIUCgV2YWx1ZRgCIAEoCVIFdmFsdWU6AjgB');

@$core.Deprecated('Use protoExtrasValueDescriptor instead')
const ProtoExtrasValue$json = {
  '1': 'ProtoExtrasValue',
  '2': [
    {'1': 'string_value', '3': 1, '4': 1, '5': 9, '9': 0, '10': 'stringValue'},
    {'1': 'number_value', '3': 2, '4': 1, '5': 1, '9': 0, '10': 'numberValue'},
    {'1': 'bool_value', '3': 3, '4': 1, '5': 8, '9': 0, '10': 'boolValue'},
  ],
  '8': [
    {'1': 'kind'},
  ],
};

/// Descriptor for `ProtoExtrasValue`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List protoExtrasValueDescriptor = $convert.base64Decode(
    'ChBQcm90b0V4dHJhc1ZhbHVlEiMKDHN0cmluZ192YWx1ZRgBIAEoCUgAUgtzdHJpbmdWYWx1ZR'
    'IjCgxudW1iZXJfdmFsdWUYAiABKAFIAFILbnVtYmVyVmFsdWUSHwoKYm9vbF92YWx1ZRgDIAEo'
    'CEgAUglib29sVmFsdWVCBgoEa2luZA==');
