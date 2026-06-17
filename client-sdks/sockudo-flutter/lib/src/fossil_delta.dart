import 'dart:typed_data';

import 'support.dart';

class FossilDelta {
  static const String _digits =
      '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~';
  static final List<int> _values = List<int>.filled(128, -1);

  static bool _initialized = false;

  static Uint8List apply(Uint8List base, Uint8List delta) {
    _ensureInitialized();
    final reader = _Reader(delta);
    final outputSize = reader.readInt();
    if (reader.readChar() != '\n') {
      throw const SockudoException('size integer not terminated by newline');
    }

    final output = BytesBuilder(copy: false);
    var total = 0;

    while (reader.hasBytes) {
      final count = reader.readInt();
      switch (reader.readChar()) {
        case '@':
          final offset = reader.readInt();
          if (reader.hasBytes && reader.readChar() != ',') {
            throw const SockudoException(
              'copy command not terminated by comma',
            );
          }
          total += count;
          if (total > outputSize) {
            throw const SockudoException('copy exceeds output file size');
          }
          if (offset + count > base.length) {
            throw const SockudoException('copy extends past end of input');
          }
          output.add(base.sublist(offset, offset + count));
        case ':':
          total += count;
          if (total > outputSize) {
            throw const SockudoException(
              'insert command gives an output larger than predicted',
            );
          }
          if (reader.position + count > delta.length) {
            throw const SockudoException('insert count exceeds size of delta');
          }
          output.add(delta.sublist(reader.position, reader.position + count));
          reader.position += count;
        case ';':
          final bytes = output.takeBytes();
          if (count != _checksum(bytes)) {
            throw const SockudoException('bad checksum');
          }
          if (total != outputSize) {
            throw const SockudoException(
              'generated size does not match predicted size',
            );
          }
          return bytes;
        default:
          throw const SockudoException('unknown delta operator');
      }
    }

    throw const SockudoException('unterminated delta');
  }

  static void _ensureInitialized() {
    if (_initialized) {
      return;
    }
    for (var index = 0; index < _digits.length; index += 1) {
      _values[_digits.codeUnitAt(index)] = index;
    }
    _initialized = true;
  }

  static int _checksum(Uint8List bytes) {
    var sum0 = 0;
    var sum1 = 0;
    var sum2 = 0;
    var sum3 = 0;
    var index = 0;
    var remaining = bytes.length;

    while (remaining >= 16) {
      sum0 +=
          bytes[index + 0] +
          bytes[index + 4] +
          bytes[index + 8] +
          bytes[index + 12];
      sum1 +=
          bytes[index + 1] +
          bytes[index + 5] +
          bytes[index + 9] +
          bytes[index + 13];
      sum2 +=
          bytes[index + 2] +
          bytes[index + 6] +
          bytes[index + 10] +
          bytes[index + 14];
      sum3 +=
          bytes[index + 3] +
          bytes[index + 7] +
          bytes[index + 11] +
          bytes[index + 15];
      index += 16;
      remaining -= 16;
    }

    while (remaining >= 4) {
      sum0 += bytes[index + 0];
      sum1 += bytes[index + 1];
      sum2 += bytes[index + 2];
      sum3 += bytes[index + 3];
      index += 4;
      remaining -= 4;
    }

    sum3 = (sum3 + (sum2 << 8) + (sum1 << 16) + (sum0 << 24)) & 0xffffffff;
    if (remaining >= 3) {
      sum3 = (sum3 + (bytes[index + 2] << 8)) & 0xffffffff;
    }
    if (remaining >= 2) {
      sum3 = (sum3 + (bytes[index + 1] << 16)) & 0xffffffff;
    }
    if (remaining >= 1) {
      sum3 = (sum3 + (bytes[index + 0] << 24)) & 0xffffffff;
    }
    return sum3;
  }
}

class _Reader {
  _Reader(this.data);

  final Uint8List data;
  int position = 0;

  bool get hasBytes => position < data.length;

  int readByte() {
    if (position >= data.length) {
      throw const SockudoException('out of bounds');
    }
    return data[position++];
  }

  String readChar() => String.fromCharCode(readByte());

  int readInt() {
    FossilDelta._ensureInitialized();
    var value = 0;
    while (hasBytes) {
      final raw = readByte();
      final mapped = raw < FossilDelta._values.length
          ? FossilDelta._values[raw]
          : -1;
      if (mapped < 0) {
        position -= 1;
        break;
      }
      value = (value << 6) + mapped;
    }
    return value;
  }
}
