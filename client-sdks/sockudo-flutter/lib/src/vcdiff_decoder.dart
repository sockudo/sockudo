import 'dart:typed_data';

import 'support.dart';

Uint8List decodeVcdiff(Uint8List delta, Uint8List source) {
  final vcdiff = _Vcdiff(delta, source);
  return vcdiff.decode();
}

class _Vcdiff {
  _Vcdiff(this.delta, this.source);

  final Uint8List delta;
  final Uint8List source;
  int position = 0;
  final _TypedArrayList targetWindows = _TypedArrayList();

  Uint8List decode() {
    _consumeHeader();
    while (_consumeWindow()) {}

    final target = Uint8List(targetWindows.totalLength);
    var offset = 0;
    for (final array in targetWindows.arrays) {
      target.setAll(offset, array);
      offset += array.length;
    }
    return target;
  }

  void _consumeHeader() {
    final hasHeader =
        delta.length >= 5 &&
        delta[0] == 214 &&
        delta[1] == 195 &&
        delta[2] == 196 &&
        delta[3] == 0;
    if (!hasHeader) {
      throw const SockudoException('first 3 bytes not VCD');
    }

    final hdrIndicator = delta[4];
    final vcdDecompress = 1 & hdrIndicator;
    final vcdCodeTable = 1 & (hdrIndicator >> 1);
    if (vcdDecompress != 0 || vcdCodeTable != 0) {
      throw const SockudoException(
        'non-zero Hdr_Indicator (VCD_DECOMPRESS or VCD_CODETABLE bit is set)',
      );
    }

    position = 5;
  }

  bool _consumeWindow() {
    final winIndicator = delta[position++];
    final vcdSource = 1 & winIndicator;
    final vcdTarget = 1 & (winIndicator >> 1);

    if (vcdSource != 0 && vcdTarget != 0) {
      throw const SockudoException(
        'VCD_SOURCE and VCD_TARGET cannot both be set in Win_Indicator',
      );
    } else if (vcdSource != 0) {
      final sourceSegmentLength = _deserializeInteger(delta, position);
      position = sourceSegmentLength.position;
      final sourceSegmentPosition = _deserializeInteger(delta, position);
      position = sourceSegmentPosition.position;
      final deltaLength = _deserializeInteger(delta, position);
      position = deltaLength.position;

      final segment = source.sublist(
        sourceSegmentPosition.value,
        sourceSegmentPosition.value + sourceSegmentLength.value,
      );
      _buildTargetWindow(position, sourceSegment: Uint8List.fromList(segment));
      position += deltaLength.value;
    } else if (vcdTarget != 0) {
      throw const SockudoException('non-zero VCD_TARGET in Win_Indicator');
    } else {
      final deltaLength = _deserializeInteger(delta, position);
      position = deltaLength.position;
      _buildTargetWindow(position);
      position += deltaLength.value;
    }

    return position < delta.length;
  }

  void _buildTargetWindow(int startPosition, {Uint8List? sourceSegment}) {
    final window = _deserializeDelta(delta, startPosition);
    final target = Uint8List(window.targetWindowLength);

    final union = _TypedArrayList();
    var targetPosition = 0;
    if (sourceSegment != null) {
      union.add(sourceSegment);
      targetPosition = sourceSegment.length;
    }
    union.add(target);

    final deltaState = _Delta(
      union,
      targetPosition,
      window.data,
      window.addresses,
    );

    for (final instruction in window.instructions) {
      instruction.execute(deltaState);
    }

    targetWindows.add(target);
  }
}

class _Delta {
  _Delta(this.buffer, this.targetPosition, this.data, this.addresses);

  final _TypedArrayList buffer;
  int targetPosition;
  final Uint8List data;
  int dataPosition = 0;
  final Uint8List addresses;
  int addressesPosition = 0;
  final _NearCache nearCache = _NearCache(4);
  final _SameCache sameCache = _SameCache(3);

  int nextAddressInteger() {
    final value = _deserializeInteger(addresses, addressesPosition);
    addressesPosition = value.position;
    return value.value;
  }

  int nextAddressByte() => addresses[addressesPosition++];
}

sealed class _Instruction {
  void execute(_Delta delta);
}

class _AddInstruction extends _Instruction {
  _AddInstruction(this.size);

  final int size;

  @override
  void execute(_Delta delta) {
    for (var index = 0; index < size; index += 1) {
      delta.buffer.set(
        delta.targetPosition + index,
        delta.data[delta.dataPosition + index],
      );
    }
    delta.dataPosition += size;
    delta.targetPosition += size;
  }
}

class _RunInstruction extends _Instruction {
  _RunInstruction(this.size);

  final int size;

  @override
  void execute(_Delta delta) {
    for (var index = 0; index < size; index += 1) {
      delta.buffer.set(
        delta.targetPosition + index,
        delta.data[delta.dataPosition],
      );
    }
    delta.dataPosition += 1;
    delta.targetPosition += size;
  }
}

class _CopyInstruction extends _Instruction {
  _CopyInstruction(this.size, this.mode);

  final int size;
  final int mode;

  @override
  void execute(_Delta delta) {
    late final int address;
    if (mode == 0) {
      address = delta.nextAddressInteger();
    } else if (mode == 1) {
      final next = delta.nextAddressInteger();
      address = delta.targetPosition - next;
    } else if (mode - 2 >= 0 && (mode - 2) < delta.nearCache.size) {
      final next = delta.nextAddressInteger();
      address = delta.nearCache.get(mode - 2, next);
    } else {
      final sameIndex = mode - (2 + delta.nearCache.size);
      final next = delta.nextAddressByte();
      address = delta.sameCache.get(sameIndex, next);
    }

    delta.nearCache.update(address);
    delta.sameCache.update(address);

    for (var index = 0; index < size; index += 1) {
      delta.buffer.set(
        delta.targetPosition + index,
        delta.buffer.get(address + index),
      );
    }
    delta.targetPosition += size;
  }
}

class _TypedArrayList {
  final List<Uint8List> arrays = <Uint8List>[];
  final List<int> startIndexes = <int>[];
  int totalLength = 0;

  void add(Uint8List typedArray) {
    final startIndex = arrays.isEmpty
        ? 0
        : startIndexes.last + arrays.last.length;
    startIndexes.add(startIndex);
    arrays.add(typedArray);
    totalLength = startIndex + typedArray.length;
  }

  int get(int index) {
    final listIndex = _getListIndex(startIndexes, index);
    final typedArrayIndex = index - startIndexes[listIndex];
    return arrays[listIndex][typedArrayIndex];
  }

  void set(int index, int value) {
    final listIndex = _getListIndex(startIndexes, index);
    final typedArrayIndex = index - startIndexes[listIndex];
    arrays[listIndex][typedArrayIndex] = value;
  }
}

int _getListIndex(List<int> indexes, int element) {
  if (indexes.length == 2) {
    return element < indexes[1] ? 0 : 1;
  }

  var low = 0;
  var high = indexes.length - 1;
  while (low < high) {
    final mid = ((low + high) / 2).floor();
    if (indexes[mid] == element) {
      return mid;
    } else if (indexes[mid] < element) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }
  return indexes[high] > element ? high - 1 : high;
}

class _NearCache {
  _NearCache(this.size) : near = List<int>.filled(size, 0);

  final int size;
  final List<int> near;
  int nextSlot = 0;

  void update(int address) {
    if (near.isNotEmpty) {
      near[nextSlot] = address;
      nextSlot = (nextSlot + 1) % near.length;
    }
  }

  int get(int mode, int offset) => near[mode] + offset;
}

class _SameCache {
  _SameCache(this.size) : same = List<int>.filled(size * 256, 0);

  final int size;
  final List<int> same;

  void update(int address) {
    if (same.isNotEmpty) {
      same[address % (size * 256)] = address;
    }
  }

  int get(int mode, int offset) => same[mode * 256 + offset];
}

class _IntegerResult {
  const _IntegerResult(this.value, this.position);

  final int value;
  final int position;
}

_IntegerResult _deserializeInteger(Uint8List buffer, int position) {
  var value = 0;
  var current = position;
  do {
    value = (value << 7) | (buffer[current] & 127);
    if (value < 0) {
      throw const SockudoException(
        'RFC 3284 Integer conversion: Buffer overflow',
      );
    }
  } while ((buffer[current++] & 128) != 0);
  return _IntegerResult(value, current);
}

class _Window {
  const _Window({
    required this.targetWindowLength,
    required this.position,
    required this.data,
    required this.instructions,
    required this.addresses,
  });

  final int targetWindowLength;
  final int position;
  final Uint8List data;
  final List<_Instruction> instructions;
  final Uint8List addresses;
}

_Window _deserializeDelta(Uint8List delta, int position) {
  final targetWindowLength = _deserializeInteger(delta, position);
  position = targetWindowLength.position;

  if (delta[position] != 0) {
    throw SockudoException(
      'VCD_DECOMPRESS is not supported, Delta_Indicator must be zero at byte $position and not ${delta[position]}',
    );
  }
  position += 1;

  final dataLength = _deserializeInteger(delta, position);
  position = dataLength.position;
  final instructionsLength = _deserializeInteger(delta, position);
  position = instructionsLength.position;
  final addressesLength = _deserializeInteger(delta, position);
  position = addressesLength.position;

  final dataNextPosition = position + dataLength.value;
  final data = delta.sublist(position, dataNextPosition);

  final instructionsNextPosition = dataNextPosition + instructionsLength.value;
  final instructions = _tokenizeInstructions(
    delta.sublist(dataNextPosition, instructionsNextPosition),
  );

  final addressesNextPosition =
      instructionsNextPosition + addressesLength.value;
  final addresses = delta.sublist(
    instructionsNextPosition,
    addressesNextPosition,
  );

  return _Window(
    targetWindowLength: targetWindowLength.value,
    position: addressesNextPosition,
    data: Uint8List.fromList(data),
    instructions: instructions,
    addresses: Uint8List.fromList(addresses),
  );
}

List<_Instruction> _tokenizeInstructions(Uint8List instructionsBuffer) {
  final deserialized = <_Instruction>[];
  var position = 0;

  while (position < instructionsBuffer.length) {
    final index = instructionsBuffer[position++];
    if (index == 0) {
      final size = _deserializeInteger(instructionsBuffer, position);
      position = size.position;
      deserialized.add(_RunInstruction(size.value));
    } else if (index == 1) {
      final size = _deserializeInteger(instructionsBuffer, position);
      position = size.position;
      deserialized.add(_AddInstruction(size.value));
    } else if (index < 19) {
      deserialized.add(_AddInstruction(index - 1));
    } else if (index == 19) {
      final size = _deserializeInteger(instructionsBuffer, position);
      position = size.position;
      deserialized.add(_CopyInstruction(size.value, 0));
    } else if (index < 35) {
      deserialized.add(_CopyInstruction(index - 16, 0));
    } else if (index == 35) {
      final size = _deserializeInteger(instructionsBuffer, position);
      position = size.position;
      deserialized.add(_CopyInstruction(size.value, 1));
    } else if (index < 51) {
      deserialized.add(_CopyInstruction(index - 32, 1));
    } else if (index == 51) {
      final size = _deserializeInteger(instructionsBuffer, position);
      position = size.position;
      deserialized.add(_CopyInstruction(size.value, 2));
    } else if (index < 67) {
      deserialized.add(_CopyInstruction(index - 48, 2));
    } else if (index == 67) {
      final size = _deserializeInteger(instructionsBuffer, position);
      position = size.position;
      deserialized.add(_CopyInstruction(size.value, 3));
    } else if (index < 83) {
      deserialized.add(_CopyInstruction(index - 64, 3));
    } else if (index == 83) {
      final size = _deserializeInteger(instructionsBuffer, position);
      position = size.position;
      deserialized.add(_CopyInstruction(size.value, 4));
    } else if (index < 99) {
      deserialized.add(_CopyInstruction(index - 80, 4));
    } else if (index == 99) {
      final size = _deserializeInteger(instructionsBuffer, position);
      position = size.position;
      deserialized.add(_CopyInstruction(size.value, 5));
    } else if (index < 115) {
      deserialized.add(_CopyInstruction(index - 96, 5));
    } else if (index == 115) {
      final size = _deserializeInteger(instructionsBuffer, position);
      position = size.position;
      deserialized.add(_CopyInstruction(size.value, 6));
    } else if (index < 131) {
      deserialized.add(_CopyInstruction(index - 112, 6));
    } else if (index == 131) {
      final size = _deserializeInteger(instructionsBuffer, position);
      position = size.position;
      deserialized.add(_CopyInstruction(size.value, 7));
    } else if (index < 147) {
      deserialized.add(_CopyInstruction(index - 128, 7));
    } else if (index == 147) {
      final size = _deserializeInteger(instructionsBuffer, position);
      position = size.position;
      deserialized.add(_CopyInstruction(size.value, 8));
    } else if (index < 163) {
      deserialized.add(_CopyInstruction(index - 144, 8));
    } else if (index < 175) {
      final sizes = _addCopy(index, 163);
      deserialized
        ..add(_AddInstruction(sizes.$1))
        ..add(_CopyInstruction(sizes.$2, 0));
    } else if (index < 187) {
      final sizes = _addCopy(index, 175);
      deserialized
        ..add(_AddInstruction(sizes.$1))
        ..add(_CopyInstruction(sizes.$2, 1));
    } else if (index < 199) {
      final sizes = _addCopy(index, 187);
      deserialized
        ..add(_AddInstruction(sizes.$1))
        ..add(_CopyInstruction(sizes.$2, 2));
    } else if (index < 211) {
      final sizes = _addCopy(index, 199);
      deserialized
        ..add(_AddInstruction(sizes.$1))
        ..add(_CopyInstruction(sizes.$2, 3));
    } else if (index < 223) {
      final sizes = _addCopy(index, 211);
      deserialized
        ..add(_AddInstruction(sizes.$1))
        ..add(_CopyInstruction(sizes.$2, 4));
    } else if (index < 235) {
      final sizes = _addCopy(index, 223);
      deserialized
        ..add(_AddInstruction(sizes.$1))
        ..add(_CopyInstruction(sizes.$2, 5));
    } else if (index < 239) {
      deserialized
        ..add(_AddInstruction(index - 235 + 1))
        ..add(_CopyInstruction(4, 6));
    } else if (index < 243) {
      deserialized
        ..add(_AddInstruction(index - 239 + 1))
        ..add(_CopyInstruction(4, 7));
    } else if (index < 247) {
      deserialized
        ..add(_AddInstruction(index - 243 + 1))
        ..add(_CopyInstruction(4, 8));
    } else if (index < 256) {
      deserialized
        ..add(_CopyInstruction(4, index - 247))
        ..add(_AddInstruction(1));
    } else {
      throw const SockudoException('Invalid VCDIFF instruction stream');
    }
  }

  return deserialized;
}

(int, int) _addCopy(int index, int baseIndex) {
  final zeroBased = index - baseIndex;
  final addSize = (zeroBased ~/ 3) + 1;
  final copySize = (zeroBased % 3) + 4;
  return (addSize, copySize);
}
