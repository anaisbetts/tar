import 'dart:async';
import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';

import 'charcodes.dart';
import 'constants.dart';
import 'exception.dart';

const _checksumEnd = checksumOffset + checksumLength;
const _checksumPlaceholder = $space;

extension ByteBufferUtils on Uint8List {
  String readString(int offset, int maxLength) {
    return readStringOrNullIfEmpty(offset, maxLength) ?? '';
  }

  Uint8List sublistView(int start, [int? end]) {
    return Uint8List.sublistView(this, start, end);
  }

  String? readStringOrNullIfEmpty(int offset, int maxLength) {
    var data = sublistView(offset, offset + maxLength);
    var contentLength = data.indexOf(0);
    // If there's no \0, assume that the string fills the whole segment
    if (contentLength.isNegative) contentLength = maxLength;

    if (contentLength == 0) return null;

    data = data.sublistView(0, contentLength);
    try {
      return utf8.decode(data);
    } on FormatException {
      return String.fromCharCodes(data).trim();
    }
  }

  /// Parse an octal string encoded from index [offset] with the maximum length
  /// [length].
  int readOctal(int offset, int length) {
    var result = 0;
    var multiplier = 1;

    for (var i = length - 1; i >= 0; i--) {
      final charCode = this[offset + i];
      // Some tar implementations add a \0 or space at the end, ignore that
      if (charCode == 0 || charCode == $space) continue;
      if (charCode < $0 || charCode > $9) {
        throw TarException('Invalid octal value');
      }

      // Obtain the numerical value of this digit
      final digit = charCode - $0;
      result += digit * multiplier;
      multiplier <<= 3; // Multiply by the base, 8
    }

    return result;
  }

  /// Parses an encoded int, either as base-256 or octal.
  ///
  /// This function may return negative numbers.
  int readNumeric(int offset, int length) {
    if (length == 0) return 0;

    // Check for base-256 (binary) format first. If the first bit is set, then
    // all following bits constitute a two's complement encoded number in big-
    // endian byte order.
    final firstByte = this[offset];
    if (firstByte & 0x80 != 0) {
      // Handling negative numbers relies on the following identity:
      // -a-1 == ~a
      //
      // If the number is negative, we use an inversion mask to invert the
      // date bytes and treat the value as an unsigned number.
      final inverseMask = firstByte & 0x40 != 0 ? 0xff : 0x00;

      // Ignore signal bit in the first byte
      var x = (firstByte ^ inverseMask) & 0x7f;

      for (var i = 1; i < length; i++) {
        var byte = this[offset + i];
        byte ^= inverseMask;

        x = x << 8 | byte;
      }

      return inverseMask == 0xff ? ~x : x;
    }

    return readOctal(offset, length);
  }

  int computeUnsignedHeaderChecksum() {
    // Accessing the last element first helps the VM eliminate bounds checks in
    // the loops below.
    this[blockSize - 1];
    var result = checksumLength * _checksumPlaceholder;

    for (var i = 0; i < checksumOffset; i++) {
      result += this[i];
    }
    for (var i = _checksumEnd; i < blockSize; i++) {
      result += this[i];
    }

    return result;
  }

  int computeSignedHeaderChecksum() {
    this[blockSize - 1];
    // Note that _checksumPlaceholder.toSigned(8) == _checksumPlaceholder
    var result = checksumLength * _checksumPlaceholder;

    for (var i = 0; i < checksumOffset; i++) {
      result += this[i];
    }
    for (var i = _checksumEnd; i < blockSize; i++) {
      result += this[i];
    }

    return result;
  }

  bool get isAllZeroes {
    for (var i = 0; i < length; i++) {
      if (this[i] != 0) return false;
    }

    return true;
  }

  bool matchesHeader(List<int> header, {int offset = magicOffset}) {
    for (var i = 0; i < header.length; i++) {
      if (this[offset + i] != header[i]) return false;
    }

    return true;
  }
}

bool isNotAscii(int i) => i > 128;

/// Like [int.parse], but throwing a [TarException] instead of the more-general
/// [FormatException] when it fails.
int parseInt(String source) {
  return int.tryParse(source, radix: 10) ??
      (throw TarException('Not an int: $source'));
}

/// Takes a [paxTimeString] of the form %d.%d as described in the PAX
/// specification. Note that this implementation allows for negative timestamps,
/// which is allowed for by the PAX specification, but not always portable.
///
/// Note that Dart's [DateTime] class only allows us to give up to microsecond
/// precision, which implies that we cannot parse all the digits in since PAX
/// allows for nanosecond level encoding.
DateTime parsePaxTime(String paxTimeString) {
  const maxMicroSecondDigits = 6;

  /// Split [paxTimeString] into seconds and sub-seconds parts.
  var secondsString = paxTimeString;
  var microSecondsString = '';
  final position = paxTimeString.indexOf('.');
  if (position >= 0) {
    secondsString = paxTimeString.substring(0, position);
    microSecondsString = paxTimeString.substring(position + 1);
  }

  /// Parse the seconds.
  final seconds = int.tryParse(secondsString);
  if (seconds == null) {
    throw TarException.header('Invalid PAX time $paxTimeString detected!');
  }

  if (microSecondsString.replaceAll(RegExp('[0-9]'), '') != '') {
    throw TarException.header(
        'Invalid nanoseconds $microSecondsString detected');
  }

  microSecondsString = microSecondsString.padRight(maxMicroSecondDigits, '0');
  microSecondsString = microSecondsString.substring(0, maxMicroSecondDigits);

  var microSeconds =
      microSecondsString.isEmpty ? 0 : int.parse(microSecondsString);
  if (paxTimeString.startsWith('-')) microSeconds = -microSeconds;

  return microsecondsSinceEpoch(microSeconds + seconds * pow(10, 6).toInt());
}

DateTime secondsSinceEpoch(int timestamp) {
  return DateTime.fromMillisecondsSinceEpoch(timestamp * 1000, isUtc: true);
}

DateTime millisecondsSinceEpoch(int milliseconds) {
  return DateTime.fromMillisecondsSinceEpoch(milliseconds, isUtc: true);
}

DateTime microsecondsSinceEpoch(int microseconds) {
  return DateTime.fromMicrosecondsSinceEpoch(microseconds, isUtc: true);
}

int numBlocks(int fileSize) {
  if (fileSize % blockSize == 0) return fileSize ~/ blockSize;

  return fileSize ~/ blockSize + 1;
}

int nextBlockSize(int fileSize) => numBlocks(fileSize) * blockSize;

extension ToTyped on List<int> {
  Uint8List asUint8List() {
    // Flow analysis doesn't work on this.
    final $this = this;
    return $this is Uint8List ? $this : Uint8List.fromList(this);
  }
}

/// Generates a chunked stream of [length] zeroes.
Stream<List<int>> zeroes(int length) async* {
  // Emit data in chunks for efficiency
  const chunkSize = 4 * 1024;
  if (length < chunkSize) {
    yield Uint8List(length);
    return;
  }

  final chunk = Uint8List(chunkSize);
  for (var i = 0; i < length ~/ chunkSize; i++) {
    yield chunk;
  }

  final remainingBytes = length % chunkSize;
  if (remainingBytes != 0) {
    yield Uint8List(remainingBytes);
  }
}

class StreamBlockReader {
  final Stream<List<int>> _source;
  StreamSubscription<List<int>>? _inputSubscription;

  Completer<Uint8List>? _nextBlockCompleter;

  StreamController<Uint8List>? _nextBlocksStream;
  int _remainingBlocksInStream = 0;

  Uint8List? _pendingBlock;
  int _missingInPendingBlock = 0;

  Uint8List? _waitingForDispatch;
  int _offsetInWaiting = 0;

  bool _closed = false;

  StreamBlockReader(this._source);

  int get _blocksToServe {
    if (_nextBlockCompleter != null) {
      return 1;
    } else {
      return _remainingBlocksInStream;
    }
  }

  void _listenOrResume() {
    final subsription = _inputSubscription;
    if (subsription == null) {
      _inputSubscription =
          _source.listen(_onData, onError: _onError, onDone: _onDone);
    } else {
      subsription.resume();
    }
  }

  /// Emits ready [blocks].
  ///
  /// Returns whether we should pause emitting further data afterwards.
  bool _emitBlocks(Uint8List blocks, {int amount = 1}) {
    final stream = _nextBlocksStream;
    final future = _nextBlockCompleter;
    final subscription = _inputSubscription;
    assert((future == null) ^ (stream == null));
    assert(subscription != null);

    final bool shouldPause;

    if (stream != null) {
      stream.add(blocks);
      final remaining = _remainingBlocksInStream -= amount;
      assert(remaining >= 0);

      final done = remaining == 0;
      if (done) {
        stream.close();
        _nextBlocksStream = null;
      }

      shouldPause = done || stream.isPausedOrClosed;
    } else if (future != null) {
      assert(amount == 1);
      future.complete(blocks);
      _nextBlockCompleter = null;
      shouldPause = true;
    } else {
      // Exactly one of them must be non-null
      throw AssertionError();
    }

    if (shouldPause && !subscription!.isPaused) {
      subscription.pause();
    }
    return shouldPause;
  }

  void _startPendingBlock(Uint8List source, int startOffset) {
    assert(_pendingBlock == null);
    final availableData = source.length - startOffset;
    assert(availableData < blockSize);

    _pendingBlock = Uint8List(blockSize)
      ..setAll(0, source.sublistView(startOffset));
    _missingInPendingBlock = blockSize - availableData;
  }

  void _saveNewUndispatchedState(Uint8List chunk, int offsetInChunk) {
    assert(_waitingForDispatch == null);
    _saveUndispatchedState(chunk, offsetInChunk);
  }

  void _saveUndispatchedState(Uint8List chunk, int offsetInChunk) {
    assert(_pendingBlock == null);
    final remaining = chunk.length - offsetInChunk;
    if (remaining < blockSize) {
      _waitingForDispatch = null;
      _offsetInWaiting = 0;
      _startPendingBlock(chunk, offsetInChunk);
    } else {
      _waitingForDispatch = chunk;
      _offsetInWaiting = offsetInChunk;
    }
  }

  void _onData(List<int> chunk) {
    assert(_waitingForDispatch == null,
        'Had pending data while receiving new event');

    final typedChunk = chunk.asUint8List();

    // The start offset of the new data that we didn't process yet.
    var offsetInData = 0;

    void savePendingChunk() {
      _saveNewUndispatchedState(typedChunk, offsetInData);
    }

    // Try finishing the pending block first, if it exists.
    if (_pendingBlock != null) {
      final started = _pendingBlock!;
      final missing = _missingInPendingBlock;
      final startOffsetInStarted = blockSize - missing;

      if (typedChunk.length >= missing) {
        // We can complete the block
        started.setAll(
            startOffsetInStarted, typedChunk.sublistView(0, missing));
        offsetInData = missing;
        final done = _emitBlocks(started);

        // We just finished serving the started block, so reset that
        _pendingBlock = null;
        _missingInPendingBlock = 0;

        if (done) {
          savePendingChunk();
          return;
        }
      } else {
        // We can't complete the pending block with the new data, but at least
        // we can continue filling it up
        started.setAll(startOffsetInStarted, typedChunk);
        _missingInPendingBlock -= typedChunk.length;
        assert(_missingInPendingBlock > 0);
        return;
      }
    }

    // When we get to this point, the started chunk has been completed and we
    // can continue adding chunks as they come.
    assert(_pendingBlock == null);

    // Serve as many blocks as we can from the current chunk
    final blocksToServe = _blocksToServe;
    assert(blocksToServe >= 1);
    final availableBlocksInChunk =
        (typedChunk.length - offsetInData) ~/ blockSize;
    final blocksToDeliverNow = min(blocksToServe, availableBlocksInChunk);

    if (blocksToDeliverNow > 0) {
      final length = blockSize * blocksToDeliverNow;
      final end = offsetInData + length;
      final done = _emitBlocks(typedChunk.sublistView(offsetInData, end),
          amount: blocksToDeliverNow);
      offsetInData = end;

      // Once again, stop and save state if this has completed the output
      if (done) {
        savePendingChunk();
        return;
      }
    }

    // If we're here, the remaining data in the chunk was not large enough to
    // serve a full block. Store the remaining data in a pending block.
    _startPendingBlock(typedChunk, offsetInData);
  }

  void _onError(Object error, [StackTrace? trace]) {
    final stream = _nextBlocksStream;
    final future = _nextBlockCompleter;

    if (stream != null) {
      stream.addError(error, trace);
    } else if (future != null) {
      future.completeError(error, trace);
      _nextBlockCompleter = null;
    }

    final subscription = _inputSubscription;
    if (!subscription!.isPaused) {
      subscription.pause();
    }
  }

  void _onDone() {
    if (_blocksToServe > 0) {
      if (_pendingBlock != null) {
        _emitBlocks(
            _pendingBlock!.sublistView(0, blockSize - _missingInPendingBlock));
        _pendingBlock = null;
        _missingInPendingBlock = 0;
      } else {
        // Complete with an empty block
        _emitBlocks(emptyUint8List);
      }

      _inputSubscription = null;
    }
    close();
  }

  void _checkIdle(String method) {
    if (_nextBlockCompleter != null) {
      throw StateError("Can't call $method before a previous call to "
          'nextBlock() has completed.');
    }
    if (_nextBlocksStream != null) {
      throw StateError("Can't call $method before a previous call to "
          'nextBlocks() was completed.');
    }
  }

  /// Reads the next block from the input stream.
  ///
  /// The returned list may be empty or smaller than [blockSize] if the input
  /// stream ends.
  Future<Uint8List> nextBlock() {
    _checkIdle('nextBlock');
    if (_closed) return Future.value(emptyUint8List);

    // If we have pending data to serve, do that before we start resuming the
    // stream subscription
    final waiting = _waitingForDispatch;
    if (waiting != null) {
      final offset = _offsetInWaiting;
      assert(waiting.length - offset >= blockSize);

      final end = offset + blockSize;
      final result = Future<Uint8List>.value(waiting.sublistView(offset, end));

      if (end == waiting.length) {
        // We just completed all data waiting for dispatch
        _waitingForDispatch = null;
        _offsetInWaiting = 0;
      } else {
        // Store new pending data
        _saveUndispatchedState(waiting, offset + blockSize);
      }
      return result;
    }

    // Note: Our logic in _emitBlocks depends on this completer NOT being sync
    final next = _nextBlockCompleter = Completer();
    _listenOrResume();
    return next.future;
  }

  Stream<Uint8List> nextBlocks(int amount) {
    _checkIdle('nextBlocks');

    if (amount == 0 || _closed) return const Stream.empty();

    // This controller must not be sync: We add events in response to onListen
    // and expect a delay after closing it in _emitBlocks
    final controller = _nextBlocksStream = StreamController();
    _remainingBlocksInStream = amount;

    void onListenOrResume() {
      // Once again, check if we have pending data to serve
      final waiting = _waitingForDispatch;
      if (waiting != null) {
        final offset = _offsetInWaiting;
        final blocksWaiting = (waiting.length - offset) ~/ blockSize;
        assert(blocksWaiting > 0);

        final blocksToEmit = min(blocksWaiting, amount);
        final end = offset + blocksToEmit * blockSize;
        final done =
            _emitBlocks(waiting.sublistView(offset, end), amount: blocksToEmit);
        _saveUndispatchedState(waiting, end);
        if (!done) {
          _listenOrResume();
        }
      } else {
        // No data waiting to be served, so we definitely have to start
        // listening
        _listenOrResume();
      }
    }

    controller
      ..onListen = onListenOrResume
      ..onPause = () {
        final sub = _inputSubscription;
        if (!sub!.isPaused) sub.pause();
      }
      ..onResume = onListenOrResume;
    return controller.stream;
  }

  Future<void> close() async {
    if (_closed) return;

    _closed = true;
    await _inputSubscription?.cancel();

    _nextBlockCompleter?.complete(emptyUint8List);

    final stream = _nextBlocksStream;
    if (stream != null) {
      await stream.close();
    }
  }
}

extension on StreamController<dynamic> {
  bool get isPausedOrClosed => isPaused || isClosed;
}
