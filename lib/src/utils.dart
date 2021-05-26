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
    var result = 0;

    for (var i = 0; i < length; i++) {
      result += (i < checksumOffset || i >= _checksumEnd)
          ? this[i] // Not in range of where the checksum is written
          : _checksumPlaceholder;
    }

    return result;
  }

  int computeSignedHeaderChecksum() {
    var result = 0;

    for (var i = 0; i < length; i++) {
      // Note that _checksumPlaceholder.toSigned(8) == _checksumPlaceholder
      result += (i < checksumOffset || i >= _checksumEnd)
          ? this[i].toSigned(8)
          : _checksumPlaceholder;
    }

    return result;
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

  bool get isAllZeroes {
    for (var i = 0; i < length; i++) {
      if (this[i] != 0) return false;
    }

    return true;
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

Stream<Uint8List> _inChunks(Stream<List<int>> input) {
  Uint8List? startedChunk;
  int missingForNextChunk = 0;

  Uint8List? pendingEvent;
  int offsetInPendingEvent = 0;

  late StreamSubscription<List<int>> inputSubscription;
  final controller = StreamController<Uint8List>(sync: true);

  var isResuming = false;

  void startChunk(Uint8List source, int startOffset) {
    assert(startedChunk == null);
    final availableData = source.length - startOffset;
    assert(availableData < blockSize);

    startedChunk = Uint8List(blockSize)
      ..setAll(0, source.sublistView(startOffset));
    missingForNextChunk = blockSize - availableData;
  }

  void handleData(List<int> data) {
    assert(pendingEvent == null,
        'Had pending events while receiving new data from source.');
    final typedData = data.asUint8List();

    // The start offset of the new data that we didn't process yet.
    var offsetInData = 0;
    bool saveStateIfPaused() {
      if (controller.isPausedOrClosed) {
        pendingEvent = typedData;
        offsetInPendingEvent = offsetInData;
        return true;
      }
      return false;
    }

    // Try completing the pending chunk first, if it exists
    if (startedChunk != null) {
      final started = startedChunk!;
      final startOffsetInStarted = blockSize - missingForNextChunk;

      if (data.length >= missingForNextChunk) {
        // Fill up the chunk, then go on with the remaining data
        started.setAll(startOffsetInStarted,
            typedData.sublistView(0, missingForNextChunk));
        controller.add(started);
        offsetInData += missingForNextChunk;

        // We just finished serving the started chunk, so reset that
        startedChunk = null;
        missingForNextChunk = 0;

        // If the controller was paused in a response to the add, stop serving
        // events and store the rest of the input for later
        if (saveStateIfPaused()) return;
      } else {
        // Ok, we can't finish the pending chunk with the new data but at least
        // we can continue filling it up
        started.setAll(startOffsetInStarted, typedData);
        missingForNextChunk -= typedData.length;
        return;
      }
    }

    // The started chunk has been completed, continue by adding chunks as they
    // come.
    assert(startedChunk == null);

    while (offsetInData < typedData.length) {
      // Can we serve a full block from the new event
      if (offsetInData <= typedData.length - blockSize) {
        // Yup, then let's do that
        final end = offsetInData + blockSize;
        controller.add(typedData.sublistView(offsetInData, end));
        offsetInData = end;

        // Once again, stop and save state if the controller was paused.
        if (saveStateIfPaused()) return;
      } else {
        // Ok, no full block but we can start a new pending chunk
        startChunk(typedData, offsetInData);
        break;
      }
    }
  }

  void startResume() {
    isResuming = false;
    if (controller.isPausedOrClosed) return;

    // Start dispatching pending events before we resume the subscription on the
    // main stream.
    if (pendingEvent != null) {
      final pending = pendingEvent!;
      assert(startedChunk == null, 'Had pending events and a started chunk');

      while (offsetInPendingEvent < pending.length) {
        // Can we serve a full block from pending data?
        if (offsetInPendingEvent <= pending.length - blockSize) {
          final end = offsetInPendingEvent + blockSize;
          controller.add(pending.sublistView(offsetInPendingEvent, end));
          offsetInPendingEvent = end;

          // Pause if we got a pause request in response to the add
          if (controller.isPausedOrClosed) return;
        } else {
          // Store pending block that we can't finish with the pending chunk.
          startChunk(pending, offsetInPendingEvent);
          break;
        }
      }

      // Pending events have been dispatched
      offsetInPendingEvent = 0;
      pendingEvent = null;
    }

    // When we're here, all pending events have been dispatched and the
    // controller is not paused. Let's continue then!
    inputSubscription.resume();
  }

  void onDone() {
    // Input stream is done. If we have a block left over, emit that now
    if (startedChunk != null) {
      controller
          .add(startedChunk!.sublistView(0, blockSize - missingForNextChunk));
    }
    controller.close();
  }

  controller
    ..onListen = () {
      inputSubscription = input.listen(handleData,
          onError: controller.addError, onDone: onDone);
    }
    ..onPause = () {
      if (!inputSubscription.isPaused) {
        inputSubscription.pause();
      }
    }
    ..onResume = () {
      // This is a bit hacky. Our subscription is most likely going to be a
      // _BufferingStreamSubscription which will buffer events that were added
      // in this callback. However, we really want to know when the subscription
      // has been paused in response to a new event because we can handle that
      // very efficiently.
      if (!isResuming) {
        scheduleMicrotask(startResume);
      }
      isResuming = true;
    }
    ..onCancel = () {
      inputSubscription.cancel();
    };

  return controller.stream;
}

extension on StreamController<dynamic> {
  bool get isPausedOrClosed => isPaused || isClosed;
}

extension ReadInChunks on Stream<List<int>> {
  /// A stream emitting values in chunks of `512` byte blocks.
  ///
  /// The last emitted chunk may be shorter than the regular block length.
  Stream<Uint8List> get inChunks => _inChunks(this);
}

extension NextOrNull<T extends Object> on StreamIterator<T> {
  Future<T?> get nextOrNull async {
    if (await moveNext()) {
      return current;
    } else {
      return null;
    }
  }

  Stream<T> nextElements(int count) {
    if (count == 0) return const Stream<Never>.empty();

    var remaining = count;
    Future<Object?>? pendingOperation;
    final controller = StreamController<T>(sync: true);

    void addNewEvent() {
      if (pendingOperation == null && remaining > 0) {
        // Start fetching data
        final fetchNext = nextOrNull.then<Object?>((value) {
          remaining--;
          if (value != null) {
            // Got data, let's add it
            controller.add(value);
          }

          if (value == null || remaining == 0) {
            // Finished, so close the controller
            return controller.close();
          } else {
            return Future.value();
          }
        }, onError: controller.addError);

        pendingOperation = fetchNext.whenComplete(() {
          pendingOperation = null;
          if (!controller.isPaused && remaining > 0) {
            // Controller isn't paused, so add another event
            addNewEvent();
          }
        });
      }
    }

    controller
      ..onListen = addNewEvent
      ..onResume = addNewEvent;

    return controller.stream;
  }
}
