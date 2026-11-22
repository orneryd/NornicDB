"use strict";
/**
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var channel_buf_1 = __importDefault(require("./channel-buf"));
var neo4j_driver_core_1 = require("neo4j-driver-core");
var buffer_1 = __importDefault(require("buffer"));
var string_decoder_1 = require("string_decoder");
var decoder = new string_decoder_1.StringDecoder('utf8');
function encode(str) {
    return new channel_buf_1.default(newBuffer(str));
}
function decode(buffer, length) {
    if (Object.prototype.hasOwnProperty.call(buffer, '_buffer')) {
        return decodeChannelBuffer(buffer, length);
    }
    else if (Object.prototype.hasOwnProperty.call(buffer, '_buffers')) {
        return decodeCombinedBuffer(buffer, length);
    }
    else {
        throw (0, neo4j_driver_core_1.newError)("Don't know how to decode strings from '".concat(buffer, "'"));
    }
}
function decodeChannelBuffer(buffer, length) {
    var start = buffer.position;
    var end = start + length;
    buffer.position = Math.min(end, buffer.length);
    return buffer._buffer.toString('utf8', start, end);
}
function decodeCombinedBuffer(buffer, length) {
    return streamDecodeCombinedBuffer(buffer, length, function (partBuffer) { return decoder.write(partBuffer._buffer); }, function () { return decoder.end(); });
}
function streamDecodeCombinedBuffer(combinedBuffers, length, decodeFn, endFn) {
    var remainingBytesToRead = length;
    var position = combinedBuffers.position;
    combinedBuffers._updatePos(Math.min(length, combinedBuffers.length - position));
    // Reduce CombinedBuffers to a decoded string
    var out = combinedBuffers._buffers.reduce(function (last, partBuffer) {
        if (remainingBytesToRead <= 0) {
            return last;
        }
        else if (position >= partBuffer.length) {
            position -= partBuffer.length;
            return '';
        }
        else {
            partBuffer._updatePos(position - partBuffer.position);
            var bytesToRead = Math.min(partBuffer.length - position, remainingBytesToRead);
            var lastSlice = partBuffer.readSlice(bytesToRead);
            partBuffer._updatePos(bytesToRead);
            remainingBytesToRead = Math.max(remainingBytesToRead - lastSlice.length, 0);
            position = 0;
            return last + decodeFn(lastSlice);
        }
    }, '');
    return out + endFn();
}
function newBuffer(str) {
    // use static factory function present in newer NodeJS versions to create a buffer containing the given string
    // or fallback to the old, potentially deprecated constructor
    if (typeof buffer_1.default.Buffer.from === 'function') {
        return buffer_1.default.Buffer.from(str, 'utf8');
    }
    else {
        // eslint-disable-next-line n/no-deprecated-api
        return new buffer_1.default.Buffer(str, 'utf8');
    }
}
exports.default = {
    encode: encode,
    decode: decode
};
