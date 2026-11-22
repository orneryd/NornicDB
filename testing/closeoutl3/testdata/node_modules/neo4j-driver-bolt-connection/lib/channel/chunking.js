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
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (Object.prototype.hasOwnProperty.call(b, p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        if (typeof b !== "function" && b !== null)
            throw new TypeError("Class extends value " + String(b) + " is not a constructor or null");
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Dechunker = exports.Chunker = void 0;
var base_buf_1 = __importDefault(require("../buf/base-buf"));
var channel_buf_1 = require("./channel-buf");
var combined_buf_1 = __importDefault(require("./combined-buf"));
var _CHUNK_HEADER_SIZE = 2;
var _MESSAGE_BOUNDARY = 0x00;
var _DEFAULT_BUFFER_SIZE = 1400; // http://stackoverflow.com/questions/2613734/maximum-packet-size-for-a-tcp-connection
/**
 * Looks like a writable buffer, chunks output transparently into a channel below.
 * @access private
 */
var Chunker = /** @class */ (function (_super) {
    __extends(Chunker, _super);
    function Chunker(channel, bufferSize) {
        var _this = _super.call(this, 0) || this;
        _this._bufferSize = bufferSize || _DEFAULT_BUFFER_SIZE;
        _this._ch = channel;
        _this._buffer = (0, channel_buf_1.alloc)(_this._bufferSize);
        _this._currentChunkStart = 0;
        _this._chunkOpen = false;
        return _this;
    }
    Chunker.prototype.putUInt8 = function (position, val) {
        this._ensure(1);
        this._buffer.writeUInt8(val);
    };
    Chunker.prototype.putInt8 = function (position, val) {
        this._ensure(1);
        this._buffer.writeInt8(val);
    };
    Chunker.prototype.putFloat64 = function (position, val) {
        this._ensure(8);
        this._buffer.writeFloat64(val);
    };
    Chunker.prototype.putBytes = function (position, data) {
        // TODO: If data is larger than our chunk size or so, we're very likely better off just passing this buffer on
        // rather than doing the copy here TODO: *however* note that we need some way to find out when the data has been
        // written (and thus the buffer can be re-used) if we take that approach
        while (data.remaining() > 0) {
            // Ensure there is an open chunk, and that it has at least one byte of space left
            this._ensure(1);
            if (this._buffer.remaining() > data.remaining()) {
                this._buffer.writeBytes(data);
            }
            else {
                this._buffer.writeBytes(data.readSlice(this._buffer.remaining()));
            }
        }
        return this;
    };
    Chunker.prototype.flush = function () {
        if (this._buffer.position > 0) {
            this._closeChunkIfOpen();
            // Local copy and clear the buffer field. This ensures that the buffer is not re-released if the flush call fails
            var out = this._buffer;
            this._buffer = null;
            this._ch.write(out.getSlice(0, out.position));
            // Alloc a new output buffer. We assume we're using NodeJS's buffer pooling under the hood here!
            this._buffer = (0, channel_buf_1.alloc)(this._bufferSize);
            this._chunkOpen = false;
        }
        return this;
    };
    /**
     * Bolt messages are encoded in one or more chunks, and the boundary between two messages
     * is encoded as a 0-length chunk, `00 00`. This inserts such a message boundary, closing
     * any currently open chunk as needed
     */
    Chunker.prototype.messageBoundary = function () {
        this._closeChunkIfOpen();
        if (this._buffer.remaining() < _CHUNK_HEADER_SIZE) {
            this.flush();
        }
        // Write message boundary
        this._buffer.writeInt16(_MESSAGE_BOUNDARY);
    };
    /** Ensure at least the given size is available for writing */
    Chunker.prototype._ensure = function (size) {
        var toWriteSize = this._chunkOpen ? size : size + _CHUNK_HEADER_SIZE;
        if (this._buffer.remaining() < toWriteSize) {
            this.flush();
        }
        if (!this._chunkOpen) {
            this._currentChunkStart = this._buffer.position;
            this._buffer.position = this._buffer.position + _CHUNK_HEADER_SIZE;
            this._chunkOpen = true;
        }
    };
    Chunker.prototype._closeChunkIfOpen = function () {
        if (this._chunkOpen) {
            var chunkSize = this._buffer.position - (this._currentChunkStart + _CHUNK_HEADER_SIZE);
            this._buffer.putUInt16(this._currentChunkStart, chunkSize);
            this._chunkOpen = false;
        }
    };
    return Chunker;
}(base_buf_1.default));
exports.Chunker = Chunker;
/**
 * Combines chunks until a complete message is gathered up, and then forwards that
 * message to an 'onmessage' listener.
 * @access private
 */
var Dechunker = /** @class */ (function () {
    function Dechunker() {
        this._currentMessage = [];
        this._partialChunkHeader = 0;
        this._state = this.AWAITING_CHUNK;
    }
    Dechunker.prototype.AWAITING_CHUNK = function (buf) {
        if (buf.remaining() >= 2) {
            // Whole header available, read that
            return this._onHeader(buf.readUInt16());
        }
        else {
            // Only one byte available, read that and wait for the second byte
            this._partialChunkHeader = buf.readUInt8() << 8;
            return this.IN_HEADER;
        }
    };
    Dechunker.prototype.IN_HEADER = function (buf) {
        // First header byte read, now we read the next one
        return this._onHeader((this._partialChunkHeader | buf.readUInt8()) & 0xffff);
    };
    Dechunker.prototype.IN_CHUNK = function (buf) {
        if (this._chunkSize <= buf.remaining()) {
            // Current packet is larger than current chunk, or same size:
            this._currentMessage.push(buf.readSlice(this._chunkSize));
            return this.AWAITING_CHUNK;
        }
        else {
            // Current packet is smaller than the chunk we're reading, split the current chunk itself up
            this._chunkSize -= buf.remaining();
            this._currentMessage.push(buf.readSlice(buf.remaining()));
            return this.IN_CHUNK;
        }
    };
    Dechunker.prototype.CLOSED = function (buf) {
        // no-op
    };
    /** Called when a complete chunk header has been received */
    Dechunker.prototype._onHeader = function (header) {
        if (header === 0) {
            // Message boundary
            var message = void 0;
            switch (this._currentMessage.length) {
                case 0:
                    // Keep alive chunk, sent by server to keep network alive.
                    return this.AWAITING_CHUNK;
                case 1:
                    // All data in one chunk, this signals the end of that chunk.
                    message = this._currentMessage[0];
                    break;
                default:
                    // A large chunk of data received, this signals that the last chunk has been received.
                    message = new combined_buf_1.default(this._currentMessage);
                    break;
            }
            this._currentMessage = [];
            this.onmessage(message);
            return this.AWAITING_CHUNK;
        }
        else {
            this._chunkSize = header;
            return this.IN_CHUNK;
        }
    };
    Dechunker.prototype.write = function (buf) {
        while (buf.hasRemaining()) {
            this._state = this._state(buf);
        }
    };
    return Dechunker;
}());
exports.Dechunker = Dechunker;
