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
exports.alloc = alloc;
var buffer_1 = __importDefault(require("buffer"));
var buf_1 = __importDefault(require("../buf"));
var ChannelBuffer = /** @class */ (function (_super) {
    __extends(ChannelBuffer, _super);
    function ChannelBuffer(arg) {
        var _this = this;
        var buffer = newChannelJSBuffer(arg);
        _this = _super.call(this, buffer.length) || this;
        _this._buffer = buffer;
        return _this;
    }
    ChannelBuffer.prototype.getUInt8 = function (position) {
        return this._buffer.readUInt8(position);
    };
    ChannelBuffer.prototype.getInt8 = function (position) {
        return this._buffer.readInt8(position);
    };
    ChannelBuffer.prototype.getFloat64 = function (position) {
        return this._buffer.readDoubleBE(position);
    };
    ChannelBuffer.prototype.getVarInt = function (position) {
        var i = 0;
        var currentValue = this._buffer.readInt8(position + i);
        var total = currentValue % 128;
        while (currentValue / 128 >= 1) {
            i += 1;
            currentValue = this._buffer.readInt8(position + i);
            total += currentValue % 128;
        }
        return { length: i + 1, value: total };
    };
    ChannelBuffer.prototype.putUInt8 = function (position, val) {
        this._buffer.writeUInt8(val, position);
    };
    ChannelBuffer.prototype.putInt8 = function (position, val) {
        this._buffer.writeInt8(val, position);
    };
    ChannelBuffer.prototype.putFloat64 = function (position, val) {
        this._buffer.writeDoubleBE(val, position);
    };
    ChannelBuffer.prototype.putBytes = function (position, val) {
        if (val instanceof ChannelBuffer) {
            var bytesToCopy = Math.min(val.length - val.position, this.length - position);
            val._buffer.copy(this._buffer, position, val.position, val.position + bytesToCopy);
            val.position += bytesToCopy;
        }
        else {
            _super.prototype.putBytes.call(this, position, val);
        }
    };
    ChannelBuffer.prototype.getSlice = function (start, length) {
        return new ChannelBuffer(this._buffer.slice(start, start + length));
    };
    return ChannelBuffer;
}(buf_1.default));
exports.default = ChannelBuffer;
/**
 * Allocate a buffer
 *
 * @param {number} size The buffer sizzer
 * @returns {BaseBuffer} The buffer
 */
function alloc(size) {
    return new ChannelBuffer(size);
}
function newChannelJSBuffer(arg) {
    if (arg instanceof buffer_1.default.Buffer) {
        return arg;
    }
    else if (typeof arg === 'number' &&
        typeof buffer_1.default.Buffer.alloc === 'function') {
        // use static factory function present in newer NodeJS versions to allocate new buffer with specified size
        return buffer_1.default.Buffer.alloc(arg);
    }
    else {
        // fallback to the old, potentially deprecated constructor
        // eslint-disable-next-line n/no-deprecated-api
        return new buffer_1.default.Buffer(arg);
    }
}
