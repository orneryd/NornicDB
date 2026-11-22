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
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * Common base with default implementation for most buffer methods.
 * Buffers are stateful - they track a current "position", this helps greatly
 * when reading and writing from them incrementally. You can also ignore the
 * stateful read/write methods.
 * readXXX and writeXXX-methods move the inner position of the buffer.
 * putXXX and getXXX-methods do not.
 * @access private
 */
var BaseBuffer = /** @class */ (function () {
    /**
     * Create a instance with the injected size.
     * @constructor
     * @param {Integer} size
     */
    function BaseBuffer(size) {
        this.position = 0;
        this.length = size;
    }
    BaseBuffer.prototype.getUInt8 = function (position) {
        throw new Error('Not implemented');
    };
    BaseBuffer.prototype.getInt8 = function (position) {
        throw new Error('Not implemented');
    };
    BaseBuffer.prototype.getFloat64 = function (position) {
        throw new Error('Not implemented');
    };
    BaseBuffer.prototype.getVarInt = function (position) {
        throw new Error('Not implemented');
    };
    BaseBuffer.prototype.putUInt8 = function (position, val) {
        throw new Error('Not implemented');
    };
    BaseBuffer.prototype.putInt8 = function (position, val) {
        throw new Error('Not implemented');
    };
    BaseBuffer.prototype.putFloat64 = function (position, val) {
        throw new Error('Not implemented');
    };
    /**
     * @param p
     */
    BaseBuffer.prototype.getInt16 = function (p) {
        return (this.getInt8(p) << 8) | this.getUInt8(p + 1);
    };
    /**
     * @param p
     */
    BaseBuffer.prototype.getUInt16 = function (p) {
        return (this.getUInt8(p) << 8) | this.getUInt8(p + 1);
    };
    /**
     * @param p
     */
    BaseBuffer.prototype.getInt32 = function (p) {
        return ((this.getInt8(p) << 24) |
            (this.getUInt8(p + 1) << 16) |
            (this.getUInt8(p + 2) << 8) |
            this.getUInt8(p + 3));
    };
    /**
     * @param p
     */
    BaseBuffer.prototype.getUInt32 = function (p) {
        return ((this.getUInt8(p) << 24) |
            (this.getUInt8(p + 1) << 16) |
            (this.getUInt8(p + 2) << 8) |
            this.getUInt8(p + 3));
    };
    /**
     * @param p
     */
    BaseBuffer.prototype.getInt64 = function (p) {
        return ((this.getInt8(p) << 56) |
            (this.getUInt8(p + 1) << 48) |
            (this.getUInt8(p + 2) << 40) |
            (this.getUInt8(p + 3) << 32) |
            (this.getUInt8(p + 4) << 24) |
            (this.getUInt8(p + 5) << 16) |
            (this.getUInt8(p + 6) << 8) |
            this.getUInt8(p + 7));
    };
    /**
     * Get a slice of this buffer. This method does not copy any data,
     * but simply provides a slice view of this buffer
     * @param start
     * @param length
     */
    BaseBuffer.prototype.getSlice = function (start, length) {
        return new SliceBuffer(start, length, this);
    };
    /**
     * @param p
     * @param val
     */
    BaseBuffer.prototype.putInt16 = function (p, val) {
        this.putInt8(p, val >> 8);
        this.putUInt8(p + 1, val & 0xff);
    };
    /**
     * @param p
     * @param val
     */
    BaseBuffer.prototype.putUInt16 = function (p, val) {
        this.putUInt8(p, (val >> 8) & 0xff);
        this.putUInt8(p + 1, val & 0xff);
    };
    /**
     * @param p
     * @param val
     */
    BaseBuffer.prototype.putInt32 = function (p, val) {
        this.putInt8(p, val >> 24);
        this.putUInt8(p + 1, (val >> 16) & 0xff);
        this.putUInt8(p + 2, (val >> 8) & 0xff);
        this.putUInt8(p + 3, val & 0xff);
    };
    /**
     * @param p
     * @param val
     */
    BaseBuffer.prototype.putUInt32 = function (p, val) {
        this.putUInt8(p, (val >> 24) & 0xff);
        this.putUInt8(p + 1, (val >> 16) & 0xff);
        this.putUInt8(p + 2, (val >> 8) & 0xff);
        this.putUInt8(p + 3, val & 0xff);
    };
    /**
     * @param p
     * @param val
     */
    BaseBuffer.prototype.putInt64 = function (p, val) {
        this.putInt8(p, val >> 48);
        this.putUInt8(p + 1, (val >> 42) & 0xff);
        this.putUInt8(p + 2, (val >> 36) & 0xff);
        this.putUInt8(p + 3, (val >> 30) & 0xff);
        this.putUInt8(p + 4, (val >> 24) & 0xff);
        this.putUInt8(p + 5, (val >> 16) & 0xff);
        this.putUInt8(p + 6, (val >> 8) & 0xff);
        this.putUInt8(p + 7, val & 0xff);
    };
    BaseBuffer.prototype.putVarInt = function (p, val) {
        var length = 0;
        while (val > 1) {
            var int = val % 128;
            if (val >= 128) {
                int += 128;
            }
            val = val / 128;
            this.putUInt8(p + length, int);
            length += 1;
        }
        return length;
    };
    /**
     * @param position
     * @param other
     */
    BaseBuffer.prototype.putBytes = function (position, other) {
        for (var i = 0, end = other.remaining(); i < end; i++) {
            this.putUInt8(position + i, other.readUInt8());
        }
    };
    /**
     * Read from state position.
     */
    BaseBuffer.prototype.readUInt8 = function () {
        return this.getUInt8(this._updatePos(1));
    };
    /**
     * Read from state position.
     */
    BaseBuffer.prototype.readInt8 = function () {
        return this.getInt8(this._updatePos(1));
    };
    /**
     * Read from state position.
     */
    BaseBuffer.prototype.readUInt16 = function () {
        return this.getUInt16(this._updatePos(2));
    };
    /**
     * Read from state position.
     */
    BaseBuffer.prototype.readUInt32 = function () {
        return this.getUInt32(this._updatePos(4));
    };
    /**
     * Read from state position.
     */
    BaseBuffer.prototype.readInt16 = function () {
        return this.getInt16(this._updatePos(2));
    };
    /**
     * Read from state position.
     */
    BaseBuffer.prototype.readInt32 = function () {
        return this.getInt32(this._updatePos(4));
    };
    /**
     * Read from state position.
     */
    BaseBuffer.prototype.readInt64 = function () {
        return this.getInt32(this._updatePos(8));
    };
    /**
     * Read from state position.
     */
    BaseBuffer.prototype.readFloat64 = function () {
        return this.getFloat64(this._updatePos(8));
    };
    /**
     * Read from state position
     */
    BaseBuffer.prototype.readVarInt = function () {
        var int = this.getVarInt(this.position);
        this._updatePos(int.length);
        return int.value;
    };
    /**
     * Write to state position.
     * @param val
     */
    BaseBuffer.prototype.writeUInt8 = function (val) {
        this.putUInt8(this._updatePos(1), val);
    };
    /**
     * Write to state position.
     * @param val
     */
    BaseBuffer.prototype.writeInt8 = function (val) {
        this.putInt8(this._updatePos(1), val);
    };
    /**
     * Write to state position.
     * @param val
     */
    BaseBuffer.prototype.writeInt16 = function (val) {
        this.putInt16(this._updatePos(2), val);
    };
    /**
     * Write to state position.
     * @param val
     */
    BaseBuffer.prototype.writeInt32 = function (val) {
        this.putInt32(this._updatePos(4), val);
    };
    /**
     * Write to state position.
     * @param val
     */
    BaseBuffer.prototype.writeUInt32 = function (val) {
        this.putUInt32(this._updatePos(4), val);
    };
    /**
     * Write to state position.
     * @param val
     */
    BaseBuffer.prototype.writeInt64 = function (val) {
        this.putInt64(this._updatePos(8), val);
    };
    /**
     * Write to state position.
     * @param val
     */
    BaseBuffer.prototype.writeFloat64 = function (val) {
        this.putFloat64(this._updatePos(8), val);
    };
    BaseBuffer.prototype.writeVarInt = function (val) {
        var length = this.putVarInt(this.position, val);
        this._updatePos(length);
    };
    /**
     * Write to state position.
     * @param val
     */
    BaseBuffer.prototype.writeBytes = function (val) {
        this.putBytes(this._updatePos(val.remaining()), val);
    };
    /**
     * Get a slice of this buffer. This method does not copy any data,
     * but simply provides a slice view of this buffer
     * @param length
     */
    BaseBuffer.prototype.readSlice = function (length) {
        return this.getSlice(this._updatePos(length), length);
    };
    BaseBuffer.prototype._updatePos = function (length) {
        var p = this.position;
        this.position += length;
        return p;
    };
    /**
     * Get remaining
     */
    BaseBuffer.prototype.remaining = function () {
        return this.length - this.position;
    };
    /**
     * Has remaining
     */
    BaseBuffer.prototype.hasRemaining = function () {
        return this.remaining() > 0;
    };
    /**
     * Reset position state
     */
    BaseBuffer.prototype.reset = function () {
        this.position = 0;
    };
    /**
     * Get string representation of buffer and it's state.
     * @return {string} Buffer as a string
     */
    BaseBuffer.prototype.toString = function () {
        return (this.constructor.name +
            '( position=' +
            this.position +
            ' )\n  ' +
            this.toHex());
    };
    /**
     * Get string representation of buffer.
     * @return {string} Buffer as a string
     */
    BaseBuffer.prototype.toHex = function () {
        var out = '';
        for (var i = 0; i < this.length; i++) {
            var hexByte = this.getUInt8(i).toString(16);
            if (hexByte.length === 1) {
                hexByte = '0' + hexByte;
            }
            out += hexByte;
            if (i !== this.length - 1) {
                out += ' ';
            }
        }
        return out;
    };
    return BaseBuffer;
}());
exports.default = BaseBuffer;
/**
 * Represents a view as slice of another buffer.
 * @access private
 */
var SliceBuffer = /** @class */ (function (_super) {
    __extends(SliceBuffer, _super);
    function SliceBuffer(start, length, inner) {
        var _this = _super.call(this, length) || this;
        _this._start = start;
        _this._inner = inner;
        return _this;
    }
    SliceBuffer.prototype.putUInt8 = function (position, val) {
        this._inner.putUInt8(this._start + position, val);
    };
    SliceBuffer.prototype.getUInt8 = function (position) {
        return this._inner.getUInt8(this._start + position);
    };
    SliceBuffer.prototype.putInt8 = function (position, val) {
        this._inner.putInt8(this._start + position, val);
    };
    SliceBuffer.prototype.putFloat64 = function (position, val) {
        this._inner.putFloat64(this._start + position, val);
    };
    SliceBuffer.prototype.getInt8 = function (position) {
        return this._inner.getInt8(this._start + position);
    };
    SliceBuffer.prototype.getFloat64 = function (position) {
        return this._inner.getFloat64(this._start + position);
    };
    return SliceBuffer;
}(BaseBuffer));
