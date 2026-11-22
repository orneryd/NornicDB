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
var buf_1 = require("../buf");
var channel_buf_1 = require("./channel-buf");
/**
 * Buffer that combines multiple buffers, exposing them as one single buffer.
 */
var CombinedBuffer = /** @class */ (function (_super) {
    __extends(CombinedBuffer, _super);
    function CombinedBuffer(buffers) {
        var _this = this;
        var length = 0;
        for (var i = 0; i < buffers.length; i++) {
            length += buffers[i].length;
        }
        _this = _super.call(this, length) || this;
        _this._buffers = buffers;
        return _this;
    }
    CombinedBuffer.prototype.getUInt8 = function (position) {
        // Surely there's a faster way to do this.. some sort of lookup table thing?
        for (var i = 0; i < this._buffers.length; i++) {
            var buffer = this._buffers[i];
            // If the position is not in the current buffer, skip the current buffer
            if (position >= buffer.length) {
                position -= buffer.length;
            }
            else {
                return buffer.getUInt8(position);
            }
        }
    };
    CombinedBuffer.prototype.getInt8 = function (position) {
        // Surely there's a faster way to do this.. some sort of lookup table thing?
        for (var i = 0; i < this._buffers.length; i++) {
            var buffer = this._buffers[i];
            // If the position is not in the current buffer, skip the current buffer
            if (position >= buffer.length) {
                position -= buffer.length;
            }
            else {
                return buffer.getInt8(position);
            }
        }
    };
    CombinedBuffer.prototype.getFloat64 = function (position) {
        // At some point, a more efficient impl. For now, we copy the 8 bytes
        // we want to read and depend on the platform impl of IEEE 754.
        var b = (0, channel_buf_1.alloc)(8);
        for (var i = 0; i < 8; i++) {
            b.putUInt8(i, this.getUInt8(position + i));
        }
        return b.getFloat64(0);
    };
    return CombinedBuffer;
}(buf_1.BaseBuffer));
exports.default = CombinedBuffer;
