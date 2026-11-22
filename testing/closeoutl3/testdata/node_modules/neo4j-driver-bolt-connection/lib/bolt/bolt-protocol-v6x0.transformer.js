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
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var bolt_protocol_v5x8_transformer_1 = __importDefault(require("./bolt-protocol-v5x8.transformer"));
var transformer_1 = require("./transformer");
var packstream_1 = require("../packstream");
var neo4j_driver_core_1 = require("neo4j-driver-core");
var VECTOR = 0x56;
var FLOAT_32 = 0xc6;
var FLOAT_64 = 0xc1;
var INT_8 = 0xc8;
var INT_16 = 0xc9;
var INT_32 = 0xca;
var INT_64 = 0xcb;
var UNSUPPORTED = 0x3F;
var typeToTypeMarker = {
    INT8: INT_8,
    INT16: INT_16,
    INT32: INT_32,
    INT64: INT_64,
    FLOAT32: FLOAT_32,
    FLOAT64: FLOAT_64
};
function createVectorTransformer() {
    return new transformer_1.TypeTransformer({
        signature: VECTOR,
        isTypeInstance: function (object) { return (0, neo4j_driver_core_1.isVector)(object); },
        toStructure: function (vector) {
            var typeMarker = typeToTypeMarker[vector.getType()];
            if (typeMarker === undefined) {
                throw (0, neo4j_driver_core_1.newError)("Vector object has unknown type: ".concat(vector.getType()));
            }
            var buffer = fixBufferEndianness(typeMarker, vector.asTypedArray().buffer);
            var struct = new packstream_1.structure.Structure(VECTOR, [Int8Array.from([typeMarker]), new Int8Array(buffer)]);
            return struct;
        },
        fromStructure: function (structure) {
            var typeMarker = Uint8Array.from(structure.fields[0])[0];
            var byteArray = structure.fields[1];
            var buffer = fixBufferEndianness(typeMarker, byteArray.buffer);
            switch (typeMarker) {
                case INT_8:
                    return new neo4j_driver_core_1.Vector(new Int8Array(buffer));
                case INT_16:
                    return new neo4j_driver_core_1.Vector(new Int16Array(buffer));
                case INT_32:
                    return new neo4j_driver_core_1.Vector(new Int32Array(buffer));
                case INT_64:
                    return new neo4j_driver_core_1.Vector(new BigInt64Array(buffer));
                case FLOAT_32:
                    return new neo4j_driver_core_1.Vector(new Float32Array(buffer));
                case FLOAT_64:
                    return new neo4j_driver_core_1.Vector(new Float64Array(buffer));
                default:
                    throw (0, neo4j_driver_core_1.newError)("Received Vector structure with unsupported type marker: ".concat(typeMarker));
            }
        }
    });
}
function fixBufferEndianness(typeMarker, buffer) {
    var isLittleEndian = checkLittleEndian();
    if (isLittleEndian) {
        var setview = new DataView(new ArrayBuffer(buffer.byteLength));
        // we want exact byte accuracy, so we cannot simply get the value from the typed array
        var getview = new DataView(buffer);
        var set = void 0;
        var get = void 0;
        var elementSize = void 0;
        switch (typeMarker) {
            case INT_8:
                elementSize = 1;
                set = setview.setInt8.bind(setview);
                get = getview.getInt8.bind(getview);
                break;
            case INT_16:
                elementSize = 2;
                set = setview.setInt16.bind(setview);
                get = getview.getInt16.bind(getview);
                break;
            case INT_32:
                elementSize = 4;
                set = setview.setInt32.bind(setview);
                get = getview.getInt32.bind(getview);
                break;
            case INT_64:
                elementSize = 8;
                set = setview.setBigInt64.bind(setview);
                get = getview.getBigInt64.bind(getview);
                break;
            case FLOAT_32:
                elementSize = 4;
                set = setview.setInt32.bind(setview);
                get = getview.getInt32.bind(getview);
                break;
            case FLOAT_64:
                elementSize = 8;
                set = setview.setBigInt64.bind(setview);
                get = getview.getBigInt64.bind(getview);
                break;
            default:
                throw (0, neo4j_driver_core_1.newError)("Vector is of unsupported type ".concat(typeMarker));
        }
        for (var i = 0; i < buffer.byteLength; i += elementSize) {
            set(i, get(i, isLittleEndian));
        }
        return setview.buffer;
    }
    else {
        return buffer;
    }
}
function checkLittleEndian() {
    var dataview = new DataView(new ArrayBuffer(2));
    dataview.setInt16(0, 1000, true);
    var typeArray = new Int16Array(dataview.buffer);
    return typeArray[0] === 1000;
}
function createUnsupportedTypeTransformer() {
    return new transformer_1.TypeTransformer({
        signature: UNSUPPORTED,
        isTypeInstance: function (object) { return (0, neo4j_driver_core_1.isUnsupportedType)(object); },
        toStructure: function (_) {
            throw (0, neo4j_driver_core_1.newError)('UnsupportedType object can not be transmitted');
        },
        fromStructure: function (structure) {
            return new neo4j_driver_core_1.UnsupportedType(structure.fields[0], structure.fields[1], structure.fields[2], structure.fields[3].message);
        }
    });
}
exports.default = __assign(__assign({}, bolt_protocol_v5x8_transformer_1.default), { createVectorTransformer: createVectorTransformer, createUnsupportedTypeTransformer: createUnsupportedTypeTransformer });
