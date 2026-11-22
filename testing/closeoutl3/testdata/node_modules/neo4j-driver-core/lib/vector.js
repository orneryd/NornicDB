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
Object.defineProperty(exports, "__esModule", { value: true });
exports.vector = vector;
exports.isVector = isVector;
var error_1 = require("./error");
var VECTOR_IDENTIFIER_PROPERTY = '__isVector__';
/**
 * @typedef {'INT8' | 'INT16' | 'INT32' | 'INT64' | 'FLOAT32' | 'FLOAT64'} VectorType
 */
var vectorTypes = {
    INT8: 'INT8',
    INT16: 'INT16',
    INT32: 'INT32',
    INT64: 'INT64',
    FLOAT32: 'FLOAT32',
    FLOAT64: 'FLOAT64'
};
Object.freeze(vectorTypes);
/**
 * A wrapper class for JavaScript TypedArrays that makes the driver send them as a Vector type to the database.
 * @access public
 * @exports Vector
 * @class A Vector class that wraps a JavaScript TypedArray to enable writing/reading the Neo4j Vector type.
 * @param {Float32Array | Float64Array | Int8Array | Int16Array | Int32Array | BigInt64Array} typedArray The TypedArray to convert to a vector
 *
 * @constructor
 *
 */
var Vector = /** @class */ (function () {
    function Vector(typedArray) {
        var _a, _b;
        if (typedArray instanceof Int8Array) {
            this._type = vectorTypes.INT8;
        }
        else if (typedArray instanceof Int16Array) {
            this._type = vectorTypes.INT16;
        }
        else if (typedArray instanceof Int32Array) {
            this._type = vectorTypes.INT32;
        }
        else if (typedArray instanceof BigInt64Array) {
            this._type = vectorTypes.INT64;
        }
        else if (typedArray instanceof Float32Array) {
            this._type = vectorTypes.FLOAT32;
        }
        else if (typedArray instanceof Float64Array) {
            this._type = vectorTypes.FLOAT64;
        }
        else {
            throw (0, error_1.newError)("Invalid argument type passed to Vector constructor: should be signed integer or float TypedArray, got: ".concat((_b = (_a = typedArray === null || typedArray === void 0 ? void 0 : typedArray.constructor) === null || _a === void 0 ? void 0 : _a.name) !== null && _b !== void 0 ? _b : 'undefined or type without constructor name'));
        }
        this._typedArray = typedArray;
    }
    /**
     * Converts the Vector back to a typedArray
     * @returns {Float32Array | Float64Array | Int8Array | Int16Array | Int32Array | BigInt64Array} - a TypedArray of the Vectors type.
     */
    Vector.prototype.asTypedArray = function () {
        return this._typedArray;
    };
    /**
     * Gets the type of the Vector
     * @returns {VectorType} - The type of the vector, corresponding to the type of the wrapped TypedArray.
     */
    Vector.prototype.getType = function () {
        return this._type;
    };
    Vector.prototype.toString = function () {
        return "vector([".concat(this._typedArray.join(', '), "], ").concat(this._typedArray.length, ", ").concat(getTypeString(this._type), ")");
    };
    return Vector;
}());
exports.default = Vector;
function getTypeString(type) {
    switch (type) {
        case 'INT8':
            return 'INTEGER8 NOT NULL';
        case 'INT16':
            return 'INTEGER16 NOT NULL';
        case 'INT32':
            return 'INTEGER32 NOT NULL';
        case 'INT64':
            return 'INTEGER NOT NULL';
        case 'FLOAT32':
            return 'FLOAT32 NOT NULL';
        case 'FLOAT64':
            return 'FLOAT NOT NULL';
        default:
            throw (0, error_1.newError)("Cannot stringify vector with unsupported type. Got type: ".concat(type));
    }
}
Object.defineProperty(Vector.prototype, VECTOR_IDENTIFIER_PROPERTY, {
    value: true,
    enumerable: false,
    configurable: false,
    writable: false
});
/**
 * Cast a TypedArray to a {@link Vector}
 * @access public
 * @param {Float32Array | Float64Array | Int8Array | Int16Array | Int32Array | BigInt64Array} typedArray - The value to use.
 * @return {Vector} - The Neo4j Vector ready to be used as a query parameter
 */
function vector(typedArray) {
    var _a, _b;
    try {
        return new Vector(typedArray);
    }
    catch (_c) {
        throw (0, error_1.newError)("Invalid argument type passed to vector constructor function: should be signed integer or float TypedArray, got: ".concat((_b = (_a = typedArray === null || typedArray === void 0 ? void 0 : typedArray.constructor) === null || _a === void 0 ? void 0 : _a.name) !== null && _b !== void 0 ? _b : 'undefined or type without constructor name'));
    }
}
/**
 * Test if given object is an instance of the {@link Vector} class.
 * @param {Object} obj the object to test.
 * @return {boolean} `true` if given object is a {@link Vector}, `false` otherwise.
 */
function isVector(obj) {
    return obj != null && obj[VECTOR_IDENTIFIER_PROPERTY] === true;
}
