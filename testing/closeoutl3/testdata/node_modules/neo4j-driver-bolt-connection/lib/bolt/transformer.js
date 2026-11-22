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
exports.TypeTransformer = void 0;
var packstream_1 = require("../packstream");
var neo4j_driver_core_1 = require("neo4j-driver-core");
var objectUtil = neo4j_driver_core_1.internal.objectUtil;
/**
 * Class responsible for applying the expected {@link TypeTransformer} to
 * transform the driver types from and to {@link struct.Structure}
 */
var Transformer = /** @class */ (function () {
    /**
     * Constructor
     * @param {TypeTransformer[]} transformers The type transformers
     */
    function Transformer(transformers) {
        this._transformers = transformers;
        this._transformersPerSignature = new Map(transformers.map(function (typeTransformer) { return [typeTransformer.signature, typeTransformer]; }));
        this.fromStructure = this.fromStructure.bind(this);
        this.toStructure = this.toStructure.bind(this);
        Object.freeze(this);
    }
    /**
     * Transform from structure to specific object
     *
     * @param {struct.Structure} struct The structure
     * @returns {<T>|structure.Structure} The driver object or the structure if the transformer was not found.
     */
    Transformer.prototype.fromStructure = function (struct) {
        try {
            if (struct instanceof packstream_1.structure.Structure && this._transformersPerSignature.has(struct.signature)) {
                var fromStructure = this._transformersPerSignature.get(struct.signature).fromStructure;
                return fromStructure(struct);
            }
            return struct;
        }
        catch (error) {
            return objectUtil.createBrokenObject(error);
        }
    };
    /**
     * Transform from object to structure
     * @param {<T>} type The object to be transoformed in structure
     * @returns {<T>|structure.Structure} The structure or the object, if any transformer was found
     */
    Transformer.prototype.toStructure = function (type) {
        var valueType = typeof type;
        // captures 'string' | 'boolean' | 'bigint' | 'number' | 'undefined' | null
        if ((valueType !== 'object' && valueType !== 'function' && valueType !== 'symbol') || type === null) {
            return type;
        }
        var transformer = this._transformers.find(function (_a) {
            var isTypeInstance = _a.isTypeInstance;
            return isTypeInstance(type);
        });
        if (transformer !== undefined) {
            return transformer.toStructure(type);
        }
        return type;
    };
    return Transformer;
}());
exports.default = Transformer;
/**
 * @callback isTypeInstanceFunction
 * @param {any} object The object
 * @return {boolean} is instance of
 */
/**
 * @callback toStructureFunction
 * @param {any} object The object
 * @return {structure.Structure} The structure
 */
/**
 * @callback fromStructureFunction
 * @param {structure.Structure} struct The structure
 * @return {any} The object
 */
/**
 * Class responsible for grouping the properties of a TypeTransformer
 */
var TypeTransformer = /** @class */ (function () {
    /**
     * @param {Object} param
     * @param {number} param.signature The signature of the structure
     * @param {isTypeInstanceFunction} param.isTypeInstance The function which checks if object is
     *                instance of the type described by the TypeTransformer
     * @param {toStructureFunction} param.toStructure The function which gets the object and converts to structure
     * @param {fromStructureFunction} param.fromStructure The function which get the structure and covnverts to object
     */
    function TypeTransformer(_a) {
        var signature = _a.signature, fromStructure = _a.fromStructure, toStructure = _a.toStructure, isTypeInstance = _a.isTypeInstance;
        this.signature = signature;
        this.isTypeInstance = isTypeInstance;
        this.fromStructure = fromStructure;
        this.toStructure = toStructure;
        Object.freeze(this);
    }
    /**
     * @param {Object} param
     * @param {number} [param.signature] The signature of the structure
     * @param {isTypeInstanceFunction} [param.isTypeInstance] The function which checks if object is
     *                instance of the type described by the TypeTransformer
     * @param {toStructureFunction} [param.toStructure] The function which gets the object and converts to structure
     * @param {fromStructureFunction} [param.fromStructure] The function which get the structure and covnverts to object
     * @returns {TypeTransformer} A new type transform extends with new methods
     */
    TypeTransformer.prototype.extendsWith = function (_a) {
        var signature = _a.signature, fromStructure = _a.fromStructure, toStructure = _a.toStructure, isTypeInstance = _a.isTypeInstance;
        return new TypeTransformer({
            signature: signature || this.signature,
            fromStructure: fromStructure || this.fromStructure,
            toStructure: toStructure || this.toStructure,
            isTypeInstance: isTypeInstance || this.isTypeInstance
        });
    };
    return TypeTransformer;
}());
exports.TypeTransformer = TypeTransformer;
