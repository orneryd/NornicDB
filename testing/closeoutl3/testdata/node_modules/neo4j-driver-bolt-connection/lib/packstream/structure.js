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
exports.Structure = void 0;
exports.verifyStructSize = verifyStructSize;
var neo4j_driver_core_1 = require("neo4j-driver-core");
var PROTOCOL_ERROR = neo4j_driver_core_1.error.PROTOCOL_ERROR;
/**
 * A Structure have a signature and fields.
 */
var Structure = /** @class */ (function () {
    /**
     * Create new instance
     */
    function Structure(signature, fields) {
        this.signature = signature;
        this.fields = fields;
    }
    Object.defineProperty(Structure.prototype, "size", {
        get: function () {
            return this.fields.length;
        },
        enumerable: false,
        configurable: true
    });
    Structure.prototype.toString = function () {
        var fieldStr = '';
        for (var i = 0; i < this.fields.length; i++) {
            if (i > 0) {
                fieldStr += ', ';
            }
            fieldStr += this.fields[i];
        }
        return 'Structure(' + this.signature + ', [' + fieldStr + '])';
    };
    return Structure;
}());
exports.Structure = Structure;
function verifyStructSize(structName, expectedSize, actualSize) {
    if (expectedSize !== actualSize) {
        throw (0, neo4j_driver_core_1.newError)("Wrong struct size for ".concat(structName, ", expected ").concat(expectedSize, " but was ").concat(actualSize), PROTOCOL_ERROR);
    }
}
exports.default = Structure;
