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
exports.stringify = stringify;
var object_util_1 = require("./internal/object-util");
/**
 * Custom version on JSON.stringify that can handle values that normally don't support serialization, such as BigInt.
 * @private
 * @param val A JavaScript value, usually an object or array, to be converted.
 * @returns A JSON string representing the given value.
 */
function stringify(val, opts) {
    return JSON.stringify(val, function (_, value) {
        if ((0, object_util_1.isBrokenObject)(value)) {
            return {
                __isBrokenObject__: true,
                __reason__: (0, object_util_1.getBrokenObjectReason)(value)
            };
        }
        if (typeof value === 'bigint') {
            return "".concat(value, "n");
        }
        if ((opts === null || opts === void 0 ? void 0 : opts.sortedElements) === true &&
            typeof value === 'object' &&
            !Array.isArray(value)) {
            return Object.keys(value).sort().reduce(function (obj, key) {
                // @ts-expect-error: no way to avoid implicit 'any'
                obj[key] = value[key];
                return obj;
            }, {});
        }
        if ((opts === null || opts === void 0 ? void 0 : opts.useCustomToString) === true &&
            typeof value === 'object' &&
            !Array.isArray(value) &&
            typeof value.toString === 'function' &&
            value.toString !== Object.prototype.toString) {
            return value === null || value === void 0 ? void 0 : value.toString();
        }
        return value;
    });
}
