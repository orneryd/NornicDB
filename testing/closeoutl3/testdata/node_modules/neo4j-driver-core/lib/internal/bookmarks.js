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
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __read = (this && this.__read) || function (o, n) {
    var m = typeof Symbol === "function" && o[Symbol.iterator];
    if (!m) return o;
    var i = m.call(o), r, ar = [], e;
    try {
        while ((n === void 0 || n-- > 0) && !(r = i.next()).done) ar.push(r.value);
    }
    catch (error) { e = { error: error }; }
    finally {
        try {
            if (r && !r.done && (m = i["return"])) m.call(i);
        }
        finally { if (e) throw e.error; }
    }
    return ar;
};
var __spreadArray = (this && this.__spreadArray) || function (to, from, pack) {
    if (pack || arguments.length === 2) for (var i = 0, l = from.length, ar; i < l; i++) {
        if (ar || !(i in from)) {
            if (!ar) ar = Array.prototype.slice.call(from, 0, i);
            ar[i] = from[i];
        }
    }
    return to.concat(ar || Array.prototype.slice.call(from));
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Bookmarks = void 0;
var util = __importStar(require("./util"));
var BOOKMARKS_KEY = 'bookmarks';
var Bookmarks = /** @class */ (function () {
    /**
     * @constructor
     * @param {string|string[]} values single bookmark as string or multiple bookmarks as a string array.
     */
    function Bookmarks(values) {
        this._values = asStringArray(values);
    }
    Bookmarks.empty = function () {
        return EMPTY_BOOKMARK;
    };
    /**
     * Check if the given Bookmarks holder is meaningful and can be send to the database.
     * @return {boolean} returns `true` bookmarks has a value, `false` otherwise.
     */
    Bookmarks.prototype.isEmpty = function () {
        return this._values.length === 0;
    };
    /**
     * Get all bookmarks values as an array.
     * @return {string[]} all values.
     */
    Bookmarks.prototype.values = function () {
        return this._values;
    };
    Bookmarks.prototype[Symbol.iterator] = function () {
        return this._values[Symbol.iterator]();
    };
    /**
     * Get these bookmarks as an object for begin transaction call.
     * @return {Object} the value of this bookmarks holder as object.
     */
    Bookmarks.prototype.asBeginTransactionParameters = function () {
        var _a;
        if (this.isEmpty()) {
            return {};
        }
        // Driver sends {bookmarks: "max", bookmarks: ["one", "two", "max"]} instead of simple
        // {bookmarks: ["one", "two", "max"]} for backwards compatibility reasons. Old servers can only accept single
        // bookmarks that is why driver has to parse and compare given list of bookmarks. This functionality will
        // eventually be removed.
        return _a = {},
            _a[BOOKMARKS_KEY] = this._values,
            _a;
    };
    return Bookmarks;
}());
exports.Bookmarks = Bookmarks;
var EMPTY_BOOKMARK = new Bookmarks(null);
/**
 * Converts given value to an array.
 * @param {string|string[]|Array} [value=undefined] argument to convert.
 * @return {string[]} value converted to an array.
 */
function asStringArray(value) {
    if (value == null || value === '') {
        return [];
    }
    if (util.isString(value)) {
        return [value];
    }
    if (Array.isArray(value)) {
        var result = new Set();
        var flattenedValue = flattenArray(value);
        for (var i = 0; i < flattenedValue.length; i++) {
            var element = flattenedValue[i];
            // if it is undefined or null, ignore it
            if (element !== undefined && element !== null) {
                if (!util.isString(element)) {
                    throw new TypeError(
                    // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
                    "Bookmark value should be a string, given: '".concat(element, "'"));
                }
                result.add(element);
            }
        }
        return __spreadArray([], __read(result), false);
    }
    throw new TypeError(
    // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
    "Bookmarks should either be a string or a string array, given: '".concat(value, "'"));
}
/**
 * Recursively flattens an array so that the result becomes a single array
 * of values, which does not include any sub-arrays
 *
 * @param {Array} value
 */
function flattenArray(values) {
    return values.reduce(function (dest, value) {
        return Array.isArray(value)
            ? dest.concat(flattenArray(value))
            : dest.concat(value);
    }, []);
}
