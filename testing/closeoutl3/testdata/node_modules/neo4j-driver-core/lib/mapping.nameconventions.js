"use strict";
/**
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
var __values = (this && this.__values) || function(o) {
    var s = typeof Symbol === "function" && Symbol.iterator, m = s && o[s], i = 0;
    if (m) return m.call(o);
    if (o && typeof o.length === "number") return {
        next: function () {
            if (o && i >= o.length) o = void 0;
            return { value: o && o[i++], done: !o };
        }
    };
    throw new TypeError(s ? "Object is not iterable." : "Symbol.iterator is not defined.");
};
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.nameConventions = exports.StandardCase = void 0;
var StandardCase;
(function (StandardCase) {
    StandardCase["SnakeCase"] = "snake_case";
    StandardCase["KebabCase"] = "kebab-case";
    StandardCase["ScreamingSnakeCase"] = "SCREAMING_SNAKE_CASE";
    StandardCase["PascalCase"] = "PascalCase";
    StandardCase["CamelCase"] = "camelCase";
})(StandardCase || (exports.StandardCase = StandardCase = {}));
exports.nameConventions = {
    snake_case: {
        tokenize: function (name) { return name.split('_'); },
        encode: function (tokens) { return tokens.join('_'); }
    },
    'kebab-case': {
        tokenize: function (name) { return name.split('-'); },
        encode: function (tokens) { return tokens.join('-'); }
    },
    PascalCase: {
        tokenize: function (name) { return name.split(/(?=[A-Z])/).map(function (token) { return token.toLowerCase(); }); },
        encode: function (tokens) {
            var e_1, _a;
            var name = '';
            try {
                for (var tokens_1 = __values(tokens), tokens_1_1 = tokens_1.next(); !tokens_1_1.done; tokens_1_1 = tokens_1.next()) {
                    var token = tokens_1_1.value;
                    token = token.charAt(0).toUpperCase() + token.slice(1);
                    name += token;
                }
            }
            catch (e_1_1) { e_1 = { error: e_1_1 }; }
            finally {
                try {
                    if (tokens_1_1 && !tokens_1_1.done && (_a = tokens_1.return)) _a.call(tokens_1);
                }
                finally { if (e_1) throw e_1.error; }
            }
            return name;
        }
    },
    camelCase: {
        tokenize: function (name) { return name.split(/(?=[A-Z])/).map(function (token) { return token.toLowerCase(); }); },
        encode: function (tokens) {
            var e_2, _a;
            var name = '';
            try {
                for (var _b = __values(tokens.entries()), _c = _b.next(); !_c.done; _c = _b.next()) {
                    var _d = __read(_c.value, 2), i = _d[0], token = _d[1];
                    if (i !== 0) {
                        token = token.charAt(0).toUpperCase() + token.slice(1);
                    }
                    name += token;
                }
            }
            catch (e_2_1) { e_2 = { error: e_2_1 }; }
            finally {
                try {
                    if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
                }
                finally { if (e_2) throw e_2.error; }
            }
            return name;
        }
    },
    SCREAMING_SNAKE_CASE: {
        tokenize: function (name) { return name.split('_').map(function (token) { return token.toLowerCase(); }); },
        encode: function (tokens) { return tokens.join('_').toUpperCase(); }
    }
};
