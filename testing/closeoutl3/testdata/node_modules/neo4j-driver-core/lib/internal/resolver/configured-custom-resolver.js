"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
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
/* eslint-disable @typescript-eslint/promise-function-async */
var server_address_1 = require("../server-address");
function resolveToSelf(address) {
    return Promise.resolve([address]);
}
var ConfiguredCustomResolver = /** @class */ (function () {
    function ConfiguredCustomResolver(resolverFunction) {
        this._resolverFunction = resolverFunction !== null && resolverFunction !== void 0 ? resolverFunction : resolveToSelf;
    }
    ConfiguredCustomResolver.prototype.resolve = function (seedRouter) {
        var _this = this;
        return new Promise(function (resolve) {
            return resolve(_this._resolverFunction(seedRouter.asHostPort()));
        }).then(function (resolved) {
            if (!Array.isArray(resolved)) {
                throw new TypeError('Configured resolver function should either return an array of addresses or a Promise resolved with an array of addresses.' +
                    // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
                    "Each address is '<host>:<port>'. Got: ".concat(resolved));
            }
            return resolved.map(function (r) { return server_address_1.ServerAddress.fromUrl(r); });
        });
    };
    return ConfiguredCustomResolver;
}());
exports.default = ConfiguredCustomResolver;
