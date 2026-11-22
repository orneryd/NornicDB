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
/* eslint-disable @typescript-eslint/promise-function-async */
Object.defineProperty(exports, "__esModule", { value: true });
var BaseHostNameResolver = /** @class */ (function () {
    function BaseHostNameResolver() {
    }
    BaseHostNameResolver.prototype.resolve = function () {
        throw new Error('Abstract function');
    };
    /**
     * @protected
     */
    BaseHostNameResolver.prototype._resolveToItself = function (address) {
        return Promise.resolve([address]);
    };
    return BaseHostNameResolver;
}());
exports.default = BaseHostNameResolver;
