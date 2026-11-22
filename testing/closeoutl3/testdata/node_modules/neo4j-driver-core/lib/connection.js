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
/**
 * Interface which defines a connection for the core driver object.
 *
 *
 * This connection exposes only methods used by the code module.
 * Methods with connection implementation details can be defined and used
 * by the implementation layer.
 *
 * @private
 * @interface
 */
var Connection = /** @class */ (function () {
    function Connection() {
    }
    /**
     *
     * @param config
     * @returns {ResultStreamObserver}
     */
    Connection.prototype.beginTransaction = function (config) {
        throw new Error('Not implemented');
    };
    /**
     *
     * @param query
     * @param parameters
     * @param config
     * @returns {ResultStreamObserver}
     */
    Connection.prototype.run = function (query, parameters, config) {
        throw new Error('Not implemented');
    };
    /**
     *
     * @param config
     * @returns {ResultStreamObserver}
     */
    Connection.prototype.commitTransaction = function (config) {
        throw new Error('Not implemented');
    };
    /**
     *
     * @param config
     * @returns {ResultStreamObserver}
     */
    Connection.prototype.rollbackTransaction = function (config) {
        throw new Error('Not implemented');
    };
    /**
     *
     * @returns {Promise<void>}
     */
    Connection.prototype.resetAndFlush = function () {
        throw new Error('Not implemented');
    };
    /**
     *
     * @returns {boolean}
     */
    Connection.prototype.isOpen = function () {
        throw new Error('Not implemented');
    };
    /**
     *
     * @returns {number}
     */
    Connection.prototype.getProtocolVersion = function () {
        throw new Error('Not implemented');
    };
    /**
     *
     * @returns {boolean}
     */
    Connection.prototype.hasOngoingObservableRequests = function () {
        throw new Error('Not implemented');
    };
    return Connection;
}());
exports.default = Connection;
