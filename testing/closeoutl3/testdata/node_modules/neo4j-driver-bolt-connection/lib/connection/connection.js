"use strict";
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (Object.prototype.hasOwnProperty.call(b, p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        if (typeof b !== "function" && b !== null)
            throw new TypeError("Class extends value " + String(b) + " is not a constructor or null");
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
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
// eslint-disable-next-line no-unused-vars
var bolt_1 = require("../bolt");
var neo4j_driver_core_1 = require("neo4j-driver-core");
var Connection = /** @class */ (function (_super) {
    __extends(Connection, _super);
    /**
     * @param {ConnectionErrorHandler} errorHandler the error handler
     */
    function Connection(errorHandler) {
        var _this = _super.call(this) || this;
        _this._errorHandler = errorHandler;
        return _this;
    }
    Object.defineProperty(Connection.prototype, "id", {
        get: function () {
            throw new Error('not implemented');
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(Connection.prototype, "databaseId", {
        get: function () {
            throw new Error('not implemented');
        },
        set: function (value) {
            throw new Error('not implemented');
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(Connection.prototype, "authToken", {
        get: function () {
            throw new Error('not implemented');
        },
        set: function (value) {
            throw new Error('not implemented');
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(Connection.prototype, "supportsReAuth", {
        get: function () {
            throw new Error('not implemented');
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(Connection.prototype, "creationTimestamp", {
        get: function () {
            throw new Error('not implemented');
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(Connection.prototype, "idleTimestamp", {
        get: function () {
            throw new Error('not implemented');
        },
        set: function (value) {
            throw new Error('not implemented');
        },
        enumerable: false,
        configurable: true
    });
    /**
     * @returns {BoltProtocol} the underlying bolt protocol assigned to this connection
     */
    Connection.prototype.protocol = function () {
        throw new Error('not implemented');
    };
    Object.defineProperty(Connection.prototype, "address", {
        /**
         * @returns {ServerAddress} the server address this connection is opened against
         */
        get: function () {
            throw new Error('not implemented');
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(Connection.prototype, "version", {
        /**
         * @returns {ServerVersion} the version of the server this connection is connected to
         */
        get: function () {
            throw new Error('not implemented');
        },
        set: function (value) {
            throw new Error('not implemented');
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(Connection.prototype, "server", {
        get: function () {
            throw new Error('not implemented');
        },
        enumerable: false,
        configurable: true
    });
    /**
     * Connect to the target address, negotiate Bolt protocol and send initialization message.
     * @param {string} userAgent the user agent for this driver.
     * @param {Object} boltAgent the bolt agent for this driver.
     * @param {Object} authToken the object containing auth information.
     * @param {boolean} shouldWaitReAuth whether ot not the connection will wait for re-authentication to happen
     * @return {Promise<Connection>} promise resolved with the current connection if connection is successful. Rejected promise otherwise.
     */
    Connection.prototype.connect = function (userAgent, boltAgent, authToken, shouldWaitReAuth) {
        throw new Error('not implemented');
    };
    /**
     * Write a message to the network channel.
     * @param {RequestMessage} message the message to write.
     * @param {ResultStreamObserver} observer the response observer.
     * @param {boolean} flush `true` if flush should happen after the message is written to the buffer.
     */
    Connection.prototype.write = function (message, observer, flush) {
        throw new Error('not implemented');
    };
    /**
     * Call close on the channel.
     * @returns {Promise<void>} - A promise that will be resolved when the connection is closed.
     *
     */
    Connection.prototype.close = function () {
        throw new Error('not implemented');
    };
    /**
     *
     * @param error
     * @param address
     * @returns {Neo4jError|*}
     */
    Connection.prototype.handleAndTransformError = function (error, address) {
        if (this._errorHandler) {
            return this._errorHandler.handleAndTransformError(error, address, this);
        }
        return error;
    };
    return Connection;
}(neo4j_driver_core_1.Connection));
exports.default = Connection;
