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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var connection_1 = __importDefault(require("./connection"));
var DelegateConnection = /** @class */ (function (_super) {
    __extends(DelegateConnection, _super);
    /**
     * @param delegate {Connection} the delegated connection
     * @param errorHandler {ConnectionErrorHandler} the error handler
     */
    function DelegateConnection(delegate, errorHandler) {
        var _this = _super.call(this, errorHandler) || this;
        if (errorHandler) {
            _this._originalErrorHandler = delegate._errorHandler;
            delegate._errorHandler = _this._errorHandler;
        }
        _this._delegate = delegate;
        return _this;
    }
    DelegateConnection.prototype.beginTransaction = function (config) {
        return this._delegate.beginTransaction(config);
    };
    DelegateConnection.prototype.run = function (query, param, config) {
        return this._delegate.run(query, param, config);
    };
    DelegateConnection.prototype.commitTransaction = function (config) {
        return this._delegate.commitTransaction(config);
    };
    DelegateConnection.prototype.rollbackTransaction = function (config) {
        return this._delegate.rollbackTransaction(config);
    };
    DelegateConnection.prototype.getProtocolVersion = function () {
        return this._delegate.getProtocolVersion();
    };
    Object.defineProperty(DelegateConnection.prototype, "id", {
        get: function () {
            return this._delegate.id;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(DelegateConnection.prototype, "databaseId", {
        get: function () {
            return this._delegate.databaseId;
        },
        set: function (value) {
            this._delegate.databaseId = value;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(DelegateConnection.prototype, "server", {
        get: function () {
            return this._delegate.server;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(DelegateConnection.prototype, "authToken", {
        get: function () {
            return this._delegate.authToken;
        },
        set: function (value) {
            this._delegate.authToken = value;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(DelegateConnection.prototype, "supportsReAuth", {
        get: function () {
            return this._delegate.supportsReAuth;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(DelegateConnection.prototype, "address", {
        get: function () {
            return this._delegate.address;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(DelegateConnection.prototype, "version", {
        get: function () {
            return this._delegate.version;
        },
        set: function (value) {
            this._delegate.version = value;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(DelegateConnection.prototype, "creationTimestamp", {
        get: function () {
            return this._delegate.creationTimestamp;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(DelegateConnection.prototype, "idleTimestamp", {
        get: function () {
            return this._delegate.idleTimestamp;
        },
        set: function (value) {
            this._delegate.idleTimestamp = value;
        },
        enumerable: false,
        configurable: true
    });
    DelegateConnection.prototype.isOpen = function () {
        return this._delegate.isOpen();
    };
    DelegateConnection.prototype.protocol = function () {
        return this._delegate.protocol();
    };
    DelegateConnection.prototype.connect = function (userAgent, boltAgent, authToken, waitReAuth) {
        return this._delegate.connect(userAgent, boltAgent, authToken, waitReAuth);
    };
    DelegateConnection.prototype.write = function (message, observer, flush) {
        return this._delegate.write(message, observer, flush);
    };
    DelegateConnection.prototype.resetAndFlush = function () {
        return this._delegate.resetAndFlush();
    };
    DelegateConnection.prototype.hasOngoingObservableRequests = function () {
        return this._delegate.hasOngoingObservableRequests();
    };
    DelegateConnection.prototype.close = function () {
        return this._delegate.close();
    };
    DelegateConnection.prototype.release = function () {
        if (this._originalErrorHandler) {
            this._delegate._errorHandler = this._originalErrorHandler;
        }
        return this._delegate.release();
    };
    return DelegateConnection;
}(connection_1.default));
exports.default = DelegateConnection;
