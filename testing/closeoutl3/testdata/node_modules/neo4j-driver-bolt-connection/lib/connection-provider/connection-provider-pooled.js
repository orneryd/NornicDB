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
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
    return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
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
var __spreadArray = (this && this.__spreadArray) || function (to, from, pack) {
    if (pack || arguments.length === 2) for (var i = 0, l = from.length, ar; i < l; i++) {
        if (ar || !(i in from)) {
            if (!ar) ar = Array.prototype.slice.call(from, 0, i);
            ar[i] = from[i];
        }
    }
    return to.concat(ar || Array.prototype.slice.call(from));
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var connection_1 = require("../connection");
var neo4j_driver_core_1 = require("neo4j-driver-core");
var authentication_provider_1 = __importDefault(require("./authentication-provider"));
var lang_1 = require("../lang");
var liveness_check_provider_1 = __importDefault(require("./liveness-check-provider"));
var client_certificate_holder_1 = __importDefault(require("./client-certificate-holder"));
var SERVICE_UNAVAILABLE = neo4j_driver_core_1.error.SERVICE_UNAVAILABLE;
var AUTHENTICATION_ERRORS = [
    'Neo.ClientError.Security.CredentialsExpired',
    'Neo.ClientError.Security.Forbidden',
    'Neo.ClientError.Security.TokenExpired',
    'Neo.ClientError.Security.Unauthorized'
];
var _a = neo4j_driver_core_1.internal.pool, Pool = _a.Pool, PoolConfig = _a.PoolConfig;
var PooledConnectionProvider = /** @class */ (function (_super) {
    __extends(PooledConnectionProvider, _super);
    function PooledConnectionProvider(_a, createChannelConnectionHook) {
        var id = _a.id, config = _a.config, log = _a.log, userAgent = _a.userAgent, boltAgent = _a.boltAgent, authTokenManager = _a.authTokenManager, _b = _a.newPool, newPool = _b === void 0 ? function () {
            var args = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                args[_i] = arguments[_i];
            }
            return new (Pool.bind.apply(Pool, __spreadArray([void 0], __read(args), false)))();
        } : _b;
        if (createChannelConnectionHook === void 0) { createChannelConnectionHook = null; }
        var _this = _super.call(this) || this;
        _this._id = id;
        _this._config = config;
        _this._log = log;
        _this._clientCertificateHolder = new client_certificate_holder_1.default({ clientCertificateProvider: _this._config.clientCertificate });
        _this._authenticationProvider = new authentication_provider_1.default({ authTokenManager: authTokenManager, userAgent: userAgent, boltAgent: boltAgent });
        _this._livenessCheckProvider = new liveness_check_provider_1.default({ connectionLivenessCheckTimeout: config.connectionLivenessCheckTimeout });
        _this._userAgent = userAgent;
        _this._boltAgent = boltAgent;
        _this._createChannelConnection =
            createChannelConnectionHook ||
                (function (address) { return __awaiter(_this, void 0, void 0, function () {
                    var _a, _b;
                    return __generator(this, function (_c) {
                        switch (_c.label) {
                            case 0:
                                _a = connection_1.createChannelConnection;
                                _b = [address,
                                    this._config,
                                    this._createConnectionErrorHandler(),
                                    this._log];
                                return [4 /*yield*/, this._clientCertificateHolder.getClientCertificate()];
                            case 1: return [2 /*return*/, _a.apply(void 0, _b.concat([_c.sent()]))];
                        }
                    });
                }); });
        _this._connectionPool = newPool({
            create: _this._createConnection.bind(_this),
            destroy: _this._destroyConnection.bind(_this),
            validateOnAcquire: _this._validateConnectionOnAcquire.bind(_this),
            validateOnRelease: _this._validateConnectionOnRelease.bind(_this),
            installIdleObserver: PooledConnectionProvider._installIdleObserverOnConnection.bind(_this),
            removeIdleObserver: PooledConnectionProvider._removeIdleObserverOnConnection.bind(_this),
            config: PoolConfig.fromDriverConfig(config),
            log: _this._log
        });
        _this._openConnections = {};
        return _this;
    }
    PooledConnectionProvider.prototype._createConnectionErrorHandler = function () {
        return new connection_1.ConnectionErrorHandler(SERVICE_UNAVAILABLE);
    };
    PooledConnectionProvider.prototype._getClientCertificate = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                return [2 /*return*/, this._config.clientCertificate.getClientCertificate()];
            });
        });
    };
    /**
     * Create a new connection and initialize it.
     * @return {Promise<Connection>} promise resolved with a new connection or rejected when failed to connect.
     * @access private
     */
    PooledConnectionProvider.prototype._createConnection = function (_a, address, release) {
        var _this = this;
        var auth = _a.auth;
        return this._createChannelConnection(address).then(function (connection) {
            connection.release = function () {
                connection.idleTimestamp = Date.now();
                return release(address, connection);
            };
            _this._openConnections[connection.id] = connection;
            return _this._authenticationProvider.authenticate({ connection: connection, auth: auth })
                .catch(function (error) {
                // let's destroy this connection
                _this._destroyConnection(connection);
                // propagate the error because connection failed to connect / initialize
                throw error;
            });
        });
    };
    PooledConnectionProvider.prototype._validateConnectionOnAcquire = function (_a, conn_1) {
        return __awaiter(this, arguments, void 0, function (_b, conn) {
            var error_1, error_2;
            var auth = _b.auth, skipReAuth = _b.skipReAuth;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        if (!this._validateConnection(conn)) {
                            return [2 /*return*/, false];
                        }
                        _c.label = 1;
                    case 1:
                        _c.trys.push([1, 3, , 4]);
                        return [4 /*yield*/, this._livenessCheckProvider.check(conn)];
                    case 2:
                        _c.sent();
                        return [3 /*break*/, 4];
                    case 3:
                        error_1 = _c.sent();
                        this._log.debug("The connection ".concat(conn.id, " is not alive because of an error ").concat(error_1.code, " '").concat(error_1.message, "'"));
                        return [2 /*return*/, false];
                    case 4:
                        _c.trys.push([4, 6, , 7]);
                        return [4 /*yield*/, this._authenticationProvider.authenticate({ connection: conn, auth: auth, skipReAuth: skipReAuth })];
                    case 5:
                        _c.sent();
                        return [2 /*return*/, true];
                    case 6:
                        error_2 = _c.sent();
                        this._log.debug("The connection ".concat(conn.id, " is not valid because of an error ").concat(error_2.code, " '").concat(error_2.message, "'"));
                        return [2 /*return*/, false];
                    case 7: return [2 /*return*/];
                }
            });
        });
    };
    PooledConnectionProvider.prototype._validateConnectionOnRelease = function (conn) {
        return conn._sticky !== true && this._validateConnection(conn);
    };
    /**
     * Check that a connection is usable
     * @return {boolean} true if the connection is open
     * @access private
     **/
    PooledConnectionProvider.prototype._validateConnection = function (conn) {
        if (!conn.isOpen()) {
            return false;
        }
        var maxConnectionLifetime = this._config.maxConnectionLifetime;
        var lifetime = Date.now() - conn.creationTimestamp;
        if (lifetime > maxConnectionLifetime) {
            return false;
        }
        return true;
    };
    /**
     * Dispose of a connection.
     * @return {Connection} the connection to dispose.
     * @access private
     */
    PooledConnectionProvider.prototype._destroyConnection = function (conn) {
        delete this._openConnections[conn.id];
        return conn.close();
    };
    /**
     * Acquire a connection from the pool and return it ServerInfo
     * @param {object} param
     * @param {string} param.address the server address
     * @return {Promise<ServerInfo>} the server info
     */
    PooledConnectionProvider.prototype._verifyConnectivityAndGetServerVersion = function (_a) {
        return __awaiter(this, arguments, void 0, function (_b) {
            var connection, serverInfo;
            var address = _b.address;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0: return [4 /*yield*/, this._connectionPool.acquire({}, address)];
                    case 1:
                        connection = _c.sent();
                        serverInfo = new neo4j_driver_core_1.ServerInfo(connection.server, connection.protocol().version);
                        _c.label = 2;
                    case 2:
                        _c.trys.push([2, , 5, 7]);
                        if (!!connection.protocol().isLastMessageLogon()) return [3 /*break*/, 4];
                        return [4 /*yield*/, connection.resetAndFlush()];
                    case 3:
                        _c.sent();
                        _c.label = 4;
                    case 4: return [3 /*break*/, 7];
                    case 5: return [4 /*yield*/, connection.release()];
                    case 6:
                        _c.sent();
                        return [7 /*endfinally*/];
                    case 7: return [2 /*return*/, serverInfo];
                }
            });
        });
    };
    PooledConnectionProvider.prototype._verifyAuthentication = function (_a) {
        return __awaiter(this, arguments, void 0, function (_b) {
            var connectionsToRelease, address, connection, lastMessageIsNotLogin, stickyConnection, error_3;
            var getAddress = _b.getAddress, auth = _b.auth;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        connectionsToRelease = [];
                        _c.label = 1;
                    case 1:
                        _c.trys.push([1, 8, 9, 11]);
                        return [4 /*yield*/, getAddress()];
                    case 2:
                        address = _c.sent();
                        return [4 /*yield*/, this._connectionPool.acquire({ auth: auth, skipReAuth: true }, address)];
                    case 3:
                        connection = _c.sent();
                        connectionsToRelease.push(connection);
                        lastMessageIsNotLogin = !connection.protocol().isLastMessageLogon();
                        if (!connection.supportsReAuth) {
                            throw (0, neo4j_driver_core_1.newError)('Driver is connected to a database that does not support user switch.');
                        }
                        if (!(lastMessageIsNotLogin && connection.supportsReAuth)) return [3 /*break*/, 5];
                        return [4 /*yield*/, this._authenticationProvider.authenticate({ connection: connection, auth: auth, waitReAuth: true, forceReAuth: true })];
                    case 4:
                        _c.sent();
                        return [3 /*break*/, 7];
                    case 5:
                        if (!(lastMessageIsNotLogin && !connection.supportsReAuth)) return [3 /*break*/, 7];
                        return [4 /*yield*/, this._connectionPool.acquire({ auth: auth }, address, { requireNew: true })];
                    case 6:
                        stickyConnection = _c.sent();
                        stickyConnection._sticky = true;
                        connectionsToRelease.push(stickyConnection);
                        _c.label = 7;
                    case 7: return [2 /*return*/, true];
                    case 8:
                        error_3 = _c.sent();
                        if (AUTHENTICATION_ERRORS.includes(error_3.code)) {
                            return [2 /*return*/, false];
                        }
                        throw error_3;
                    case 9: return [4 /*yield*/, Promise.all(connectionsToRelease.map(function (conn) { return conn.release(); }))];
                    case 10:
                        _c.sent();
                        return [7 /*endfinally*/];
                    case 11: return [2 /*return*/];
                }
            });
        });
    };
    PooledConnectionProvider.prototype._verifyStickyConnection = function (_a) {
        return __awaiter(this, arguments, void 0, function (_b) {
            var connectionWithSameCredentials, shouldCreateStickyConnection;
            var auth = _b.auth, connection = _b.connection, address = _b.address;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        connectionWithSameCredentials = lang_1.object.equals(auth, connection.authToken);
                        shouldCreateStickyConnection = !connectionWithSameCredentials;
                        connection._sticky = connectionWithSameCredentials && !connection.supportsReAuth;
                        if (!(shouldCreateStickyConnection || connection._sticky)) return [3 /*break*/, 2];
                        return [4 /*yield*/, connection.release()];
                    case 1:
                        _c.sent();
                        throw (0, neo4j_driver_core_1.newError)('Driver is connected to a database that does not support user switch.');
                    case 2: return [2 /*return*/];
                }
            });
        });
    };
    PooledConnectionProvider.prototype.close = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: 
                    // purge all idle connections in the connection pool
                    return [4 /*yield*/, this._connectionPool.close()
                        // then close all connections driver has ever created
                        // it is needed to close connections that are active right now and are acquired from the pool
                    ];
                    case 1:
                        // purge all idle connections in the connection pool
                        _a.sent();
                        // then close all connections driver has ever created
                        // it is needed to close connections that are active right now and are acquired from the pool
                        return [4 /*yield*/, Promise.all(Object.values(this._openConnections).map(function (c) { return c.close(); }))];
                    case 2:
                        // then close all connections driver has ever created
                        // it is needed to close connections that are active right now and are acquired from the pool
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    PooledConnectionProvider._installIdleObserverOnConnection = function (conn, observer) {
        conn._setIdle(observer);
    };
    PooledConnectionProvider._removeIdleObserverOnConnection = function (conn) {
        conn._unsetIdle();
    };
    PooledConnectionProvider.prototype._handleSecurityError = function (error, address, connection) {
        var handled = this._authenticationProvider.handleError({ connection: connection, code: error.code });
        if (handled) {
            error.retriable = true; // remove in 7.0
            error.retryable = true;
        }
        if (error.code === 'Neo.ClientError.Security.AuthorizationExpired') {
            this._connectionPool.apply(address, function (conn) { conn.authToken = null; });
        }
        if (connection) {
            connection.close().catch(function () { return undefined; });
        }
        return error;
    };
    return PooledConnectionProvider;
}(neo4j_driver_core_1.ConnectionProvider));
exports.default = PooledConnectionProvider;
