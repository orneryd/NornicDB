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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.createChannelConnection = createChannelConnection;
var channel_1 = require("../channel");
var neo4j_driver_core_1 = require("neo4j-driver-core");
var connection_1 = __importDefault(require("./connection"));
var bolt_1 = __importDefault(require("../bolt"));
var PROTOCOL_ERROR = neo4j_driver_core_1.error.PROTOCOL_ERROR;
var Logger = neo4j_driver_core_1.internal.logger.Logger;
var idGenerator = 0;
/**
 * Crete new connection to the provided address. Returned connection is not connected.
 * @param {ServerAddress} address - the Bolt endpoint to connect to.
 * @param {Object} config - the driver configuration.
 * @param {ConnectionErrorHandler} errorHandler - the error handler for connection errors.
 * @param {Logger} log - configured logger.
 * @param {clientCertificate} clientCertificate - configured client certificate
 * @param ssrCallback - callback function used to update the counts of ssr enabled and disabled connections
 * @param createChannel - function taking a channelConfig object and creating a channel with it
 * @return {Connection} - new connection.
 */
function createChannelConnection(address, config, errorHandler, log, clientCertificate, serversideRouting, ssrCallback, createChannel) {
    if (serversideRouting === void 0) { serversideRouting = null; }
    if (createChannel === void 0) { createChannel = function (channelConfig) { return new channel_1.Channel(channelConfig); }; }
    var channelConfig = new channel_1.ChannelConfig(address, config, errorHandler.errorCode(), clientCertificate);
    var channel = createChannel(channelConfig);
    return bolt_1.default.handshake(channel, log)
        .then(function (_a) {
        var version = _a.protocolVersion, consumeRemainingBuffer = _a.consumeRemainingBuffer;
        var chunker = new channel_1.Chunker(channel);
        var dechunker = new channel_1.Dechunker();
        var createProtocol = function (conn) {
            return bolt_1.default.create({
                version: version,
                channel: channel,
                chunker: chunker,
                dechunker: dechunker,
                disableLosslessIntegers: config.disableLosslessIntegers,
                useBigInt: config.useBigInt,
                serversideRouting: serversideRouting,
                server: conn.server,
                log: conn.logger,
                observer: {
                    onObserversCountChange: conn._handleOngoingRequestsNumberChange.bind(conn),
                    onError: conn._handleFatalError.bind(conn),
                    onFailure: conn._resetOnFailure.bind(conn),
                    onProtocolError: conn._handleProtocolError.bind(conn),
                    onErrorApplyTransformation: function (error) {
                        return conn.handleAndTransformError(error, conn._address);
                    }
                }
            });
        };
        var connection = new ChannelConnection(channel, errorHandler, address, log, config.disableLosslessIntegers, serversideRouting, chunker, config.notificationFilter, createProtocol, config.telemetryDisabled, ssrCallback);
        // forward all pending bytes to the dechunker
        consumeRemainingBuffer(function (buffer) { return dechunker.write(buffer); });
        return connection;
    })
        .catch(function (reason) {
        return channel.close().then(function () {
            throw reason;
        });
    });
}
var ChannelConnection = /** @class */ (function (_super) {
    __extends(ChannelConnection, _super);
    /**
     * @constructor
     * @param {Channel} channel - channel with a 'write' function and a 'onmessage' callback property.
     * @param {ConnectionErrorHandler} errorHandler the error handler.
     * @param {ServerAddress} address - the server address to connect to.
     * @param {Logger} log - the configured logger.
     * @param {boolean} disableLosslessIntegers - if this connection should convert all received integers to native JS numbers.
     * @param {Chunker} chunker - the chunker
     * @param protocolSupplier - Bolt protocol supplier
     * @param {boolean} telemetryDisabled - wether telemetry has been disabled in driver config.
     * @param ssrCallback - callback function used to update the counts of ssr enabled and disabled connections.
     */
    function ChannelConnection(channel, errorHandler, address, log, disableLosslessIntegers, serversideRouting, chunker, // to be removed,
    notificationFilter, protocolSupplier, telemetryDisabled, ssrCallback) {
        if (disableLosslessIntegers === void 0) { disableLosslessIntegers = false; }
        if (serversideRouting === void 0) { serversideRouting = null; }
        if (ssrCallback === void 0) { ssrCallback = function (_) { }; }
        var _this = _super.call(this, errorHandler) || this;
        _this._authToken = null;
        _this._idle = false;
        _this._reseting = false;
        _this._resetObservers = [];
        _this._id = idGenerator++;
        _this._address = address;
        _this._server = { address: address.asHostPort() };
        _this._creationTimestamp = Date.now();
        _this._disableLosslessIntegers = disableLosslessIntegers;
        _this._ch = channel;
        _this._chunker = chunker;
        _this._log = createConnectionLogger(_this, log);
        _this._serversideRouting = serversideRouting;
        _this._notificationFilter = notificationFilter;
        _this._telemetryDisabledDriverConfig = telemetryDisabled === true;
        _this._telemetryDisabledConnection = true;
        _this._ssrCallback = ssrCallback;
        // connection from the database, returned in response for HELLO message and might not be available
        _this._dbConnectionId = null;
        // bolt protocol is initially not initialized
        /**
         * @private
         * @type {BoltProtocol}
         */
        _this._protocol = protocolSupplier(_this);
        // Set to true on fatal errors, to get this out of connection pool.
        _this._isBroken = false;
        if (_this._log.isDebugEnabled()) {
            _this._log.debug("created towards ".concat(address));
        }
        return _this;
    }
    ChannelConnection.prototype.beginTransaction = function (config) {
        this._sendTelemetryIfEnabled(config);
        return this._protocol.beginTransaction(config);
    };
    ChannelConnection.prototype.run = function (query, parameters, config) {
        this._sendTelemetryIfEnabled(config);
        return this._protocol.run(query, parameters, config);
    };
    ChannelConnection.prototype._sendTelemetryIfEnabled = function (config) {
        if (this._telemetryDisabledConnection ||
            this._telemetryDisabledDriverConfig ||
            config == null ||
            config.apiTelemetryConfig == null) {
            return;
        }
        this._protocol.telemetry({
            api: config.apiTelemetryConfig.api
        }, {
            onCompleted: config.apiTelemetryConfig.onTelemetrySuccess,
            onError: config.beforeError
        });
    };
    ChannelConnection.prototype.commitTransaction = function (config) {
        return this._protocol.commitTransaction(config);
    };
    ChannelConnection.prototype.rollbackTransaction = function (config) {
        return this._protocol.rollbackTransaction(config);
    };
    ChannelConnection.prototype.getProtocolVersion = function () {
        return this._protocol.version;
    };
    Object.defineProperty(ChannelConnection.prototype, "authToken", {
        get: function () {
            return this._authToken;
        },
        set: function (value) {
            this._authToken = value;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(ChannelConnection.prototype, "supportsReAuth", {
        get: function () {
            return this._protocol.supportsReAuth;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(ChannelConnection.prototype, "id", {
        get: function () {
            return this._id;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(ChannelConnection.prototype, "databaseId", {
        get: function () {
            return this._dbConnectionId;
        },
        set: function (value) {
            this._dbConnectionId = value;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(ChannelConnection.prototype, "idleTimestamp", {
        get: function () {
            return this._idleTimestamp;
        },
        set: function (value) {
            this._idleTimestamp = value;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(ChannelConnection.prototype, "creationTimestamp", {
        get: function () {
            return this._creationTimestamp;
        },
        enumerable: false,
        configurable: true
    });
    /**
     * Send initialization message.
     * @param {string} userAgent the user agent for this driver.
     * @param {Object} boltAgent the bolt agent for this driver.
     * @param {Object} authToken the object containing auth information.
     * @param {boolean} waitReAuth whether ot not the connection will wait for re-authentication to happen
     * @return {Promise<Connection>} promise resolved with the current connection if connection is successful. Rejected promise otherwise.
     */
    ChannelConnection.prototype.connect = function (userAgent, boltAgent, authToken, waitReAuth) {
        return __awaiter(this, void 0, void 0, function () {
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        if (this._protocol.initialized && !this._protocol.supportsReAuth) {
                            throw (0, neo4j_driver_core_1.newError)('Connection does not support re-auth');
                        }
                        this._authToken = authToken;
                        if (!!this._protocol.initialized) return [3 /*break*/, 2];
                        return [4 /*yield*/, this._initialize(userAgent, boltAgent, authToken)];
                    case 1: return [2 /*return*/, _a.sent()];
                    case 2:
                        if (!waitReAuth) return [3 /*break*/, 4];
                        return [4 /*yield*/, new Promise(function (resolve, reject) {
                                _this._protocol.logoff({
                                    onError: reject
                                });
                                _this._protocol.logon({
                                    authToken: authToken,
                                    onError: reject,
                                    onComplete: function () { return resolve(_this); },
                                    flush: true
                                });
                            })];
                    case 3: return [2 /*return*/, _a.sent()];
                    case 4:
                        this._protocol.logoff();
                        this._protocol.logon({ authToken: authToken, flush: true });
                        return [2 /*return*/, this];
                }
            });
        });
    };
    /**
     * Perform protocol-specific initialization which includes authentication.
     * @param {string} userAgent the user agent for this driver.
     * @param {string} boltAgent the bolt agent for this driver.
     * @param {Object} authToken the object containing auth information.
     * @return {Promise<Connection>} promise resolved with the current connection if initialization is successful. Rejected promise otherwise.
     */
    ChannelConnection.prototype._initialize = function (userAgent, boltAgent, authToken) {
        var _this = this;
        var self = this;
        return new Promise(function (resolve, reject) {
            _this._protocol.initialize({
                userAgent: userAgent,
                boltAgent: boltAgent,
                authToken: authToken,
                notificationFilter: _this._notificationFilter,
                onError: function (err) { return reject(err); },
                onComplete: function (metadata) {
                    var _a;
                    if (metadata) {
                        // read server version from the response metadata, if it is available
                        var serverVersion = metadata.server;
                        if (!_this.version || serverVersion) {
                            _this.version = serverVersion;
                        }
                        // read database connection id from the response metadata, if it is available
                        var dbConnectionId = metadata.connection_id;
                        if (!_this.databaseId) {
                            _this.databaseId = dbConnectionId;
                        }
                        if (metadata.hints) {
                            var receiveTimeoutRaw = metadata.hints['connection.recv_timeout_seconds'];
                            if (receiveTimeoutRaw !== null &&
                                receiveTimeoutRaw !== undefined) {
                                var receiveTimeoutInSeconds = (0, neo4j_driver_core_1.toNumber)(receiveTimeoutRaw);
                                if (Number.isInteger(receiveTimeoutInSeconds) &&
                                    receiveTimeoutInSeconds > 0) {
                                    _this._ch.setupReceiveTimeout(receiveTimeoutInSeconds * 1000);
                                }
                                else {
                                    _this._log.info("Server located at ".concat(_this._address, " supplied an invalid connection receive timeout value (").concat(receiveTimeoutInSeconds, "). ") +
                                        'Please, verify the server configuration and status because this can be the symptom of a bigger issue.');
                                }
                            }
                            var telemetryEnabledHint = metadata.hints['telemetry.enabled'];
                            if (telemetryEnabledHint === true) {
                                _this._telemetryDisabledConnection = false;
                            }
                            _this.SSREnabledHint = metadata.hints['ssr.enabled'];
                        }
                        _this._ssrCallback((_a = _this.SSREnabledHint) !== null && _a !== void 0 ? _a : false, 'OPEN');
                    }
                    resolve(self);
                }
            });
        });
    };
    /**
     * Get the Bolt protocol for the connection.
     * @return {BoltProtocol} the protocol.
     */
    ChannelConnection.prototype.protocol = function () {
        return this._protocol;
    };
    Object.defineProperty(ChannelConnection.prototype, "address", {
        get: function () {
            return this._address;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(ChannelConnection.prototype, "version", {
        /**
         * Get the version of the connected server.
         * Available only after initialization
         *
         * @returns {ServerVersion} version
         */
        get: function () {
            return this._server.version;
        },
        set: function (value) {
            this._server.version = value;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(ChannelConnection.prototype, "server", {
        get: function () {
            return this._server;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(ChannelConnection.prototype, "logger", {
        get: function () {
            return this._log;
        },
        enumerable: false,
        configurable: true
    });
    /**
     * "Fatal" means the connection is dead. Only call this if something
     * happens that cannot be recovered from. This will lead to all subscribers
     * failing, and the connection getting ejected from the session pool.
     *
     * @param error an error object, forwarded to all current and future subscribers
     */
    ChannelConnection.prototype._handleFatalError = function (error) {
        this._isBroken = true;
        this._error = this.handleAndTransformError(this._protocol.currentFailure || error, this._address);
        if (this._log.isErrorEnabled()) {
            this._log.error("experienced a fatal error caused by ".concat(this._error, " (").concat(neo4j_driver_core_1.json.stringify(this._error), ")"));
        }
        this._protocol.notifyFatalError(this._error);
    };
    /**
     * This method is used by the {@link PooledConnectionProvider}
     *
     * @param {any} observer
     */
    ChannelConnection.prototype._setIdle = function (observer) {
        this._idle = true;
        this._ch.stopReceiveTimeout();
        this._protocol.queueObserverIfProtocolIsNotBroken(observer);
    };
    /**
     * This method is used by the {@link PooledConnectionProvider}
     */
    ChannelConnection.prototype._unsetIdle = function () {
        this._idle = false;
        this._updateCurrentObserver();
    };
    /**
     * This method still here because of the connection-channel.tests.js
     *
     * @param {any} observer
     */
    ChannelConnection.prototype._queueObserver = function (observer) {
        return this._protocol.queueObserverIfProtocolIsNotBroken(observer);
    };
    ChannelConnection.prototype.hasOngoingObservableRequests = function () {
        return !this._idle && this._protocol.hasOngoingObservableRequests();
    };
    /**
     * Send a RESET-message to the database. Message is immediately flushed to the network.
     * @return {Promise<void>} promise resolved when SUCCESS-message response arrives, or failed when other response messages arrives.
     */
    ChannelConnection.prototype.resetAndFlush = function () {
        var _this = this;
        return new Promise(function (resolve, reject) {
            _this._reset({
                onError: function (error) {
                    if (_this._isBroken) {
                        // handling a fatal error, no need to raise a protocol violation
                        reject(error);
                    }
                    else {
                        var neo4jError = _this._handleProtocolError("Received FAILURE as a response for RESET: ".concat(error));
                        reject(neo4jError);
                    }
                },
                onComplete: function () {
                    resolve();
                }
            });
        });
    };
    ChannelConnection.prototype._resetOnFailure = function () {
        var _this = this;
        if (!this.isOpen()) {
            return;
        }
        this._reset({
            onError: function () {
                _this._protocol.resetFailure();
            },
            onComplete: function () {
                _this._protocol.resetFailure();
            }
        });
    };
    ChannelConnection.prototype._reset = function (observer) {
        var _this = this;
        if (this._reseting) {
            if (!this._protocol.isLastMessageReset()) {
                this._protocol.reset({
                    onError: function (error) {
                        observer.onError(error);
                    },
                    onComplete: function () {
                        observer.onComplete();
                    }
                });
            }
            else {
                this._resetObservers.push(observer);
            }
            return;
        }
        this._resetObservers.push(observer);
        this._reseting = true;
        var notifyFinish = function (notify) {
            _this._reseting = false;
            var observers = _this._resetObservers;
            _this._resetObservers = [];
            observers.forEach(notify);
        };
        this._protocol.reset({
            onError: function (error) {
                notifyFinish(function (obs) { return obs.onError(error); });
            },
            onComplete: function () {
                notifyFinish(function (obs) { return obs.onComplete(); });
            }
        });
    };
    /*
     * Pop next pending observer form the list of observers and make it current observer.
     * @protected
     */
    ChannelConnection.prototype._updateCurrentObserver = function () {
        this._protocol.updateCurrentObserver();
    };
    /** Check if this connection is in working condition */
    ChannelConnection.prototype.isOpen = function () {
        return !this._isBroken && this._ch._open;
    };
    /**
     * Starts and stops the receive timeout timer.
     * @param {number} requestsNumber Ongoing requests number
     */
    ChannelConnection.prototype._handleOngoingRequestsNumberChange = function (requestsNumber) {
        if (this._idle) {
            return;
        }
        if (requestsNumber === 0) {
            this._ch.stopReceiveTimeout();
        }
        else {
            this._ch.startReceiveTimeout();
        }
    };
    /**
     * Call close on the channel.
     * @returns {Promise<void>} - A promise that will be resolved when the underlying channel is closed.
     */
    ChannelConnection.prototype.close = function () {
        return __awaiter(this, void 0, void 0, function () {
            var _a;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        this._ssrCallback((_a = this.SSREnabledHint) !== null && _a !== void 0 ? _a : false, 'CLOSE');
                        if (this._log.isDebugEnabled()) {
                            this._log.debug('closing');
                        }
                        if (this._protocol && this.isOpen()) {
                            // protocol has been initialized and this connection is healthy
                            // notify the database about the upcoming close of the connection
                            this._protocol.prepareToClose();
                        }
                        return [4 /*yield*/, this._ch.close()];
                    case 1:
                        _b.sent();
                        if (this._log.isDebugEnabled()) {
                            this._log.debug('closed');
                        }
                        return [2 /*return*/];
                }
            });
        });
    };
    ChannelConnection.prototype.toString = function () {
        return "Connection [".concat(this.id, "][").concat(this.databaseId || '', "]");
    };
    ChannelConnection.prototype._handleProtocolError = function (message) {
        this._protocol.resetFailure();
        this._updateCurrentObserver();
        var error = (0, neo4j_driver_core_1.newError)(message, PROTOCOL_ERROR);
        this._handleFatalError(error);
        return error;
    };
    return ChannelConnection;
}(connection_1.default));
exports.default = ChannelConnection;
/**
 * Creates a log with the connection info as prefix
 * @param {Connection} connection The connection
 * @param {Logger} logger The logger
 * @returns {Logger} The new logger with enriched messages
 */
function createConnectionLogger(connection, logger) {
    return new Logger(logger._level, function (level, message) {
        return logger._loggerFunction(level, "".concat(connection, " ").concat(message));
    });
}
