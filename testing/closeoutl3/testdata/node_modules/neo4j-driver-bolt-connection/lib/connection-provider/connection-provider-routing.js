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
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var neo4j_driver_core_1 = require("neo4j-driver-core");
var rediscovery_1 = __importStar(require("../rediscovery"));
var channel_1 = require("../channel");
var connection_provider_single_1 = __importDefault(require("./connection-provider-single"));
var connection_provider_pooled_1 = __importDefault(require("./connection-provider-pooled"));
var load_balancing_1 = require("../load-balancing");
var connection_1 = require("../connection");
var lang_1 = require("../lang");
var SERVICE_UNAVAILABLE = neo4j_driver_core_1.error.SERVICE_UNAVAILABLE, SESSION_EXPIRED = neo4j_driver_core_1.error.SESSION_EXPIRED;
var Bookmarks = neo4j_driver_core_1.internal.bookmarks.Bookmarks, _a = neo4j_driver_core_1.internal.constants, READ = _a.ACCESS_MODE_READ, WRITE = _a.ACCESS_MODE_WRITE, BOLT_PROTOCOL_V3 = _a.BOLT_PROTOCOL_V3, BOLT_PROTOCOL_V4_0 = _a.BOLT_PROTOCOL_V4_0, BOLT_PROTOCOL_V4_4 = _a.BOLT_PROTOCOL_V4_4, BOLT_PROTOCOL_V5_1 = _a.BOLT_PROTOCOL_V5_1;
var PROCEDURE_NOT_FOUND_CODE = 'Neo.ClientError.Procedure.ProcedureNotFound';
var DATABASE_NOT_FOUND_CODE = 'Neo.ClientError.Database.DatabaseNotFound';
var INVALID_BOOKMARK_CODE = 'Neo.ClientError.Transaction.InvalidBookmark';
var INVALID_BOOKMARK_MIXTURE_CODE = 'Neo.ClientError.Transaction.InvalidBookmarkMixture';
var AUTHORIZATION_EXPIRED_CODE = 'Neo.ClientError.Security.AuthorizationExpired';
var INVALID_ARGUMENT_ERROR = 'Neo.ClientError.Statement.ArgumentError';
var INVALID_REQUEST_ERROR = 'Neo.ClientError.Request.Invalid';
var STATEMENT_TYPE_ERROR = 'Neo.ClientError.Statement.TypeError';
var NOT_AVAILABLE = 'N/A';
var SYSTEM_DB_NAME = 'system';
var DEFAULT_DB_NAME = null;
var DEFAULT_ROUTING_TABLE_PURGE_DELAY = (0, neo4j_driver_core_1.int)(30000);
var RoutingConnectionProvider = /** @class */ (function (_super) {
    __extends(RoutingConnectionProvider, _super);
    function RoutingConnectionProvider(_a) {
        var id = _a.id, address = _a.address, routingContext = _a.routingContext, hostNameResolver = _a.hostNameResolver, config = _a.config, log = _a.log, userAgent = _a.userAgent, boltAgent = _a.boltAgent, authTokenManager = _a.authTokenManager, routingTablePurgeDelay = _a.routingTablePurgeDelay, newPool = _a.newPool;
        var _this = _super.call(this, { id: id, config: config, log: log, userAgent: userAgent, boltAgent: boltAgent, authTokenManager: authTokenManager, newPool: newPool }, function (address) { return __awaiter(_this, void 0, void 0, function () {
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
                    case 1: return [2 /*return*/, _a.apply(void 0, _b.concat([_c.sent(), this._routingContext,
                            this._channelSsrCallback.bind(this)]))];
                }
            });
        }); }) || this;
        _this._routingContext = __assign(__assign({}, routingContext), { address: address.toString() });
        _this._seedRouter = address;
        _this._rediscovery = new rediscovery_1.default(_this._routingContext);
        _this._loadBalancingStrategy = new load_balancing_1.LeastConnectedLoadBalancingStrategy(_this._connectionPool);
        _this._startTime = 0;
        _this._hostNameResolver = hostNameResolver;
        _this._dnsResolver = new channel_1.HostNameResolver();
        _this._log = log;
        _this._useSeedRouter = true;
        _this._routingTableRegistry = new RoutingTableRegistry(routingTablePurgeDelay
            ? (0, neo4j_driver_core_1.int)(routingTablePurgeDelay)
            : DEFAULT_ROUTING_TABLE_PURGE_DELAY);
        _this._refreshRoutingTable = lang_1.functional.reuseOngoingRequest(_this._refreshRoutingTable, _this);
        _this._withSSR = 0;
        _this._withoutSSR = 0;
        return _this;
    }
    RoutingConnectionProvider.prototype._createConnectionErrorHandler = function () {
        // connection errors mean SERVICE_UNAVAILABLE for direct driver but for routing driver they should only
        // result in SESSION_EXPIRED because there might still exist other servers capable of serving the request
        return new connection_1.ConnectionErrorHandler(SESSION_EXPIRED);
    };
    RoutingConnectionProvider.prototype._handleUnavailability = function (error, address, database) {
        this._log.warn("Routing driver ".concat(this._id, " will forget ").concat(address, " for database '").concat(database, "' because of an error ").concat(error.code, " '").concat(error.message, "'"));
        this.forget(address, database || DEFAULT_DB_NAME);
        return error;
    };
    RoutingConnectionProvider.prototype._handleSecurityError = function (error, address, connection, database) {
        this._log.warn("Routing driver ".concat(this._id, " will close connections to ").concat(address, " for database '").concat(database, "' because of an error ").concat(error.code, " '").concat(error.message, "'"));
        return _super.prototype._handleSecurityError.call(this, error, address, connection, database);
    };
    RoutingConnectionProvider.prototype._handleWriteFailure = function (error, address, database) {
        this._log.warn("Routing driver ".concat(this._id, " will forget writer ").concat(address, " for database '").concat(database, "' because of an error ").concat(error.code, " '").concat(error.message, "'"));
        this.forgetWriter(address, database || DEFAULT_DB_NAME);
        return (0, neo4j_driver_core_1.newError)('No longer possible to write to server at ' + address, SESSION_EXPIRED, error);
    };
    /**
     * See {@link ConnectionProvider} for more information about this method and
     * its arguments.
     */
    RoutingConnectionProvider.prototype.acquireConnection = function () {
        return __awaiter(this, arguments, void 0, function (_a) {
            var context, databaseSpecificErrorHandler, conn, currentRoutingTable, routingTable;
            var _this = this;
            var _b = _a === void 0 ? {} : _a, accessMode = _b.accessMode, database = _b.database, bookmarks = _b.bookmarks, impersonatedUser = _b.impersonatedUser, onDatabaseNameResolved = _b.onDatabaseNameResolved, auth = _b.auth, homeDb = _b.homeDb;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        context = { database: database || DEFAULT_DB_NAME };
                        databaseSpecificErrorHandler = new connection_1.ConnectionErrorHandler(SESSION_EXPIRED, function (error, address) { return _this._handleUnavailability(error, address, context.database); }, function (error, address) { return _this._handleWriteFailure(error, address, homeDb !== null && homeDb !== void 0 ? homeDb : context.database); }, function (error, address, conn) { return _this._handleSecurityError(error, address, conn, context.database); });
                        this._startTime = new Date().getTime();
                        if (!(this.SSREnabled() && homeDb !== undefined && database === '')) return [3 /*break*/, 2];
                        currentRoutingTable = this._routingTableRegistry.get(homeDb, function () { return new rediscovery_1.RoutingTable({ database: homeDb }); });
                        if (!(currentRoutingTable && !currentRoutingTable.isStaleFor(accessMode))) return [3 /*break*/, 2];
                        return [4 /*yield*/, this._getConnectionFromRoutingTable(currentRoutingTable, auth, accessMode, databaseSpecificErrorHandler)];
                    case 1:
                        conn = _c.sent();
                        if (this.SSREnabled()) {
                            return [2 /*return*/, conn];
                        }
                        conn.release();
                        _c.label = 2;
                    case 2: return [4 /*yield*/, this._freshRoutingTable({
                            accessMode: accessMode,
                            database: context.database,
                            bookmarks: bookmarks,
                            impersonatedUser: impersonatedUser,
                            auth: auth,
                            onDatabaseNameResolved: function (databaseName) {
                                context.database = context.database || databaseName;
                                if (onDatabaseNameResolved) {
                                    onDatabaseNameResolved(databaseName);
                                }
                            }
                        })];
                    case 3:
                        routingTable = _c.sent();
                        return [2 /*return*/, this._getConnectionFromRoutingTable(routingTable, auth, accessMode, databaseSpecificErrorHandler)];
                }
            });
        });
    };
    RoutingConnectionProvider.prototype._getConnectionFromRoutingTable = function (routingTable, auth, accessMode, databaseSpecificErrorHandler) {
        return __awaiter(this, void 0, void 0, function () {
            var name, address, connection, error_1, transformed;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        // select a target server based on specified access mode
                        if (accessMode === READ) {
                            address = this._loadBalancingStrategy.selectReader(routingTable.readers);
                            name = 'read';
                        }
                        else if (accessMode === WRITE) {
                            address = this._loadBalancingStrategy.selectWriter(routingTable.writers);
                            name = 'write';
                        }
                        else {
                            throw (0, neo4j_driver_core_1.newError)('Illegal mode ' + accessMode);
                        }
                        // we couldn't select a target server
                        if (!address) {
                            throw (0, neo4j_driver_core_1.newError)("Failed to obtain connection towards ".concat(name, " server. Known routing table is: ").concat(routingTable), SESSION_EXPIRED);
                        }
                        _a.label = 1;
                    case 1:
                        _a.trys.push([1, 5, , 6]);
                        return [4 /*yield*/, this._connectionPool.acquire({ auth: auth, startTime: this._startTime }, address)];
                    case 2:
                        connection = _a.sent();
                        if (!auth) return [3 /*break*/, 4];
                        return [4 /*yield*/, this._verifyStickyConnection({
                                auth: auth,
                                connection: connection,
                                address: address
                            })];
                    case 3:
                        _a.sent();
                        return [2 /*return*/, connection];
                    case 4: return [2 /*return*/, new connection_1.DelegateConnection(connection, databaseSpecificErrorHandler)];
                    case 5:
                        error_1 = _a.sent();
                        transformed = databaseSpecificErrorHandler.handleAndTransformError(error_1, address);
                        throw transformed;
                    case 6: return [2 /*return*/];
                }
            });
        });
    };
    RoutingConnectionProvider.prototype._hasProtocolVersion = function (versionPredicate) {
        return __awaiter(this, void 0, void 0, function () {
            var addresses, lastError, i, connection, protocolVersion, error_2;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._resolveSeedRouter(this._seedRouter)];
                    case 1:
                        addresses = _a.sent();
                        i = 0;
                        _a.label = 2;
                    case 2:
                        if (!(i < addresses.length)) return [3 /*break*/, 8];
                        _a.label = 3;
                    case 3:
                        _a.trys.push([3, 6, , 7]);
                        return [4 /*yield*/, this._createChannelConnection(addresses[i])];
                    case 4:
                        connection = _a.sent();
                        protocolVersion = connection.protocol()
                            ? connection.protocol().version
                            : null;
                        return [4 /*yield*/, connection.close()];
                    case 5:
                        _a.sent();
                        if (protocolVersion) {
                            return [2 /*return*/, versionPredicate(protocolVersion)];
                        }
                        return [2 /*return*/, false];
                    case 6:
                        error_2 = _a.sent();
                        lastError = error_2;
                        return [3 /*break*/, 7];
                    case 7:
                        i++;
                        return [3 /*break*/, 2];
                    case 8:
                        if (lastError) {
                            throw lastError;
                        }
                        return [2 /*return*/, false];
                }
            });
        });
    };
    RoutingConnectionProvider.prototype.supportsMultiDb = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._hasProtocolVersion(function (version) { return version.isGreaterOrEqualTo(BOLT_PROTOCOL_V4_0); })];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    RoutingConnectionProvider.prototype.supportsTransactionConfig = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._hasProtocolVersion(function (version) { return version.isGreaterOrEqualTo(BOLT_PROTOCOL_V3); })];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    RoutingConnectionProvider.prototype.supportsUserImpersonation = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._hasProtocolVersion(function (version) { return version.isGreaterOrEqualTo(BOLT_PROTOCOL_V4_4); })];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    RoutingConnectionProvider.prototype.supportsSessionAuth = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._hasProtocolVersion(function (version) { return version.isGreaterOrEqualTo(BOLT_PROTOCOL_V5_1); })];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    RoutingConnectionProvider.prototype.getNegotiatedProtocolVersion = function () {
        var _this = this;
        return new Promise(function (resolve, reject) {
            _this._hasProtocolVersion(resolve)
                .catch(reject);
        });
    };
    RoutingConnectionProvider.prototype.verifyAuthentication = function (_a) {
        return __awaiter(this, arguments, void 0, function (_b) {
            var _this = this;
            var database = _b.database, accessMode = _b.accessMode, auth = _b.auth;
            return __generator(this, function (_c) {
                this._startTime = new Date().getTime();
                return [2 /*return*/, this._verifyAuthentication({
                        auth: auth,
                        getAddress: function () { return __awaiter(_this, void 0, void 0, function () {
                            var context, routingTable, servers;
                            return __generator(this, function (_a) {
                                switch (_a.label) {
                                    case 0:
                                        context = { database: database || DEFAULT_DB_NAME };
                                        return [4 /*yield*/, this._freshRoutingTable({
                                                accessMode: accessMode,
                                                database: context.database,
                                                auth: auth,
                                                onDatabaseNameResolved: function (databaseName) {
                                                    context.database = context.database || databaseName;
                                                }
                                            })];
                                    case 1:
                                        routingTable = _a.sent();
                                        servers = accessMode === WRITE ? routingTable.writers : routingTable.readers;
                                        if (servers.length === 0) {
                                            throw (0, neo4j_driver_core_1.newError)("No servers available for database '".concat(context.database, "' with access mode '").concat(accessMode, "'"), SERVICE_UNAVAILABLE);
                                        }
                                        return [2 /*return*/, servers[0]];
                                }
                            });
                        }); }
                    })];
            });
        });
    };
    RoutingConnectionProvider.prototype.verifyConnectivityAndGetServerInfo = function (_a) {
        return __awaiter(this, arguments, void 0, function (_b) {
            var context, routingTable, servers, error, servers_1, servers_1_1, address, serverInfo, e_1, e_2_1;
            var e_2, _c;
            var database = _b.database, accessMode = _b.accessMode;
            return __generator(this, function (_d) {
                switch (_d.label) {
                    case 0:
                        context = { database: database || DEFAULT_DB_NAME };
                        this._startTime = new Date().getTime();
                        return [4 /*yield*/, this._freshRoutingTable({
                                accessMode: accessMode,
                                database: context.database,
                                onDatabaseNameResolved: function (databaseName) {
                                    context.database = context.database || databaseName;
                                }
                            })];
                    case 1:
                        routingTable = _d.sent();
                        servers = accessMode === WRITE ? routingTable.writers : routingTable.readers;
                        error = (0, neo4j_driver_core_1.newError)("No servers available for database '".concat(context.database, "' with access mode '").concat(accessMode, "'"), SERVICE_UNAVAILABLE);
                        _d.label = 2;
                    case 2:
                        _d.trys.push([2, 9, 10, 11]);
                        servers_1 = __values(servers), servers_1_1 = servers_1.next();
                        _d.label = 3;
                    case 3:
                        if (!!servers_1_1.done) return [3 /*break*/, 8];
                        address = servers_1_1.value;
                        _d.label = 4;
                    case 4:
                        _d.trys.push([4, 6, , 7]);
                        return [4 /*yield*/, this._verifyConnectivityAndGetServerVersion({ address: address })];
                    case 5:
                        serverInfo = _d.sent();
                        return [2 /*return*/, serverInfo];
                    case 6:
                        e_1 = _d.sent();
                        error = e_1;
                        return [3 /*break*/, 7];
                    case 7:
                        servers_1_1 = servers_1.next();
                        return [3 /*break*/, 3];
                    case 8: return [3 /*break*/, 11];
                    case 9:
                        e_2_1 = _d.sent();
                        e_2 = { error: e_2_1 };
                        return [3 /*break*/, 11];
                    case 10:
                        try {
                            if (servers_1_1 && !servers_1_1.done && (_c = servers_1.return)) _c.call(servers_1);
                        }
                        finally { if (e_2) throw e_2.error; }
                        return [7 /*endfinally*/];
                    case 11: throw error;
                }
            });
        });
    };
    RoutingConnectionProvider.prototype.forget = function (address, database) {
        this._routingTableRegistry.apply(database, {
            applyWhenExists: function (routingTable) { return routingTable.forget(address); }
        });
        // We're firing and forgetting this operation explicitly and listening for any
        // errors to avoid unhandled promise rejection
        this._connectionPool.purge(address).catch(function () { });
    };
    RoutingConnectionProvider.prototype.forgetWriter = function (address, database) {
        this._routingTableRegistry.apply(database, {
            applyWhenExists: function (routingTable) { return routingTable.forgetWriter(address); }
        });
    };
    RoutingConnectionProvider.prototype._freshRoutingTable = function (_a) {
        var _b = _a === void 0 ? {} : _a, accessMode = _b.accessMode, database = _b.database, bookmarks = _b.bookmarks, impersonatedUser = _b.impersonatedUser, onDatabaseNameResolved = _b.onDatabaseNameResolved, auth = _b.auth;
        var currentRoutingTable = this._routingTableRegistry.get(database, function () { return new rediscovery_1.RoutingTable({ database: database }); });
        if (!currentRoutingTable.isStaleFor(accessMode)) {
            return currentRoutingTable;
        }
        this._log.info("Routing table is stale for database: \"".concat(database, "\" and access mode: \"").concat(accessMode, "\": ").concat(currentRoutingTable));
        return this._refreshRoutingTable(currentRoutingTable, bookmarks, impersonatedUser, auth)
            .then(function (newRoutingTable) {
            onDatabaseNameResolved(newRoutingTable.database);
            return newRoutingTable;
        });
    };
    RoutingConnectionProvider.prototype._refreshRoutingTable = function (currentRoutingTable, bookmarks, impersonatedUser, auth) {
        var knownRouters = currentRoutingTable.routers;
        if (this._useSeedRouter) {
            return this._fetchRoutingTableFromSeedRouterFallbackToKnownRouters(knownRouters, currentRoutingTable, bookmarks, impersonatedUser, auth);
        }
        return this._fetchRoutingTableFromKnownRoutersFallbackToSeedRouter(knownRouters, currentRoutingTable, bookmarks, impersonatedUser, auth);
    };
    RoutingConnectionProvider.prototype._fetchRoutingTableFromSeedRouterFallbackToKnownRouters = function (knownRouters, currentRoutingTable, bookmarks, impersonatedUser, auth) {
        return __awaiter(this, void 0, void 0, function () {
            var seenRouters, _a, newRoutingTable, error, _b, newRoutingTable2, error2;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        seenRouters = [];
                        return [4 /*yield*/, this._fetchRoutingTableUsingSeedRouter(seenRouters, this._seedRouter, currentRoutingTable, bookmarks, impersonatedUser, auth)];
                    case 1:
                        _a = __read.apply(void 0, [_c.sent(), 2]), newRoutingTable = _a[0], error = _a[1];
                        if (!newRoutingTable) return [3 /*break*/, 2];
                        this._useSeedRouter = false;
                        return [3 /*break*/, 4];
                    case 2: return [4 /*yield*/, this._fetchRoutingTableUsingKnownRouters(knownRouters, currentRoutingTable, bookmarks, impersonatedUser, auth)];
                    case 3:
                        _b = __read.apply(void 0, [_c.sent(), 2]), newRoutingTable2 = _b[0], error2 = _b[1];
                        newRoutingTable = newRoutingTable2;
                        error = error2 || error;
                        _c.label = 4;
                    case 4: return [4 /*yield*/, this._applyRoutingTableIfPossible(currentRoutingTable, newRoutingTable, error)];
                    case 5: return [2 /*return*/, _c.sent()];
                }
            });
        });
    };
    RoutingConnectionProvider.prototype._fetchRoutingTableFromKnownRoutersFallbackToSeedRouter = function (knownRouters, currentRoutingTable, bookmarks, impersonatedUser, auth) {
        return __awaiter(this, void 0, void 0, function () {
            var _a, newRoutingTable, error;
            var _b;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0: return [4 /*yield*/, this._fetchRoutingTableUsingKnownRouters(knownRouters, currentRoutingTable, bookmarks, impersonatedUser, auth)];
                    case 1:
                        _a = __read.apply(void 0, [_c.sent(), 2]), newRoutingTable = _a[0], error = _a[1];
                        if (!!newRoutingTable) return [3 /*break*/, 3];
                        return [4 /*yield*/, this._fetchRoutingTableUsingSeedRouter(knownRouters, this._seedRouter, currentRoutingTable, bookmarks, impersonatedUser, auth)];
                    case 2:
                        // none of the known routers returned a valid routing table - try to use seed router address for rediscovery
                        _b = __read.apply(void 0, [_c.sent(), 2]), newRoutingTable = _b[0], error = _b[1];
                        _c.label = 3;
                    case 3: return [4 /*yield*/, this._applyRoutingTableIfPossible(currentRoutingTable, newRoutingTable, error)];
                    case 4: return [2 /*return*/, _c.sent()];
                }
            });
        });
    };
    RoutingConnectionProvider.prototype._fetchRoutingTableUsingKnownRouters = function (knownRouters, currentRoutingTable, bookmarks, impersonatedUser, auth) {
        return __awaiter(this, void 0, void 0, function () {
            var _a, newRoutingTable, error, lastRouterIndex;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0: return [4 /*yield*/, this._fetchRoutingTable(knownRouters, currentRoutingTable, bookmarks, impersonatedUser, auth)];
                    case 1:
                        _a = __read.apply(void 0, [_b.sent(), 2]), newRoutingTable = _a[0], error = _a[1];
                        if (newRoutingTable) {
                            // one of the known routers returned a valid routing table - use it
                            return [2 /*return*/, [newRoutingTable, null]];
                        }
                        lastRouterIndex = knownRouters.length - 1;
                        RoutingConnectionProvider._forgetRouter(currentRoutingTable, knownRouters, lastRouterIndex);
                        return [2 /*return*/, [null, error]];
                }
            });
        });
    };
    RoutingConnectionProvider.prototype._fetchRoutingTableUsingSeedRouter = function (seenRouters, seedRouter, routingTable, bookmarks, impersonatedUser, auth) {
        return __awaiter(this, void 0, void 0, function () {
            var resolvedAddresses, newAddresses;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._resolveSeedRouter(seedRouter)
                        // filter out all addresses that we've already tried
                    ];
                    case 1:
                        resolvedAddresses = _a.sent();
                        newAddresses = resolvedAddresses.filter(function (address) { return seenRouters.indexOf(address) < 0; });
                        return [4 /*yield*/, this._fetchRoutingTable(newAddresses, routingTable, bookmarks, impersonatedUser, auth)];
                    case 2: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    RoutingConnectionProvider.prototype._resolveSeedRouter = function (seedRouter) {
        return __awaiter(this, void 0, void 0, function () {
            var resolvedAddresses, dnsResolvedAddresses;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._hostNameResolver.resolve(seedRouter)];
                    case 1:
                        resolvedAddresses = _a.sent();
                        return [4 /*yield*/, Promise.all(resolvedAddresses.map(function (address) { return _this._dnsResolver.resolve(address); }))];
                    case 2:
                        dnsResolvedAddresses = _a.sent();
                        return [2 /*return*/, [].concat.apply([], dnsResolvedAddresses)];
                }
            });
        });
    };
    RoutingConnectionProvider.prototype._fetchRoutingTable = function (routerAddresses, routingTable, bookmarks, impersonatedUser, auth) {
        return __awaiter(this, void 0, void 0, function () {
            var _this = this;
            return __generator(this, function (_a) {
                return [2 /*return*/, routerAddresses.reduce(function (refreshedTablePromise, currentRouter, currentIndex) { return __awaiter(_this, void 0, void 0, function () {
                        var _a, newRoutingTable, previousRouterIndex, _b, session, error, error_3;
                        return __generator(this, function (_c) {
                            switch (_c.label) {
                                case 0: return [4 /*yield*/, refreshedTablePromise];
                                case 1:
                                    _a = __read.apply(void 0, [_c.sent(), 1]), newRoutingTable = _a[0];
                                    if (newRoutingTable) {
                                        // valid routing table was fetched - just return it, try next router otherwise
                                        return [2 /*return*/, [newRoutingTable, null]];
                                    }
                                    else {
                                        previousRouterIndex = currentIndex - 1;
                                        RoutingConnectionProvider._forgetRouter(routingTable, routerAddresses, previousRouterIndex);
                                    }
                                    return [4 /*yield*/, this._createSessionForRediscovery(currentRouter, bookmarks, impersonatedUser, auth)];
                                case 2:
                                    _b = __read.apply(void 0, [_c.sent(), 2]), session = _b[0], error = _b[1];
                                    if (!session) return [3 /*break*/, 8];
                                    _c.label = 3;
                                case 3:
                                    _c.trys.push([3, 5, 6, 7]);
                                    return [4 /*yield*/, this._rediscovery.lookupRoutingTableOnRouter(session, routingTable.database, currentRouter, impersonatedUser)];
                                case 4: return [2 /*return*/, [_c.sent(), null]];
                                case 5:
                                    error_3 = _c.sent();
                                    return [2 /*return*/, this._handleRediscoveryError(error_3, currentRouter)];
                                case 6:
                                    session.close();
                                    return [7 /*endfinally*/];
                                case 7: return [3 /*break*/, 9];
                                case 8: 
                                // unable to acquire connection and create session towards the current router
                                // return null to signal that the next router should be tried
                                return [2 /*return*/, [null, error]];
                                case 9: return [2 /*return*/];
                            }
                        });
                    }); }, Promise.resolve([null, null]))];
            });
        });
    };
    RoutingConnectionProvider.prototype._createSessionForRediscovery = function (routerAddress, bookmarks, impersonatedUser, auth) {
        return __awaiter(this, void 0, void 0, function () {
            var connection, databaseSpecificErrorHandler, delegateConnection, connectionProvider, protocolVersion, error_4;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        _a.trys.push([0, 4, , 5]);
                        return [4 /*yield*/, this._connectionPool.acquire({ auth: auth, startTime: this._startTime }, routerAddress)];
                    case 1:
                        connection = _a.sent();
                        if (!auth) return [3 /*break*/, 3];
                        return [4 /*yield*/, this._verifyStickyConnection({
                                auth: auth,
                                connection: connection,
                                address: routerAddress
                            })];
                    case 2:
                        _a.sent();
                        _a.label = 3;
                    case 3:
                        databaseSpecificErrorHandler = connection_1.ConnectionErrorHandler.create({
                            errorCode: SESSION_EXPIRED,
                            handleSecurityError: function (error, address, conn) { return _this._handleSecurityError(error, address, conn); }
                        });
                        delegateConnection = !connection._sticky
                            ? new connection_1.DelegateConnection(connection, databaseSpecificErrorHandler)
                            : new connection_1.DelegateConnection(connection);
                        connectionProvider = new connection_provider_single_1.default(delegateConnection);
                        protocolVersion = connection.protocol().version;
                        if (protocolVersion.isLessThan({ major: 4, minor: 0 })) {
                            return [2 /*return*/, [new neo4j_driver_core_1.Session({
                                        mode: WRITE,
                                        bookmarks: Bookmarks.empty(),
                                        connectionProvider: connectionProvider
                                    }), null]];
                        }
                        return [2 /*return*/, [new neo4j_driver_core_1.Session({
                                    mode: READ,
                                    database: SYSTEM_DB_NAME,
                                    bookmarks: bookmarks,
                                    connectionProvider: connectionProvider,
                                    impersonatedUser: impersonatedUser
                                }), null]];
                    case 4:
                        error_4 = _a.sent();
                        return [2 /*return*/, this._handleRediscoveryError(error_4, routerAddress)];
                    case 5: return [2 /*return*/];
                }
            });
        });
    };
    RoutingConnectionProvider.prototype._handleRediscoveryError = function (error, routerAddress) {
        if (_isFailFastError(error) || _isFailFastSecurityError(error)) {
            throw error;
        }
        else if (error.code === PROCEDURE_NOT_FOUND_CODE) {
            // throw when getServers procedure not found because this is clearly a configuration issue
            throw (0, neo4j_driver_core_1.newError)("Server at ".concat(routerAddress.asHostPort(), " can't perform routing. Make sure you are connecting to a causal cluster"), SERVICE_UNAVAILABLE, error);
        }
        this._log.warn("unable to fetch routing table because of an error ".concat(error));
        return [null, error];
    };
    RoutingConnectionProvider.prototype._applyRoutingTableIfPossible = function (currentRoutingTable, newRoutingTable, error) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        if (!newRoutingTable) {
                            // none of routing servers returned valid routing table, throw exception
                            throw (0, neo4j_driver_core_1.newError)("Could not perform discovery. No routing servers available. Known routing table: ".concat(currentRoutingTable), SERVICE_UNAVAILABLE, error);
                        }
                        if (newRoutingTable.writers.length === 0) {
                            // use seed router next time. this is important when cluster is partitioned. it tries to make sure driver
                            // does not always get routing table without writers because it talks exclusively to a minority partition
                            this._useSeedRouter = true;
                        }
                        return [4 /*yield*/, this._updateRoutingTable(newRoutingTable)];
                    case 1:
                        _a.sent();
                        return [2 /*return*/, newRoutingTable];
                }
            });
        });
    };
    RoutingConnectionProvider.prototype._updateRoutingTable = function (newRoutingTable) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: 
                    // close old connections to servers not present in the new routing table
                    return [4 /*yield*/, this._connectionPool.keepAll(newRoutingTable.allServers())];
                    case 1:
                        // close old connections to servers not present in the new routing table
                        _a.sent();
                        this._routingTableRegistry.removeExpired();
                        this._routingTableRegistry.register(newRoutingTable);
                        this._log.info("Updated routing table ".concat(newRoutingTable));
                        return [2 /*return*/];
                }
            });
        });
    };
    RoutingConnectionProvider._forgetRouter = function (routingTable, routersArray, routerIndex) {
        var address = routersArray[routerIndex];
        if (routingTable && address) {
            routingTable.forgetRouter(address);
        }
    };
    RoutingConnectionProvider.prototype._channelSsrCallback = function (isEnabled, action) {
        if (action === 'OPEN') {
            if (isEnabled === true) {
                this._withSSR = this._withSSR + 1;
            }
            else {
                this._withoutSSR = this._withoutSSR + 1;
            }
        }
        else if (action === 'CLOSE') {
            if (isEnabled === true) {
                this._withSSR = this._withSSR - 1;
            }
            else {
                this._withoutSSR = this._withoutSSR - 1;
            }
        }
        else {
            throw (0, neo4j_driver_core_1.newError)("Channel SSR Callback invoked with action other than 'OPEN' or 'CLOSE'");
        }
    };
    RoutingConnectionProvider.prototype.SSREnabled = function () {
        return this._withSSR > 0 && this._withoutSSR === 0;
    };
    return RoutingConnectionProvider;
}(connection_provider_pooled_1.default));
exports.default = RoutingConnectionProvider;
/**
 * Responsible for keeping track of the existing routing tables
 */
var RoutingTableRegistry = /** @class */ (function () {
    /**
     * Constructor
     * @param {int} routingTablePurgeDelay The routing table purge delay
     */
    function RoutingTableRegistry(routingTablePurgeDelay) {
        this._tables = new Map();
        this._routingTablePurgeDelay = routingTablePurgeDelay;
    }
    /**
     * Put a routing table in the registry
     *
     * @param {RoutingTable} table The routing table
     * @returns {RoutingTableRegistry} this
     */
    RoutingTableRegistry.prototype.register = function (table) {
        this._tables.set(table.database, table);
        return this;
    };
    /**
     * Apply function in the routing table for an specific database. If the database name is not defined, the function will
     * be applied for each element
     *
     * @param {string} database The database name
     * @param {object} callbacks The actions
     * @param {function (RoutingTable)} callbacks.applyWhenExists Call when the db exists or when the database property is not informed
     * @param {function ()} callbacks.applyWhenDontExists Call when the database doesn't have the routing table registred
     * @returns {RoutingTableRegistry} this
     */
    RoutingTableRegistry.prototype.apply = function (database, _a) {
        var _b = _a === void 0 ? {} : _a, applyWhenExists = _b.applyWhenExists, _c = _b.applyWhenDontExists, applyWhenDontExists = _c === void 0 ? function () { } : _c;
        if (this._tables.has(database)) {
            applyWhenExists(this._tables.get(database));
        }
        else if (typeof database === 'string' || database === null) {
            applyWhenDontExists();
        }
        else {
            this._forEach(applyWhenExists);
        }
        return this;
    };
    /**
     * Retrieves a routing table from a given database name
     *
     * @param {string|impersonatedUser} impersonatedUser The impersonated User
     * @param {string} database The database name
     * @param {function()|RoutingTable} defaultSupplier The routing table supplier, if it's not a function or not exists, it will return itself as default value
     * @returns {RoutingTable} The routing table for the respective database
     */
    RoutingTableRegistry.prototype.get = function (database, defaultSupplier) {
        if (this._tables.has(database)) {
            return this._tables.get(database);
        }
        return typeof defaultSupplier === 'function'
            ? defaultSupplier()
            : defaultSupplier;
    };
    /**
     * Remove the routing table which is already expired
     * @returns {RoutingTableRegistry} this
     */
    RoutingTableRegistry.prototype.removeExpired = function () {
        var _this = this;
        return this._removeIf(function (value) {
            return value.isExpiredFor(_this._routingTablePurgeDelay);
        });
    };
    RoutingTableRegistry.prototype._forEach = function (apply) {
        var e_3, _a;
        try {
            for (var _b = __values(this._tables), _c = _b.next(); !_c.done; _c = _b.next()) {
                var _d = __read(_c.value, 2), value = _d[1];
                apply(value);
            }
        }
        catch (e_3_1) { e_3 = { error: e_3_1 }; }
        finally {
            try {
                if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
            }
            finally { if (e_3) throw e_3.error; }
        }
        return this;
    };
    RoutingTableRegistry.prototype._remove = function (key) {
        this._tables.delete(key);
        return this;
    };
    RoutingTableRegistry.prototype._removeIf = function (predicate) {
        var e_4, _a;
        try {
            for (var _b = __values(this._tables), _c = _b.next(); !_c.done; _c = _b.next()) {
                var _d = __read(_c.value, 2), key = _d[0], value = _d[1];
                if (predicate(value)) {
                    this._remove(key);
                }
            }
        }
        catch (e_4_1) { e_4 = { error: e_4_1 }; }
        finally {
            try {
                if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
            }
            finally { if (e_4) throw e_4.error; }
        }
        return this;
    };
    return RoutingTableRegistry;
}());
function _isFailFastError(error) {
    return [
        DATABASE_NOT_FOUND_CODE,
        INVALID_BOOKMARK_CODE,
        INVALID_BOOKMARK_MIXTURE_CODE,
        INVALID_ARGUMENT_ERROR,
        INVALID_REQUEST_ERROR,
        STATEMENT_TYPE_ERROR,
        NOT_AVAILABLE
    ].includes(error.code);
}
function _isFailFastSecurityError(error) {
    var _a;
    return ((_a = error.code) === null || _a === void 0 ? void 0 : _a.startsWith('Neo.ClientError.Security.')) &&
        ![
            AUTHORIZATION_EXPIRED_CODE
        ].includes(error.code);
}
