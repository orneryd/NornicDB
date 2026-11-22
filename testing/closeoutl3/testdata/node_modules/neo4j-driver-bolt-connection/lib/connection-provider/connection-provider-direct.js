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
var connection_provider_pooled_1 = __importDefault(require("./connection-provider-pooled"));
var connection_1 = require("../connection");
var neo4j_driver_core_1 = require("neo4j-driver-core");
var _a = neo4j_driver_core_1.internal.constants, BOLT_PROTOCOL_V3 = _a.BOLT_PROTOCOL_V3, BOLT_PROTOCOL_V4_0 = _a.BOLT_PROTOCOL_V4_0, BOLT_PROTOCOL_V4_4 = _a.BOLT_PROTOCOL_V4_4, BOLT_PROTOCOL_V5_1 = _a.BOLT_PROTOCOL_V5_1;
var SERVICE_UNAVAILABLE = neo4j_driver_core_1.error.SERVICE_UNAVAILABLE;
var DirectConnectionProvider = /** @class */ (function (_super) {
    __extends(DirectConnectionProvider, _super);
    function DirectConnectionProvider(_a) {
        var id = _a.id, config = _a.config, log = _a.log, address = _a.address, userAgent = _a.userAgent, boltAgent = _a.boltAgent, authTokenManager = _a.authTokenManager, newPool = _a.newPool;
        var _this = _super.call(this, { id: id, config: config, log: log, userAgent: userAgent, boltAgent: boltAgent, authTokenManager: authTokenManager, newPool: newPool }) || this;
        _this._address = address;
        return _this;
    }
    /**
     * See {@link ConnectionProvider} for more information about this method and
     * its arguments.
     */
    DirectConnectionProvider.prototype.acquireConnection = function () {
        return __awaiter(this, arguments, void 0, function (_a) {
            var databaseSpecificErrorHandler, connection;
            var _this = this;
            var _b = _a === void 0 ? {} : _a, accessMode = _b.accessMode, database = _b.database, bookmarks = _b.bookmarks, auth = _b.auth, forceReAuth = _b.forceReAuth;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        databaseSpecificErrorHandler = connection_1.ConnectionErrorHandler.create({
                            errorCode: SERVICE_UNAVAILABLE,
                            handleSecurityError: function (error, address, conn) {
                                return _this._handleSecurityError(error, address, conn, database);
                            }
                        });
                        return [4 /*yield*/, this._connectionPool.acquire({ auth: auth, forceReAuth: forceReAuth }, this._address)];
                    case 1:
                        connection = _c.sent();
                        if (!auth) return [3 /*break*/, 3];
                        return [4 /*yield*/, this._verifyStickyConnection({
                                auth: auth,
                                connection: connection,
                                address: this._address
                            })];
                    case 2:
                        _c.sent();
                        return [2 /*return*/, connection];
                    case 3: return [2 /*return*/, new connection_1.DelegateConnection(connection, databaseSpecificErrorHandler)];
                }
            });
        });
    };
    DirectConnectionProvider.prototype._handleSecurityError = function (error, address, connection, database) {
        this._log.warn("Direct driver ".concat(this._id, " will close connection to ").concat(address, " for database '").concat(database, "' because of an error ").concat(error.code, " '").concat(error.message, "'"));
        return _super.prototype._handleSecurityError.call(this, error, address, connection);
    };
    DirectConnectionProvider.prototype._hasProtocolVersion = function (versionPredicate) {
        return __awaiter(this, void 0, void 0, function () {
            var connection, protocolVersion;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._createChannelConnection(this._address)];
                    case 1:
                        connection = _a.sent();
                        protocolVersion = connection.protocol()
                            ? connection.protocol().version
                            : null;
                        return [4 /*yield*/, connection.close()];
                    case 2:
                        _a.sent();
                        if (protocolVersion) {
                            return [2 /*return*/, versionPredicate(protocolVersion)];
                        }
                        return [2 /*return*/, false];
                }
            });
        });
    };
    DirectConnectionProvider.prototype.supportsMultiDb = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._hasProtocolVersion(function (version) { return version.isGreaterOrEqualTo(BOLT_PROTOCOL_V4_0); })];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    DirectConnectionProvider.prototype.getNegotiatedProtocolVersion = function () {
        var _this = this;
        return new Promise(function (resolve, reject) {
            _this._hasProtocolVersion(resolve)
                .catch(reject);
        });
    };
    DirectConnectionProvider.prototype.supportsTransactionConfig = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._hasProtocolVersion(function (version) { return version.isGreaterOrEqualTo(BOLT_PROTOCOL_V3); })];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    DirectConnectionProvider.prototype.supportsUserImpersonation = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._hasProtocolVersion(function (version) { return version.isGreaterOrEqualTo(BOLT_PROTOCOL_V4_4); })];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    DirectConnectionProvider.prototype.supportsSessionAuth = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._hasProtocolVersion(function (version) { return version.isGreaterOrEqualTo(BOLT_PROTOCOL_V5_1); })];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    DirectConnectionProvider.prototype.verifyAuthentication = function (_a) {
        return __awaiter(this, arguments, void 0, function (_b) {
            var _this = this;
            var auth = _b.auth;
            return __generator(this, function (_c) {
                return [2 /*return*/, this._verifyAuthentication({
                        auth: auth,
                        getAddress: function () { return _this._address; }
                    })];
            });
        });
    };
    DirectConnectionProvider.prototype.verifyConnectivityAndGetServerInfo = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._verifyConnectivityAndGetServerVersion({ address: this._address })];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    return DirectConnectionProvider;
}(connection_provider_pooled_1.default));
exports.default = DirectConnectionProvider;
