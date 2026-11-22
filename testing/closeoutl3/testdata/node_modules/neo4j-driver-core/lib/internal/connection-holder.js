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
Object.defineProperty(exports, "__esModule", { value: true });
exports.EMPTY_CONNECTION_HOLDER = exports.ReadOnlyConnectionHolder = exports.ConnectionHolder = void 0;
var error_1 = require("../error");
var util_1 = require("./util");
var constants_1 = require("./constants");
var bookmarks_1 = require("./bookmarks");
var logger_1 = require("./logger");
/**
 * Utility to lazily initialize connections and return them back to the pool when unused.
 * @private
 */
var ConnectionHolder = /** @class */ (function () {
    /**
     * @constructor
     * @param {object} params
     * @property {string} params.mode - the access mode for new connection holder.
     * @property {string} params.database - the target database name.
     * @property {Bookmarks} params.bookmarks - initial bookmarks
     * @property {ConnectionProvider} params.connectionProvider - the connection provider to acquire connections from.
     * @property {string?} params.impersonatedUser - the user which will be impersonated
     * @property {function(databaseName:string)} params.onDatabaseNameResolved - callback called when the database name is resolved
     * @property {function():Promise<Bookmarks>} params.getConnectionAcquistionBookmarks - called for getting Bookmarks for acquiring connections
     * @property {AuthToken} params.auth - the target auth for the to-be-acquired connection
     */
    function ConnectionHolder(_a) {
        var mode = _a.mode, _b = _a.database, database = _b === void 0 ? '' : _b, bookmarks = _a.bookmarks, connectionProvider = _a.connectionProvider, impersonatedUser = _a.impersonatedUser, onDatabaseNameResolved = _a.onDatabaseNameResolved, getConnectionAcquistionBookmarks = _a.getConnectionAcquistionBookmarks, auth = _a.auth, log = _a.log;
        this._mode = mode !== null && mode !== void 0 ? mode : constants_1.ACCESS_MODE_WRITE;
        this._closed = false;
        this._database = database != null ? (0, util_1.assertString)(database, 'database') : '';
        this._bookmarks = bookmarks !== null && bookmarks !== void 0 ? bookmarks : bookmarks_1.Bookmarks.empty();
        this._connectionProvider = connectionProvider;
        this._impersonatedUser = impersonatedUser;
        this._referenceCount = 0;
        this._connectionPromise = Promise.resolve(null);
        this._onDatabaseNameResolved = onDatabaseNameResolved;
        this._auth = auth;
        this._log = log;
        this._logError = this._logError.bind(this);
        this._getConnectionAcquistionBookmarks = getConnectionAcquistionBookmarks !== null && getConnectionAcquistionBookmarks !== void 0 ? getConnectionAcquistionBookmarks : (function () { return Promise.resolve(bookmarks_1.Bookmarks.empty()); });
    }
    ConnectionHolder.prototype.mode = function () {
        return this._mode;
    };
    ConnectionHolder.prototype.database = function () {
        return this._database;
    };
    ConnectionHolder.prototype.setDatabase = function (database) {
        this._database = database;
    };
    ConnectionHolder.prototype.bookmarks = function () {
        return this._bookmarks;
    };
    ConnectionHolder.prototype.connectionProvider = function () {
        return this._connectionProvider;
    };
    ConnectionHolder.prototype.referenceCount = function () {
        return this._referenceCount;
    };
    ConnectionHolder.prototype.initializeConnection = function (homeDatabase) {
        if (this._referenceCount === 0 && (this._connectionProvider != null)) {
            this._connectionPromise = this._createConnectionPromise(this._connectionProvider, homeDatabase);
        }
        else {
            this._referenceCount++;
            return false;
        }
        this._referenceCount++;
        return true;
    };
    ConnectionHolder.prototype._createConnectionPromise = function (connectionProvider, homeDatabase) {
        return __awaiter(this, void 0, void 0, function () {
            var _a, _b;
            var _c;
            return __generator(this, function (_d) {
                switch (_d.label) {
                    case 0:
                        _b = (_a = connectionProvider).acquireConnection;
                        _c = {
                            accessMode: this._mode,
                            database: this._database
                        };
                        return [4 /*yield*/, this._getBookmarks()];
                    case 1: return [4 /*yield*/, _b.apply(_a, [(_c.bookmarks = _d.sent(),
                                _c.impersonatedUser = this._impersonatedUser,
                                _c.onDatabaseNameResolved = this._onDatabaseNameResolved,
                                _c.auth = this._auth,
                                _c.homeDb = homeDatabase,
                                _c)])];
                    case 2: return [2 /*return*/, _d.sent()];
                }
            });
        });
    };
    ConnectionHolder.prototype._getBookmarks = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._getConnectionAcquistionBookmarks()];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    ConnectionHolder.prototype.getConnection = function () {
        return this._connectionPromise;
    };
    ConnectionHolder.prototype.releaseConnection = function () {
        if (this._referenceCount === 0) {
            return this._connectionPromise;
        }
        this._referenceCount--;
        if (this._referenceCount === 0) {
            return this._releaseConnection();
        }
        return this._connectionPromise;
    };
    ConnectionHolder.prototype.close = function (hasTx) {
        this._closed = true;
        if (this._referenceCount === 0) {
            return this._connectionPromise;
        }
        this._referenceCount = 0;
        return this._releaseConnection(hasTx);
    };
    ConnectionHolder.prototype.log = function () {
        return this._log;
    };
    /**
     * Return the current pooled connection instance to the connection pool.
     * We don't pool Session instances, to avoid users using the Session after they've called close.
     * The `Session` object is just a thin wrapper around Connection anyway, so it makes little difference.
     * @return {Promise} - promise resolved then connection is returned to the pool.
     * @private
     */
    ConnectionHolder.prototype._releaseConnection = function (hasTx) {
        this._connectionPromise = this._connectionPromise
            .then(function (connection) {
            if (connection != null) {
                if (connection.isOpen() && (connection.hasOngoingObservableRequests() || hasTx === true)) {
                    return connection
                        .resetAndFlush()
                        .catch(ignoreError)
                        .then(function () { return connection.release().then(function () { return null; }); });
                }
                return connection.release().then(function () { return null; });
            }
            else {
                return Promise.resolve(null);
            }
        })
            .catch(this._logError);
        return this._connectionPromise;
    };
    ConnectionHolder.prototype._logError = function (error) {
        if (this._log.isWarnEnabled()) {
            // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
            this._log.warn("ConnectionHolder got an error while releasing the connection. Error ".concat(error, ". Stacktrace: ").concat(error.stack));
        }
        return null;
    };
    return ConnectionHolder;
}());
exports.ConnectionHolder = ConnectionHolder;
/**
 * Provides a interaction with a ConnectionHolder without change it state by
 * releasing or initilizing
 */
var ReadOnlyConnectionHolder = /** @class */ (function (_super) {
    __extends(ReadOnlyConnectionHolder, _super);
    /**
     * Constructor
     * @param {ConnectionHolder} connectionHolder the connection holder which will treat the requests
     */
    function ReadOnlyConnectionHolder(connectionHolder) {
        var _this = _super.call(this, {
            mode: connectionHolder.mode(),
            database: connectionHolder.database(),
            bookmarks: connectionHolder.bookmarks(),
            // @ts-expect-error
            getConnectionAcquistionBookmarks: connectionHolder._getConnectionAcquistionBookmarks,
            connectionProvider: connectionHolder.connectionProvider(),
            log: connectionHolder.log()
        }) || this;
        _this._connectionHolder = connectionHolder;
        return _this;
    }
    /**
     * Return the true if the connection is suppose to be initilized with the command.
     *
     * @return {boolean}
     */
    ReadOnlyConnectionHolder.prototype.initializeConnection = function () {
        if (this._connectionHolder.referenceCount() === 0) {
            return false;
        }
        return true;
    };
    /**
     * Get the current connection promise.
     * @return {Promise<Connection>} promise resolved with the current connection.
     */
    ReadOnlyConnectionHolder.prototype.getConnection = function () {
        return this._connectionHolder.getConnection();
    };
    /**
     * Get the current connection promise, doesn't performs the release
     * @return {Promise<Connection>} promise with the resolved current connection
     */
    ReadOnlyConnectionHolder.prototype.releaseConnection = function () {
        return this._connectionHolder.getConnection().catch(function () { return Promise.resolve(null); });
    };
    /**
     * Get the current connection promise, doesn't performs the connection close
     * @return {Promise<Connection>} promise with the resolved current connection
     */
    ReadOnlyConnectionHolder.prototype.close = function () {
        return this._connectionHolder.getConnection().catch(function () { return Promise.resolve(null); });
    };
    return ReadOnlyConnectionHolder;
}(ConnectionHolder));
exports.ReadOnlyConnectionHolder = ReadOnlyConnectionHolder;
exports.default = ReadOnlyConnectionHolder;
var EmptyConnectionHolder = /** @class */ (function (_super) {
    __extends(EmptyConnectionHolder, _super);
    function EmptyConnectionHolder() {
        return _super.call(this, {
            // Empty logger
            log: logger_1.Logger.create({})
        }) || this;
    }
    EmptyConnectionHolder.prototype.mode = function () {
        return undefined;
    };
    EmptyConnectionHolder.prototype.database = function () {
        return undefined;
    };
    EmptyConnectionHolder.prototype.initializeConnection = function () {
        // nothing to initialize
        return true;
    };
    EmptyConnectionHolder.prototype.getConnection = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, Promise.reject((0, error_1.newError)('This connection holder does not serve connections'))];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    EmptyConnectionHolder.prototype.releaseConnection = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, Promise.resolve(null)];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    EmptyConnectionHolder.prototype.close = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, Promise.resolve(null)];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    return EmptyConnectionHolder;
}(ConnectionHolder));
/**
 * Connection holder that does not manage any connections.
 * @type {ConnectionHolder}
 * @private
 */
var EMPTY_CONNECTION_HOLDER = new EmptyConnectionHolder();
exports.EMPTY_CONNECTION_HOLDER = EMPTY_CONNECTION_HOLDER;
// eslint-disable-next-line n/handle-callback-err
function ignoreError(error) {
    return null;
}
