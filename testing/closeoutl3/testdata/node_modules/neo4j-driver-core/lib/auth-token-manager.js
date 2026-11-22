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
exports.authTokenManagers = exports.AuthTokenAndExpiration = void 0;
exports.staticAuthTokenManager = staticAuthTokenManager;
var auth_1 = __importDefault(require("./auth"));
var internal_1 = require("./internal");
/**
 * Interface for the piece of software responsible for keeping track of current active {@link AuthToken} across the driver.
 * @interface
 * @since 5.14
 */
var AuthTokenManager = /** @class */ (function () {
    function AuthTokenManager() {
    }
    /**
     * Returns a valid token.
     *
     * **Warning**: This method must only ever return auth information belonging to the same identity.
     * Switching identities using the `AuthTokenManager` is undefined behavior.
     *
     * @returns {Promise<AuthToken>|AuthToken} The valid auth token or a promise for a valid auth token
     */
    AuthTokenManager.prototype.getToken = function () {
        throw new Error('Not Implemented');
    };
    /**
     * Handles an error notification emitted by the server if a security error happened.
     *
     * @param {AuthToken} token The expired token.
     * @param {`Neo.ClientError.Security.${string}`} securityErrorCode the security error code returned by the server
     * @return {boolean} whether the exception was handled by the manager, so the driver knows if it can be retried..
     */
    AuthTokenManager.prototype.handleSecurityException = function (token, securityErrorCode) {
        throw new Error('Not implemented');
    };
    return AuthTokenManager;
}());
exports.default = AuthTokenManager;
/**
 * Interface which defines an {@link AuthToken} with an expiration data time associated
 * @interface
 * @since 5.14
 */
var AuthTokenAndExpiration = /** @class */ (function () {
    function AuthTokenAndExpiration() {
        /**
         * The {@link AuthToken} used for authenticate connections.
         *
         * @type {AuthToken}
         * @see {auth}
         */
        this.token = auth_1.default.none();
        /**
         * The expected expiration date of the auth token.
         *
         * This information will be used for triggering the auth token refresh
         * in managers created with {@link authTokenManagers#bearer}.
         *
         * If this value is not defined, the {@link AuthToken} will be considered valid
         * until a `Neo.ClientError.Security.TokenExpired` error happens.
         *
         * @type {Date|undefined}
         */
        this.expiration = undefined;
    }
    return AuthTokenAndExpiration;
}());
exports.AuthTokenAndExpiration = AuthTokenAndExpiration;
/**
 * Defines the object which holds the common {@link AuthTokenManager} used in the Driver
 */
var AuthTokenManagers = /** @class */ (function () {
    function AuthTokenManagers() {
    }
    /**
     * Creates a {@link AuthTokenManager} for handle {@link AuthToken} which is expires.
     *
     * **Warning**: `tokenProvider` must only ever return auth information belonging to the same identity.
     * Switching identities using the `AuthTokenManager` is undefined behavior.
     *
     * @param {object} param0 - The params
     * @param {function(): Promise<AuthTokenAndExpiration>} param0.tokenProvider - Retrieves a new valid auth token.
     * Must only ever return auth information belonging to the same identity.
     * @returns {AuthTokenManager} The temporal auth data manager.
     */
    AuthTokenManagers.prototype.bearer = function (_a) {
        var tokenProvider = _a.tokenProvider;
        if (typeof tokenProvider !== 'function') {
            throw new TypeError("tokenProvider should be function, but got: ".concat(typeof tokenProvider));
        }
        return new ExpirationBasedAuthTokenManager(tokenProvider, [
            'Neo.ClientError.Security.Unauthorized',
            'Neo.ClientError.Security.TokenExpired'
        ]);
    };
    /**
     * Creates a {@link AuthTokenManager} for handle {@link AuthToken} and password rotation.
     *
     * **Warning**: `tokenProvider` must only ever return auth information belonging to the same identity.
     * Switching identities using the `AuthTokenManager` is undefined behavior.
     *
     * @param {object} param0 - The params
     * @param {function(): Promise<AuthToken>} param0.tokenProvider - Retrieves a new valid auth token.
     * Must only ever return auth information belonging to the same identity.
     * @returns {AuthTokenManager} The basic auth data manager.
     */
    AuthTokenManagers.prototype.basic = function (_a) {
        var _this = this;
        var tokenProvider = _a.tokenProvider;
        if (typeof tokenProvider !== 'function') {
            throw new TypeError("tokenProvider should be function, but got: ".concat(typeof tokenProvider));
        }
        return new ExpirationBasedAuthTokenManager(function () { return __awaiter(_this, void 0, void 0, function () {
            var _a;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        _a = {};
                        return [4 /*yield*/, tokenProvider()];
                    case 1: return [2 /*return*/, (_a.token = _b.sent(), _a)];
                }
            });
        }); }, ['Neo.ClientError.Security.Unauthorized']);
    };
    return AuthTokenManagers;
}());
/**
 * Holds the common {@link AuthTokenManagers} used in the Driver.
 */
var authTokenManagers = new AuthTokenManagers();
exports.authTokenManagers = authTokenManagers;
Object.freeze(authTokenManagers);
/**
 * Create a {@link AuthTokenManager} for handle static {@link AuthToken}
 *
 * @private
 * @param {param} args - The args
 * @param {AuthToken} args.authToken - The static auth token which will always used in the driver.
 * @returns {AuthTokenManager} The temporal auth data manager.
 */
function staticAuthTokenManager(_a) {
    var authToken = _a.authToken;
    return new StaticAuthTokenManager(authToken);
}
var TokenRefreshObservable = /** @class */ (function () {
    function TokenRefreshObservable(_subscribers) {
        if (_subscribers === void 0) { _subscribers = []; }
        this._subscribers = _subscribers;
    }
    TokenRefreshObservable.prototype.subscribe = function (sub) {
        this._subscribers.push(sub);
    };
    TokenRefreshObservable.prototype.onCompleted = function (data) {
        this._subscribers.forEach(function (sub) { return sub.onCompleted(data); });
    };
    TokenRefreshObservable.prototype.onError = function (error) {
        this._subscribers.forEach(function (sub) { return sub.onError(error); });
    };
    return TokenRefreshObservable;
}());
var ExpirationBasedAuthTokenManager = /** @class */ (function () {
    function ExpirationBasedAuthTokenManager(_tokenProvider, _handledSecurityCodes, _currentAuthData, _refreshObservable) {
        this._tokenProvider = _tokenProvider;
        this._handledSecurityCodes = _handledSecurityCodes;
        this._currentAuthData = _currentAuthData;
        this._refreshObservable = _refreshObservable;
    }
    ExpirationBasedAuthTokenManager.prototype.getToken = function () {
        return __awaiter(this, void 0, void 0, function () {
            var _a;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        if (!(this._currentAuthData === undefined ||
                            (this._currentAuthData.expiration !== undefined &&
                                this._currentAuthData.expiration < new Date()))) return [3 /*break*/, 2];
                        return [4 /*yield*/, this._refreshAuthToken()];
                    case 1:
                        _b.sent();
                        _b.label = 2;
                    case 2: return [2 /*return*/, (_a = this._currentAuthData) === null || _a === void 0 ? void 0 : _a.token];
                }
            });
        });
    };
    ExpirationBasedAuthTokenManager.prototype.handleSecurityException = function (token, securityErrorCode) {
        var _a;
        if (this._handledSecurityCodes.includes(securityErrorCode)) {
            if (internal_1.util.equals(token, (_a = this._currentAuthData) === null || _a === void 0 ? void 0 : _a.token)) {
                this._scheduleRefreshAuthToken();
            }
            return true;
        }
        return false;
    };
    ExpirationBasedAuthTokenManager.prototype._scheduleRefreshAuthToken = function (observer) {
        var _this = this;
        if (this._refreshObservable === undefined) {
            this._currentAuthData = undefined;
            this._refreshObservable = new TokenRefreshObservable();
            Promise.resolve(this._tokenProvider())
                .then(function (data) {
                var _a;
                _this._currentAuthData = data;
                (_a = _this._refreshObservable) === null || _a === void 0 ? void 0 : _a.onCompleted(data);
            })
                .catch(function (error) {
                var _a;
                (_a = _this._refreshObservable) === null || _a === void 0 ? void 0 : _a.onError(error);
            })
                .finally(function () {
                _this._refreshObservable = undefined;
            });
        }
        if (observer !== undefined) {
            this._refreshObservable.subscribe(observer);
        }
    };
    ExpirationBasedAuthTokenManager.prototype._refreshAuthToken = function () {
        return __awaiter(this, void 0, void 0, function () {
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, new Promise(function (resolve, reject) {
                            _this._scheduleRefreshAuthToken({
                                onCompleted: resolve,
                                onError: reject
                            });
                        })];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    return ExpirationBasedAuthTokenManager;
}());
var StaticAuthTokenManager = /** @class */ (function () {
    function StaticAuthTokenManager(_authToken) {
        this._authToken = _authToken;
    }
    StaticAuthTokenManager.prototype.getToken = function () {
        return this._authToken;
    };
    StaticAuthTokenManager.prototype.handleSecurityException = function (_, __) {
        return false;
    };
    return StaticAuthTokenManager;
}());
