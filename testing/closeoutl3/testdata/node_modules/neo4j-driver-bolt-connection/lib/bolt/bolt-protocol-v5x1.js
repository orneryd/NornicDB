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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
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
var bolt_protocol_v5x0_1 = __importDefault(require("./bolt-protocol-v5x0"));
var bolt_protocol_util_1 = require("./bolt-protocol-util");
var bolt_protocol_v5x1_transformer_1 = __importDefault(require("./bolt-protocol-v5x1.transformer"));
var transformer_1 = __importDefault(require("./transformer"));
var request_message_1 = __importDefault(require("./request-message"));
var stream_observers_1 = require("./stream-observers");
var neo4j_driver_core_1 = require("neo4j-driver-core");
var BOLT_PROTOCOL_V5_1 = neo4j_driver_core_1.internal.constants.BOLT_PROTOCOL_V5_1;
var BoltProtocol = /** @class */ (function (_super) {
    __extends(BoltProtocol, _super);
    function BoltProtocol() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    Object.defineProperty(BoltProtocol.prototype, "version", {
        get: function () {
            return BOLT_PROTOCOL_V5_1;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(BoltProtocol.prototype, "transformer", {
        get: function () {
            var _this = this;
            if (this._transformer === undefined) {
                this._transformer = new transformer_1.default(Object.values(bolt_protocol_v5x1_transformer_1.default).map(function (create) { return create(_this._config, _this._log); }));
            }
            return this._transformer;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(BoltProtocol.prototype, "supportsReAuth", {
        get: function () {
            return true;
        },
        enumerable: false,
        configurable: true
    });
    /**
     * Initialize a connection with the server
     *
     * @param {Object} args The params
     * @param {string} args.userAgent The user agent
     * @param {any} args.authToken The auth token
     * @param {NotificationFilter} args.notificationFilter The notification filters.
     * @param {function(error)} args.onError On error callback
     * @param {function(onComplete)} args.onComplete On complete callback
     * @returns {LoginObserver} The Login observer
     */
    BoltProtocol.prototype.initialize = function (_a) {
        var _this = this;
        var _b = _a === void 0 ? {} : _a, userAgent = _b.userAgent, boltAgent = _b.boltAgent, authToken = _b.authToken, notificationFilter = _b.notificationFilter, onError = _b.onError, onComplete = _b.onComplete;
        var state = {};
        var observer = new stream_observers_1.LoginObserver({
            onError: function (error) { return _this._onLoginError(error, onError); },
            onCompleted: function (metadata) {
                state.metadata = metadata;
                return _this._onLoginCompleted(metadata);
            }
        });
        // passing notification filter on this protocol version throws an error
        (0, bolt_protocol_util_1.assertNotificationFilterIsEmpty)(notificationFilter, this._onProtocolError, observer);
        this.write(request_message_1.default.hello5x1(userAgent, this._serversideRouting), observer, false);
        return this.logon({
            authToken: authToken,
            onComplete: function (metadata) { return onComplete(__assign(__assign({}, metadata), state.metadata)); },
            onError: onError,
            flush: true
        });
    };
    /**
     * Performs login of the underlying connection
     *
     * @param {Object} args
     * @param {Object} args.authToken the authentication token.
     * @param {function(err: Error)} args.onError the callback to invoke on error.
     * @param {function()} args.onComplete the callback to invoke on completion.
     * @param {boolean} args.flush whether to flush the buffered messages.
     *
     * @returns {StreamObserver} the stream observer that monitors the corresponding server response.
     */
    BoltProtocol.prototype.logon = function (_a) {
        var _this = this;
        var _b = _a === void 0 ? {} : _a, authToken = _b.authToken, onComplete = _b.onComplete, onError = _b.onError, flush = _b.flush;
        var observer = new stream_observers_1.LoginObserver({
            onCompleted: function () { return _this._onLoginCompleted(null, authToken, onComplete); },
            onError: function (error) { return _this._onLoginError(error, onError); }
        });
        this.write(request_message_1.default.logon(authToken), observer, flush);
        return observer;
    };
    /**
     * Performs logoff of the underlying connection
     *
     * @param {Object} param
     * @param {function(err: Error)} param.onError the callback to invoke on error.
     * @param {function()} param.onComplete the callback to invoke on completion.
     * @param {boolean} param.flush whether to flush the buffered messages.
     *
     * @returns {StreamObserver} the stream observer that monitors the corresponding server response.
    */
    BoltProtocol.prototype.logoff = function (_a) {
        var _b = _a === void 0 ? {} : _a, onComplete = _b.onComplete, onError = _b.onError, flush = _b.flush;
        var observer = new stream_observers_1.LogoffObserver({
            onCompleted: onComplete,
            onError: onError
        });
        this.write(request_message_1.default.logoff(), observer, flush);
        return observer;
    };
    return BoltProtocol;
}(bolt_protocol_v5x0_1.default));
exports.default = BoltProtocol;
