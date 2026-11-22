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
var bolt_protocol_v4x2_1 = __importDefault(require("./bolt-protocol-v4x2"));
var request_message_1 = __importDefault(require("./request-message"));
var stream_observers_1 = require("./stream-observers");
var bolt_protocol_util_1 = require("./bolt-protocol-util");
var bolt_protocol_v4x3_transformer_1 = __importDefault(require("./bolt-protocol-v4x3.transformer"));
var bolt_protocol_v5x0_utc_transformer_1 = __importDefault(require("./bolt-protocol-v5x0.utc.transformer"));
var transformer_1 = __importDefault(require("./transformer"));
var neo4j_driver_core_1 = require("neo4j-driver-core");
var Bookmarks = neo4j_driver_core_1.internal.bookmarks.Bookmarks, BOLT_PROTOCOL_V4_3 = neo4j_driver_core_1.internal.constants.BOLT_PROTOCOL_V4_3;
var BoltProtocol = /** @class */ (function (_super) {
    __extends(BoltProtocol, _super);
    function BoltProtocol() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    Object.defineProperty(BoltProtocol.prototype, "version", {
        get: function () {
            return BOLT_PROTOCOL_V4_3;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(BoltProtocol.prototype, "transformer", {
        get: function () {
            var _this = this;
            if (this._transformer === undefined) {
                this._transformer = new transformer_1.default(Object.values(bolt_protocol_v4x3_transformer_1.default).map(function (create) { return create(_this._config, _this._log); }));
            }
            return this._transformer;
        },
        enumerable: false,
        configurable: true
    });
    /**
     * Request routing information
     *
     * @param {Object} param -
     * @param {object} param.routingContext The routing context used to define the routing table.
     *  Multi-datacenter deployments is one of its use cases
     * @param {string} param.databaseName The database name
     * @param {Bookmarks} params.sessionContext.bookmarks The bookmarks used for requesting the routing table
     * @param {function(err: Error)} param.onError
     * @param {function(RawRoutingTable)} param.onCompleted
     * @returns {RouteObserver} the route observer
     */
    BoltProtocol.prototype.requestRoutingInformation = function (_a) {
        var _b = _a.routingContext, routingContext = _b === void 0 ? {} : _b, _c = _a.databaseName, databaseName = _c === void 0 ? null : _c, _d = _a.sessionContext, sessionContext = _d === void 0 ? {} : _d, onError = _a.onError, onCompleted = _a.onCompleted;
        var observer = new stream_observers_1.RouteObserver({
            onProtocolError: this._onProtocolError,
            onError: onError,
            onCompleted: onCompleted
        });
        var bookmarks = sessionContext.bookmarks || Bookmarks.empty();
        this.write(request_message_1.default.route(routingContext, bookmarks.values(), databaseName), observer, true);
        return observer;
    };
    /**
     * Initialize a connection with the server
     *
     * @param {Object} args The params
     * @param {string} args.userAgent The user agent
     * @param {any} args.authToken The auth token
     * @param {NotificationFilter} args.notificationFilter The notification filter.
     * @param {function(error)} args.onError On error callback
     * @param {function(onComplte)} args.onComplete On complete callback
     * @returns {LoginObserver} The Login observer
     */
    BoltProtocol.prototype.initialize = function (_a) {
        var _this = this;
        var _b = _a === void 0 ? {} : _a, userAgent = _b.userAgent, boltAgent = _b.boltAgent, authToken = _b.authToken, notificationFilter = _b.notificationFilter, onError = _b.onError, onComplete = _b.onComplete;
        var observer = new stream_observers_1.LoginObserver({
            onError: function (error) { return _this._onLoginError(error, onError); },
            onCompleted: function (metadata) {
                if (metadata.patch_bolt !== undefined) {
                    _this._applyPatches(metadata.patch_bolt);
                }
                return _this._onLoginCompleted(metadata, authToken, onComplete);
            }
        });
        // passing notification filter on this protocol version throws an error
        (0, bolt_protocol_util_1.assertNotificationFilterIsEmpty)(notificationFilter, this._onProtocolError, observer);
        this.write(request_message_1.default.hello(userAgent, authToken, this._serversideRouting, ['utc']), observer, true);
        return observer;
    };
    /**
     *
     * @param {string[]} patches Patches to be applied to the protocol
     */
    BoltProtocol.prototype._applyPatches = function (patches) {
        if (patches.includes('utc')) {
            this._applyUtcPatch();
        }
    };
    BoltProtocol.prototype._applyUtcPatch = function () {
        var _this = this;
        this._transformer = new transformer_1.default(Object.values(__assign(__assign({}, bolt_protocol_v4x3_transformer_1.default), bolt_protocol_v5x0_utc_transformer_1.default)).map(function (create) { return create(_this._config, _this._log); }));
    };
    return BoltProtocol;
}(bolt_protocol_v4x2_1.default));
exports.default = BoltProtocol;
