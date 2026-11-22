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
var bolt_protocol_v2_1 = __importDefault(require("./bolt-protocol-v2"));
var request_message_1 = __importDefault(require("./request-message"));
var bolt_protocol_util_1 = require("./bolt-protocol-util");
var stream_observers_1 = require("./stream-observers");
var bolt_protocol_v3_transformer_1 = __importDefault(require("./bolt-protocol-v3.transformer"));
var transformer_1 = __importDefault(require("./transformer"));
var neo4j_driver_core_1 = require("neo4j-driver-core");
var Bookmarks = neo4j_driver_core_1.internal.bookmarks.Bookmarks, BOLT_PROTOCOL_V3 = neo4j_driver_core_1.internal.constants.BOLT_PROTOCOL_V3, TxConfig = neo4j_driver_core_1.internal.txConfig.TxConfig;
var CONTEXT = 'context';
var CALL_GET_ROUTING_TABLE = "CALL dbms.cluster.routing.getRoutingTable($".concat(CONTEXT, ")");
var noOpObserver = new stream_observers_1.StreamObserver();
var BoltProtocol = /** @class */ (function (_super) {
    __extends(BoltProtocol, _super);
    function BoltProtocol() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    Object.defineProperty(BoltProtocol.prototype, "version", {
        get: function () {
            return BOLT_PROTOCOL_V3;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(BoltProtocol.prototype, "transformer", {
        get: function () {
            var _this = this;
            if (this._transformer === undefined) {
                this._transformer = new transformer_1.default(Object.values(bolt_protocol_v3_transformer_1.default).map(function (create) { return create(_this._config, _this._log); }));
            }
            return this._transformer;
        },
        enumerable: false,
        configurable: true
    });
    BoltProtocol.prototype.transformMetadata = function (metadata) {
        if ('t_first' in metadata) {
            // Bolt V3 uses shorter key 't_first' to represent 'result_available_after'
            // adjust the key to be the same as in Bolt V1 so that ResultSummary can retrieve the value
            metadata.result_available_after = metadata.t_first;
            delete metadata.t_first;
        }
        if ('t_last' in metadata) {
            // Bolt V3 uses shorter key 't_last' to represent 'result_consumed_after'
            // adjust the key to be the same as in Bolt V1 so that ResultSummary can retrieve the value
            metadata.result_consumed_after = metadata.t_last;
            delete metadata.t_last;
        }
        return metadata;
    };
    BoltProtocol.prototype.initialize = function (_a) {
        var _this = this;
        var _b = _a === void 0 ? {} : _a, userAgent = _b.userAgent, boltAgent = _b.boltAgent, authToken = _b.authToken, notificationFilter = _b.notificationFilter, onError = _b.onError, onComplete = _b.onComplete;
        var observer = new stream_observers_1.LoginObserver({
            onError: function (error) { return _this._onLoginError(error, onError); },
            onCompleted: function (metadata) { return _this._onLoginCompleted(metadata, authToken, onComplete); }
        });
        // passing notification filter on this protocol version throws an error
        (0, bolt_protocol_util_1.assertNotificationFilterIsEmpty)(notificationFilter, this._onProtocolError, observer);
        this.write(request_message_1.default.hello(userAgent, authToken), observer, true);
        return observer;
    };
    BoltProtocol.prototype.prepareToClose = function () {
        this.write(request_message_1.default.goodbye(), noOpObserver, true);
    };
    BoltProtocol.prototype.beginTransaction = function (_a) {
        var _b = _a === void 0 ? {} : _a, bookmarks = _b.bookmarks, txConfig = _b.txConfig, database = _b.database, impersonatedUser = _b.impersonatedUser, notificationFilter = _b.notificationFilter, mode = _b.mode, beforeError = _b.beforeError, afterError = _b.afterError, beforeComplete = _b.beforeComplete, afterComplete = _b.afterComplete;
        var observer = new stream_observers_1.ResultStreamObserver({
            server: this._server,
            beforeError: beforeError,
            afterError: afterError,
            beforeComplete: beforeComplete,
            afterComplete: afterComplete
        });
        observer.prepareToHandleSingleResponse();
        // passing in a database name on this protocol version throws an error
        (0, bolt_protocol_util_1.assertDatabaseIsEmpty)(database, this._onProtocolError, observer);
        // passing impersonated user on this protocol version throws an error
        (0, bolt_protocol_util_1.assertImpersonatedUserIsEmpty)(impersonatedUser, this._onProtocolError, observer);
        // passing notification filter on this protocol version throws an error
        (0, bolt_protocol_util_1.assertNotificationFilterIsEmpty)(notificationFilter, this._onProtocolError, observer);
        this.write(request_message_1.default.begin({ bookmarks: bookmarks, txConfig: txConfig, mode: mode }), observer, true);
        return observer;
    };
    BoltProtocol.prototype.commitTransaction = function (_a) {
        var _b = _a === void 0 ? {} : _a, beforeError = _b.beforeError, afterError = _b.afterError, beforeComplete = _b.beforeComplete, afterComplete = _b.afterComplete;
        var observer = new stream_observers_1.ResultStreamObserver({
            server: this._server,
            beforeError: beforeError,
            afterError: afterError,
            beforeComplete: beforeComplete,
            afterComplete: afterComplete
        });
        observer.prepareToHandleSingleResponse();
        this.write(request_message_1.default.commit(), observer, true);
        return observer;
    };
    BoltProtocol.prototype.rollbackTransaction = function (_a) {
        var _b = _a === void 0 ? {} : _a, beforeError = _b.beforeError, afterError = _b.afterError, beforeComplete = _b.beforeComplete, afterComplete = _b.afterComplete;
        var observer = new stream_observers_1.ResultStreamObserver({
            server: this._server,
            beforeError: beforeError,
            afterError: afterError,
            beforeComplete: beforeComplete,
            afterComplete: afterComplete
        });
        observer.prepareToHandleSingleResponse();
        this.write(request_message_1.default.rollback(), observer, true);
        return observer;
    };
    BoltProtocol.prototype.run = function (query, parameters, _a) {
        var _b = _a === void 0 ? {} : _a, bookmarks = _b.bookmarks, txConfig = _b.txConfig, database = _b.database, impersonatedUser = _b.impersonatedUser, notificationFilter = _b.notificationFilter, mode = _b.mode, beforeKeys = _b.beforeKeys, afterKeys = _b.afterKeys, beforeError = _b.beforeError, afterError = _b.afterError, beforeComplete = _b.beforeComplete, afterComplete = _b.afterComplete, _c = _b.flush, flush = _c === void 0 ? true : _c, _d = _b.highRecordWatermark, highRecordWatermark = _d === void 0 ? Number.MAX_VALUE : _d, _e = _b.lowRecordWatermark, lowRecordWatermark = _e === void 0 ? Number.MAX_VALUE : _e;
        var observer = new stream_observers_1.ResultStreamObserver({
            server: this._server,
            beforeKeys: beforeKeys,
            afterKeys: afterKeys,
            beforeError: beforeError,
            afterError: afterError,
            beforeComplete: beforeComplete,
            afterComplete: afterComplete,
            highRecordWatermark: highRecordWatermark,
            lowRecordWatermark: lowRecordWatermark
        });
        // passing in a database name on this protocol version throws an error
        (0, bolt_protocol_util_1.assertDatabaseIsEmpty)(database, this._onProtocolError, observer);
        // passing impersonated user on this protocol version throws an error
        (0, bolt_protocol_util_1.assertImpersonatedUserIsEmpty)(impersonatedUser, this._onProtocolError, observer);
        // passing notification filter on this protocol version throws an error
        (0, bolt_protocol_util_1.assertNotificationFilterIsEmpty)(notificationFilter, this._onProtocolError, observer);
        this.write(request_message_1.default.runWithMetadata(query, parameters, {
            bookmarks: bookmarks,
            txConfig: txConfig,
            mode: mode
        }), observer, false);
        this.write(request_message_1.default.pullAll(), observer, flush);
        return observer;
    };
    /**
     * Request routing information
     *
     * @param {Object} param -
     * @param {object} param.routingContext The routing context used to define the routing table.
     *  Multi-datacenter deployments is one of its use cases
     * @param {string} param.databaseName The database name
     * @param {Bookmarks} params.sessionContext.bookmarks The bookmarks used for requesting the routing table
     * @param {string} params.sessionContext.mode The session mode
     * @param {string} params.sessionContext.database The database name used on the session
     * @param {function()} params.sessionContext.afterComplete The session param used after the session closed
     * @param {function(err: Error)} param.onError
     * @param {function(RawRoutingTable)} param.onCompleted
     * @returns {RouteObserver} the route observer
     */
    BoltProtocol.prototype.requestRoutingInformation = function (_a) {
        var _b;
        var _c = _a.routingContext, routingContext = _c === void 0 ? {} : _c, _d = _a.sessionContext, sessionContext = _d === void 0 ? {} : _d, onError = _a.onError, onCompleted = _a.onCompleted;
        var resultObserver = this.run(CALL_GET_ROUTING_TABLE, (_b = {}, _b[CONTEXT] = routingContext, _b), __assign(__assign({}, sessionContext), { txConfig: TxConfig.empty() }));
        return new stream_observers_1.ProcedureRouteObserver({
            resultObserver: resultObserver,
            onProtocolError: this._onProtocolError,
            onError: onError,
            onCompleted: onCompleted
        });
    };
    return BoltProtocol;
}(bolt_protocol_v2_1.default));
exports.default = BoltProtocol;
