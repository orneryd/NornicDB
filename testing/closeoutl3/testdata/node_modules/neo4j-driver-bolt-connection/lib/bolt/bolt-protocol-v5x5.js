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
var bolt_protocol_v5x4_1 = __importDefault(require("./bolt-protocol-v5x4"));
var bolt_protocol_v5x5_transformer_1 = __importDefault(require("./bolt-protocol-v5x5.transformer"));
var transformer_1 = __importDefault(require("./transformer"));
var request_message_1 = __importDefault(require("./request-message"));
var stream_observers_1 = require("./stream-observers");
var neo4j_driver_core_1 = require("neo4j-driver-core");
var _a = neo4j_driver_core_1.internal.constants, BOLT_PROTOCOL_V5_5 = _a.BOLT_PROTOCOL_V5_5, FETCH_ALL = _a.FETCH_ALL;
var DEFAULT_DIAGNOSTIC_RECORD = Object.freeze({
    OPERATION: '',
    OPERATION_CODE: '0',
    CURRENT_SCHEMA: '/'
});
var BoltProtocol = /** @class */ (function (_super) {
    __extends(BoltProtocol, _super);
    function BoltProtocol() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    Object.defineProperty(BoltProtocol.prototype, "version", {
        get: function () {
            return BOLT_PROTOCOL_V5_5;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(BoltProtocol.prototype, "transformer", {
        get: function () {
            var _this = this;
            if (this._transformer === undefined) {
                this._transformer = new transformer_1.default(Object.values(bolt_protocol_v5x5_transformer_1.default).map(function (create) { return create(_this._config, _this._log); }));
            }
            return this._transformer;
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
        this.write(request_message_1.default.hello5x5(userAgent, boltAgent, notificationFilter, this._serversideRouting), observer, false);
        return this.logon({
            authToken: authToken,
            onComplete: function (metadata) { return onComplete(__assign(__assign({}, metadata), state.metadata)); },
            onError: onError,
            flush: true
        });
    };
    BoltProtocol.prototype.beginTransaction = function (_a) {
        var _b = _a === void 0 ? {} : _a, bookmarks = _b.bookmarks, txConfig = _b.txConfig, database = _b.database, mode = _b.mode, impersonatedUser = _b.impersonatedUser, notificationFilter = _b.notificationFilter, beforeError = _b.beforeError, afterError = _b.afterError, beforeComplete = _b.beforeComplete, afterComplete = _b.afterComplete;
        var observer = new stream_observers_1.ResultStreamObserver({
            server: this._server,
            beforeError: beforeError,
            afterError: afterError,
            beforeComplete: beforeComplete,
            afterComplete: afterComplete
        });
        observer.prepareToHandleSingleResponse();
        this.write(request_message_1.default.begin5x5({ bookmarks: bookmarks, txConfig: txConfig, database: database, mode: mode, impersonatedUser: impersonatedUser, notificationFilter: notificationFilter }), observer, true);
        return observer;
    };
    BoltProtocol.prototype.run = function (query, parameters, _a) {
        var _b = _a === void 0 ? {} : _a, bookmarks = _b.bookmarks, txConfig = _b.txConfig, database = _b.database, mode = _b.mode, impersonatedUser = _b.impersonatedUser, notificationFilter = _b.notificationFilter, beforeKeys = _b.beforeKeys, afterKeys = _b.afterKeys, beforeError = _b.beforeError, afterError = _b.afterError, beforeComplete = _b.beforeComplete, afterComplete = _b.afterComplete, _c = _b.flush, flush = _c === void 0 ? true : _c, _d = _b.reactive, reactive = _d === void 0 ? false : _d, _e = _b.fetchSize, fetchSize = _e === void 0 ? FETCH_ALL : _e, _f = _b.highRecordWatermark, highRecordWatermark = _f === void 0 ? Number.MAX_VALUE : _f, _g = _b.lowRecordWatermark, lowRecordWatermark = _g === void 0 ? Number.MAX_VALUE : _g;
        var observer = new stream_observers_1.ResultStreamObserver({
            server: this._server,
            reactive: reactive,
            fetchSize: fetchSize,
            moreFunction: this._requestMore.bind(this),
            discardFunction: this._requestDiscard.bind(this),
            beforeKeys: beforeKeys,
            afterKeys: afterKeys,
            beforeError: beforeError,
            afterError: afterError,
            beforeComplete: beforeComplete,
            afterComplete: afterComplete,
            highRecordWatermark: highRecordWatermark,
            lowRecordWatermark: lowRecordWatermark,
            enrichMetadata: this._enrichMetadata
        });
        var flushRun = reactive;
        this.write(request_message_1.default.runWithMetadata5x5(query, parameters, {
            bookmarks: bookmarks,
            txConfig: txConfig,
            database: database,
            mode: mode,
            impersonatedUser: impersonatedUser,
            notificationFilter: notificationFilter
        }), observer, flushRun && flush);
        if (!reactive) {
            this.write(request_message_1.default.pull({ n: fetchSize }), observer, flush);
        }
        return observer;
    };
    /**
     *
     * @param {object} metadata
     * @returns {object}
     */
    BoltProtocol.prototype._enrichMetadata = function (metadata) {
        if (Array.isArray(metadata.statuses)) {
            metadata.statuses = metadata.statuses.map(function (status) { return (__assign(__assign({}, status), { description: status.neo4j_code != null ? status.status_description : status.description, diagnostic_record: status.diagnostic_record !== null ? __assign(__assign({}, DEFAULT_DIAGNOSTIC_RECORD), status.diagnostic_record) : null })); });
        }
        return metadata;
    };
    return BoltProtocol;
}(bolt_protocol_v5x4_1.default));
exports.default = BoltProtocol;
