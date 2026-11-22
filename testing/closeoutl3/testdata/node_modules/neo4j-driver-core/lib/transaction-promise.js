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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
var _a;
Object.defineProperty(exports, "__esModule", { value: true });
/* eslint-disable @typescript-eslint/promise-function-async */
var transaction_1 = __importDefault(require("./transaction"));
/**
 * Represents a Promise<{@link Transaction}> object and a {@link Transaction} object.
 *
 * Resolving this object promise verifies the result of the transaction begin and returns the {@link Transaction} object in case of success.
 *
 * The object can still also used as {@link Transaction} for convenience. The result of begin will be checked
 * during the next API calls in the object as it is in the transaction.
 *
 * @access public
 */
var TransactionPromise = /** @class */ (function (_super) {
    __extends(TransactionPromise, _super);
    /**
     * @constructor
     * @param {object} args
     * @param {ConnectionHolder} args.connectionHolder - the connection holder to get connection from.
     * @param {function()} args.onClose - Function to be called when transaction is committed or rolled back.
     * @param {function(bookmarks: Bookmarks)} args.onBookmarks callback invoked when new bookmark is produced.
     * @param {function()} args.onConnection - Function to be called when a connection is obtained to ensure the connection
     * is not yet released.
     * @param {boolean} args.reactive whether this transaction generates reactive streams
     * @param {number} args.fetchSize - the record fetch size in each pulling batch.
     * @param {string} args.impersonatedUser - The name of the user which should be impersonated for the duration of the session.
     * @param {NotificationFilter} args.notificationFilter - The notification filter used for this transaction.
     * @param {NonAutoCommitApiTelemetryConfig} args.apiTelemetryConfig - The api telemetry configuration. Empty/Null for disabling telemetry
     */
    function TransactionPromise(_b) {
        var connectionHolder = _b.connectionHolder, onClose = _b.onClose, onBookmarks = _b.onBookmarks, onConnection = _b.onConnection, reactive = _b.reactive, fetchSize = _b.fetchSize, impersonatedUser = _b.impersonatedUser, highRecordWatermark = _b.highRecordWatermark, lowRecordWatermark = _b.lowRecordWatermark, notificationFilter = _b.notificationFilter, apiTelemetryConfig = _b.apiTelemetryConfig, onDbCallback = _b.onDbCallback;
        var _this = _super.call(this, {
            connectionHolder: connectionHolder,
            onClose: onClose,
            onBookmarks: onBookmarks,
            onConnection: onConnection,
            reactive: reactive,
            fetchSize: fetchSize,
            impersonatedUser: impersonatedUser,
            highRecordWatermark: highRecordWatermark,
            lowRecordWatermark: lowRecordWatermark,
            notificationFilter: notificationFilter,
            apiTelemetryConfig: apiTelemetryConfig
        }) || this;
        _this[_a] = 'TransactionPromise';
        _this._onDbCallback = onDbCallback;
        return _this;
    }
    /**
     * Waits for the begin to complete.
     *
     * @param {function(transaction: Transaction)} onFulfilled - function to be called when finished.
     * @param {function(error: {message:string, code:string})} onRejected - function to be called upon errors.
     * @return {Promise} promise.
     */
    TransactionPromise.prototype.then = function (onfulfilled, onrejected) {
        return this._getOrCreateBeginPromise().then(onfulfilled, onrejected);
    };
    /**
     * Catch errors when using promises.
     *
     * @param {function(error: Neo4jError)} onRejected - Function to be called upon errors.
     * @return {Promise} promise.
     */
    TransactionPromise.prototype.catch = function (onrejected) {
        return this._getOrCreateBeginPromise().catch(onrejected);
    };
    /**
     * Called when finally the begin is done
     *
     * @param {function()|null} onfinally - function when the promise finished
     * @return {Promise} promise.
     */
    TransactionPromise.prototype.finally = function (onfinally) {
        return this._getOrCreateBeginPromise().finally(onfinally);
    };
    TransactionPromise.prototype._getOrCreateBeginPromise = function () {
        var _this = this;
        if (this._beginPromise == null) {
            this._beginPromise = new Promise(function (resolve, reject) {
                _this._resolve = resolve;
                _this._reject = reject;
                if (_this._beginError != null) {
                    reject(_this._beginError);
                }
                if (_this._beginMetadata != null) {
                    resolve(_this._toTransaction());
                }
            });
        }
        return this._beginPromise;
    };
    /**
     * @access private
     */
    TransactionPromise.prototype._toTransaction = function () {
        return __assign(__assign({}, this), { run: _super.prototype.run.bind(this), commit: _super.prototype.commit.bind(this), rollback: _super.prototype.rollback.bind(this), close: _super.prototype.close.bind(this), isOpen: _super.prototype.isOpen.bind(this), _begin: this._begin.bind(this) });
    };
    /**
     * @access private
     */
    TransactionPromise.prototype._begin = function (bookmarks, txConfig) {
        return _super.prototype._begin.call(this, bookmarks, txConfig, {
            onError: this._onBeginError.bind(this),
            onComplete: this._onBeginMetadata.bind(this),
            onDB: this._onDbCallback
        });
    };
    /**
     * @access private
     * @returns {void}
     */
    TransactionPromise.prototype._onBeginError = function (error) {
        this._beginError = error;
        if (this._reject != null) {
            this._reject(error);
        }
    };
    /**
     * @access private
     * @returns {void}
     */
    TransactionPromise.prototype._onBeginMetadata = function (metadata) {
        this._beginMetadata = metadata !== null && metadata !== void 0 ? metadata : {};
        if (this._resolve != null) {
            this._resolve(this._toTransaction());
        }
    };
    return TransactionPromise;
}(transaction_1.default));
_a = Symbol.toStringTag;
exports.default = TransactionPromise;
