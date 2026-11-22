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
var bolt_protocol_v4x0_1 = __importDefault(require("./bolt-protocol-v4x0"));
var request_message_1 = __importDefault(require("./request-message"));
var stream_observers_1 = require("./stream-observers");
var neo4j_driver_core_1 = require("neo4j-driver-core");
var bolt_protocol_util_1 = require("./bolt-protocol-util");
var bolt_protocol_v4x1_transformer_1 = __importDefault(require("./bolt-protocol-v4x1.transformer"));
var transformer_1 = __importDefault(require("./transformer"));
var BOLT_PROTOCOL_V4_1 = neo4j_driver_core_1.internal.constants.BOLT_PROTOCOL_V4_1;
var BoltProtocol = /** @class */ (function (_super) {
    __extends(BoltProtocol, _super);
    /**
     * @constructor
     * @param {Object} server the server informatio.
     * @param {Chunker} chunker the chunker.
     * @param {Object} packstreamConfig Packstream configuration
     * @param {boolean} packstreamConfig.disableLosslessIntegers if this connection should convert all received integers to native JS numbers.
     * @param {boolean} packstreamConfig.useBigInt if this connection should convert all received integers to native BigInt numbers.
     * @param {CreateResponseHandler} createResponseHandler Function which creates the response handler
     * @param {Logger} log the logger
     * @param {Object} serversideRouting
     *
     */
    function BoltProtocol(server, chunker, packstreamConfig, createResponseHandler, log, onProtocolError, serversideRouting) {
        if (createResponseHandler === void 0) { createResponseHandler = function () { return null; }; }
        var _this = _super.call(this, server, chunker, packstreamConfig, createResponseHandler, log, onProtocolError) || this;
        _this._serversideRouting = serversideRouting;
        return _this;
    }
    Object.defineProperty(BoltProtocol.prototype, "version", {
        get: function () {
            return BOLT_PROTOCOL_V4_1;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(BoltProtocol.prototype, "transformer", {
        get: function () {
            var _this = this;
            if (this._transformer === undefined) {
                this._transformer = new transformer_1.default(Object.values(bolt_protocol_v4x1_transformer_1.default).map(function (create) { return create(_this._config, _this._log); }));
            }
            return this._transformer;
        },
        enumerable: false,
        configurable: true
    });
    BoltProtocol.prototype.initialize = function (_a) {
        var _this = this;
        var _b = _a === void 0 ? {} : _a, userAgent = _b.userAgent, boltAgent = _b.boltAgent, authToken = _b.authToken, notificationFilter = _b.notificationFilter, onError = _b.onError, onComplete = _b.onComplete;
        var observer = new stream_observers_1.LoginObserver({
            onError: function (error) { return _this._onLoginError(error, onError); },
            onCompleted: function (metadata) { return _this._onLoginCompleted(metadata, authToken, onComplete); }
        });
        // passing notification filter on this protocol version throws an error
        (0, bolt_protocol_util_1.assertNotificationFilterIsEmpty)(notificationFilter, this._onProtocolError, observer);
        this.write(request_message_1.default.hello(userAgent, authToken, this._serversideRouting), observer, true);
        return observer;
    };
    return BoltProtocol;
}(bolt_protocol_v4x0_1.default));
exports.default = BoltProtocol;
