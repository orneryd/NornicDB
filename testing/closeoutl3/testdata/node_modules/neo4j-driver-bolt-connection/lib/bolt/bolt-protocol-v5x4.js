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
var bolt_protocol_v5x3_1 = __importDefault(require("./bolt-protocol-v5x3"));
var bolt_protocol_v5x4_transformer_1 = __importDefault(require("./bolt-protocol-v5x4.transformer"));
var request_message_1 = __importDefault(require("./request-message"));
var stream_observers_1 = require("./stream-observers");
var transformer_1 = __importDefault(require("./transformer"));
var neo4j_driver_core_1 = require("neo4j-driver-core");
var BOLT_PROTOCOL_V5_4 = neo4j_driver_core_1.internal.constants.BOLT_PROTOCOL_V5_4;
var BoltProtocol = /** @class */ (function (_super) {
    __extends(BoltProtocol, _super);
    function BoltProtocol() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    Object.defineProperty(BoltProtocol.prototype, "version", {
        get: function () {
            return BOLT_PROTOCOL_V5_4;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(BoltProtocol.prototype, "transformer", {
        get: function () {
            var _this = this;
            if (this._transformer === undefined) {
                this._transformer = new transformer_1.default(Object.values(bolt_protocol_v5x4_transformer_1.default).map(function (create) { return create(_this._config, _this._log); }));
            }
            return this._transformer;
        },
        enumerable: false,
        configurable: true
    });
    /**
     * Send a TELEMETRY through the underlying connection.
     *
     * @param {object} param0 Message params
     * @param {number} param0.api The API called
     * @param {object} param1 Configuration and callbacks callbacks
     * @param {function()} param1.onCompleted Called when completed
     * @param {function()} param1.onError Called when error
     * @return {StreamObserver} the stream observer that monitors the corresponding server response.
     */
    BoltProtocol.prototype.telemetry = function (_a, _b) {
        var api = _a.api;
        var _c = _b === void 0 ? {} : _b, onError = _c.onError, onCompleted = _c.onCompleted;
        var observer = new stream_observers_1.TelemetryObserver({ onCompleted: onCompleted, onError: onError });
        this.write(request_message_1.default.telemetry({ api: api }), observer, false);
        return observer;
    };
    return BoltProtocol;
}(bolt_protocol_v5x3_1.default));
exports.default = BoltProtocol;
