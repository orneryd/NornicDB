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
var bolt_protocol_v5x5_1 = __importDefault(require("./bolt-protocol-v5x5"));
var bolt_protocol_v5x6_transformer_1 = __importDefault(require("./bolt-protocol-v5x6.transformer"));
var transformer_1 = __importDefault(require("./transformer"));
var neo4j_driver_core_1 = require("neo4j-driver-core");
var BOLT_PROTOCOL_V5_6 = neo4j_driver_core_1.internal.constants.BOLT_PROTOCOL_V5_6;
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
            return BOLT_PROTOCOL_V5_6;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(BoltProtocol.prototype, "transformer", {
        get: function () {
            var _this = this;
            if (this._transformer === undefined) {
                this._transformer = new transformer_1.default(Object.values(bolt_protocol_v5x6_transformer_1.default).map(function (create) { return create(_this._config, _this._log); }));
            }
            return this._transformer;
        },
        enumerable: false,
        configurable: true
    });
    /**
     *
     * @param {object} metadata
     * @returns {object}
     */
    BoltProtocol.prototype._enrichMetadata = function (metadata) {
        if (Array.isArray(metadata.statuses)) {
            metadata.statuses = metadata.statuses.map(function (status) { return (__assign(__assign({}, status), { diagnostic_record: status.diagnostic_record !== null ? __assign(__assign({}, DEFAULT_DIAGNOSTIC_RECORD), status.diagnostic_record) : null })); });
        }
        return metadata;
    };
    return BoltProtocol;
}(bolt_protocol_v5x5_1.default));
exports.default = BoltProtocol;
