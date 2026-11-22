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
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.TxConfig = void 0;
var util = __importStar(require("./util"));
var error_1 = require("../error");
var integer_1 = require("../integer");
/**
 * Internal holder of the transaction configuration.
 * It performs input validation and value conversion for further serialization by the Bolt protocol layer.
 * Users of the driver provide transaction configuration as regular objects `{timeout: 10, metadata: {key: 'value'}}`.
 * Driver converts such objects to {@link TxConfig} immediately and uses converted values everywhere.
 */
var TxConfig = /** @class */ (function () {
    /**
     * @constructor
     * @param {Object} config the raw configuration object.
     */
    function TxConfig(config, log) {
        assertValidConfig(config);
        this.timeout = extractTimeout(config, log);
        this.metadata = extractMetadata(config);
    }
    /**
     * Get an empty config object.
     * @return {TxConfig} an empty config.
     */
    TxConfig.empty = function () {
        return EMPTY_CONFIG;
    };
    /**
     * Check if this config object is empty. I.e. has no configuration values specified.
     * @return {boolean} `true` if this object is empty, `false` otherwise.
     */
    TxConfig.prototype.isEmpty = function () {
        return Object.values(this).every(function (value) { return value == null; });
    };
    return TxConfig;
}());
exports.TxConfig = TxConfig;
var EMPTY_CONFIG = new TxConfig({});
/**
 * @return {Integer|null}
 */
function extractTimeout(config, log) {
    if (util.isObject(config) && config.timeout != null) {
        util.assertNumberOrInteger(config.timeout, 'Transaction timeout');
        if (isTimeoutFloat(config) && (log === null || log === void 0 ? void 0 : log.isInfoEnabled()) === true) {
            // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
            log === null || log === void 0 ? void 0 : log.info("Transaction timeout expected to be an integer, got: ".concat(config.timeout, ". The value will be rounded up."));
        }
        var timeout = (0, integer_1.int)(config.timeout, { ceilFloat: true });
        if (timeout.isNegative()) {
            throw (0, error_1.newError)('Transaction timeout should not be negative');
        }
        return timeout;
    }
    return null;
}
function isTimeoutFloat(config) {
    return typeof config.timeout === 'number' && !Number.isInteger(config.timeout);
}
/**
 * @return {object|null}
 */
function extractMetadata(config) {
    if (util.isObject(config) && config.metadata != null) {
        var metadata = config.metadata;
        util.assertObject(metadata, 'config.metadata');
        if (Object.keys(metadata).length !== 0) {
            // not an empty object
            return metadata;
        }
    }
    return null;
}
function assertValidConfig(config) {
    if (config != null) {
        util.assertObject(config, 'Transaction config');
    }
}
