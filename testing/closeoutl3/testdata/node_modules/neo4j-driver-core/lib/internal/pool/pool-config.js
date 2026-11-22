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
Object.defineProperty(exports, "__esModule", { value: true });
exports.DEFAULT_ACQUISITION_TIMEOUT = exports.DEFAULT_MAX_SIZE = void 0;
var DEFAULT_MAX_SIZE = 100;
exports.DEFAULT_MAX_SIZE = DEFAULT_MAX_SIZE;
var DEFAULT_ACQUISITION_TIMEOUT = 60 * 1000; // 60 seconds
exports.DEFAULT_ACQUISITION_TIMEOUT = DEFAULT_ACQUISITION_TIMEOUT;
var PoolConfig = /** @class */ (function () {
    function PoolConfig(maxSize, acquisitionTimeout) {
        this.maxSize = valueOrDefault(maxSize, DEFAULT_MAX_SIZE);
        this.acquisitionTimeout = valueOrDefault(acquisitionTimeout, DEFAULT_ACQUISITION_TIMEOUT);
    }
    PoolConfig.defaultConfig = function () {
        return new PoolConfig(DEFAULT_MAX_SIZE, DEFAULT_ACQUISITION_TIMEOUT);
    };
    PoolConfig.fromDriverConfig = function (config) {
        var maxSize = isConfigured(config.maxConnectionPoolSize)
            ? config.maxConnectionPoolSize
            : DEFAULT_MAX_SIZE;
        var acquisitionTimeout = isConfigured(config.connectionAcquisitionTimeout)
            ? config.connectionAcquisitionTimeout
            : DEFAULT_ACQUISITION_TIMEOUT;
        return new PoolConfig(maxSize, acquisitionTimeout);
    };
    return PoolConfig;
}());
exports.default = PoolConfig;
function valueOrDefault(value, defaultValue) {
    return isConfigured(value) ? value : defaultValue;
}
function isConfigured(value) {
    return value === 0 || value != null;
}
