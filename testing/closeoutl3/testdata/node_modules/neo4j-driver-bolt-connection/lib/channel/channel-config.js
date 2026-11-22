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
var neo4j_driver_core_1 = require("neo4j-driver-core");
var _a = neo4j_driver_core_1.internal.util, ENCRYPTION_OFF = _a.ENCRYPTION_OFF, ENCRYPTION_ON = _a.ENCRYPTION_ON;
var SERVICE_UNAVAILABLE = neo4j_driver_core_1.error.SERVICE_UNAVAILABLE;
var ALLOWED_VALUES_ENCRYPTED = [
    null,
    undefined,
    true,
    false,
    ENCRYPTION_ON,
    ENCRYPTION_OFF
];
var ALLOWED_VALUES_TRUST = [
    null,
    undefined,
    'TRUST_ALL_CERTIFICATES',
    'TRUST_CUSTOM_CA_SIGNED_CERTIFICATES',
    'TRUST_SYSTEM_CA_SIGNED_CERTIFICATES'
];
var ChannelConfig = /** @class */ (function () {
    /**
     * @constructor
     * @param {ServerAddress} address the address for the channel to connect to.
     * @param {Object} driverConfig the driver config provided by the user when driver is created.
     * @param {string} connectionErrorCode the default error code to use on connection errors.
     * @param {object} clientCertificate the client certificate
     */
    function ChannelConfig(address, driverConfig, connectionErrorCode, clientCertificate) {
        this.address = address;
        this.encrypted = extractEncrypted(driverConfig);
        this.trust = extractTrust(driverConfig);
        this.trustedCertificates = extractTrustedCertificates(driverConfig);
        this.connectionErrorCode = connectionErrorCode || SERVICE_UNAVAILABLE;
        this.connectionTimeout = driverConfig.connectionTimeout;
        this.clientCertificate = clientCertificate;
    }
    return ChannelConfig;
}());
exports.default = ChannelConfig;
function extractEncrypted(driverConfig) {
    var value = driverConfig.encrypted;
    if (ALLOWED_VALUES_ENCRYPTED.indexOf(value) === -1) {
        throw (0, neo4j_driver_core_1.newError)("Illegal value of the encrypted setting ".concat(value, ". Expected one of ").concat(ALLOWED_VALUES_ENCRYPTED));
    }
    return value;
}
function extractTrust(driverConfig) {
    var value = driverConfig.trust;
    if (ALLOWED_VALUES_TRUST.indexOf(value) === -1) {
        throw (0, neo4j_driver_core_1.newError)("Illegal value of the trust setting ".concat(value, ". Expected one of ").concat(ALLOWED_VALUES_TRUST));
    }
    return value;
}
function extractTrustedCertificates(driverConfig) {
    return driverConfig.trustedCertificates || [];
}
