"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.fromVersion = fromVersion;
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
var os_1 = require("os");
/**
 * Constructs a BoltAgent structure from a given product version.
 *
 * @param {string} version The product version
 * @param {function():SystemInfo} getSystemInfo Parameter used of inject system information and mock calls to the APIs.
 * @returns {BoltAgent} The bolt agent
 */
function fromVersion(version, getSystemInfo) {
    if (getSystemInfo === void 0) { getSystemInfo = function () { return ({
        hostArch: process.config.variables.host_arch,
        nodeVersion: process.versions.node,
        v8Version: process.versions.v8,
        get platform() {
            return (0, os_1.platform)();
        },
        get release() {
            return (0, os_1.release)();
        }
    }); }; }
    var systemInfo = getSystemInfo();
    var HOST_ARCH = systemInfo.hostArch;
    var NODE_VERSION = 'Node/' + systemInfo.nodeVersion;
    var NODE_V8_VERSION = systemInfo.v8Version;
    var OS_NAME_VERSION = "".concat(systemInfo.platform, " ").concat(systemInfo.release);
    return {
        product: "neo4j-javascript/".concat(version),
        platform: "".concat(OS_NAME_VERSION, "; ").concat(HOST_ARCH),
        languageDetails: "".concat(NODE_VERSION, " (v8 ").concat(NODE_V8_VERSION, ")")
    };
}
