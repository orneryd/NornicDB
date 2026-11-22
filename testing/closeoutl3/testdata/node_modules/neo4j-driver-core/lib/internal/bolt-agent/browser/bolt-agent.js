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
/* eslint-disable */
Object.defineProperty(exports, "__esModule", { value: true });
exports.fromVersion = fromVersion;
/**
 * Constructs a BoltAgent structure from a given product version.
 *
 * @param {string} version The product version
 * @param {function():SystemInfo} getSystemInfo Parameter used of inject system information and mock calls to the APIs.
 * @returns {BoltAgent} The bolt agent
 */
function fromVersion(version, getSystemInfo) {
    if (getSystemInfo === void 0) { getSystemInfo = function () { return ({
        get userAgent() {
            // this should be defined as an `var` since we need to get information
            // came from the global scope which not always will be defined
            // and we don't want to override the information
            var navigator;
            // @ts-ignore: browser code so must be skipped by ts
            return navigator === null || navigator === void 0 ? void 0 : navigator.userAgent;
        }
    }); }; }
    var systemInfo = getSystemInfo();
    //USER_AGENT looks like 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'
    var platform = systemInfo.userAgent != null ? systemInfo.userAgent.split("(")[1].split(")")[0] : undefined;
    var languageDetails = systemInfo.userAgent || undefined;
    return {
        product: "neo4j-javascript/".concat(version),
        platform: platform,
        languageDetails: languageDetails
    };
}
/* eslint-enable */
