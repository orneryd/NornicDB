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
exports.ProtocolVersion = void 0;
/**
 *
 * @access public
 * @class A class representing a protocol version the driver is using to communicate with the server.
 * @param {number} major the major version of the protocol.
 * @param {number} minor the minor version of the protocol.
 */
var ProtocolVersion = /** @class */ (function () {
    function ProtocolVersion(major, minor) {
        this.major = major;
        this.minor = minor;
    }
    /**
     *
     * @returns {number} The major version of the protocol
     */
    ProtocolVersion.prototype.getMajor = function () {
        return this.major;
    };
    /**
     * @returns {number} The minor version of the protocol
     */
    ProtocolVersion.prototype.getMinor = function () {
        return this.major;
    };
    /**
     *
     * @param {ProtocolVersion | {major: number, minor: number}} other the protocol version to compare to
     * @returns {boolean} If this version semantically smaller than the other version.
     */
    ProtocolVersion.prototype.isLessThan = function (other) {
        if (this.major < other.major) {
            return true;
        }
        else if (this.major === other.major && this.minor < other.minor) {
            return true;
        }
        return false;
    };
    /**
     *
     * @param {ProtocolVersion | {major: number, minor: number}} other the protocol version to compare to
     * @returns {boolean} If this version is semantically larger than the other version.
     */
    ProtocolVersion.prototype.isGreaterThan = function (other) {
        if (this.major > other.major) {
            return true;
        }
        else if (this.major === other.major && this.minor > other.minor) {
            return true;
        }
        return false;
    };
    /**
     *
     * @param {ProtocolVersion | {major: number, minor: number}} other the protocol version to compare to
     * @returns {boolean} if this version is semantically larger or equal to the other version.
     */
    ProtocolVersion.prototype.isGreaterOrEqualTo = function (other) {
        return !this.isLessThan(other);
    };
    /**
     *
     * @param {ProtocolVersion | {major: number, minor: number}} other the protocol version to compare to
     * @returns {boolean} if this version is semantically smaller or equal to the other version.
     */
    ProtocolVersion.prototype.isLessOrEqualTo = function (other) {
        return !this.isGreaterThan(other);
    };
    /**
     *
     * @param {ProtocolVersion | {major: number, minor: number}} other the protocol version to compare to
     * @returns {boolean} If this version is the equal to the other version.
     */
    ProtocolVersion.prototype.isEqualTo = function (other) {
        return this.major === other.major && this.minor === other.minor;
    };
    ProtocolVersion.prototype.toString = function () {
        return this.major.toString() + '.' + this.minor.toString();
    };
    return ProtocolVersion;
}());
exports.ProtocolVersion = ProtocolVersion;
