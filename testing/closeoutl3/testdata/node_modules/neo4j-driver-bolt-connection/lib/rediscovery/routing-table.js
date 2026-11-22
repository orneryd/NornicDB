"use strict";
var __read = (this && this.__read) || function (o, n) {
    var m = typeof Symbol === "function" && o[Symbol.iterator];
    if (!m) return o;
    var i = m.call(o), r, ar = [], e;
    try {
        while ((n === void 0 || n-- > 0) && !(r = i.next()).done) ar.push(r.value);
    }
    catch (error) { e = { error: error }; }
    finally {
        try {
            if (r && !r.done && (m = i["return"])) m.call(i);
        }
        finally { if (e) throw e.error; }
    }
    return ar;
};
var __spreadArray = (this && this.__spreadArray) || function (to, from, pack) {
    if (pack || arguments.length === 2) for (var i = 0, l = from.length, ar; i < l; i++) {
        if (ar || !(i in from)) {
            if (!ar) ar = Array.prototype.slice.call(from, 0, i);
            ar[i] = from[i];
        }
    }
    return to.concat(ar || Array.prototype.slice.call(from));
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.createValidRoutingTable = createValidRoutingTable;
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
var neo4j_driver_core_1 = require("neo4j-driver-core");
var _a = neo4j_driver_core_1.internal.constants, WRITE = _a.ACCESS_MODE_WRITE, READ = _a.ACCESS_MODE_READ, ServerAddress = neo4j_driver_core_1.internal.serverAddress.ServerAddress;
var PROTOCOL_ERROR = neo4j_driver_core_1.error.PROTOCOL_ERROR;
var MIN_ROUTERS = 1;
/**
 * The routing table object used to determine the role of the servers in the driver.
 */
var RoutingTable = /** @class */ (function () {
    function RoutingTable(_a) {
        var _b = _a === void 0 ? {} : _a, database = _b.database, routers = _b.routers, readers = _b.readers, writers = _b.writers, expirationTime = _b.expirationTime, ttl = _b.ttl;
        this.database = database || null;
        this.databaseName = database || 'default database';
        this.routers = routers || [];
        this.readers = readers || [];
        this.writers = writers || [];
        this.expirationTime = expirationTime || (0, neo4j_driver_core_1.int)(0);
        this.ttl = ttl;
    }
    /**
     * Create a valid routing table from a raw object
     *
     * @param {string} database the database name. It is used for logging purposes
     * @param {ServerAddress} routerAddress The router address, it is used for loggin purposes
     * @param {RawRoutingTable} rawRoutingTable Method used to get the raw routing table to be processed
     * @param {RoutingTable} The valid Routing Table
     */
    RoutingTable.fromRawRoutingTable = function (database, routerAddress, rawRoutingTable) {
        return createValidRoutingTable(database, routerAddress, rawRoutingTable);
    };
    RoutingTable.prototype.forget = function (address) {
        // Don't remove it from the set of routers, since that might mean we lose our ability to re-discover,
        // just remove it from the set of readers and writers, so that we don't use it for actual work without
        // performing discovery first.
        this.readers = removeFromArray(this.readers, address);
        this.writers = removeFromArray(this.writers, address);
    };
    RoutingTable.prototype.forgetRouter = function (address) {
        this.routers = removeFromArray(this.routers, address);
    };
    RoutingTable.prototype.forgetWriter = function (address) {
        this.writers = removeFromArray(this.writers, address);
    };
    /**
     * Check if this routing table is fresh to perform the required operation.
     * @param {string} accessMode the type of operation. Allowed values are {@link READ} and {@link WRITE}.
     * @return {boolean} `true` when this table contains servers to serve the required operation, `false` otherwise.
     */
    RoutingTable.prototype.isStaleFor = function (accessMode) {
        return (this.expirationTime.lessThan(Date.now()) ||
            this.routers.length < MIN_ROUTERS ||
            (accessMode === READ && this.readers.length === 0) ||
            (accessMode === WRITE && this.writers.length === 0));
    };
    /**
     * Check if this routing table is expired for specified amount of duration
     *
     * @param {Integer} duration amount of duration in milliseconds to check for expiration
     * @returns {boolean}
     */
    RoutingTable.prototype.isExpiredFor = function (duration) {
        return this.expirationTime.add(duration).lessThan(Date.now());
    };
    RoutingTable.prototype.allServers = function () {
        return __spreadArray(__spreadArray(__spreadArray([], __read(this.routers), false), __read(this.readers), false), __read(this.writers), false);
    };
    RoutingTable.prototype.toString = function () {
        return ('RoutingTable[' +
            "database=".concat(this.databaseName, ", ") +
            "expirationTime=".concat(this.expirationTime, ", ") +
            "currentTime=".concat(Date.now(), ", ") +
            "routers=[".concat(this.routers, "], ") +
            "readers=[".concat(this.readers, "], ") +
            "writers=[".concat(this.writers, "]]"));
    };
    return RoutingTable;
}());
exports.default = RoutingTable;
/**
 * Remove all occurrences of the element in the array.
 * @param {Array} array the array to filter.
 * @param {Object} element the element to remove.
 * @return {Array} new filtered array.
 */
function removeFromArray(array, element) {
    return array.filter(function (item) { return item.asKey() !== element.asKey(); });
}
/**
 * Create a valid routing table from a raw object
 *
 * @param {string} db the database name. It is used for logging purposes
 * @param {ServerAddress} routerAddress The router address, it is used for loggin purposes
 * @param {RawRoutingTable} rawRoutingTable Method used to get the raw routing table to be processed
 * @param {RoutingTable} The valid Routing Table
 */
function createValidRoutingTable(database, routerAddress, rawRoutingTable) {
    var ttl = rawRoutingTable.ttl;
    var expirationTime = calculateExpirationTime(rawRoutingTable, routerAddress);
    var _a = parseServers(rawRoutingTable, routerAddress), routers = _a.routers, readers = _a.readers, writers = _a.writers;
    assertNonEmpty(routers, 'routers', routerAddress);
    assertNonEmpty(readers, 'readers', routerAddress);
    return new RoutingTable({
        database: database || rawRoutingTable.db,
        routers: routers,
        readers: readers,
        writers: writers,
        expirationTime: expirationTime,
        ttl: ttl
    });
}
/**
 * Parse server from the RawRoutingTable.
 *
 * @param {RawRoutingTable} rawRoutingTable the raw routing table
 * @param {string} routerAddress the router address
 * @returns {Object} The object with the list of routers, readers and writers
 */
function parseServers(rawRoutingTable, routerAddress) {
    try {
        var routers_1 = [];
        var readers_1 = [];
        var writers_1 = [];
        rawRoutingTable.servers.forEach(function (server) {
            var role = server.role;
            var addresses = server.addresses;
            if (role === 'ROUTE') {
                routers_1 = parseArray(addresses).map(function (address) {
                    return ServerAddress.fromUrl(address);
                });
            }
            else if (role === 'WRITE') {
                writers_1 = parseArray(addresses).map(function (address) {
                    return ServerAddress.fromUrl(address);
                });
            }
            else if (role === 'READ') {
                readers_1 = parseArray(addresses).map(function (address) {
                    return ServerAddress.fromUrl(address);
                });
            }
        });
        return {
            routers: routers_1,
            readers: readers_1,
            writers: writers_1
        };
    }
    catch (error) {
        throw (0, neo4j_driver_core_1.newError)("Unable to parse servers entry from router ".concat(routerAddress, " from addresses:\n").concat(neo4j_driver_core_1.json.stringify(rawRoutingTable.servers), "\nError message: ").concat(error.message), PROTOCOL_ERROR);
    }
}
/**
 * Call the expiration time using the ttls from the raw routing table and return it
 *
 * @param {RawRoutingTable} rawRoutingTable the routing table
 * @param {string} routerAddress the router address
 * @returns {number} the ttl
 */
function calculateExpirationTime(rawRoutingTable, routerAddress) {
    try {
        var now = (0, neo4j_driver_core_1.int)(Date.now());
        var expires = (0, neo4j_driver_core_1.int)(rawRoutingTable.ttl)
            .multiply(1000)
            .add(now);
        // if the server uses a really big expire time like Long.MAX_VALUE this may have overflowed
        if (expires.lessThan(now)) {
            return neo4j_driver_core_1.Integer.MAX_VALUE;
        }
        return expires;
    }
    catch (error) {
        throw (0, neo4j_driver_core_1.newError)("Unable to parse TTL entry from router ".concat(routerAddress, " from raw routing table:\n").concat(neo4j_driver_core_1.json.stringify(rawRoutingTable), "\nError message: ").concat(error.message), PROTOCOL_ERROR);
    }
}
/**
 * Assert if serverAddressesArray is not empty, throws and PROTOCOL_ERROR otherwise
 *
 * @param {string[]} serverAddressesArray array of addresses
 * @param {string} serversName the server name
 * @param {string} routerAddress the router address
 */
function assertNonEmpty(serverAddressesArray, serversName, routerAddress) {
    if (serverAddressesArray.length === 0) {
        throw (0, neo4j_driver_core_1.newError)('Received no ' + serversName + ' from router ' + routerAddress, PROTOCOL_ERROR);
    }
}
function parseArray(addresses) {
    if (!Array.isArray(addresses)) {
        throw new TypeError('Array expected but got: ' + addresses);
    }
    return Array.from(addresses);
}
