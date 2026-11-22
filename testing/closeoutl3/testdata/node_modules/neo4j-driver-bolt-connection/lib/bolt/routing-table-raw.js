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
// eslint-disable-next-line no-unused-vars
var neo4j_driver_core_1 = __importDefault(require("neo4j-driver-core"));
/**
 * Represente the raw version of the routing table
 */
var RawRoutingTable = /** @class */ (function () {
    function RawRoutingTable() {
    }
    /**
     * Constructs the raw routing table for Record based result
     * @param {Record} record The record which will be used get the raw routing table
     * @returns {RawRoutingTable} The raw routing table
     */
    RawRoutingTable.ofRecord = function (record) {
        if (record === null) {
            return RawRoutingTable.ofNull();
        }
        return new RecordRawRoutingTable(record);
    };
    /**
     * Constructs the raw routing table for Success result for a Routing Message
     * @param {object} response The result
     * @returns {RawRoutingTable} The raw routing table
     */
    RawRoutingTable.ofMessageResponse = function (response) {
        if (response === null) {
            return RawRoutingTable.ofNull();
        }
        return new ResponseRawRoutingTable(response);
    };
    /**
     * Construct the raw routing table of a null response
     *
     * @returns {RawRoutingTable} the raw routing table
     */
    RawRoutingTable.ofNull = function () {
        return new NullRawRoutingTable();
    };
    Object.defineProperty(RawRoutingTable.prototype, "ttl", {
        /**
         * Get raw ttl
         *
         * @returns {number|string} ttl Time to live
         */
        get: function () {
            throw new Error('Not implemented');
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(RawRoutingTable.prototype, "db", {
        /**
         * Get raw db
         *
         * @returns {string?} The database name
         */
        get: function () {
            throw new Error('Not implemented');
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(RawRoutingTable.prototype, "servers", {
        /**
         *
         * @typedef {Object} ServerRole
         * @property {string} role the role of the address on the cluster
         * @property {string[]} addresses the address within the role
         *
         * @return {ServerRole[]} list of servers addresses
         */
        get: function () {
            throw new Error('Not implemented');
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(RawRoutingTable.prototype, "isNull", {
        /**
         * Indicates the result is null
         *
         * @returns {boolean} Is null
         */
        get: function () {
            throw new Error('Not implemented');
        },
        enumerable: false,
        configurable: true
    });
    return RawRoutingTable;
}());
exports.default = RawRoutingTable;
/**
 * Get the raw routing table information from route message response
 */
var ResponseRawRoutingTable = /** @class */ (function (_super) {
    __extends(ResponseRawRoutingTable, _super);
    function ResponseRawRoutingTable(response) {
        var _this = _super.call(this) || this;
        _this._response = response;
        return _this;
    }
    Object.defineProperty(ResponseRawRoutingTable.prototype, "ttl", {
        get: function () {
            return this._response.rt.ttl;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(ResponseRawRoutingTable.prototype, "servers", {
        get: function () {
            return this._response.rt.servers;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(ResponseRawRoutingTable.prototype, "db", {
        get: function () {
            return this._response.rt.db;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(ResponseRawRoutingTable.prototype, "isNull", {
        get: function () {
            return this._response === null;
        },
        enumerable: false,
        configurable: true
    });
    return ResponseRawRoutingTable;
}(RawRoutingTable));
/**
 * Null routing table
 */
var NullRawRoutingTable = /** @class */ (function (_super) {
    __extends(NullRawRoutingTable, _super);
    function NullRawRoutingTable() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    Object.defineProperty(NullRawRoutingTable.prototype, "isNull", {
        get: function () {
            return true;
        },
        enumerable: false,
        configurable: true
    });
    return NullRawRoutingTable;
}(RawRoutingTable));
/**
 * Get the raw routing table information from the record
 */
var RecordRawRoutingTable = /** @class */ (function (_super) {
    __extends(RecordRawRoutingTable, _super);
    function RecordRawRoutingTable(record) {
        var _this = _super.call(this) || this;
        _this._record = record;
        return _this;
    }
    Object.defineProperty(RecordRawRoutingTable.prototype, "ttl", {
        get: function () {
            return this._record.get('ttl');
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(RecordRawRoutingTable.prototype, "servers", {
        get: function () {
            return this._record.get('servers');
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(RecordRawRoutingTable.prototype, "db", {
        get: function () {
            return this._record.has('db') ? this._record.get('db') : null;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(RecordRawRoutingTable.prototype, "isNull", {
        get: function () {
            return this._record === null;
        },
        enumerable: false,
        configurable: true
    });
    return RecordRawRoutingTable;
}(RawRoutingTable));
