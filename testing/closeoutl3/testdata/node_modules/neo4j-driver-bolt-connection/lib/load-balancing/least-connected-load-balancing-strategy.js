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
var round_robin_array_index_1 = __importDefault(require("./round-robin-array-index"));
var load_balancing_strategy_1 = __importDefault(require("./load-balancing-strategy"));
var LeastConnectedLoadBalancingStrategy = /** @class */ (function (_super) {
    __extends(LeastConnectedLoadBalancingStrategy, _super);
    /**
     * @constructor
     * @param {Pool} connectionPool the connection pool of this driver.
     */
    function LeastConnectedLoadBalancingStrategy(connectionPool) {
        var _this = _super.call(this) || this;
        _this._readersIndex = new round_robin_array_index_1.default();
        _this._writersIndex = new round_robin_array_index_1.default();
        _this._connectionPool = connectionPool;
        return _this;
    }
    /**
     * @inheritDoc
     */
    LeastConnectedLoadBalancingStrategy.prototype.selectReader = function (knownReaders) {
        return this._select(knownReaders, this._readersIndex);
    };
    /**
     * @inheritDoc
     */
    LeastConnectedLoadBalancingStrategy.prototype.selectWriter = function (knownWriters) {
        return this._select(knownWriters, this._writersIndex);
    };
    LeastConnectedLoadBalancingStrategy.prototype._select = function (addresses, roundRobinIndex) {
        var length = addresses.length;
        if (length === 0) {
            return null;
        }
        // choose start index for iteration in round-robin fashion
        var startIndex = roundRobinIndex.next(length);
        var index = startIndex;
        var leastConnectedAddress = null;
        var leastActiveConnections = Number.MAX_SAFE_INTEGER;
        // iterate over the array to find least connected address
        do {
            var address = addresses[index];
            var activeConnections = this._connectionPool.activeResourceCount(address);
            if (activeConnections < leastActiveConnections) {
                leastConnectedAddress = address;
                leastActiveConnections = activeConnections;
            }
            // loop over to the start of the array when end is reached
            if (index === length - 1) {
                index = 0;
            }
            else {
                index++;
            }
        } while (index !== startIndex);
        return leastConnectedAddress;
    };
    return LeastConnectedLoadBalancingStrategy;
}(load_balancing_strategy_1.default));
exports.default = LeastConnectedLoadBalancingStrategy;
