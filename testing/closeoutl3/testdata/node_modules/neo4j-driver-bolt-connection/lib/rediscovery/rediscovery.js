"use strict";
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
var routing_table_1 = __importDefault(require("./routing-table"));
// eslint-disable-next-line no-unused-vars
var neo4j_driver_core_1 = require("neo4j-driver-core");
var Rediscovery = /** @class */ (function () {
    /**
     * @constructor
     * @param {object} routingContext
     */
    function Rediscovery(routingContext) {
        this._routingContext = routingContext;
    }
    /**
     * Try to fetch new routing table from the given router.
     * @param {Session} session the session to use.
     * @param {string} database the database for which to lookup routing table.
     * @param {ServerAddress} routerAddress the URL of the router.
     * @param {string} impersonatedUser The impersonated user
     * @return {Promise<RoutingTable>} promise resolved with new routing table or null when connection error happened.
     */
    Rediscovery.prototype.lookupRoutingTableOnRouter = function (session, database, routerAddress, impersonatedUser) {
        var _this = this;
        return session._acquireConnection(function (connection) {
            return _this._requestRawRoutingTable(connection, session, database, routerAddress, impersonatedUser).then(function (rawRoutingTable) {
                if (rawRoutingTable.isNull) {
                    return null;
                }
                return routing_table_1.default.fromRawRoutingTable(database, routerAddress, rawRoutingTable);
            });
        });
    };
    Rediscovery.prototype._requestRawRoutingTable = function (connection, session, database, routerAddress, impersonatedUser) {
        var _this = this;
        return new Promise(function (resolve, reject) {
            connection.protocol().requestRoutingInformation({
                routingContext: _this._routingContext,
                databaseName: database,
                impersonatedUser: impersonatedUser,
                sessionContext: {
                    bookmarks: session._lastBookmarks,
                    mode: session._mode,
                    database: session._database,
                    afterComplete: session._onComplete
                },
                onCompleted: resolve,
                onError: reject
            });
        });
    };
    return Rediscovery;
}());
exports.default = Rediscovery;
