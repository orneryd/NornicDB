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
/**
 * Cache which maps users to their last known home database, along with the last time the entry was accessed.
 *
 * @private
 */
var HomeDatabaseCache = /** @class */ (function () {
    function HomeDatabaseCache(maxSize) {
        this.maxSize = maxSize;
        this.pruneCount = Math.max(Math.round(0.01 * maxSize * Math.log(maxSize)), 1);
        this.map = new Map();
    }
    /**
     * Updates or adds an entry to the cache, and prunes the cache if above the maximum allowed size
     *
     * @param {string} user cache key for the user to set
     * @param {string} database new home database to set for the user
     */
    HomeDatabaseCache.prototype.set = function (user, database) {
        this.map.set(user, { database: database, lastUsed: Date.now() });
        this._pruneCache();
    };
    /**
     * retrieves the last known home database for a user
     *
     * @param {string} user cache key for the user to get
     */
    HomeDatabaseCache.prototype.get = function (user) {
        var value = this.map.get(user);
        if (value !== undefined) {
            value.lastUsed = Date.now();
            return value.database;
        }
        return undefined;
    };
    /**
     * removes the entry for a given user in the cache
     *
     * @param {string} user cache key for the user to remove
     */
    HomeDatabaseCache.prototype.delete = function (user) {
        this.map.delete(user);
    };
    /**
     * Removes a number of the oldest entries in the cache if the number of entries has exceeded the maximum size.
     */
    HomeDatabaseCache.prototype._pruneCache = function () {
        if (this.map.size > this.maxSize) {
            var sortedArray = Array.from(this.map.entries()).sort(function (a, b) { return a[1].lastUsed - b[1].lastUsed; });
            for (var i = 0; i < this.pruneCount; i++) {
                this.map.delete(sortedArray[i][0]);
            }
        }
    };
    return HomeDatabaseCache;
}());
exports.default = HomeDatabaseCache;
