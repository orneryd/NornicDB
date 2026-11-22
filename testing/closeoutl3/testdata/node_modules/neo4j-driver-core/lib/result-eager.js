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
 * Represents the fully streamed result
 */
var EagerResult = /** @class */ (function () {
    /**
     * @constructor
     * @private
     * @param {string[]} keys The records keys
     * @param {Record[]} records The resulted records
     * @param {ResultSummary[]} summary The result Summary
     */
    function EagerResult(keys, records, summary) {
        /**
         * Field keys, in the order the fields appear in the records.
         * @type {string[]}
         */
        this.keys = keys;
        /**
         * Field records, in the order the records arrived from the server.
         * @type {Record[]}
         */
        this.records = records;
        /**
         * Field summary
         * @type {ResultSummary}
         */
        this.summary = summary;
    }
    return EagerResult;
}());
exports.default = EagerResult;
