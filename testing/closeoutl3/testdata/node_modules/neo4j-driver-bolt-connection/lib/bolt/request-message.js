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
exports.SIGNATURES = void 0;
var neo4j_driver_core_1 = require("neo4j-driver-core");
var _a = neo4j_driver_core_1.internal.constants, ACCESS_MODE_READ = _a.ACCESS_MODE_READ, FETCH_ALL = _a.FETCH_ALL, assertString = neo4j_driver_core_1.internal.util.assertString;
/* eslint-disable no-unused-vars */
// Signature bytes for each request message type
var INIT = 0x01; // 0000 0001 // INIT <user_agent> <authentication_token>
var ACK_FAILURE = 0x0e; // 0000 1110 // ACK_FAILURE - unused
var RESET = 0x0f; // 0000 1111 // RESET
var RUN = 0x10; // 0001 0000 // RUN <query> <parameters>
var DISCARD_ALL = 0x2f; // 0010 1111 // DISCARD_ALL - unused
var PULL_ALL = 0x3f; // 0011 1111 // PULL_ALL
var HELLO = 0x01; // 0000 0001 // HELLO <metadata>
var GOODBYE = 0x02; // 0000 0010 // GOODBYE
var BEGIN = 0x11; // 0001 0001 // BEGIN <metadata>
var COMMIT = 0x12; // 0001 0010 // COMMIT
var ROLLBACK = 0x13; // 0001 0011 // ROLLBACK
var TELEMETRY = 0x54; // 0101 0100 // TELEMETRY <api>
var ROUTE = 0x66; // 0110 0110 // ROUTE
var LOGON = 0x6A; // LOGON
var LOGOFF = 0x6B; // LOGOFF
var DISCARD = 0x2f; // 0010 1111 // DISCARD
var PULL = 0x3f; // 0011 1111 // PULL
var READ_MODE = 'r';
/* eslint-enable no-unused-vars */
var NO_STATEMENT_ID = -1;
var SIGNATURES = Object.freeze({
    INIT: INIT,
    RESET: RESET,
    RUN: RUN,
    PULL_ALL: PULL_ALL,
    HELLO: HELLO,
    GOODBYE: GOODBYE,
    BEGIN: BEGIN,
    COMMIT: COMMIT,
    ROLLBACK: ROLLBACK,
    TELEMETRY: TELEMETRY,
    ROUTE: ROUTE,
    LOGON: LOGON,
    LOGOFF: LOGOFF,
    DISCARD: DISCARD,
    PULL: PULL
});
exports.SIGNATURES = SIGNATURES;
var RequestMessage = /** @class */ (function () {
    function RequestMessage(signature, fields, toString) {
        this.signature = signature;
        this.fields = fields;
        this.toString = toString;
    }
    /**
     * Create a new INIT message.
     * @param {string} clientName the client name.
     * @param {Object} authToken the authentication token.
     * @return {RequestMessage} new INIT message.
     */
    RequestMessage.init = function (clientName, authToken) {
        return new RequestMessage(INIT, [clientName, authToken], function () { return "INIT ".concat(clientName, " {...}"); });
    };
    /**
     * Create a new RUN message.
     * @param {string} query the cypher query.
     * @param {Object} parameters the query parameters.
     * @return {RequestMessage} new RUN message.
     */
    RequestMessage.run = function (query, parameters) {
        return new RequestMessage(RUN, [query, parameters], function () { return "RUN ".concat(query, " ").concat(neo4j_driver_core_1.json.stringify(parameters)); });
    };
    /**
     * Get a PULL_ALL message.
     * @return {RequestMessage} the PULL_ALL message.
     */
    RequestMessage.pullAll = function () {
        return PULL_ALL_MESSAGE;
    };
    /**
     * Get a RESET message.
     * @return {RequestMessage} the RESET message.
     */
    RequestMessage.reset = function () {
        return RESET_MESSAGE;
    };
    /**
     * Create a new HELLO message.
     * @param {string} userAgent the user agent.
     * @param {Object} authToken the authentication token.
     * @param {Object} optional server side routing, set to routing context to turn on server side routing (> 4.1)
     * @return {RequestMessage} new HELLO message.
     */
    RequestMessage.hello = function (userAgent, authToken, routing, patchs) {
        if (routing === void 0) { routing = null; }
        if (patchs === void 0) { patchs = null; }
        var metadata = Object.assign({ user_agent: userAgent }, authToken);
        if (routing) {
            metadata.routing = routing;
        }
        if (patchs) {
            metadata.patch_bolt = patchs;
        }
        return new RequestMessage(HELLO, [metadata], function () { return "HELLO {user_agent: '".concat(userAgent, "', ...}"); });
    };
    /**
     * Create a new HELLO message.
     * @param {string} userAgent the user agent.
     * @param {Object} optional server side routing, set to routing context to turn on server side routing (> 4.1)
     * @return {RequestMessage} new HELLO message.
     */
    RequestMessage.hello5x1 = function (userAgent, routing) {
        if (routing === void 0) { routing = null; }
        var metadata = { user_agent: userAgent };
        if (routing) {
            metadata.routing = routing;
        }
        return new RequestMessage(HELLO, [metadata], function () { return "HELLO {user_agent: '".concat(userAgent, "', ...}"); });
    };
    /**
     * Create a new HELLO message.
     * @param {string} userAgent the user agent.
     * @param {NotificationFilter} notificationFilter the notification filter configured
     * @param {Object} routing server side routing, set to routing context to turn on server side routing (> 4.1)
     * @return {RequestMessage} new HELLO message.
     */
    RequestMessage.hello5x2 = function (userAgent, notificationFilter, routing) {
        if (notificationFilter === void 0) { notificationFilter = null; }
        if (routing === void 0) { routing = null; }
        var metadata = { user_agent: userAgent };
        appendLegacyNotificationFilterToMetadata(metadata, notificationFilter);
        if (routing) {
            metadata.routing = routing;
        }
        return new RequestMessage(HELLO, [metadata], function () { return "HELLO ".concat(neo4j_driver_core_1.json.stringify(metadata)); });
    };
    /**
     * Create a new HELLO message.
     * @param {string} userAgent the user agent.
     * @param {string} boltAgent the bolt agent.
     * @param {NotificationFilter} notificationFilter the notification filter configured
     * @param {Object} routing server side routing, set to routing context to turn on server side routing (> 4.1)
     * @return {RequestMessage} new HELLO message.
     */
    RequestMessage.hello5x3 = function (userAgent, boltAgent, notificationFilter, routing) {
        if (notificationFilter === void 0) { notificationFilter = null; }
        if (routing === void 0) { routing = null; }
        var metadata = {};
        if (userAgent) {
            metadata.user_agent = userAgent;
        }
        if (boltAgent) {
            metadata.bolt_agent = {
                product: boltAgent.product,
                platform: boltAgent.platform,
                language: boltAgent.language,
                language_details: boltAgent.languageDetails
            };
        }
        appendLegacyNotificationFilterToMetadata(metadata, notificationFilter);
        if (routing) {
            metadata.routing = routing;
        }
        return new RequestMessage(HELLO, [metadata], function () { return "HELLO ".concat(neo4j_driver_core_1.json.stringify(metadata)); });
    };
    /**
     * Create a new HELLO message.
     * @param {string} userAgent the user agent.
     * @param {string} boltAgent the bolt agent.
     * @param {NotificationFilter} notificationFilter the notification filter configured
     * @param {Object} routing server side routing, set to routing context to turn on server side routing (> 4.1)
     * @return {RequestMessage} new HELLO message.
     */
    RequestMessage.hello5x5 = function (userAgent, boltAgent, notificationFilter, routing) {
        if (notificationFilter === void 0) { notificationFilter = null; }
        if (routing === void 0) { routing = null; }
        var metadata = {};
        if (userAgent) {
            metadata.user_agent = userAgent;
        }
        if (boltAgent) {
            metadata.bolt_agent = {
                product: boltAgent.product,
                platform: boltAgent.platform,
                language: boltAgent.language,
                language_details: boltAgent.languageDetails
            };
        }
        appendGqlNotificationFilterToMetadata(metadata, notificationFilter);
        if (routing) {
            metadata.routing = routing;
        }
        return new RequestMessage(HELLO, [metadata], function () { return "HELLO ".concat(neo4j_driver_core_1.json.stringify(metadata)); });
    };
    /**
     * Create a new LOGON message.
     *
     * @param {object} authToken The auth token
     * @returns {RequestMessage} new LOGON message
     */
    RequestMessage.logon = function (authToken) {
        return new RequestMessage(LOGON, [authToken], function () { return 'LOGON { ... }'; });
    };
    /**
     * Create a new LOGOFF message.
     *
     * @returns {RequestMessage} new LOGOFF message
     */
    RequestMessage.logoff = function () {
        return new RequestMessage(LOGOFF, [], function () { return 'LOGOFF'; });
    };
    /**
     * Create a new BEGIN message.
     * @param {Bookmarks} bookmarks the bookmarks.
     * @param {TxConfig} txConfig the configuration.
     * @param {string} database the database name.
     * @param {string} mode the access mode.
     * @param {string} impersonatedUser the impersonated user.
     * @param {NotificationFilter} notificationFilter the notification filter
     * @return {RequestMessage} new BEGIN message.
     */
    RequestMessage.begin = function (_a) {
        var _b = _a === void 0 ? {} : _a, bookmarks = _b.bookmarks, txConfig = _b.txConfig, database = _b.database, mode = _b.mode, impersonatedUser = _b.impersonatedUser, notificationFilter = _b.notificationFilter;
        var metadata = buildTxMetadata(bookmarks, txConfig, database, mode, impersonatedUser, notificationFilter);
        return new RequestMessage(BEGIN, [metadata], function () { return "BEGIN ".concat(neo4j_driver_core_1.json.stringify(metadata)); });
    };
    /**
     * Create a new BEGIN message.
     * @param {Bookmarks} bookmarks the bookmarks.
     * @param {TxConfig} txConfig the configuration.
     * @param {string} database the database name.
     * @param {string} mode the access mode.
     * @param {string} impersonatedUser the impersonated user.
     * @param {NotificationFilter} notificationFilter the notification filter
     * @return {RequestMessage} new BEGIN message.
     */
    RequestMessage.begin5x5 = function (_a) {
        var _b = _a === void 0 ? {} : _a, bookmarks = _b.bookmarks, txConfig = _b.txConfig, database = _b.database, mode = _b.mode, impersonatedUser = _b.impersonatedUser, notificationFilter = _b.notificationFilter;
        var metadata = buildTxMetadata(bookmarks, txConfig, database, mode, impersonatedUser, notificationFilter, {
            appendNotificationFilter: appendGqlNotificationFilterToMetadata
        });
        return new RequestMessage(BEGIN, [metadata], function () { return "BEGIN ".concat(neo4j_driver_core_1.json.stringify(metadata)); });
    };
    /**
     * Get a COMMIT message.
     * @return {RequestMessage} the COMMIT message.
     */
    RequestMessage.commit = function () {
        return COMMIT_MESSAGE;
    };
    /**
     * Get a ROLLBACK message.
     * @return {RequestMessage} the ROLLBACK message.
     */
    RequestMessage.rollback = function () {
        return ROLLBACK_MESSAGE;
    };
    /**
     * Create a new RUN message with additional metadata.
     * @param {string} query the cypher query.
     * @param {Object} parameters the query parameters.
     * @param {Object} extra - extra params
     * @param {Bookmarks} extra.bookmarks the bookmarks.
     * @param {TxConfig} extra.txConfig the configuration.
     * @param {string} extra.database the database name.
     * @param {string} extra.mode the access mode.
     * @param {string} extra.impersonatedUser the impersonated user.
     * @param {notificationFilter} extra.notificationFilter the notification filter
     * @return {RequestMessage} new RUN message with additional metadata.
     */
    RequestMessage.runWithMetadata = function (query, parameters, _a) {
        var _b = _a === void 0 ? {} : _a, bookmarks = _b.bookmarks, txConfig = _b.txConfig, database = _b.database, mode = _b.mode, impersonatedUser = _b.impersonatedUser, notificationFilter = _b.notificationFilter;
        var metadata = buildTxMetadata(bookmarks, txConfig, database, mode, impersonatedUser, notificationFilter);
        return new RequestMessage(RUN, [query, parameters, metadata], function () {
            return "RUN ".concat(query, " ").concat(neo4j_driver_core_1.json.stringify(parameters), " ").concat(neo4j_driver_core_1.json.stringify(metadata));
        });
    };
    /**
     * Create a new RUN message with additional metadata.
     * @param {string} query the cypher query.
     * @param {Object} parameters the query parameters.
     * @param {Object} extra - extra params
     * @param {Bookmarks} extra.bookmarks the bookmarks.
     * @param {TxConfig} extra.txConfig the configuration.
     * @param {string} extra.database the database name.
     * @param {string} extra.mode the access mode.
     * @param {string} extra.impersonatedUser the impersonated user.
     * @param {notificationFilter} extra.notificationFilter the notification filter
     * @return {RequestMessage} new RUN message with additional metadata.
     */
    RequestMessage.runWithMetadata5x5 = function (query, parameters, _a) {
        var _b = _a === void 0 ? {} : _a, bookmarks = _b.bookmarks, txConfig = _b.txConfig, database = _b.database, mode = _b.mode, impersonatedUser = _b.impersonatedUser, notificationFilter = _b.notificationFilter;
        var metadata = buildTxMetadata(bookmarks, txConfig, database, mode, impersonatedUser, notificationFilter, {
            appendNotificationFilter: appendGqlNotificationFilterToMetadata
        });
        return new RequestMessage(RUN, [query, parameters, metadata], function () {
            return "RUN ".concat(query, " ").concat(neo4j_driver_core_1.json.stringify(parameters), " ").concat(neo4j_driver_core_1.json.stringify(metadata));
        });
    };
    /**
     * Get a GOODBYE message.
     * @return {RequestMessage} the GOODBYE message.
     */
    RequestMessage.goodbye = function () {
        return GOODBYE_MESSAGE;
    };
    /**
     * Generates a new PULL message with additional metadata.
     * @param {Integer|number} stmtId
     * @param {Integer|number} n
     * @return {RequestMessage} the PULL message.
     */
    RequestMessage.pull = function (_a) {
        var _b = _a === void 0 ? {} : _a, _c = _b.stmtId, stmtId = _c === void 0 ? NO_STATEMENT_ID : _c, _d = _b.n, n = _d === void 0 ? FETCH_ALL : _d;
        var metadata = buildStreamMetadata(stmtId === null || stmtId === undefined ? NO_STATEMENT_ID : stmtId, n || FETCH_ALL);
        return new RequestMessage(PULL, [metadata], function () { return "PULL ".concat(neo4j_driver_core_1.json.stringify(metadata)); });
    };
    /**
     * Generates a new DISCARD message with additional metadata.
     * @param {Integer|number} stmtId
     * @param {Integer|number} n
     * @return {RequestMessage} the PULL message.
     */
    RequestMessage.discard = function (_a) {
        var _b = _a === void 0 ? {} : _a, _c = _b.stmtId, stmtId = _c === void 0 ? NO_STATEMENT_ID : _c, _d = _b.n, n = _d === void 0 ? FETCH_ALL : _d;
        var metadata = buildStreamMetadata(stmtId === null || stmtId === undefined ? NO_STATEMENT_ID : stmtId, n || FETCH_ALL);
        return new RequestMessage(DISCARD, [metadata], function () { return "DISCARD ".concat(neo4j_driver_core_1.json.stringify(metadata)); });
    };
    RequestMessage.telemetry = function (_a) {
        var api = _a.api;
        var parsedApi = (0, neo4j_driver_core_1.int)(api);
        return new RequestMessage(TELEMETRY, [parsedApi], function () { return "TELEMETRY ".concat(parsedApi.toString()); });
    };
    /**
     * Generate the ROUTE message, this message is used to fetch the routing table from the server
     *
     * @param {object} routingContext The routing context used to define the routing table. Multi-datacenter deployments is one of its use cases
     * @param {string[]} bookmarks The list of the bookmarks should be used
     * @param {string} databaseName The name of the database to get the routing table for.
     * @return {RequestMessage} the ROUTE message.
     */
    RequestMessage.route = function (routingContext, bookmarks, databaseName) {
        if (routingContext === void 0) { routingContext = {}; }
        if (bookmarks === void 0) { bookmarks = []; }
        if (databaseName === void 0) { databaseName = null; }
        return new RequestMessage(ROUTE, [routingContext, bookmarks, databaseName], function () {
            return "ROUTE ".concat(neo4j_driver_core_1.json.stringify(routingContext), " ").concat(neo4j_driver_core_1.json.stringify(bookmarks), " ").concat(databaseName);
        });
    };
    /**
     * Generate the ROUTE message, this message is used to fetch the routing table from the server
     *
     * @param {object} routingContext The routing context used to define the routing table. Multi-datacenter deployments is one of its use cases
     * @param {string[]} bookmarks The list of the bookmarks should be used
     * @param {object} databaseContext The context inforamtion of the database to get the routing table for.
     * @param {string} databaseContext.databaseName The name of the database to get the routing table.
     * @param {string} databaseContext.impersonatedUser The name of the user to impersonation when getting the routing table.
     * @return {RequestMessage} the ROUTE message.
     */
    RequestMessage.routeV4x4 = function (routingContext, bookmarks, databaseContext) {
        if (routingContext === void 0) { routingContext = {}; }
        if (bookmarks === void 0) { bookmarks = []; }
        if (databaseContext === void 0) { databaseContext = {}; }
        var dbContext = {};
        if (databaseContext.databaseName) {
            dbContext.db = databaseContext.databaseName;
        }
        if (databaseContext.impersonatedUser) {
            dbContext.imp_user = databaseContext.impersonatedUser;
        }
        return new RequestMessage(ROUTE, [routingContext, bookmarks, dbContext], function () {
            return "ROUTE ".concat(neo4j_driver_core_1.json.stringify(routingContext), " ").concat(neo4j_driver_core_1.json.stringify(bookmarks), " ").concat(neo4j_driver_core_1.json.stringify(dbContext));
        });
    };
    return RequestMessage;
}());
exports.default = RequestMessage;
/**
 * Create an object that represent transaction metadata.
 * @param {Bookmarks} bookmarks the bookmarks.
 * @param {TxConfig} txConfig the configuration.
 * @param {string} database the database name.
 * @param {string} mode the access mode.
 * @param {string} impersonatedUser the impersonated user mode.
 * @param {notificationFilter} notificationFilter the notification filter
 * @param {Object} functions Transformation functions applied to metadata
 * @param {function(metadata,notificationFilter):void} functions.appendNotificationFilter Changes metadata by appending the Notification Filter to it.
 * @return {Object} a metadata object.
 */
function buildTxMetadata(bookmarks, txConfig, database, mode, impersonatedUser, notificationFilter, functions) {
    var _a;
    if (functions === void 0) { functions = {}; }
    var metadata = {};
    if (!bookmarks.isEmpty()) {
        metadata.bookmarks = bookmarks.values();
    }
    if (txConfig.timeout !== null) {
        metadata.tx_timeout = txConfig.timeout;
    }
    if (txConfig.metadata) {
        metadata.tx_metadata = txConfig.metadata;
    }
    if (database) {
        metadata.db = assertString(database, 'database');
    }
    if (impersonatedUser) {
        metadata.imp_user = assertString(impersonatedUser, 'impersonatedUser');
    }
    if (mode === ACCESS_MODE_READ) {
        metadata.mode = READ_MODE;
    }
    var appendNotificationFilter = (_a = functions.appendNotificationFilter) !== null && _a !== void 0 ? _a : appendLegacyNotificationFilterToMetadata;
    appendNotificationFilter(metadata, notificationFilter);
    return metadata;
}
/**
 * Create an object that represents streaming metadata.
 * @param {Integer|number} stmtId The query id to stream its results.
 * @param {Integer|number} n The number of records to stream.
 * @returns {Object} a metadata object.
 */
function buildStreamMetadata(stmtId, n) {
    var metadata = { n: (0, neo4j_driver_core_1.int)(n) };
    if (stmtId !== NO_STATEMENT_ID) {
        metadata.qid = (0, neo4j_driver_core_1.int)(stmtId);
    }
    return metadata;
}
function appendLegacyNotificationFilterToMetadata(metadata, notificationFilter) {
    if (notificationFilter) {
        if (notificationFilter.minimumSeverityLevel) {
            metadata.notifications_minimum_severity = notificationFilter.minimumSeverityLevel;
        }
        if (notificationFilter.disabledCategories) {
            metadata.notifications_disabled_categories = notificationFilter.disabledCategories;
        }
        if (notificationFilter.disabledClassifications) {
            metadata.notifications_disabled_categories = notificationFilter.disabledClassifications;
        }
    }
}
function appendGqlNotificationFilterToMetadata(metadata, notificationFilter) {
    if (notificationFilter) {
        if (notificationFilter.minimumSeverityLevel) {
            metadata.notifications_minimum_severity = notificationFilter.minimumSeverityLevel;
        }
        if (notificationFilter.disabledCategories) {
            metadata.notifications_disabled_classifications = notificationFilter.disabledCategories;
        }
        if (notificationFilter.disabledClassifications) {
            metadata.notifications_disabled_classifications = notificationFilter.disabledClassifications;
        }
    }
}
// constants for messages that never change
var PULL_ALL_MESSAGE = new RequestMessage(PULL_ALL, [], function () { return 'PULL_ALL'; });
var RESET_MESSAGE = new RequestMessage(RESET, [], function () { return 'RESET'; });
var COMMIT_MESSAGE = new RequestMessage(COMMIT, [], function () { return 'COMMIT'; });
var ROLLBACK_MESSAGE = new RequestMessage(ROLLBACK, [], function () { return 'ROLLBACK'; });
var GOODBYE_MESSAGE = new RequestMessage(GOODBYE, [], function () { return 'GOODBYE'; });
