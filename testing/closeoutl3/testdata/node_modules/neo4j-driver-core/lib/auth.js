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
var error_1 = require("./error");
var json_1 = require("./json");
/**
 * @property {function(username: string, password: string, realm: ?string)} basic the function to create a
 * basic authentication token.
 * @property {function(base64EncodedTicket: string)} kerberos the function to create a Kerberos authentication token.
 * Accepts a single string argument - base64 encoded Kerberos ticket.
 * @property {function(base64EncodedTicket: string)} bearer the function to create a Bearer authentication token.
 * Accepts a single string argument - base64 encoded Bearer ticket.
 * @property {function(principal: string, credentials: string, realm: string, scheme: string, parameters: ?object)} custom
 * the function to create a custom authentication token.
 */
var auth = {
    basic: function (username, password, realm) {
        if (realm != null) {
            return {
                scheme: 'basic',
                principal: username,
                credentials: password,
                realm: realm
            };
        }
        else {
            return { scheme: 'basic', principal: username, credentials: password };
        }
    },
    kerberos: function (base64EncodedTicket) {
        return {
            scheme: 'kerberos',
            principal: '', // This empty string is required for backwards compatibility.
            credentials: base64EncodedTicket
        };
    },
    bearer: function (base64EncodedToken) {
        return {
            scheme: 'bearer',
            credentials: base64EncodedToken
        };
    },
    none: function () {
        return {
            scheme: 'none'
        };
    },
    custom: function (principal, credentials, realm, scheme, parameters) {
        var output = {
            scheme: scheme,
            principal: principal
        };
        if (isNotEmpty(credentials)) {
            output.credentials = credentials;
        }
        if (isNotEmpty(realm)) {
            output.realm = realm;
        }
        if (isNotEmpty(parameters)) {
            try {
                (0, json_1.stringify)(parameters);
            }
            catch (e) {
                // eslint-disable-next-line
                // @ts-ignore
                throw (0, error_1.newError)('Circular references in custom auth token parameters', undefined, e);
            }
            output.parameters = parameters;
        }
        return output;
    }
};
function isNotEmpty(value) {
    return !(value === null ||
        value === undefined ||
        value === '' ||
        (Object.getPrototypeOf(value) === Object.prototype && Object.keys(value).length === 0));
}
exports.default = auth;
