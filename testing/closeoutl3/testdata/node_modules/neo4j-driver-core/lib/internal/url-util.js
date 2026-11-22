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
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.Url = void 0;
exports.parseDatabaseUrl = parseDatabaseUrl;
exports.defaultPortForScheme = defaultPortForScheme;
exports.formatIPv4Address = formatIPv4Address;
exports.formatIPv6Address = formatIPv6Address;
var util_1 = require("./util");
var DEFAULT_BOLT_PORT = 7687;
var DEFAULT_HTTP_PORT = 7474;
var DEFAULT_HTTPS_PORT = 7473;
var Url = /** @class */ (function () {
    function Url(scheme, host, port, hostAndPort, query) {
        /**
         * Nullable scheme (protocol) of the URL.
         * Example: 'bolt', 'neo4j', 'http', 'https', etc.
         * @type {string}
         */
        this.scheme = scheme;
        /**
         * Nonnull host name or IP address. IPv6 not wrapped in square brackets.
         * Example: 'neo4j.com', 'localhost', '127.0.0.1', '192.168.10.15', '::1', '2001:4860:4860::8844', etc.
         * @type {string}
         */
        this.host = host;
        /**
         * Nonnull number representing port. Default port for the given scheme is used if given URL string
         * does not contain port. Example: 7687 for bolt, 7474 for HTTP and 7473 for HTTPS.
         * @type {number}
         */
        this.port = port;
        /**
         * Nonnull host name or IP address plus port, separated by ':'. IPv6 wrapped in square brackets.
         * Example: 'neo4j.com', 'neo4j.com:7687', '127.0.0.1', '127.0.0.1:8080', '[2001:4860:4860::8844]',
         * '[2001:4860:4860::8844]:9090', etc.
         * @type {string}
         */
        this.hostAndPort = hostAndPort;
        /**
         * Nonnull object representing parsed query string key-value pairs. Duplicated keys not supported.
         * Example: '{}', '{'key1': 'value1', 'key2': 'value2'}', etc.
         * @type {Object}
         */
        this.query = query;
    }
    return Url;
}());
exports.Url = Url;
function parseDatabaseUrl(url) {
    var _a;
    (0, util_1.assertString)(url, 'URL');
    var sanitized = sanitizeUrl(url);
    var parsedUrl = uriJsParse(sanitized.url);
    var scheme = sanitized.schemeMissing
        ? null
        : extractScheme(parsedUrl.scheme);
    var host = extractHost(parsedUrl.host); // no square brackets for IPv6
    var formattedHost = formatHost(host); // has square brackets for IPv6
    var port = extractPort(parsedUrl.port, scheme);
    var hostAndPort = "".concat(formattedHost, ":").concat(port);
    var query = extractQuery(
    // @ts-expect-error
    (_a = parsedUrl.query) !== null && _a !== void 0 ? _a : extractResourceQueryString(parsedUrl.resourceName), url);
    return new Url(scheme, host, port, hostAndPort, query);
}
function extractResourceQueryString(resource) {
    if (typeof resource !== 'string') {
        return null;
    }
    var _a = __read(resource.split('?'), 2), query = _a[1];
    return query;
}
function sanitizeUrl(url) {
    url = url.trim();
    if (!url.includes('://')) {
        // url does not contain scheme, add dummy 'none://' to make parser work correctly
        return { schemeMissing: true, url: "none://".concat(url) };
    }
    return { schemeMissing: false, url: url };
}
function extractScheme(scheme) {
    if (scheme != null) {
        scheme = scheme.trim();
        if (scheme.charAt(scheme.length - 1) === ':') {
            scheme = scheme.substring(0, scheme.length - 1);
        }
        return scheme;
    }
    return null;
}
function extractHost(host, url) {
    if (host == null) {
        throw new Error('Unable to extract host from null or undefined URL');
    }
    return host.trim();
}
function extractPort(portString, scheme) {
    var port = typeof portString === 'string' ? parseInt(portString, 10) : portString;
    return port != null && !isNaN(port) ? port : defaultPortForScheme(scheme);
}
function extractQuery(queryString, url) {
    var query = queryString != null ? trimAndSanitizeQuery(queryString) : null;
    var context = {};
    if (query != null) {
        query.split('&').forEach(function (pair) {
            var keyValue = pair.split('=');
            if (keyValue.length !== 2) {
                throw new Error("Invalid parameters: '".concat(keyValue.toString(), "' in URL '").concat(url, "'."));
            }
            var key = trimAndVerifyQueryElement(keyValue[0], 'key', url);
            var value = trimAndVerifyQueryElement(keyValue[1], 'value', url);
            if (context[key] !== undefined) {
                throw new Error("Duplicated query parameters with key '".concat(key, "' in URL '").concat(url, "'"));
            }
            context[key] = value;
        });
    }
    return context;
}
function trimAndSanitizeQuery(query) {
    query = (query !== null && query !== void 0 ? query : '').trim();
    if ((query === null || query === void 0 ? void 0 : query.charAt(0)) === '?') {
        query = query.substring(1, query.length);
    }
    return query;
}
function trimAndVerifyQueryElement(element, name, url) {
    element = (element !== null && element !== void 0 ? element : '').trim();
    if (element === '') {
        throw new Error("Illegal empty ".concat(name, " in URL query '").concat(url, "'"));
    }
    return element;
}
function escapeIPv6Address(address) {
    var startsWithSquareBracket = address.charAt(0) === '[';
    var endsWithSquareBracket = address.charAt(address.length - 1) === ']';
    if (!startsWithSquareBracket && !endsWithSquareBracket) {
        return "[".concat(address, "]");
    }
    else if (startsWithSquareBracket && endsWithSquareBracket) {
        return address;
    }
    else {
        throw new Error("Illegal IPv6 address ".concat(address));
    }
}
function formatHost(host) {
    if (host === '' || host == null) {
        throw new Error("Illegal host ".concat(host));
    }
    var isIPv6Address = host.includes(':');
    return isIPv6Address ? escapeIPv6Address(host) : host;
}
function formatIPv4Address(address, port) {
    return "".concat(address, ":").concat(port);
}
function formatIPv6Address(address, port) {
    var escapedAddress = escapeIPv6Address(address);
    return "".concat(escapedAddress, ":").concat(port);
}
function defaultPortForScheme(scheme) {
    if (scheme === 'http') {
        return DEFAULT_HTTP_PORT;
    }
    else if (scheme === 'https') {
        return DEFAULT_HTTPS_PORT;
    }
    else {
        return DEFAULT_BOLT_PORT;
    }
}
function uriJsParse(value) {
    // JS version of Python partition function
    function partition(s, delimiter) {
        var i = s.indexOf(delimiter);
        if (i >= 0)
            return [s.substring(0, i), s[i], s.substring(i + 1)];
        else
            return [s, '', ''];
    }
    // JS version of Python rpartition function
    function rpartition(s, delimiter) {
        var i = s.lastIndexOf(delimiter);
        if (i >= 0)
            return [s.substring(0, i), s[i], s.substring(i + 1)];
        else
            return ['', '', s];
    }
    function between(s, ldelimiter, rdelimiter) {
        var lpartition = partition(s, ldelimiter);
        var rpartition = partition(lpartition[2], rdelimiter);
        return [rpartition[0], rpartition[2]];
    }
    // Parse an authority string into an object
    // with the following keys:
    // - userInfo (optional, might contain both user name and password)
    // - host
    // - port (optional, included only as a string)
    function parseAuthority(value) {
        var parsed = {};
        var parts;
        // Parse user info
        parts = rpartition(value, '@');
        if (parts[1] === '@') {
            parsed.userInfo = decodeURIComponent(parts[0]);
            value = parts[2];
        }
        // Parse host and port
        var _a = __read(between(value, '[', ']'), 2), ipv6Host = _a[0], rest = _a[1];
        if (ipv6Host !== '') {
            parsed.host = ipv6Host;
            parts = partition(rest, ':');
        }
        else {
            parts = partition(value, ':');
            parsed.host = parts[0];
        }
        if (parts[1] === ':') {
            parsed.port = parts[2];
        }
        return parsed;
    }
    var parsed = {};
    var parts;
    // Parse scheme
    parts = partition(value, ':');
    if (parts[1] === ':') {
        parsed.scheme = decodeURIComponent(parts[0]);
        value = parts[2];
    }
    // Parse fragment
    parts = partition(value, '#');
    if (parts[1] === '#') {
        parsed.fragment = decodeURIComponent(parts[2]);
        value = parts[0];
    }
    // Parse query
    parts = partition(value, '?');
    if (parts[1] === '?') {
        parsed.query = parts[2];
        value = parts[0];
    }
    // Parse authority and path
    if (value.startsWith('//')) {
        parts = partition(value.substr(2), '/');
        parsed = __assign(__assign({}, parsed), parseAuthority(parts[0]));
        parsed.path = parts[1] + parts[2];
    }
    else {
        parsed.path = value;
    }
    return parsed;
}
