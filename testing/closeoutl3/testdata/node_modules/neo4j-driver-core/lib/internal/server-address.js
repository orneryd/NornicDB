"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.ServerAddress = void 0;
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
var util_1 = require("./util");
var urlUtil = __importStar(require("./url-util"));
var ServerAddress = /** @class */ (function () {
    function ServerAddress(host, resolved, port, hostPort) {
        this._host = (0, util_1.assertString)(host, 'host');
        this._resolved = resolved != null ? (0, util_1.assertString)(resolved, 'resolved') : null;
        this._port = (0, util_1.assertNumber)(port, 'port');
        this._hostPort = hostPort;
        this._stringValue = resolved != null ? "".concat(hostPort, "(").concat(resolved, ")") : "".concat(hostPort);
    }
    ServerAddress.prototype.host = function () {
        return this._host;
    };
    ServerAddress.prototype.resolvedHost = function () {
        return this._resolved != null ? this._resolved : this._host;
    };
    ServerAddress.prototype.port = function () {
        return this._port;
    };
    ServerAddress.prototype.resolveWith = function (resolved) {
        return new ServerAddress(this._host, resolved, this._port, this._hostPort);
    };
    ServerAddress.prototype.asHostPort = function () {
        return this._hostPort;
    };
    ServerAddress.prototype.asKey = function () {
        return this._hostPort;
    };
    ServerAddress.prototype.toString = function () {
        return this._stringValue;
    };
    ServerAddress.fromUrl = function (url) {
        var urlParsed = urlUtil.parseDatabaseUrl(url);
        return new ServerAddress(urlParsed.host, null, urlParsed.port, urlParsed.hostAndPort);
    };
    return ServerAddress;
}());
exports.ServerAddress = ServerAddress;
