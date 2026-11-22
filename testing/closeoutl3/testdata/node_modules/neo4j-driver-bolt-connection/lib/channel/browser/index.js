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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.ClientCertificatesLoader = exports.HostNameResolver = exports.Channel = void 0;
var browser_channel_1 = __importDefault(require("./browser-channel"));
var browser_host_name_resolver_1 = __importDefault(require("./browser-host-name-resolver"));
var browser_client_certificates_loader_1 = __importDefault(require("./browser-client-certificates-loader"));
/*

This module exports a set of components to be used in browser environment.
They are not compatible with NodeJS environment.
All files import/require APIs from `node/index.js` by default.
Such imports are replaced at build time with `browser/index.js` when building a browser bundle.

NOTE: exports in this module should have exactly the same names/structure as exports in `node/index.js`.

 */
exports.Channel = browser_channel_1.default;
exports.HostNameResolver = browser_host_name_resolver_1.default;
exports.ClientCertificatesLoader = browser_client_certificates_loader_1.default;
