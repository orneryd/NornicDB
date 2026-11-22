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
var node_channel_1 = __importDefault(require("./node-channel"));
var node_host_name_resolver_1 = __importDefault(require("./node-host-name-resolver"));
var node_client_certificates_loader_1 = __importDefault(require("./node-client-certificates-loader"));
/*

This module exports a set of components to be used in NodeJS environment.
They are not compatible with browser environment.
All files that require environment-dependent APIs should import this file by default.
Imports/requires are replaced at build time with `browser/index.js` when building a browser bundle.

NOTE: exports in this module should have exactly the same names/structure as exports in `browser/index.js`.

 */
exports.Channel = node_channel_1.default;
exports.HostNameResolver = node_host_name_resolver_1.default;
exports.ClientCertificatesLoader = node_client_certificates_loader_1.default;
