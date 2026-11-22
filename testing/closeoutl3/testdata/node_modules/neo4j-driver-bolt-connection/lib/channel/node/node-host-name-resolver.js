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
var dns_1 = __importDefault(require("dns"));
var neo4j_driver_core_1 = require("neo4j-driver-core");
var BaseHostNameResolver = neo4j_driver_core_1.internal.resolver.BaseHostNameResolver;
var NodeHostNameResolver = /** @class */ (function (_super) {
    __extends(NodeHostNameResolver, _super);
    function NodeHostNameResolver() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    NodeHostNameResolver.prototype.resolve = function (address) {
        return new Promise(function (resolve) {
            dns_1.default.lookup(address.host(), { all: true }, function (error, resolvedTo) {
                if (error) {
                    resolve([address]);
                }
                else {
                    var resolvedAddresses = resolvedTo.map(function (a) {
                        return address.resolveWith(a.address);
                    });
                    resolve(resolvedAddresses);
                }
            });
        });
    };
    return NodeHostNameResolver;
}(BaseHostNameResolver));
exports.default = NodeHostNameResolver;
