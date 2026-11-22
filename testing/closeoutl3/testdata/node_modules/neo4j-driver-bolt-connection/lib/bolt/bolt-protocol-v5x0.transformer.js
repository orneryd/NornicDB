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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var packstream_1 = require("../packstream");
var neo4j_driver_core_1 = require("neo4j-driver-core");
var bolt_protocol_v4x4_transformer_1 = __importDefault(require("./bolt-protocol-v4x4.transformer"));
var bolt_protocol_v5x0_utc_transformer_1 = __importDefault(require("./bolt-protocol-v5x0.utc.transformer"));
var NODE_STRUCT_SIZE = 4;
var RELATIONSHIP_STRUCT_SIZE = 8;
var UNBOUND_RELATIONSHIP_STRUCT_SIZE = 4;
/**
 * Create an extend Node transformer with support to elementId
 * @param {any} config
 * @returns {TypeTransformer}
 */
function createNodeTransformer(config) {
    var node4x4Transformer = bolt_protocol_v4x4_transformer_1.default.createNodeTransformer(config);
    return node4x4Transformer.extendsWith({
        fromStructure: function (struct) {
            packstream_1.structure.verifyStructSize('Node', NODE_STRUCT_SIZE, struct.size);
            var _a = __read(struct.fields, 4), identity = _a[0], lables = _a[1], properties = _a[2], elementId = _a[3];
            return new neo4j_driver_core_1.Node(identity, lables, properties, elementId);
        }
    });
}
/**
 * Create an extend Relationship transformer with support to elementId
 * @param {any} config
 * @returns {TypeTransformer}
 */
function createRelationshipTransformer(config) {
    var relationship4x4Transformer = bolt_protocol_v4x4_transformer_1.default.createRelationshipTransformer(config);
    return relationship4x4Transformer.extendsWith({
        fromStructure: function (struct) {
            packstream_1.structure.verifyStructSize('Relationship', RELATIONSHIP_STRUCT_SIZE, struct.size);
            var _a = __read(struct.fields, 8), identity = _a[0], startNodeIdentity = _a[1], endNodeIdentity = _a[2], type = _a[3], properties = _a[4], elementId = _a[5], startNodeElementId = _a[6], endNodeElementId = _a[7];
            return new neo4j_driver_core_1.Relationship(identity, startNodeIdentity, endNodeIdentity, type, properties, elementId, startNodeElementId, endNodeElementId);
        }
    });
}
/**
 * Create an extend Unbound Relationship transformer with support to elementId
 * @param {any} config
 * @returns {TypeTransformer}
 */
function createUnboundRelationshipTransformer(config) {
    var unboundRelationshipTransformer = bolt_protocol_v4x4_transformer_1.default.createUnboundRelationshipTransformer(config);
    return unboundRelationshipTransformer.extendsWith({
        fromStructure: function (struct) {
            packstream_1.structure.verifyStructSize('UnboundRelationship', UNBOUND_RELATIONSHIP_STRUCT_SIZE, struct.size);
            var _a = __read(struct.fields, 4), identity = _a[0], type = _a[1], properties = _a[2], elementId = _a[3];
            return new neo4j_driver_core_1.UnboundRelationship(identity, type, properties, elementId);
        }
    });
}
exports.default = __assign(__assign(__assign({}, bolt_protocol_v4x4_transformer_1.default), bolt_protocol_v5x0_utc_transformer_1.default), { createNodeTransformer: createNodeTransformer, createRelationshipTransformer: createRelationshipTransformer, createUnboundRelationshipTransformer: createUnboundRelationshipTransformer });
