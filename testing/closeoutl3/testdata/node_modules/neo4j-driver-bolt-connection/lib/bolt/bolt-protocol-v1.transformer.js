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
var neo4j_driver_core_1 = require("neo4j-driver-core");
var packstream_1 = require("../packstream");
var transformer_1 = require("./transformer");
var PROTOCOL_ERROR = neo4j_driver_core_1.error.PROTOCOL_ERROR;
var NODE = 0x4e;
var NODE_STRUCT_SIZE = 3;
var RELATIONSHIP = 0x52;
var RELATIONSHIP_STRUCT_SIZE = 5;
var UNBOUND_RELATIONSHIP = 0x72;
var UNBOUND_RELATIONSHIP_STRUCT_SIZE = 3;
var PATH = 0x50;
var PATH_STRUCT_SIZE = 3;
var VECTOR = 0x56;
/**
 * Creates the Node Transformer
 * @returns {TypeTransformer}
 */
function createNodeTransformer() {
    return new transformer_1.TypeTransformer({
        signature: NODE,
        isTypeInstance: function (object) { return object instanceof neo4j_driver_core_1.Node; },
        toStructure: function (object) {
            throw (0, neo4j_driver_core_1.newError)("It is not allowed to pass nodes in query parameters, given: ".concat(object), PROTOCOL_ERROR);
        },
        fromStructure: function (struct) {
            packstream_1.structure.verifyStructSize('Node', NODE_STRUCT_SIZE, struct.size);
            var _a = __read(struct.fields, 3), identity = _a[0], labels = _a[1], properties = _a[2];
            return new neo4j_driver_core_1.Node(identity, labels, properties);
        }
    });
}
/**
 * Creates the Relationship Transformer
 * @returns {TypeTransformer}
 */
function createRelationshipTransformer() {
    return new transformer_1.TypeTransformer({
        signature: RELATIONSHIP,
        isTypeInstance: function (object) { return object instanceof neo4j_driver_core_1.Relationship; },
        toStructure: function (object) {
            throw (0, neo4j_driver_core_1.newError)("It is not allowed to pass relationships in query parameters, given: ".concat(object), PROTOCOL_ERROR);
        },
        fromStructure: function (struct) {
            packstream_1.structure.verifyStructSize('Relationship', RELATIONSHIP_STRUCT_SIZE, struct.size);
            var _a = __read(struct.fields, 5), identity = _a[0], startNodeIdentity = _a[1], endNodeIdentity = _a[2], type = _a[3], properties = _a[4];
            return new neo4j_driver_core_1.Relationship(identity, startNodeIdentity, endNodeIdentity, type, properties);
        }
    });
}
/**
 * Creates the Unbound Relationship Transformer
 * @returns {TypeTransformer}
 */
function createUnboundRelationshipTransformer() {
    return new transformer_1.TypeTransformer({
        signature: UNBOUND_RELATIONSHIP,
        isTypeInstance: function (object) { return object instanceof neo4j_driver_core_1.UnboundRelationship; },
        toStructure: function (object) {
            throw (0, neo4j_driver_core_1.newError)("It is not allowed to pass unbound relationships in query parameters, given: ".concat(object), PROTOCOL_ERROR);
        },
        fromStructure: function (struct) {
            packstream_1.structure.verifyStructSize('UnboundRelationship', UNBOUND_RELATIONSHIP_STRUCT_SIZE, struct.size);
            var _a = __read(struct.fields, 3), identity = _a[0], type = _a[1], properties = _a[2];
            return new neo4j_driver_core_1.UnboundRelationship(identity, type, properties);
        }
    });
}
/**
 * Creates the Path Transformer
 * @returns {TypeTransformer}
 */
function createPathTransformer() {
    return new transformer_1.TypeTransformer({
        signature: PATH,
        isTypeInstance: function (object) { return object instanceof neo4j_driver_core_1.Path; },
        toStructure: function (object) {
            throw (0, neo4j_driver_core_1.newError)("It is not allowed to pass paths in query parameters, given: ".concat(object), PROTOCOL_ERROR);
        },
        fromStructure: function (struct) {
            packstream_1.structure.verifyStructSize('Path', PATH_STRUCT_SIZE, struct.size);
            var _a = __read(struct.fields, 3), nodes = _a[0], rels = _a[1], sequence = _a[2];
            var segments = [];
            var prevNode = nodes[0];
            for (var i = 0; i < sequence.length; i += 2) {
                var nextNode = nodes[sequence[i + 1]];
                var relIndex = (0, neo4j_driver_core_1.toNumber)(sequence[i]);
                var rel = void 0;
                if (relIndex > 0) {
                    rel = rels[relIndex - 1];
                    if (rel instanceof neo4j_driver_core_1.UnboundRelationship) {
                        // To avoid duplication, relationships in a path do not contain
                        // information about their start and end nodes, that's instead
                        // inferred from the path sequence. This is us inferring (and,
                        // for performance reasons remembering) the start/end of a rel.
                        rels[relIndex - 1] = rel = rel.bindTo(prevNode, nextNode);
                    }
                }
                else {
                    rel = rels[-relIndex - 1];
                    if (rel instanceof neo4j_driver_core_1.UnboundRelationship) {
                        // See above
                        rels[-relIndex - 1] = rel = rel.bindTo(nextNode, prevNode);
                    }
                }
                // Done hydrating one path segment.
                segments.push(new neo4j_driver_core_1.PathSegment(prevNode, rel, nextNode));
                prevNode = nextNode;
            }
            return new neo4j_driver_core_1.Path(nodes[0], nodes[nodes.length - 1], segments);
        }
    });
}
/**
 * Creates a typeTransformer that throws errors if vectors are transmitted.
 * @returns {TypeTransformer}
 */
function createVectorTransformer() {
    return new transformer_1.TypeTransformer({
        signature: VECTOR,
        isTypeInstance: function (object) { return object instanceof neo4j_driver_core_1.Vector; },
        toStructure: function (_) {
            throw (0, neo4j_driver_core_1.newError)('Sending vector types require server and driver to be communicating with Bolt protocol 6.0 or later. Please update your database version.');
        },
        fromStructure: function (_) {
            throw (0, neo4j_driver_core_1.newError)('Server tried to send Vector object, but server and driver are communicating on a version of the Bolt protocol that does not support vectors.');
        }
    });
}
exports.default = {
    createNodeTransformer: createNodeTransformer,
    createRelationshipTransformer: createRelationshipTransformer,
    createUnboundRelationshipTransformer: createUnboundRelationshipTransformer,
    createPathTransformer: createPathTransformer,
    createVectorTransformer: createVectorTransformer
};
