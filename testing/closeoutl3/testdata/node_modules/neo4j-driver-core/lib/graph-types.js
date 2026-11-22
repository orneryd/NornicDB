"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PathSegment = exports.Path = exports.UnboundRelationship = exports.Relationship = exports.Node = void 0;
exports.isNode = isNode;
exports.isRelationship = isRelationship;
exports.isUnboundRelationship = isUnboundRelationship;
exports.isPath = isPath;
exports.isPathSegment = isPathSegment;
var json_1 = require("./json");
var mapping_highlevel_1 = require("./mapping.highlevel");
var IDENTIFIER_PROPERTY_ATTRIBUTES = {
    value: true,
    enumerable: false,
    configurable: false,
    writable: false
};
var NODE_IDENTIFIER_PROPERTY = '__isNode__';
var RELATIONSHIP_IDENTIFIER_PROPERTY = '__isRelationship__';
var UNBOUND_RELATIONSHIP_IDENTIFIER_PROPERTY = '__isUnboundRelationship__';
var PATH_IDENTIFIER_PROPERTY = '__isPath__';
var PATH_SEGMENT_IDENTIFIER_PROPERTY = '__isPathSegment__';
function hasIdentifierProperty(obj, property) {
    return obj != null && obj[property] === true;
}
/**
 * Class for Node Type.
 */
var Node = /** @class */ (function () {
    /**
     * @constructor
     * @protected
     * @param {NumberOrInteger} identity - Unique identity
     * @param {Array<string>} labels - Array for all labels
     * @param {Properties} properties - Map with node properties
     * @param {string} elementId - Node element identifier
     */
    function Node(identity, labels, properties, elementId) {
        /**
         * Identity of the node.
         * @type {NumberOrInteger}
         * @deprecated use {@link Node#elementId} instead
         */
        this.identity = identity;
        /**
         * Labels of the node.
         * @type {string[]}
         */
        this.labels = labels;
        /**
         * Properties of the node.
         * @type {Properties}
         */
        this.properties = properties;
        /**
         * The Node element identifier.
         * @type {string}
         */
        this.elementId = _valueOrGetDefault(elementId, function () { return identity.toString(); });
    }
    Node.prototype.as = function (constructorOrRules, rules) {
        var _this = this;
        return (0, mapping_highlevel_1.as)({
            get: function (key) { return _this.properties[key]; }
        }, constructorOrRules, rules);
    };
    /**
     * @ignore
     */
    Node.prototype.toString = function () {
        var s = '(' + this.elementId;
        for (var i = 0; i < this.labels.length; i++) {
            s += ':' + this.labels[i];
        }
        var keys = Object.keys(this.properties);
        if (keys.length > 0) {
            s += ' {';
            for (var i = 0; i < keys.length; i++) {
                if (i > 0)
                    s += ',';
                s += keys[i] + ':' + (0, json_1.stringify)(this.properties[keys[i]]);
            }
            s += '}';
        }
        s += ')';
        return s;
    };
    return Node;
}());
exports.Node = Node;
Object.defineProperty(Node.prototype, NODE_IDENTIFIER_PROPERTY, IDENTIFIER_PROPERTY_ATTRIBUTES);
/**
 * Test if given object is an instance of {@link Node} class.
 * @param {Object} obj the object to test.
 * @return {boolean} `true` if given object is a {@link Node}, `false` otherwise.
 */
function isNode(obj) {
    return hasIdentifierProperty(obj, NODE_IDENTIFIER_PROPERTY);
}
/**
 * Class for Relationship Type.
 */
var Relationship = /** @class */ (function () {
    /**
     * @constructor
     * @protected
     * @param {NumberOrInteger} identity - Unique identity
     * @param {NumberOrInteger} start - Identity of start Node
     * @param {NumberOrInteger} end - Identity of end Node
     * @param {string} type - Relationship type
     * @param {Properties} properties - Map with relationship properties
     * @param {string} elementId - Relationship element identifier
     * @param {string} startNodeElementId - Start Node element identifier
     * @param {string} endNodeElementId - End Node element identifier
     */
    function Relationship(identity, start, end, type, properties, elementId, startNodeElementId, endNodeElementId) {
        /**
         * Identity of the relationship.
         * @type {NumberOrInteger}
         * @deprecated use {@link Relationship#elementId} instead
         */
        this.identity = identity;
        /**
         * Identity of the start node.
         * @type {NumberOrInteger}
         * @deprecated use {@link Relationship#startNodeElementId} instead
         */
        this.start = start;
        /**
         * Identity of the end node.
         * @type {NumberOrInteger}
         * @deprecated use {@link Relationship#endNodeElementId} instead
         */
        this.end = end;
        /**
         * Type of the relationship.
         * @type {string}
         */
        this.type = type;
        /**
         * Properties of the relationship.
         * @type {Properties}
         */
        this.properties = properties;
        /**
         * The Relationship element identifier.
         * @type {string}
         */
        this.elementId = _valueOrGetDefault(elementId, function () { return identity.toString(); });
        /**
         * The Start Node element identifier.
         * @type {string}
         */
        this.startNodeElementId = _valueOrGetDefault(startNodeElementId, function () { return start.toString(); });
        /**
         * The End Node element identifier.
         * @type {string}
         */
        this.endNodeElementId = _valueOrGetDefault(endNodeElementId, function () { return end.toString(); });
    }
    Relationship.prototype.as = function (constructorOrRules, rules) {
        var _this = this;
        return (0, mapping_highlevel_1.as)({
            get: function (key) { return _this.properties[key]; }
        }, constructorOrRules, rules);
    };
    /**
     * @ignore
     */
    Relationship.prototype.toString = function () {
        var s = '(' + this.startNodeElementId + ')-[:' + this.type;
        var keys = Object.keys(this.properties);
        if (keys.length > 0) {
            s += ' {';
            for (var i = 0; i < keys.length; i++) {
                if (i > 0)
                    s += ',';
                s += keys[i] + ':' + (0, json_1.stringify)(this.properties[keys[i]]);
            }
            s += '}';
        }
        s += ']->(' + this.endNodeElementId + ')';
        return s;
    };
    return Relationship;
}());
exports.Relationship = Relationship;
Object.defineProperty(Relationship.prototype, RELATIONSHIP_IDENTIFIER_PROPERTY, IDENTIFIER_PROPERTY_ATTRIBUTES);
/**
 * Test if given object is an instance of {@link Relationship} class.
 * @param {Object} obj the object to test.
 * @return {boolean} `true` if given object is a {@link Relationship}, `false` otherwise.
 */
function isRelationship(obj) {
    return hasIdentifierProperty(obj, RELATIONSHIP_IDENTIFIER_PROPERTY);
}
/**
 * Class for UnboundRelationship Type.
 * @access private
 */
var UnboundRelationship = /** @class */ (function () {
    /**
     * @constructor
     * @protected
     * @param {NumberOrInteger} identity - Unique identity
     * @param {string} type - Relationship type
     * @param {Properties} properties - Map with relationship properties
     * @param {string} elementId - Relationship element identifier
     */
    function UnboundRelationship(identity, type, properties, elementId) {
        /**
         * Identity of the relationship.
         * @type {NumberOrInteger}
         * @deprecated use {@link UnboundRelationship#elementId} instead
         */
        this.identity = identity;
        /**
         * Type of the relationship.
         * @type {string}
         */
        this.type = type;
        /**
         * Properties of the relationship.
         * @type {Properties}
         */
        this.properties = properties;
        /**
         * The Relationship element identifier.
         * @type {string}
         */
        this.elementId = _valueOrGetDefault(elementId, function () { return identity.toString(); });
    }
    /**
     * Bind relationship
     *
     * @protected
     * @deprecated use {@link UnboundRelationship#bindTo} instead
     * @param {Integer} start - Identity of start node
     * @param {Integer} end - Identity of end node
     * @return {Relationship} - Created relationship
     */
    UnboundRelationship.prototype.bind = function (start, end) {
        return new Relationship(this.identity, start, end, this.type, this.properties, this.elementId);
    };
    /**
     * Bind relationship
     *
     * @protected
     * @param {Node} start - Start Node
     * @param {Node} end - End Node
     * @return {Relationship} - Created relationship
     */
    UnboundRelationship.prototype.bindTo = function (start, end) {
        return new Relationship(this.identity, start.identity, end.identity, this.type, this.properties, this.elementId, start.elementId, end.elementId);
    };
    UnboundRelationship.prototype.as = function (constructorOrRules, rules) {
        var _this = this;
        return (0, mapping_highlevel_1.as)({
            get: function (key) { return _this.properties[key]; }
        }, constructorOrRules, rules);
    };
    /**
     * @ignore
     */
    UnboundRelationship.prototype.toString = function () {
        var s = '-[:' + this.type;
        var keys = Object.keys(this.properties);
        if (keys.length > 0) {
            s += ' {';
            for (var i = 0; i < keys.length; i++) {
                if (i > 0)
                    s += ',';
                s += keys[i] + ':' + (0, json_1.stringify)(this.properties[keys[i]]);
            }
            s += '}';
        }
        s += ']->';
        return s;
    };
    return UnboundRelationship;
}());
exports.UnboundRelationship = UnboundRelationship;
Object.defineProperty(UnboundRelationship.prototype, UNBOUND_RELATIONSHIP_IDENTIFIER_PROPERTY, IDENTIFIER_PROPERTY_ATTRIBUTES);
/**
 * Test if given object is an instance of {@link UnboundRelationship} class.
 * @param {Object} obj the object to test.
 * @return {boolean} `true` if given object is a {@link UnboundRelationship}, `false` otherwise.
 * @access private
 */
function isUnboundRelationship(obj) {
    return hasIdentifierProperty(obj, UNBOUND_RELATIONSHIP_IDENTIFIER_PROPERTY);
}
/**
 * Class for PathSegment Type.
 */
var PathSegment = /** @class */ (function () {
    /**
     * @constructor
     * @protected
     * @param {Node} start - start node
     * @param {Relationship} rel - relationship that connects start and end node
     * @param {Node} end - end node
     */
    function PathSegment(start, rel, end) {
        /**
         * Start node.
         * @type {Node}
         */
        this.start = start;
        /**
         * Relationship.
         * @type {Relationship}
         */
        this.relationship = rel;
        /**
         * End node.
         * @type {Node}
         */
        this.end = end;
    }
    return PathSegment;
}());
exports.PathSegment = PathSegment;
Object.defineProperty(PathSegment.prototype, PATH_SEGMENT_IDENTIFIER_PROPERTY, IDENTIFIER_PROPERTY_ATTRIBUTES);
/**
 * Test if given object is an instance of {@link PathSegment} class.
 * @param {Object} obj the object to test.
 * @return {boolean} `true` if given object is a {@link PathSegment}, `false` otherwise.
 */
function isPathSegment(obj) {
    return hasIdentifierProperty(obj, PATH_SEGMENT_IDENTIFIER_PROPERTY);
}
/**
 * Class for Path Type.
 */
var Path = /** @class */ (function () {
    /**
     * @constructor
     * @protected
     * @param {Node} start  - start node
     * @param {Node} end - end node
     * @param {Array<PathSegment>} segments - Array of Segments
     */
    function Path(start, end, segments) {
        /**
         * Start node.
         * @type {Node}
         */
        this.start = start;
        /**
         * End node.
         * @type {Node}
         */
        this.end = end;
        /**
         * Segments.
         * @type {Array<PathSegment>}
         */
        this.segments = segments;
        /**
         * Length of the segments.
         * @type {Number}
         */
        this.length = segments.length;
    }
    return Path;
}());
exports.Path = Path;
Object.defineProperty(Path.prototype, PATH_IDENTIFIER_PROPERTY, IDENTIFIER_PROPERTY_ATTRIBUTES);
/**
 * Test if given object is an instance of {@link Path} class.
 * @param {Object} obj the object to test.
 * @return {boolean} `true` if given object is a {@link Path}, `false` otherwise.
 */
function isPath(obj) {
    return hasIdentifierProperty(obj, PATH_IDENTIFIER_PROPERTY);
}
function _valueOrGetDefault(value, getDefault) {
    return value === undefined || value === null ? getDefault() : value;
}
