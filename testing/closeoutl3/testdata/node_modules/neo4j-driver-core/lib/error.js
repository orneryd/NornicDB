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
exports.PROTOCOL_ERROR = exports.SESSION_EXPIRED = exports.SERVICE_UNAVAILABLE = exports.GQLError = exports.Neo4jError = exports.isRetryableError = exports.isRetriableError = void 0;
exports.newError = newError;
exports.newGQLError = newGQLError;
// A common place for constructing error objects, to keep them
// uniform across the driver surface.
var json = __importStar(require("./json"));
var gql_constants_1 = require("./gql-constants");
/**
 * @typedef { 'DATABASE_ERROR' | 'CLIENT_ERROR' | 'TRANSIENT_ERROR' | 'UNKNOWN' } ErrorClassification
 */
var errorClassification = {
    DATABASE_ERROR: 'DATABASE_ERROR',
    CLIENT_ERROR: 'CLIENT_ERROR',
    TRANSIENT_ERROR: 'TRANSIENT_ERROR',
    UNKNOWN: 'UNKNOWN'
};
Object.freeze(errorClassification);
var classifications = Object.values(errorClassification);
/**
 * Error code representing complete loss of service. Used by {@link Neo4jError#code}.
 * @type {string}
 */
var SERVICE_UNAVAILABLE = 'ServiceUnavailable';
exports.SERVICE_UNAVAILABLE = SERVICE_UNAVAILABLE;
/**
 * Error code representing transient loss of service. Used by {@link Neo4jError#code}.
 * @type {string}
 */
var SESSION_EXPIRED = 'SessionExpired';
exports.SESSION_EXPIRED = SESSION_EXPIRED;
/**
 * Error code representing serialization/deserialization issue in the Bolt protocol. Used by {@link Neo4jError#code}.
 * @type {string}
 */
var PROTOCOL_ERROR = 'ProtocolError';
exports.PROTOCOL_ERROR = PROTOCOL_ERROR;
/**
 * Error code representing an no classified error. Used by {@link Neo4jError#code}.
 * @type {string}
 */
var NOT_AVAILABLE = 'N/A';
/// TODO: Remove definitions of this.constructor and this.__proto__
/**
 * Class for nested errors, to be used as causes in {@link Neo4jError}
 */
var GQLError = /** @class */ (function (_super) {
    __extends(GQLError, _super);
    /**
     * @constructor
     * @param {string} message - the error message
     * @param {string} gqlStatus - the GQL status code of the error
     * @param {string} gqlStatusDescription - the GQL status description of the error
     * @param {ErrorDiagnosticRecord} diagnosticRecord - the error diagnostic record
     * @param {Error} cause - Optional nested error, the cause of the error
     */
    function GQLError(message, gqlStatus, gqlStatusDescription, diagnosticRecord, cause) {
        var _a;
        // eslint-disable-next-line
        // @ts-ignore: not available in ES6 yet
        var _this = _super.call(this, message, cause != null ? { cause: cause } : undefined) || this;
        _this.constructor = GQLError;
        // eslint-disable-next-line no-proto
        _this.__proto__ = GQLError.prototype;
        /**
         * Optional, nested error which caused the error
         *
         * @type {Error?}
         * @public
         */
        _this.cause = cause != null ? cause : undefined;
        /**
         * The GQL Status code
         *
         * @type {string}
         * @public
         */
        _this.gqlStatus = gqlStatus;
        /**
         * The GQL Status Description
         *
         * @type {string}
         * @public
         */
        _this.gqlStatusDescription = gqlStatusDescription;
        /**
         * The GQL diagnostic record
         *
         * @type {DiagnosticRecord}
         * @public
         */
        _this.diagnosticRecord = diagnosticRecord;
        /**
         * The GQL error classification, extracted from the diagnostic record
         *
         * @type {ErrorClassification}
         * @public
         */
        _this.classification = _extractClassification(_this.diagnosticRecord);
        /**
         * The GQL error classification, extracted from the diagnostic record as a raw string
         *
         * @type {string}
         * @public
         */
        _this.rawClassification = (_a = diagnosticRecord === null || diagnosticRecord === void 0 ? void 0 : diagnosticRecord._classification) !== null && _a !== void 0 ? _a : undefined;
        /**
         * Represents the name for the type of error, inherited from base JavaScript {@link Error}.
         * Will be 'GQLError' for {@link GQLError}s and 'Neo4jError' for {@link Neo4jError}s.
         *
         * @type {string}
         * @public
         */
        _this.name = 'GQLError';
        return _this;
    }
    /**
     * Returns whether a given GqlStatus code can be found in the cause chain of the error (including the error itself).
     *
     * @param {string} gqlStatus the GqlStatus code to find
     * @returns {boolean}
     */
    GQLError.prototype.containsGqlCause = function (gqlStatus) {
        return this.findByGqlStatus(gqlStatus) !== undefined;
    };
    /**
     * Returns the first error in the cause chain (including the error itself) with a given GqlStatus code.
     * Returns undefined if the GqlStatus code is not present anywhere in the chain.
     *
     * @param {string} gqlStatus the GqlStatus code to find
     * @returns {GQLError | Neo4jError | undefined}
     */
    GQLError.prototype.findByGqlStatus = function (gqlStatus) {
        if (this.gqlStatus === gqlStatus) {
            return this;
        }
        if (this.cause !== undefined && (this.cause instanceof GQLError || this.cause instanceof Neo4jError)) {
            return this.cause.findByGqlStatus(gqlStatus);
        }
        return undefined;
    };
    Object.defineProperty(GQLError.prototype, "diagnosticRecordAsJsonString", {
        /**
         * The json string representation of the diagnostic record.
         * The goal of this method is provide a serialized object for human inspection.
         *
         * @type {string}
         * @public
         */
        get: function () {
            return json.stringify(this.diagnosticRecord, { useCustomToString: true });
        },
        enumerable: false,
        configurable: true
    });
    return GQLError;
}(Error));
exports.GQLError = GQLError;
/**
 * Class for all errors thrown/returned by the driver.
 */
var Neo4jError = /** @class */ (function (_super) {
    __extends(Neo4jError, _super);
    /**
     * @constructor
     * @param {string} message - the error message
     * @param {string} code - Optional error code. Will be populated when error originates in the database.
     * @param {string} gqlStatus - the GQL status code of the error
     * @param {string} gqlStatusDescription - the GQL status description of the error
     * @param {DiagnosticRecord} diagnosticRecord - the error diagnostic record
     * @param {Error} cause - Optional nested error, the cause of the error
     */
    function Neo4jError(message, code, gqlStatus, gqlStatusDescription, diagnosticRecord, cause) {
        var _this = _super.call(this, message, gqlStatus, gqlStatusDescription, diagnosticRecord, cause) || this;
        _this.constructor = Neo4jError;
        // eslint-disable-next-line no-proto
        _this.__proto__ = Neo4jError.prototype;
        /**
         * The Neo4j Error code
         *
         * @type {string}
         * @public
         */
        _this.code = code;
        /**
         * The name of the type of error.
         *
         * @type {string}
         * @public
         */
        _this.name = 'Neo4jError';
        var isRetryableCode = _isRetryableCode(code);
        /**
         * If the error is considered retriable.
         * This does not apply when running auto-commit transactions using {@link Session#run}
         *
         * @deprecated members using the spelling 'retriable' will be removed in 7.0. Use {@link retryable} instead.
         * @type {boolean}
         * @public
         */
        _this.retriable = isRetryableCode;
        /**
         * If the error is considered retryable.
         * This does not apply when running auto-commit transactions using {@link Session#run}
         *
         * @type {boolean}
         * @public
         */
        _this.retryable = isRetryableCode;
        return _this;
    }
    /**
     * Verifies if the given error is retriable.
     *
     * @deprecated members using the spelling 'retriable' will be removed in 7.0. Use {@link isRetryable} instead.
     * @param {object|undefined|null} error the error object
     * @returns {boolean} true if the error is retriable
     */
    Neo4jError.isRetriable = function (error) {
        return this.isRetryable(error);
    };
    /**
     * Verifies if the given error is retryable.
     *
     * @param {object|undefined|null} error the error object
     * @returns {boolean} true if the error is retryable
     */
    Neo4jError.isRetryable = function (error) {
        return error !== null &&
            error !== undefined &&
            error instanceof Neo4jError &&
            error.retryable;
    };
    return Neo4jError;
}(GQLError));
exports.Neo4jError = Neo4jError;
/**
 * Create a new error from a message and optional data
 * @param message the error message
 * @param {Neo4jErrorCode} [code] the error code
 * @param {Neo4jError} [cause]
 * @param {String} [gqlStatus]
 * @param {String} [gqlStatusDescription]
 * @param {DiagnosticRecord} diagnosticRecord - the error message
 * @return {Neo4jError} an {@link Neo4jError}
 * @private
 */
function newError(message, code, cause, gqlStatus, gqlStatusDescription, diagnosticRecord) {
    return new Neo4jError(message, code !== null && code !== void 0 ? code : NOT_AVAILABLE, gqlStatus !== null && gqlStatus !== void 0 ? gqlStatus : '50N42', gqlStatusDescription !== null && gqlStatusDescription !== void 0 ? gqlStatusDescription : 'error: general processing exception - unexpected error. ' + message, diagnosticRecord !== null && diagnosticRecord !== void 0 ? diagnosticRecord : gql_constants_1.rawPolyfilledDiagnosticRecord, cause);
}
/**
 * Create a new GQL error from a message and optional data
 * @param message the error message
 * @param {Neo4jError} [cause]
 * @param {String} [gqlStatus]
 * @param {String} [gqlStatusDescription]
 * @param {DiagnosticRecord} diagnosticRecord - the error message
 * @return {Neo4jError} an {@link Neo4jError}
 * @private
 */
function newGQLError(message, cause, gqlStatus, gqlStatusDescription, diagnosticRecord) {
    return new GQLError(message, gqlStatus !== null && gqlStatus !== void 0 ? gqlStatus : '50N42', gqlStatusDescription !== null && gqlStatusDescription !== void 0 ? gqlStatusDescription : 'error: general processing exception - unexpected error. ' + message, diagnosticRecord !== null && diagnosticRecord !== void 0 ? diagnosticRecord : gql_constants_1.rawPolyfilledDiagnosticRecord, cause);
}
/**
 * Verifies if the given error is retriable.
 *
 * @deprecated members using the spelling 'retriable' will be removed in 7.0. Use {@link isRetryableError} instead.
 * @public
 * @param {object|undefined|null} error the error object
 * @returns {boolean} true if the error is retriable
 */
var isRetriableError = Neo4jError.isRetriable;
exports.isRetriableError = isRetriableError;
/**
 * Verifies if the given error is retryable.
 *
 * @public
 * @param {object|undefined|null} error the error object
 * @returns {boolean} true if the error is retryable
 */
var isRetryableError = Neo4jError.isRetryable;
exports.isRetryableError = isRetryableError;
/**
 * @private
 * @param {string} code the error code
 * @returns {boolean} true if the error is a retriable error
 */
function _isRetryableCode(code) {
    return code === SERVICE_UNAVAILABLE ||
        code === SESSION_EXPIRED ||
        _isAuthorizationExpired(code) ||
        _isTransientError(code);
}
/**
 * @private
 * @param {string} code the error to check
 * @return {boolean} true if the error is a transient error
 */
function _isTransientError(code) {
    return (code === null || code === void 0 ? void 0 : code.includes('TransientError')) === true;
}
/**
 * @private
 * @param {string} code the error to check
 * @returns {boolean} true if the error is a service unavailable error
 */
function _isAuthorizationExpired(code) {
    return code === 'Neo.ClientError.Security.AuthorizationExpired';
}
/**
 * extracts a typed classification from the diagnostic record.
 */
function _extractClassification(diagnosticRecord) {
    if (diagnosticRecord === undefined || diagnosticRecord._classification === undefined) {
        return 'UNKNOWN';
    }
    return classifications.includes(diagnosticRecord._classification) ? diagnosticRecord === null || diagnosticRecord === void 0 ? void 0 : diagnosticRecord._classification : 'UNKNOWN';
}
