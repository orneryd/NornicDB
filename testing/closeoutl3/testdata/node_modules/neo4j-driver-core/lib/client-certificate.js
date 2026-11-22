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
exports.clientCertificateProviders = exports.RotatingClientCertificateProvider = exports.ClientCertificateProvider = void 0;
exports.resolveCertificateProvider = resolveCertificateProvider;
var json = __importStar(require("./json"));
/**
 * Represents KeyFile represented as file.
 *
 * @typedef {object} KeyFileObject
 * @property {string} path - The path of the file
 * @property {string|undefined} password - the password of the key. If none,
 * the password defined at {@link ClientCertificate} will be used.
 */
/**
 * Holds the Client TLS certificate information.
 *
 * Browser instances of the driver should configure the certificate
 * in the system.
 *
 * Files defined in the {@link ClientCertificate#certfile}
 * and {@link ClientCertificate#keyfile} will read and loaded to
 * memory to fill the fields `cert` and `key` in security context.
 *
 * @interface
 * @see https://nodejs.org/api/tls.html#tlscreatesecurecontextoptions
 * @since 5.27
 */
var ClientCertificate = /** @class */ (function () {
    function ClientCertificate() {
        /**
         * The path to client certificate file.
         *
         * @type {string|string[]}
         */
        this.certfile = '';
        /**
         * The path to the key file.
         *
         * @type {string|string[]|KeyFileObject|KeyFileObject[]}
         */
        this.keyfile = '';
        /**
         * The key's password.
         *
         * @type {string|undefined}
         */
        this.password = undefined;
    }
    return ClientCertificate;
}());
exports.default = ClientCertificate;
/**
 * Provides a client certificate to the driver for mutual TLS.
 *
 * The driver will call {@link ClientCertificateProvider#hasUpdate} to check if the client wants to update the certificate.
 * If so, it will call {@link ClientCertificateProvider#getClientCertificate} to get the new certificate.
 *
 * The certificate is only used as a second factor for authentication authenticating the client.
 * The DMBS user still needs to authenticate with an authentication token.
 *
 * All implementations of this interface must be thread-safe and non-blocking for caller threads.
 * For instance, IO operations must not be done on the calling thread.
 *
 * Note that the work done in the methods of this interface count towards the connectionAcquisition.
 * Should fetching the certificate be particularly slow, it might be necessary to increase the timeout.
 *
 * @interface
 * @since 5.27
 */
var ClientCertificateProvider = /** @class */ (function () {
    function ClientCertificateProvider() {
    }
    /**
     * Indicates whether the client wants the driver to update the certificate.
     *
     * @returns {Promise<boolean>|boolean} true if the client wants the driver to update the certificate
     */
    ClientCertificateProvider.prototype.hasUpdate = function () {
        throw new Error('Not Implemented');
    };
    /**
     * Returns the certificate to use for new connections.
     *
     * Will be called by the driver after {@link ClientCertificateProvider#hasUpdate} returned true
     * or when the driver establishes the first connection.
     *
     * @returns {Promise<ClientCertificate>|ClientCertificate} the certificate to use for new connections
     */
    ClientCertificateProvider.prototype.getClientCertificate = function () {
        throw new Error('Not Implemented');
    };
    return ClientCertificateProvider;
}());
exports.ClientCertificateProvider = ClientCertificateProvider;
/**
 * Interface for  {@link ClientCertificateProvider} which provides update certificate function.
 * @interface
 * @since 5.27
 */
var RotatingClientCertificateProvider = /** @class */ (function (_super) {
    __extends(RotatingClientCertificateProvider, _super);
    function RotatingClientCertificateProvider() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    /**
     * Updates the certificate stored in the provider.
     *
     * To be called by user-code when a new client certificate is available.
     *
     * @param {ClientCertificate} certificate - the new certificate
     * @throws {TypeError} If initialCertificate is not a ClientCertificate.
     */
    RotatingClientCertificateProvider.prototype.updateCertificate = function (certificate) {
        throw new Error('Not implemented');
    };
    return RotatingClientCertificateProvider;
}(ClientCertificateProvider));
exports.RotatingClientCertificateProvider = RotatingClientCertificateProvider;
/**
 * Defines the object which holds the common {@link ClientCertificateProviders} used in the Driver
 *
 * @since 5.27
 */
var ClientCertificateProviders = /** @class */ (function () {
    function ClientCertificateProviders() {
    }
    /**
     *
     * @param {object} param0 - The params
     * @param {ClientCertificate} param0.initialCertificate - The certificated used by the driver until {@link RotatingClientCertificateProvider#updateCertificate} get called.
     *
     * @returns {RotatingClientCertificateProvider} The rotating client certificate provider
     * @throws {TypeError} If initialCertificate is not a ClientCertificate.
     */
    ClientCertificateProviders.prototype.rotating = function (_a) {
        var initialCertificate = _a.initialCertificate;
        if (initialCertificate == null || !isClientClientCertificate(initialCertificate)) {
            throw new TypeError("initialCertificate should be ClientCertificate, but got ".concat(json.stringify(initialCertificate)));
        }
        var certificate = __assign({}, initialCertificate);
        return new InternalRotatingClientCertificateProvider(certificate);
    };
    return ClientCertificateProviders;
}());
/**
 * Holds the common {@link ClientCertificateProviders} used in the Driver.
 *
 * @since 5.27
 */
var clientCertificateProviders = new ClientCertificateProviders();
exports.clientCertificateProviders = clientCertificateProviders;
Object.freeze(clientCertificateProviders);
/**
 * Resolves ClientCertificate or ClientCertificateProvider to a ClientCertificateProvider
 *
 * Method validates the input.
 *
 * @private
 * @param input
 * @returns {ClientCertificateProvider?} A client certificate provider if provided a ClientCertificate or a ClientCertificateProvider
 * @throws {TypeError} If input is not a ClientCertificate, ClientCertificateProvider, undefined or null.
 */
function resolveCertificateProvider(input) {
    if (input == null) {
        return undefined;
    }
    if (typeof input === 'object' && 'hasUpdate' in input && 'getClientCertificate' in input &&
        typeof input.getClientCertificate === 'function' && typeof input.hasUpdate === 'function') {
        return input;
    }
    if (isClientClientCertificate(input)) {
        var certificate_1 = __assign({}, input);
        return {
            getClientCertificate: function () { return certificate_1; },
            hasUpdate: function () { return false; }
        };
    }
    throw new TypeError("clientCertificate should be configured with ClientCertificate or ClientCertificateProvider, but got ".concat(json.stringify(input)));
}
/**
 * Verify if object is a client certificate
 * @private
 * @param maybeClientCertificate - Maybe the certificate
 * @returns {boolean} if maybeClientCertificate is a client certificate object
 */
function isClientClientCertificate(maybeClientCertificate) {
    return maybeClientCertificate != null &&
        typeof maybeClientCertificate === 'object' &&
        'certfile' in maybeClientCertificate && isCertFile(maybeClientCertificate.certfile) &&
        'keyfile' in maybeClientCertificate && isKeyFile(maybeClientCertificate.keyfile) &&
        isStringOrNotPresent('password', maybeClientCertificate);
}
/**
 * Check value is a cert file
 * @private
 * @param {any} value the value
 * @returns {boolean} is a cert file
 */
function isCertFile(value) {
    return isString(value) || isArrayOf(value, isString);
}
/**
 * Check if the value is a keyfile.
 *
 * @private
 * @param {any} maybeKeyFile might be a keyfile value
 * @returns {boolean} the value is a KeyFile
 */
function isKeyFile(maybeKeyFile) {
    function check(obj) {
        return typeof obj === 'string' ||
            (obj != null &&
                typeof obj === 'object' &&
                'path' in obj && typeof obj.path === 'string' &&
                isStringOrNotPresent('password', obj));
    }
    return check(maybeKeyFile) || isArrayOf(maybeKeyFile, check);
}
/**
 * Verify if value is string
 *
 * @private
 * @param {any} value the value
 * @returns {boolean} is string
 */
function isString(value) {
    return typeof value === 'string';
}
/**
 * Verifies if value is a array of type
 *
 * @private
 * @param {any} value the value
 * @param {function} isType the type checker
 * @returns {boolean} value is array of type
 */
function isArrayOf(value, isType, allowEmpty) {
    if (allowEmpty === void 0) { allowEmpty = false; }
    return Array.isArray(value) &&
        (allowEmpty || value.length > 0) &&
        value.filter(isType).length === value.length;
}
/**
 * Verify if valueName is present in the object and is a string, or not present at all.
 *
 * @private
 * @param {string} valueName The value in the object
 * @param {object} obj The object
 * @returns {boolean} if the value is present in object as string or not present
 */
function isStringOrNotPresent(valueName, obj) {
    return !(valueName in obj) || obj[valueName] == null || typeof obj[valueName] === 'string';
}
/**
 * Internal implementation
 *
 * @private
 */
var InternalRotatingClientCertificateProvider = /** @class */ (function () {
    function InternalRotatingClientCertificateProvider(_certificate, _updated) {
        if (_updated === void 0) { _updated = false; }
        this._certificate = _certificate;
        this._updated = _updated;
    }
    /**
     *
     * @returns {boolean|Promise<boolean>}
     */
    InternalRotatingClientCertificateProvider.prototype.hasUpdate = function () {
        try {
            return this._updated;
        }
        finally {
            this._updated = false;
        }
    };
    /**
     *
     * @returns {ClientCertificate|Promise<ClientCertificate>}
     */
    InternalRotatingClientCertificateProvider.prototype.getClientCertificate = function () {
        return this._certificate;
    };
    /**
     *
     * @param certificate
     * @returns {void}
     */
    InternalRotatingClientCertificateProvider.prototype.updateCertificate = function (certificate) {
        if (!isClientClientCertificate(certificate)) {
            throw new TypeError("certificate should be ClientCertificate, but got ".concat(json.stringify(certificate)));
        }
        this._certificate = __assign({}, certificate);
        this._updated = true;
    };
    return InternalRotatingClientCertificateProvider;
}());
