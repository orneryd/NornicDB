"use strict";
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
var _a;
Object.defineProperty(exports, "__esModule", { value: true });
exports.Logger = void 0;
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
var error_1 = require("../error");
var ERROR = 'error';
var WARN = 'warn';
var INFO = 'info';
var DEBUG = 'debug';
var DEFAULT_LEVEL = INFO;
var levels = (_a = {},
    _a[ERROR] = 0,
    _a[WARN] = 1,
    _a[INFO] = 2,
    _a[DEBUG] = 3,
    _a);
/**
 * Logger used by the driver to notify about various internal events. Single logger should be used per driver.
 */
var Logger = /** @class */ (function () {
    /**
     * @constructor
     * @param {string} level the enabled logging level.
     * @param {function(level: string, message: string)} loggerFunction the function to write the log level and message.
     */
    function Logger(level, loggerFunction) {
        this._level = level;
        this._loggerFunction = loggerFunction;
    }
    /**
     * Create a new logger based on the given driver configuration.
     * @param {Object} driverConfig the driver configuration as supplied by the user.
     * @return {Logger} a new logger instance or a no-op logger when not configured.
     */
    Logger.create = function (driverConfig) {
        if ((driverConfig === null || driverConfig === void 0 ? void 0 : driverConfig.logging) != null) {
            var loggingConfig = driverConfig.logging;
            var level = extractConfiguredLevel(loggingConfig);
            var loggerFunction = extractConfiguredLogger(loggingConfig);
            return new Logger(level, loggerFunction);
        }
        return this.noOp();
    };
    /**
     * Create a no-op logger implementation.
     * @return {Logger} the no-op logger implementation.
     */
    Logger.noOp = function () {
        return noOpLogger;
    };
    /**
     * Check if error logging is enabled, i.e. it is not a no-op implementation.
     * @return {boolean} `true` when enabled, `false` otherwise.
     */
    Logger.prototype.isErrorEnabled = function () {
        return isLevelEnabled(this._level, ERROR);
    };
    /**
     * Log an error message.
     * @param {string} message the message to log.
     */
    Logger.prototype.error = function (message) {
        if (this.isErrorEnabled()) {
            this._loggerFunction(ERROR, message);
        }
    };
    /**
     * Check if warn logging is enabled, i.e. it is not a no-op implementation.
     * @return {boolean} `true` when enabled, `false` otherwise.
     */
    Logger.prototype.isWarnEnabled = function () {
        return isLevelEnabled(this._level, WARN);
    };
    /**
     * Log an warning message.
     * @param {string} message the message to log.
     */
    Logger.prototype.warn = function (message) {
        if (this.isWarnEnabled()) {
            this._loggerFunction(WARN, message);
        }
    };
    /**
     * Check if info logging is enabled, i.e. it is not a no-op implementation.
     * @return {boolean} `true` when enabled, `false` otherwise.
     */
    Logger.prototype.isInfoEnabled = function () {
        return isLevelEnabled(this._level, INFO);
    };
    /**
     * Log an info message.
     * @param {string} message the message to log.
     */
    Logger.prototype.info = function (message) {
        if (this.isInfoEnabled()) {
            this._loggerFunction(INFO, message);
        }
    };
    /**
     * Check if debug logging is enabled, i.e. it is not a no-op implementation.
     * @return {boolean} `true` when enabled, `false` otherwise.
     */
    Logger.prototype.isDebugEnabled = function () {
        return isLevelEnabled(this._level, DEBUG);
    };
    /**
     * Log a debug message.
     * @param {string} message the message to log.
     */
    Logger.prototype.debug = function (message) {
        if (this.isDebugEnabled()) {
            this._loggerFunction(DEBUG, message);
        }
    };
    return Logger;
}());
exports.Logger = Logger;
var NoOpLogger = /** @class */ (function (_super) {
    __extends(NoOpLogger, _super);
    function NoOpLogger() {
        return _super.call(this, INFO, function (level, message) { }) || this;
    }
    NoOpLogger.prototype.isErrorEnabled = function () {
        return false;
    };
    NoOpLogger.prototype.error = function (message) { };
    NoOpLogger.prototype.isWarnEnabled = function () {
        return false;
    };
    NoOpLogger.prototype.warn = function (message) { };
    NoOpLogger.prototype.isInfoEnabled = function () {
        return false;
    };
    NoOpLogger.prototype.info = function (message) { };
    NoOpLogger.prototype.isDebugEnabled = function () {
        return false;
    };
    NoOpLogger.prototype.debug = function (message) { };
    return NoOpLogger;
}(Logger));
var noOpLogger = new NoOpLogger();
/**
 * Check if the given logging level is enabled.
 * @param {string} configuredLevel the configured level.
 * @param {string} targetLevel the level to check.
 * @return {boolean} value of `true` when enabled, `false` otherwise.
 */
function isLevelEnabled(configuredLevel, targetLevel) {
    return levels[configuredLevel] >= levels[targetLevel];
}
/**
 * Extract the configured logging level from the driver's logging configuration.
 * @param {Object} loggingConfig the logging configuration.
 * @return {string} the configured log level or default when none configured.
 */
function extractConfiguredLevel(loggingConfig) {
    if ((loggingConfig === null || loggingConfig === void 0 ? void 0 : loggingConfig.level) != null) {
        var configuredLevel = loggingConfig.level;
        var value = levels[configuredLevel];
        if (value == null && value !== 0) {
            throw (0, error_1.newError)("Illegal logging level: ".concat(configuredLevel, ". Supported levels are: ").concat(Object.keys(levels).toString()));
        }
        return configuredLevel;
    }
    return DEFAULT_LEVEL;
}
/**
 * Extract the configured logger function from the driver's logging configuration.
 * @param {Object} loggingConfig the logging configuration.
 * @return {function(level: string, message: string)} the configured logging function.
 */
function extractConfiguredLogger(loggingConfig) {
    var _a, _b;
    if ((loggingConfig === null || loggingConfig === void 0 ? void 0 : loggingConfig.logger) != null) {
        var configuredLogger = loggingConfig.logger;
        if (configuredLogger != null && typeof configuredLogger === 'function') {
            return configuredLogger;
        }
    }
    throw (0, error_1.newError)("Illegal logger function: ".concat((_b = (_a = loggingConfig === null || loggingConfig === void 0 ? void 0 : loggingConfig.logger) === null || _a === void 0 ? void 0 : _a.toString()) !== null && _b !== void 0 ? _b : 'undefined'));
}
