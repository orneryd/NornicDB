"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.notificationFilterDisabledClassification = exports.notificationFilterDisabledCategory = exports.notificationFilterMinimumSeverityLevel = void 0;
/**
 * @typedef {'WARNING' | 'INFORMATION' | 'OFF'} NotificationFilterMinimumSeverityLevel
 */
/**
 * Constants that represents the minimum Severity level in the {@link NotificationFilter}
 */
var notificationFilterMinimumSeverityLevel = {
    OFF: 'OFF',
    WARNING: 'WARNING',
    INFORMATION: 'INFORMATION'
};
exports.notificationFilterMinimumSeverityLevel = notificationFilterMinimumSeverityLevel;
Object.freeze(notificationFilterMinimumSeverityLevel);
/**
 * @typedef {'HINT' | 'UNRECOGNIZED' | 'UNSUPPORTED' |'PERFORMANCE' | 'TOPOLOGY' | 'SECURITY' | 'DEPRECATION' | 'GENERIC' | 'SCHEMA'} NotificationFilterDisabledCategory
 */
/**
 * Constants that represents the disabled categories in the {@link NotificationFilter}
 * @deprecated use {@link notificationFilterDisabledClassification} instead.
 */
var notificationFilterDisabledCategory = {
    HINT: 'HINT',
    UNRECOGNIZED: 'UNRECOGNIZED',
    UNSUPPORTED: 'UNSUPPORTED',
    PERFORMANCE: 'PERFORMANCE',
    TOPOLOGY: 'TOPOLOGY',
    SECURITY: 'SECURITY',
    DEPRECATION: 'DEPRECATION',
    GENERIC: 'GENERIC',
    SCHEMA: 'SCHEMA'
};
exports.notificationFilterDisabledCategory = notificationFilterDisabledCategory;
Object.freeze(notificationFilterDisabledCategory);
/**
 * @typedef {NotificationFilterDisabledCategory} NotificationFilterDisabledClassification
 */
/**
 * Constants that represents the disabled classifications in the {@link NotificationFilter}
 *
 * @type {NotificationFilterDisabledClassification}
 */
var notificationFilterDisabledClassification = notificationFilterDisabledCategory;
exports.notificationFilterDisabledClassification = notificationFilterDisabledClassification;
/**
 * The notification filter object which can be configured in
 * the session and driver creation.
 *
 * Values not defined are interpreted as default.
 *
 * @interface
 */
var NotificationFilter = /** @class */ (function () {
    /**
     * @constructor
     * @private
     */
    function NotificationFilter() {
        /**
         * The minimum level of all notifications to receive.
         *
         * @public
         * @type {?NotificationFilterMinimumSeverityLevel}
         */
        this.minimumSeverityLevel = undefined;
        /**
         * Categories the user would like to opt-out of receiving.
         *
         *
         * This property is equivalent to {@link NotificationFilter#disabledClassifications}
         * and it must not be enabled at same time.
         *
         * @type {?NotificationFilterDisabledCategory[]}
         * @deprecated use{@link NotificationFilter#disabledClassifications}
         */
        this.disabledCategories = undefined;
        /**
         * Classifications the user would like to opt-out of receiving.
         *
         * This property is equivalent to {@link NotificationFilter#disabledCategories}
         * and it must not be enabled at same time.
         *
         * @type {?NotificationFilterDisabledClassification[]}
         */
        this.disabledClassifications = undefined;
        throw new Error('Not implemented');
    }
    return NotificationFilter;
}());
exports.default = NotificationFilter;
