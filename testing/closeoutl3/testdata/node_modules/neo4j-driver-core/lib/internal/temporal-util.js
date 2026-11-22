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
exports.SECONDS_PER_DAY = exports.DAYS_PER_400_YEAR_CYCLE = exports.DAYS_0000_TO_1970 = exports.NANOS_PER_HOUR = exports.NANOS_PER_MINUTE = exports.NANOS_PER_MILLISECOND = exports.NANOS_PER_SECOND = exports.SECONDS_PER_HOUR = exports.SECONDS_PER_MINUTE = exports.MINUTES_PER_HOUR = exports.NANOSECOND_OF_SECOND_RANGE = exports.SECOND_OF_MINUTE_RANGE = exports.MINUTE_OF_HOUR_RANGE = exports.HOUR_OF_DAY_RANGE = exports.DAY_OF_MONTH_RANGE = exports.MONTH_OF_YEAR_RANGE = exports.YEAR_RANGE = void 0;
exports.normalizeYearsForDuration = normalizeYearsForDuration;
exports.normalizeMonthsForDuration = normalizeMonthsForDuration;
exports.normalizeHoursForDuration = normalizeHoursForDuration;
exports.normalizeMinutesForDuration = normalizeMinutesForDuration;
exports.normalizeSecondsForDuration = normalizeSecondsForDuration;
exports.normalizeNanosecondsForDuration = normalizeNanosecondsForDuration;
exports.localTimeToNanoOfDay = localTimeToNanoOfDay;
exports.localDateTimeToEpochSecond = localDateTimeToEpochSecond;
exports.dateToEpochDay = dateToEpochDay;
exports.durationToIsoString = durationToIsoString;
exports.timeToIsoString = timeToIsoString;
exports.timeZoneOffsetToIsoString = timeZoneOffsetToIsoString;
exports.dateToIsoString = dateToIsoString;
exports.isoStringToStandardDate = isoStringToStandardDate;
exports.toStandardDate = toStandardDate;
exports.newDate = newDate;
exports.totalNanoseconds = totalNanoseconds;
exports.timeZoneOffsetInSeconds = timeZoneOffsetInSeconds;
exports.assertValidYear = assertValidYear;
exports.assertValidMonth = assertValidMonth;
exports.assertValidDay = assertValidDay;
exports.assertValidHour = assertValidHour;
exports.assertValidMinute = assertValidMinute;
exports.assertValidSecond = assertValidSecond;
exports.assertValidNanosecond = assertValidNanosecond;
exports.assertValidZoneId = assertValidZoneId;
exports.floorDiv = floorDiv;
exports.floorMod = floorMod;
var integer_1 = __importStar(require("../integer"));
var error_1 = require("../error");
var util_1 = require("./util");
/*
  Code in this util should be compatible with code in the database that uses JSR-310 java.time APIs.

  It is based on a library called ThreeTen (https://github.com/ThreeTen/threetenbp) which was derived
  from JSR-310 reference implementation previously hosted on GitHub. Code uses `Integer` type everywhere
  to correctly handle large integer values that are greater than `Number.MAX_SAFE_INTEGER`.

  Please consult either ThreeTen or js-joda (https://github.com/js-joda/js-joda) when working with the
  conversion functions.
 */
var ValueRange = /** @class */ (function () {
    function ValueRange(min, max) {
        this._minNumber = min;
        this._maxNumber = max;
        this._minInteger = (0, integer_1.int)(min);
        this._maxInteger = (0, integer_1.int)(max);
    }
    ValueRange.prototype.contains = function (value) {
        if ((0, integer_1.isInt)(value) && value instanceof integer_1.default) {
            return (value.greaterThanOrEqual(this._minInteger) &&
                value.lessThanOrEqual(this._maxInteger));
        }
        else if (typeof value === 'bigint') {
            var intValue = (0, integer_1.int)(value);
            return (intValue.greaterThanOrEqual(this._minInteger) &&
                intValue.lessThanOrEqual(this._maxInteger));
        }
        else {
            return value >= this._minNumber && value <= this._maxNumber;
        }
    };
    ValueRange.prototype.toString = function () {
        return "[".concat(this._minNumber, ", ").concat(this._maxNumber, "]");
    };
    return ValueRange;
}());
exports.YEAR_RANGE = new ValueRange(-999999999, 999999999);
exports.MONTH_OF_YEAR_RANGE = new ValueRange(1, 12);
exports.DAY_OF_MONTH_RANGE = new ValueRange(1, 31);
exports.HOUR_OF_DAY_RANGE = new ValueRange(0, 23);
exports.MINUTE_OF_HOUR_RANGE = new ValueRange(0, 59);
exports.SECOND_OF_MINUTE_RANGE = new ValueRange(0, 59);
exports.NANOSECOND_OF_SECOND_RANGE = new ValueRange(0, 999999999);
exports.MINUTES_PER_HOUR = 60;
exports.SECONDS_PER_MINUTE = 60;
exports.SECONDS_PER_HOUR = exports.SECONDS_PER_MINUTE * exports.MINUTES_PER_HOUR;
exports.NANOS_PER_SECOND = 1000000000;
exports.NANOS_PER_MILLISECOND = 1000000;
exports.NANOS_PER_MINUTE = exports.NANOS_PER_SECOND * exports.SECONDS_PER_MINUTE;
exports.NANOS_PER_HOUR = exports.NANOS_PER_MINUTE * exports.MINUTES_PER_HOUR;
exports.DAYS_0000_TO_1970 = 719528;
exports.DAYS_PER_400_YEAR_CYCLE = 146097;
exports.SECONDS_PER_DAY = 86400;
function normalizeYearsForDuration(months) {
    return (0, integer_1.int)(months).div(12);
}
function normalizeMonthsForDuration(months) {
    return (0, integer_1.int)(months).modulo(12);
}
function normalizeHoursForDuration(seconds, nanoseconds) {
    if ((0, integer_1.int)(nanoseconds).greaterThan(0) && (0, integer_1.int)(seconds).lessThan(0)) {
        seconds = (0, integer_1.int)(seconds).add(1);
    }
    return (0, integer_1.int)(seconds).div(exports.SECONDS_PER_HOUR);
}
function normalizeMinutesForDuration(seconds, nanoseconds) {
    if ((0, integer_1.int)(nanoseconds).greaterThan(0) && (0, integer_1.int)(seconds).lessThan(0)) {
        seconds = (0, integer_1.int)(seconds).add(1);
    }
    var minutes = (0, integer_1.int)(seconds).div(exports.SECONDS_PER_MINUTE);
    var negativeMinutes = minutes.isNegative();
    if (negativeMinutes) {
        minutes = minutes.negate();
    }
    return floorMod(minutes, exports.MINUTES_PER_HOUR).multiply(negativeMinutes ? -1 : 1);
}
function normalizeSecondsForDuration(seconds, nanoseconds) {
    return (0, integer_1.int)(seconds).add(floorDiv(nanoseconds, exports.NANOS_PER_SECOND));
}
function normalizeNanosecondsForDuration(nanoseconds) {
    return floorMod(nanoseconds, exports.NANOS_PER_SECOND);
}
/**
 * Converts given local time into a single integer representing this same time in nanoseconds of the day.
 * @param {Integer|number|string} hour the hour of the local time to convert.
 * @param {Integer|number|string} minute the minute of the local time to convert.
 * @param {Integer|number|string} second the second of the local time to convert.
 * @param {Integer|number|string} nanosecond the nanosecond of the local time to convert.
 * @return {Integer} nanoseconds representing the given local time.
 */
function localTimeToNanoOfDay(hour, minute, second, nanosecond) {
    hour = (0, integer_1.int)(hour);
    minute = (0, integer_1.int)(minute);
    second = (0, integer_1.int)(second);
    nanosecond = (0, integer_1.int)(nanosecond);
    var totalNanos = hour.multiply(exports.NANOS_PER_HOUR);
    totalNanos = totalNanos.add(minute.multiply(exports.NANOS_PER_MINUTE));
    totalNanos = totalNanos.add(second.multiply(exports.NANOS_PER_SECOND));
    return totalNanos.add(nanosecond);
}
/**
 * Converts given local date time into a single integer representing this same time in epoch seconds UTC.
 * @param {Integer|number|string} year the year of the local date-time to convert.
 * @param {Integer|number|string} month the month of the local date-time to convert.
 * @param {Integer|number|string} day the day of the local date-time to convert.
 * @param {Integer|number|string} hour the hour of the local date-time to convert.
 * @param {Integer|number|string} minute the minute of the local date-time to convert.
 * @param {Integer|number|string} second the second of the local date-time to convert.
 * @param {Integer|number|string} nanosecond the nanosecond of the local date-time to convert.
 * @return {Integer} epoch second in UTC representing the given local date time.
 */
function localDateTimeToEpochSecond(year, month, day, hour, minute, second, nanosecond) {
    var epochDay = dateToEpochDay(year, month, day);
    var localTimeSeconds = localTimeToSecondOfDay(hour, minute, second);
    return epochDay.multiply(exports.SECONDS_PER_DAY).add(localTimeSeconds);
}
/**
 * Converts given local date into a single integer representing it's epoch day.
 * @param {Integer|number|string} year the year of the local date to convert.
 * @param {Integer|number|string} month the month of the local date to convert.
 * @param {Integer|number|string} day the day of the local date to convert.
 * @return {Integer} epoch day representing the given date.
 */
function dateToEpochDay(year, month, day) {
    year = (0, integer_1.int)(year);
    month = (0, integer_1.int)(month);
    day = (0, integer_1.int)(day);
    var epochDay = year.multiply(365);
    if (year.greaterThanOrEqual(0)) {
        epochDay = epochDay.add(year
            .add(3)
            .div(4)
            .subtract(year.add(99).div(100))
            .add(year.add(399).div(400)));
    }
    else {
        epochDay = epochDay.subtract(year
            .div(-4)
            .subtract(year.div(-100))
            .add(year.div(-400)));
    }
    epochDay = epochDay.add(month
        .multiply(367)
        .subtract(362)
        .div(12));
    epochDay = epochDay.add(day.subtract(1));
    if (month.greaterThan(2)) {
        epochDay = epochDay.subtract(1);
        if (!isLeapYear(year)) {
            epochDay = epochDay.subtract(1);
        }
    }
    return epochDay.subtract(exports.DAYS_0000_TO_1970);
}
/**
 * Format given duration to an ISO 8601 string.
 * @param {Integer|number|string} months the number of months.
 * @param {Integer|number|string} days the number of days.
 * @param {Integer|number|string} seconds the number of seconds.
 * @param {Integer|number|string} nanoseconds the number of nanoseconds.
 * @return {string} ISO string that represents given duration.
 */
function durationToIsoString(months, days, seconds, nanoseconds) {
    if ((0, integer_1.int)(months).equals(0) && (0, integer_1.int)(days).equals(0) && (0, integer_1.int)(seconds).equals(0) && (0, integer_1.int)(nanoseconds).equals(0)) {
        return 'PT0S';
    }
    var yearString = formatNumber(normalizeYearsForDuration(months));
    var monthString = formatNumber(normalizeMonthsForDuration(months));
    var dayString = formatNumber(days);
    var hourString = formatNumber(normalizeHoursForDuration(seconds, nanoseconds));
    var minuteString = formatNumber(normalizeMinutesForDuration(seconds, nanoseconds));
    var secondsAndNanosecondsString = formatSecondsAndNanosecondsForDuration(seconds, nanoseconds);
    return "P".concat(yearString !== '0' ? yearString + 'Y' : '') +
        "".concat(monthString !== '0' ? monthString + 'M' : '') +
        "".concat(dayString !== '0' ? dayString + 'D' : '', "T") +
        "".concat(hourString !== '0' ? hourString + 'H' : '') +
        "".concat(minuteString !== '0' ? minuteString + 'M' : '') +
        "".concat(secondsAndNanosecondsString !== '0' ? secondsAndNanosecondsString + 'S' : '');
}
/**
 * Formats given time to an ISO 8601 string.
 * @param {Integer|number|string} hour the hour value.
 * @param {Integer|number|string} minute the minute value.
 * @param {Integer|number|string} second the second value.
 * @param {Integer|number|string} nanosecond the nanosecond value.
 * @return {string} ISO string that represents given time.
 */
function timeToIsoString(hour, minute, second, nanosecond) {
    var hourString = formatNumber(hour, 2);
    var minuteString = formatNumber(minute, 2);
    var secondString = formatNumber(second, 2);
    var nanosecondString = formatNanosecond(nanosecond);
    return "".concat(hourString, ":").concat(minuteString, ":").concat(secondString).concat(nanosecondString);
}
/**
 * Formats given time zone offset in seconds to string representation like '±HH:MM', '±HH:MM:SS' or 'Z' for UTC.
 * @param {Integer|number|string} offsetSeconds the offset in seconds.
 * @return {string} ISO string that represents given offset.
 */
function timeZoneOffsetToIsoString(offsetSeconds) {
    offsetSeconds = (0, integer_1.int)(offsetSeconds);
    if (offsetSeconds.equals(0)) {
        return 'Z';
    }
    var isNegative = offsetSeconds.isNegative();
    if (isNegative) {
        offsetSeconds = offsetSeconds.multiply(-1);
    }
    var signPrefix = isNegative ? '-' : '+';
    var hours = formatNumber(offsetSeconds.div(exports.SECONDS_PER_HOUR), 2);
    var minutes = formatNumber(offsetSeconds.div(exports.SECONDS_PER_MINUTE).modulo(exports.MINUTES_PER_HOUR), 2);
    var secondsValue = offsetSeconds.modulo(exports.SECONDS_PER_MINUTE);
    var seconds = secondsValue.equals(0) ? null : formatNumber(secondsValue, 2);
    return seconds != null
        ? "".concat(signPrefix).concat(hours, ":").concat(minutes, ":").concat(seconds)
        : "".concat(signPrefix).concat(hours, ":").concat(minutes);
}
/**
 * Formats given date to an ISO 8601 string.
 * @param {Integer|number|string} year the date year.
 * @param {Integer|number|string} month the date month.
 * @param {Integer|number|string} day the date day.
 * @return {string} ISO string that represents given date.
 */
function dateToIsoString(year, month, day) {
    var yearString = formatYear(year);
    var monthString = formatNumber(month, 2);
    var dayString = formatNumber(day, 2);
    return "".concat(yearString, "-").concat(monthString, "-").concat(dayString);
}
/**
 * Convert the given iso date string to a JavaScript Date object
 *
 * @param {string} isoString The iso date string
 * @returns {Date} the date
 */
function isoStringToStandardDate(isoString) {
    return new Date(isoString);
}
/**
 * Convert the given utc timestamp to a JavaScript Date object
 *
 * @param {number} utc Timestamp in UTC
 * @returns {Date} the date
 */
function toStandardDate(utc) {
    return new Date(utc);
}
/**
 * Shortcut for creating a new StandardDate
 * @param date
 * @returns {Date} the standard date
 */
function newDate(date) {
    return new Date(date);
}
/**
 * Get the total number of nanoseconds from the milliseconds of the given standard JavaScript date and optional nanosecond part.
 * @param {global.Date} standardDate the standard JavaScript date.
 * @param {Integer|number|bigint|undefined} nanoseconds the optional number of nanoseconds.
 * @return {Integer|number|bigint} the total amount of nanoseconds.
 */
function totalNanoseconds(standardDate, nanoseconds) {
    nanoseconds = nanoseconds !== null && nanoseconds !== void 0 ? nanoseconds : 0;
    var nanosFromMillis = standardDate.getMilliseconds() * exports.NANOS_PER_MILLISECOND;
    return add(nanoseconds, nanosFromMillis);
}
/**
 * Get the time zone offset in seconds from the given standard JavaScript date.
 *
 * @param {global.Date} standardDate the standard JavaScript date.
 * @return {number} the time zone offset in seconds.
 */
function timeZoneOffsetInSeconds(standardDate) {
    var secondsPortion = standardDate.getSeconds() - standardDate.getUTCSeconds();
    var minutesPortion = standardDate.getMinutes() - standardDate.getUTCMinutes();
    var hoursPortion = standardDate.getHours() - standardDate.getUTCHours();
    var daysPortion = _getDayOffset(standardDate);
    return hoursPortion * exports.SECONDS_PER_HOUR + minutesPortion * exports.SECONDS_PER_MINUTE + secondsPortion + daysPortion * exports.SECONDS_PER_DAY;
}
/**
 * Get the difference in days from the given JavaScript date in local time and UTC.
 *
 * @private
 * @param {global.Date} standardDate the date to evaluate
 * @returns  {number} the difference in days between date local time and UTC
 */
function _getDayOffset(standardDate) {
    if (standardDate.getMonth() === standardDate.getUTCMonth()) {
        return standardDate.getDate() - standardDate.getUTCDate();
    }
    else if ((standardDate.getFullYear() > standardDate.getUTCFullYear()) || (standardDate.getMonth() > standardDate.getUTCMonth() && standardDate.getFullYear() === standardDate.getUTCFullYear())) {
        return standardDate.getDate() + _daysUntilNextMonth(standardDate.getUTCMonth(), standardDate.getUTCFullYear()) - standardDate.getUTCDate();
    }
    else {
        return standardDate.getDate() - (standardDate.getUTCDate() + _daysUntilNextMonth(standardDate.getMonth(), standardDate.getFullYear()));
    }
}
/**
 * Get the number of days in a month, including a check for leap years.
 *
 * @private
 * @param {number} month the month of the date to evalutate
 * @param {number} year the month of the date to evalutate
 * @returns {number} the total number of days in the month evaluated
 */
function _daysUntilNextMonth(month, year) {
    if (month === 1) {
        if (year % 400 === 0 || (year % 4 === 0 && year % 100 !== 0)) {
            return 29;
        }
        else {
            return 28;
        }
    }
    else if ([0, 2, 4, 6, 7, 9, 11].includes(month)) {
        return 31;
    }
    else {
        return 30;
    }
}
/**
 * Assert that the year value is valid.
 * @param {Integer|number} year the value to check.
 * @return {Integer|number} the value of the year if it is valid. Exception is thrown otherwise.
 */
function assertValidYear(year) {
    return assertValidTemporalValue(year, exports.YEAR_RANGE, 'Year');
}
/**
 * Assert that the month value is valid.
 * @param {Integer|number} month the value to check.
 * @return {Integer|number} the value of the month if it is valid. Exception is thrown otherwise.
 */
function assertValidMonth(month) {
    return assertValidTemporalValue(month, exports.MONTH_OF_YEAR_RANGE, 'Month');
}
/**
 * Assert that the day value is valid.
 * @param {Integer|number} day the value to check.
 * @return {Integer|number} the value of the day if it is valid. Exception is thrown otherwise.
 */
function assertValidDay(day) {
    return assertValidTemporalValue(day, exports.DAY_OF_MONTH_RANGE, 'Day');
}
/**
 * Assert that the hour value is valid.
 * @param {Integer|number} hour the value to check.
 * @return {Integer|number} the value of the hour if it is valid. Exception is thrown otherwise.
 */
function assertValidHour(hour) {
    return assertValidTemporalValue(hour, exports.HOUR_OF_DAY_RANGE, 'Hour');
}
/**
 * Assert that the minute value is valid.
 * @param {Integer|number} minute the value to check.
 * @return {Integer|number} the value of the minute if it is valid. Exception is thrown otherwise.
 */
function assertValidMinute(minute) {
    return assertValidTemporalValue(minute, exports.MINUTE_OF_HOUR_RANGE, 'Minute');
}
/**
 * Assert that the second value is valid.
 * @param {Integer|number} second the value to check.
 * @return {Integer|number} the value of the second if it is valid. Exception is thrown otherwise.
 */
function assertValidSecond(second) {
    return assertValidTemporalValue(second, exports.SECOND_OF_MINUTE_RANGE, 'Second');
}
/**
 * Assert that the nanosecond value is valid.
 * @param {Integer|number} nanosecond the value to check.
 * @return {Integer|number} the value of the nanosecond if it is valid. Exception is thrown otherwise.
 */
function assertValidNanosecond(nanosecond) {
    return assertValidTemporalValue(nanosecond, exports.NANOSECOND_OF_SECOND_RANGE, 'Nanosecond');
}
var timeZoneValidityCache = new Map();
var newInvalidZoneIdError = function (zoneId, fieldName) { return (0, error_1.newError)("".concat(fieldName, " is expected to be a valid ZoneId but was: \"").concat(zoneId, "\"")); };
function assertValidZoneId(fieldName, zoneId) {
    var cachedResult = timeZoneValidityCache.get(zoneId);
    if (cachedResult === true) {
        return;
    }
    if (cachedResult === false) {
        throw newInvalidZoneIdError(zoneId, fieldName);
    }
    try {
        Intl.DateTimeFormat(undefined, { timeZone: zoneId });
        timeZoneValidityCache.set(zoneId, true);
    }
    catch (e) {
        timeZoneValidityCache.set(zoneId, false);
        throw newInvalidZoneIdError(zoneId, fieldName);
    }
}
/**
 * Check if the given value is of expected type and is in the expected range.
 * @param {Integer|number} value the value to check.
 * @param {ValueRange} range the range.
 * @param {string} name the name of the value.
 * @return {Integer|number} the value if valid. Exception is thrown otherwise.
 */
function assertValidTemporalValue(value, range, name) {
    (0, util_1.assertNumberOrInteger)(value, name);
    if (!range.contains(value)) {
        throw (0, error_1.newError)("".concat(name, " is expected to be in range ").concat(range.toString(), " but was: ").concat(value.toString()));
    }
    return value;
}
/**
 * Converts given local time into a single integer representing this same time in seconds of the day. Nanoseconds are skipped.
 * @param {Integer|number|string} hour the hour of the local time.
 * @param {Integer|number|string} minute the minute of the local time.
 * @param {Integer|number|string} second the second of the local time.
 * @return {Integer} seconds representing the given local time.
 */
function localTimeToSecondOfDay(hour, minute, second) {
    hour = (0, integer_1.int)(hour);
    minute = (0, integer_1.int)(minute);
    second = (0, integer_1.int)(second);
    var totalSeconds = hour.multiply(exports.SECONDS_PER_HOUR);
    totalSeconds = totalSeconds.add(minute.multiply(exports.SECONDS_PER_MINUTE));
    return totalSeconds.add(second);
}
/**
 * Check if given year is a leap year. Uses algorithm described here {@link https://en.wikipedia.org/wiki/Leap_year#Algorithm}.
 * @param {Integer|number|string} year the year to check. Will be converted to {@link Integer} for all calculations.
 * @return {boolean} `true` if given year is a leap year, `false` otherwise.
 */
function isLeapYear(year) {
    year = (0, integer_1.int)(year);
    if (!year.modulo(4).equals(0)) {
        return false;
    }
    else if (!year.modulo(100).equals(0)) {
        return true;
    }
    else if (!year.modulo(400).equals(0)) {
        return false;
    }
    else {
        return true;
    }
}
/**
 * @param {Integer|number|string} x the divident.
 * @param {Integer|number|string} y the divisor.
 * @return {Integer} the result.
 */
function floorDiv(x, y) {
    x = (0, integer_1.int)(x);
    y = (0, integer_1.int)(y);
    var result = x.div(y);
    if (x.isPositive() !== y.isPositive() && result.multiply(y).notEquals(x)) {
        result = result.subtract(1);
    }
    return result;
}
/**
 * @param {Integer|number|string} x the divident.
 * @param {Integer|number|string} y the divisor.
 * @return {Integer} the result.
 */
function floorMod(x, y) {
    x = (0, integer_1.int)(x);
    y = (0, integer_1.int)(y);
    return x.subtract(floorDiv(x, y).multiply(y));
}
/**
 * @param {Integer|number|string} seconds the number of seconds to format.
 * @param {Integer|number|string} nanoseconds the number of nanoseconds to format.
 * @return {string} formatted value.
 */
function formatSecondsAndNanosecondsForDuration(seconds, nanoseconds) {
    seconds = (0, integer_1.int)(seconds);
    nanoseconds = (0, integer_1.int)(nanoseconds);
    var secondsString;
    var nanosecondsString;
    var secondsNegative = seconds.isNegative();
    var nanosecondsGreaterThanZero = nanoseconds.greaterThan(0);
    if (secondsNegative && nanosecondsGreaterThanZero) {
        seconds = seconds.add(1).negate().modulo(60).negate();
        if (seconds.equals(0)) {
            secondsString = '-0';
        }
        else {
            secondsString = seconds.toString();
        }
    }
    else {
        secondsString = seconds.modulo(60).toString();
    }
    if (nanosecondsGreaterThanZero) {
        if (secondsNegative) {
            nanosecondsString = formatNanosecond(nanoseconds
                .negate()
                .add(2 * exports.NANOS_PER_SECOND)
                .modulo(exports.NANOS_PER_SECOND));
        }
        else {
            nanosecondsString = formatNanosecond(nanoseconds.add(exports.NANOS_PER_SECOND).modulo(exports.NANOS_PER_SECOND));
        }
    }
    return nanosecondsString != null ? secondsString + nanosecondsString : secondsString;
}
/**
 * @param {Integer|number|string} value the number of nanoseconds to format.
 * @return {string} formatted and possibly left-padded nanoseconds part as string.
 */
function formatNanosecond(value) {
    value = (0, integer_1.int)(value);
    return value.equals(0) ? '' : '.' + formatNumber(value, 9);
}
/**
 *
 * @param {Integer|number|string} year The year to be formatted
 * @return {string} formatted year
 */
function formatYear(year) {
    var yearInteger = (0, integer_1.int)(year);
    if (yearInteger.isNegative() || yearInteger.greaterThan(9999)) {
        return formatNumber(yearInteger, 6, { usePositiveSign: true });
    }
    return formatNumber(yearInteger, 4);
}
/**
 * @param {Integer|number|string} num the number to format.
 * @param {number} [stringLength=undefined] the string length to left-pad to.
 * @return {string} formatted and possibly left-padded number as string.
 */
function formatNumber(num, stringLength, params) {
    num = (0, integer_1.int)(num);
    var isNegative = num.isNegative();
    if (isNegative) {
        num = num.negate();
    }
    var numString = num.toString();
    if (stringLength != null) {
        // left pad the string with zeroes
        while (numString.length < stringLength) {
            numString = '0' + numString;
        }
    }
    if (isNegative) {
        return '-' + numString;
    }
    else if ((params === null || params === void 0 ? void 0 : params.usePositiveSign) === true) {
        return '+' + numString;
    }
    return numString;
}
function add(x, y) {
    if (x instanceof integer_1.default) {
        return x.add(y);
    }
    else if (typeof x === 'bigint') {
        return x + BigInt(y);
    }
    return x + y;
}
