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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var packstream_1 = require("../packstream");
var neo4j_driver_core_1 = require("neo4j-driver-core");
var bolt_protocol_v4x4_transformer_1 = __importDefault(require("./bolt-protocol-v4x4.transformer"));
var temporal_factory_1 = require("./temporal-factory");
var functional_1 = require("../lang/functional");
var localDateTimeToEpochSecond = neo4j_driver_core_1.internal.temporalUtil.localDateTimeToEpochSecond;
var DATE_TIME_WITH_ZONE_OFFSET = 0x49;
var DATE_TIME_WITH_ZONE_OFFSET_STRUCT_SIZE = 3;
var DATE_TIME_WITH_ZONE_ID = 0x69;
var DATE_TIME_WITH_ZONE_ID_STRUCT_SIZE = 3;
function createDateTimeWithZoneIdTransformer(config, logger) {
    var disableLosslessIntegers = config.disableLosslessIntegers, useBigInt = config.useBigInt;
    var dateTimeWithZoneIdTransformer = bolt_protocol_v4x4_transformer_1.default.createDateTimeWithZoneIdTransformer(config);
    return dateTimeWithZoneIdTransformer.extendsWith({
        signature: DATE_TIME_WITH_ZONE_ID,
        fromStructure: function (struct) {
            packstream_1.structure.verifyStructSize('DateTimeWithZoneId', DATE_TIME_WITH_ZONE_ID_STRUCT_SIZE, struct.size);
            var _a = __read(struct.fields, 3), epochSecond = _a[0], nano = _a[1], timeZoneId = _a[2];
            var localDateTime = getTimeInZoneId(timeZoneId, epochSecond, nano);
            var result = new neo4j_driver_core_1.DateTime(localDateTime.year, localDateTime.month, localDateTime.day, localDateTime.hour, localDateTime.minute, localDateTime.second, (0, neo4j_driver_core_1.int)(nano), localDateTime.timeZoneOffsetSeconds, timeZoneId);
            return convertIntegerPropsIfNeeded(result, disableLosslessIntegers, useBigInt);
        },
        toStructure: function (value) {
            var epochSecond = localDateTimeToEpochSecond(value.year, value.month, value.day, value.hour, value.minute, value.second, value.nanosecond);
            var offset = value.timeZoneOffsetSeconds != null
                ? value.timeZoneOffsetSeconds
                : getOffsetFromZoneId(value.timeZoneId, epochSecond, value.nanosecond);
            if (value.timeZoneOffsetSeconds == null) {
                logger.warn('DateTime objects without "timeZoneOffsetSeconds" property ' +
                    'are prune to bugs related to ambiguous times. For instance, ' +
                    '2022-10-30T2:30:00[Europe/Berlin] could be GMT+1 or GMT+2.');
            }
            var utc = epochSecond.subtract(offset);
            var nano = (0, neo4j_driver_core_1.int)(value.nanosecond);
            var timeZoneId = value.timeZoneId;
            return new packstream_1.structure.Structure(DATE_TIME_WITH_ZONE_ID, [utc, nano, timeZoneId]);
        }
    });
}
/**
 * Returns the offset for a given timezone id
 *
 * Javascript doesn't have support for direct getting the timezone offset from a given
 * TimeZoneId and DateTime in the given TimeZoneId. For solving this issue,
 *
 * 1. The ZoneId is applied to the timestamp, so we could make the difference between the
 * given timestamp and the new calculated one. This is the offset for the timezone
 * in the utc is equal to epoch (some time in the future or past)
 * 2. The offset is subtracted from the timestamp, so we have an estimated utc timestamp.
 * 3. The ZoneId is applied to the new timestamp, se we could could make the difference
 * between the new timestamp and the calculated one. This is the offset for the given timezone.
 *
 * Example:
 *    Input: 2022-3-27 1:59:59 'Europe/Berlin'
 *    Apply 1, 2022-3-27 1:59:59 => 2022-3-27 3:59:59 'Europe/Berlin' +2:00
 *    Apply 2, 2022-3-27 1:59:59 - 2:00 => 2022-3-26 23:59:59
 *    Apply 3, 2022-3-26 23:59:59 => 2022-3-27 00:59:59 'Europe/Berlin' +1:00
 *  The offset is +1 hour.
 *
 * @param {string} timeZoneId The timezone id
 * @param {Integer} epochSecond The epoch second in the timezone id
 * @param {Integerable} nanosecond The nanoseconds in the timezone id
 * @returns The timezone offset
 */
function getOffsetFromZoneId(timeZoneId, epochSecond, nanosecond) {
    var dateTimeWithZoneAppliedTwice = getTimeInZoneId(timeZoneId, epochSecond, nanosecond);
    // The wallclock form the current date time
    var epochWithZoneAppliedTwice = localDateTimeToEpochSecond(dateTimeWithZoneAppliedTwice.year, dateTimeWithZoneAppliedTwice.month, dateTimeWithZoneAppliedTwice.day, dateTimeWithZoneAppliedTwice.hour, dateTimeWithZoneAppliedTwice.minute, dateTimeWithZoneAppliedTwice.second, nanosecond);
    var offsetOfZoneInTheFutureUtc = epochWithZoneAppliedTwice.subtract(epochSecond);
    var guessedUtc = epochSecond.subtract(offsetOfZoneInTheFutureUtc);
    var zonedDateTimeFromGuessedUtc = getTimeInZoneId(timeZoneId, guessedUtc, nanosecond);
    var zonedEpochFromGuessedUtc = localDateTimeToEpochSecond(zonedDateTimeFromGuessedUtc.year, zonedDateTimeFromGuessedUtc.month, zonedDateTimeFromGuessedUtc.day, zonedDateTimeFromGuessedUtc.hour, zonedDateTimeFromGuessedUtc.minute, zonedDateTimeFromGuessedUtc.second, nanosecond);
    var offset = zonedEpochFromGuessedUtc.subtract(guessedUtc);
    return offset;
}
var dateTimeFormatCache = new Map();
function getDateTimeFormatForZoneId(timeZoneId) {
    if (!dateTimeFormatCache.has(timeZoneId)) {
        var formatter = new Intl.DateTimeFormat('en-US', {
            timeZone: timeZoneId,
            year: 'numeric',
            month: 'numeric',
            day: 'numeric',
            hour: 'numeric',
            minute: 'numeric',
            second: 'numeric',
            hour12: false,
            era: 'narrow'
        });
        dateTimeFormatCache.set(timeZoneId, formatter);
    }
    return dateTimeFormatCache.get(timeZoneId);
}
function getTimeInZoneId(timeZoneId, epochSecond, nano) {
    var formatter = getDateTimeFormatForZoneId(timeZoneId);
    var utc = (0, neo4j_driver_core_1.int)(epochSecond)
        .multiply(1000)
        .add((0, neo4j_driver_core_1.int)(nano).div(1000000))
        .toNumber();
    var formattedUtcParts = formatter.formatToParts(utc);
    var localDateTime = formattedUtcParts.reduce(function (obj, currentValue) {
        if (currentValue.type === 'era') {
            obj.adjustEra =
                currentValue.value.toUpperCase() === 'B'
                    ? function (year) { return year.subtract(1).negate(); } // 1BC equals to year 0 in astronomical year numbering
                    : functional_1.identity;
        }
        else if (currentValue.type === 'hour') {
            obj.hour = (0, neo4j_driver_core_1.int)(currentValue.value).modulo(24);
        }
        else if (currentValue.type !== 'literal') {
            obj[currentValue.type] = (0, neo4j_driver_core_1.int)(currentValue.value);
        }
        return obj;
    }, {});
    localDateTime.year = localDateTime.adjustEra(localDateTime.year);
    var epochInTimeZone = localDateTimeToEpochSecond(localDateTime.year, localDateTime.month, localDateTime.day, localDateTime.hour, localDateTime.minute, localDateTime.second, localDateTime.nanosecond);
    localDateTime.timeZoneOffsetSeconds = epochInTimeZone.subtract(epochSecond);
    localDateTime.hour = localDateTime.hour.modulo(24);
    return localDateTime;
}
function createDateTimeWithOffsetTransformer(config) {
    var disableLosslessIntegers = config.disableLosslessIntegers, useBigInt = config.useBigInt;
    var dateTimeWithOffsetTransformer = bolt_protocol_v4x4_transformer_1.default.createDateTimeWithOffsetTransformer(config);
    return dateTimeWithOffsetTransformer.extendsWith({
        signature: DATE_TIME_WITH_ZONE_OFFSET,
        toStructure: function (value) {
            var epochSecond = localDateTimeToEpochSecond(value.year, value.month, value.day, value.hour, value.minute, value.second, value.nanosecond);
            var nano = (0, neo4j_driver_core_1.int)(value.nanosecond);
            var timeZoneOffsetSeconds = (0, neo4j_driver_core_1.int)(value.timeZoneOffsetSeconds);
            var utcSecond = epochSecond.subtract(timeZoneOffsetSeconds);
            return new packstream_1.structure.Structure(DATE_TIME_WITH_ZONE_OFFSET, [utcSecond, nano, timeZoneOffsetSeconds]);
        },
        fromStructure: function (struct) {
            packstream_1.structure.verifyStructSize('DateTimeWithZoneOffset', DATE_TIME_WITH_ZONE_OFFSET_STRUCT_SIZE, struct.size);
            var _a = __read(struct.fields, 3), utcSecond = _a[0], nano = _a[1], timeZoneOffsetSeconds = _a[2];
            var epochSecond = (0, neo4j_driver_core_1.int)(utcSecond).add(timeZoneOffsetSeconds);
            var localDateTime = (0, temporal_factory_1.epochSecondAndNanoToLocalDateTime)(epochSecond, nano);
            var result = new neo4j_driver_core_1.DateTime(localDateTime.year, localDateTime.month, localDateTime.day, localDateTime.hour, localDateTime.minute, localDateTime.second, localDateTime.nanosecond, timeZoneOffsetSeconds, null);
            return convertIntegerPropsIfNeeded(result, disableLosslessIntegers, useBigInt);
        }
    });
}
function convertIntegerPropsIfNeeded(obj, disableLosslessIntegers, useBigInt) {
    if (!disableLosslessIntegers && !useBigInt) {
        return obj;
    }
    var convert = function (value) {
        return useBigInt ? value.toBigInt() : value.toNumberOrInfinity();
    };
    var clone = Object.create(Object.getPrototypeOf(obj));
    for (var prop in obj) {
        if (Object.prototype.hasOwnProperty.call(obj, prop) === true) {
            var value = obj[prop];
            clone[prop] = (0, neo4j_driver_core_1.isInt)(value) ? convert(value) : value;
        }
    }
    Object.freeze(clone);
    return clone;
}
exports.default = {
    createDateTimeWithZoneIdTransformer: createDateTimeWithZoneIdTransformer,
    createDateTimeWithOffsetTransformer: createDateTimeWithOffsetTransformer
};
