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
var neo4j_driver_core_1 = require("neo4j-driver-core");
var packstream_1 = require("../packstream");
var transformer_1 = require("./transformer");
var temporal_factory_1 = require("./temporal-factory");
var bolt_protocol_v1_transformer_1 = __importDefault(require("./bolt-protocol-v1.transformer"));
var _a = neo4j_driver_core_1.internal.temporalUtil, dateToEpochDay = _a.dateToEpochDay, localDateTimeToEpochSecond = _a.localDateTimeToEpochSecond, localTimeToNanoOfDay = _a.localTimeToNanoOfDay;
var POINT_2D = 0x58;
var POINT_2D_STRUCT_SIZE = 3;
var POINT_3D = 0x59;
var POINT_3D_STRUCT_SIZE = 4;
var DURATION = 0x45;
var DURATION_STRUCT_SIZE = 4;
var LOCAL_TIME = 0x74;
var LOCAL_TIME_STRUCT_SIZE = 1;
var TIME = 0x54;
var TIME_STRUCT_SIZE = 2;
var DATE = 0x44;
var DATE_STRUCT_SIZE = 1;
var LOCAL_DATE_TIME = 0x64;
var LOCAL_DATE_TIME_STRUCT_SIZE = 2;
var DATE_TIME_WITH_ZONE_OFFSET = 0x46;
var DATE_TIME_WITH_ZONE_OFFSET_STRUCT_SIZE = 3;
var DATE_TIME_WITH_ZONE_ID = 0x66;
var DATE_TIME_WITH_ZONE_ID_STRUCT_SIZE = 3;
/**
 * Creates the Point2D Transformer
 * @returns {TypeTransformer}
 */
function createPoint2DTransformer() {
    return new transformer_1.TypeTransformer({
        signature: POINT_2D,
        isTypeInstance: function (point) { return (0, neo4j_driver_core_1.isPoint)(point) && (point.z === null || point.z === undefined); },
        toStructure: function (point) { return new packstream_1.structure.Structure(POINT_2D, [
            (0, neo4j_driver_core_1.int)(point.srid),
            point.x,
            point.y
        ]); },
        fromStructure: function (struct) {
            packstream_1.structure.verifyStructSize('Point2D', POINT_2D_STRUCT_SIZE, struct.size);
            var _a = __read(struct.fields, 3), srid = _a[0], x = _a[1], y = _a[2];
            return new neo4j_driver_core_1.Point(srid, x, y, undefined // z
            );
        }
    });
}
/**
 * Creates the Point3D Transformer
 * @returns {TypeTransformer}
 */
function createPoint3DTransformer() {
    return new transformer_1.TypeTransformer({
        signature: POINT_3D,
        isTypeInstance: function (point) { return (0, neo4j_driver_core_1.isPoint)(point) && point.z !== null && point.z !== undefined; },
        toStructure: function (point) { return new packstream_1.structure.Structure(POINT_3D, [
            (0, neo4j_driver_core_1.int)(point.srid),
            point.x,
            point.y,
            point.z
        ]); },
        fromStructure: function (struct) {
            packstream_1.structure.verifyStructSize('Point3D', POINT_3D_STRUCT_SIZE, struct.size);
            var _a = __read(struct.fields, 4), srid = _a[0], x = _a[1], y = _a[2], z = _a[3];
            return new neo4j_driver_core_1.Point(srid, x, y, z);
        }
    });
}
/**
 * Creates the Duration Transformer
 * @returns {TypeTransformer}
 */
function createDurationTransformer() {
    return new transformer_1.TypeTransformer({
        signature: DURATION,
        isTypeInstance: neo4j_driver_core_1.isDuration,
        toStructure: function (value) {
            var months = (0, neo4j_driver_core_1.int)(value.months);
            var days = (0, neo4j_driver_core_1.int)(value.days);
            var seconds = (0, neo4j_driver_core_1.int)(value.seconds);
            var nanoseconds = (0, neo4j_driver_core_1.int)(value.nanoseconds);
            return new packstream_1.structure.Structure(DURATION, [months, days, seconds, nanoseconds]);
        },
        fromStructure: function (struct) {
            packstream_1.structure.verifyStructSize('Duration', DURATION_STRUCT_SIZE, struct.size);
            var _a = __read(struct.fields, 4), months = _a[0], days = _a[1], seconds = _a[2], nanoseconds = _a[3];
            return new neo4j_driver_core_1.Duration(months, days, seconds, nanoseconds);
        }
    });
}
/**
 * Creates the LocalTime Transformer
 * @param {Object} param
 * @param {boolean} param.disableLosslessIntegers Disables lossless integers
 * @param {boolean} param.useBigInt Uses BigInt instead of number or Integer
 * @returns {TypeTransformer}
 */
function createLocalTimeTransformer(_a) {
    var disableLosslessIntegers = _a.disableLosslessIntegers, useBigInt = _a.useBigInt;
    return new transformer_1.TypeTransformer({
        signature: LOCAL_TIME,
        isTypeInstance: neo4j_driver_core_1.isLocalTime,
        toStructure: function (value) {
            var nanoOfDay = localTimeToNanoOfDay(value.hour, value.minute, value.second, value.nanosecond);
            return new packstream_1.structure.Structure(LOCAL_TIME, [nanoOfDay]);
        },
        fromStructure: function (struct) {
            packstream_1.structure.verifyStructSize('LocalTime', LOCAL_TIME_STRUCT_SIZE, struct.size);
            var _a = __read(struct.fields, 1), nanoOfDay = _a[0];
            var result = (0, temporal_factory_1.nanoOfDayToLocalTime)(nanoOfDay);
            return convertIntegerPropsIfNeeded(result, disableLosslessIntegers, useBigInt);
        }
    });
}
/**
 * Creates the Time Transformer
 * @param {Object} param
 * @param {boolean} param.disableLosslessIntegers Disables lossless integers
 * @param {boolean} param.useBigInt Uses BigInt instead of number or Integer
 * @returns {TypeTransformer}
 */
function createTimeTransformer(_a) {
    var disableLosslessIntegers = _a.disableLosslessIntegers, useBigInt = _a.useBigInt;
    return new transformer_1.TypeTransformer({
        signature: TIME,
        isTypeInstance: neo4j_driver_core_1.isTime,
        toStructure: function (value) {
            var nanoOfDay = localTimeToNanoOfDay(value.hour, value.minute, value.second, value.nanosecond);
            var offsetSeconds = (0, neo4j_driver_core_1.int)(value.timeZoneOffsetSeconds);
            return new packstream_1.structure.Structure(TIME, [nanoOfDay, offsetSeconds]);
        },
        fromStructure: function (struct) {
            packstream_1.structure.verifyStructSize('Time', TIME_STRUCT_SIZE, struct.size);
            var _a = __read(struct.fields, 2), nanoOfDay = _a[0], offsetSeconds = _a[1];
            var localTime = (0, temporal_factory_1.nanoOfDayToLocalTime)(nanoOfDay);
            var result = new neo4j_driver_core_1.Time(localTime.hour, localTime.minute, localTime.second, localTime.nanosecond, offsetSeconds);
            return convertIntegerPropsIfNeeded(result, disableLosslessIntegers, useBigInt);
        }
    });
}
/**
 * Creates the Date Transformer
 * @param {Object} param
 * @param {boolean} param.disableLosslessIntegers Disables lossless integers
 * @param {boolean} param.useBigInt Uses BigInt instead of number or Integer
 * @returns {TypeTransformer}
 */
function createDateTransformer(_a) {
    var disableLosslessIntegers = _a.disableLosslessIntegers, useBigInt = _a.useBigInt;
    return new transformer_1.TypeTransformer({
        signature: DATE,
        isTypeInstance: neo4j_driver_core_1.isDate,
        toStructure: function (value) {
            var epochDay = dateToEpochDay(value.year, value.month, value.day);
            return new packstream_1.structure.Structure(DATE, [epochDay]);
        },
        fromStructure: function (struct) {
            packstream_1.structure.verifyStructSize('Date', DATE_STRUCT_SIZE, struct.size);
            var _a = __read(struct.fields, 1), epochDay = _a[0];
            var result = (0, temporal_factory_1.epochDayToDate)(epochDay);
            return convertIntegerPropsIfNeeded(result, disableLosslessIntegers, useBigInt);
        }
    });
}
/**
 * Creates the LocalDateTime Transformer
 * @param {Object} param
 * @param {boolean} param.disableLosslessIntegers Disables lossless integers
 * @param {boolean} param.useBigInt Uses BigInt instead of number or Integer
 * @returns {TypeTransformer}
 */
function createLocalDateTimeTransformer(_a) {
    var disableLosslessIntegers = _a.disableLosslessIntegers, useBigInt = _a.useBigInt;
    return new transformer_1.TypeTransformer({
        signature: LOCAL_DATE_TIME,
        isTypeInstance: neo4j_driver_core_1.isLocalDateTime,
        toStructure: function (value) {
            var epochSecond = localDateTimeToEpochSecond(value.year, value.month, value.day, value.hour, value.minute, value.second, value.nanosecond);
            var nano = (0, neo4j_driver_core_1.int)(value.nanosecond);
            return new packstream_1.structure.Structure(LOCAL_DATE_TIME, [epochSecond, nano]);
        },
        fromStructure: function (struct) {
            packstream_1.structure.verifyStructSize('LocalDateTime', LOCAL_DATE_TIME_STRUCT_SIZE, struct.size);
            var _a = __read(struct.fields, 2), epochSecond = _a[0], nano = _a[1];
            var result = (0, temporal_factory_1.epochSecondAndNanoToLocalDateTime)(epochSecond, nano);
            return convertIntegerPropsIfNeeded(result, disableLosslessIntegers, useBigInt);
        }
    });
}
/**
 * Creates the DateTime with ZoneId Transformer
 * @param {Object} param
 * @param {boolean} param.disableLosslessIntegers Disables lossless integers
 * @param {boolean} param.useBigInt Uses BigInt instead of number or Integer
 * @returns {TypeTransformer}
 */
function createDateTimeWithZoneIdTransformer(_a) {
    var disableLosslessIntegers = _a.disableLosslessIntegers, useBigInt = _a.useBigInt;
    return new transformer_1.TypeTransformer({
        signature: DATE_TIME_WITH_ZONE_ID,
        isTypeInstance: function (object) { return (0, neo4j_driver_core_1.isDateTime)(object) && object.timeZoneId != null; },
        toStructure: function (value) {
            var epochSecond = localDateTimeToEpochSecond(value.year, value.month, value.day, value.hour, value.minute, value.second, value.nanosecond);
            var nano = (0, neo4j_driver_core_1.int)(value.nanosecond);
            var timeZoneId = value.timeZoneId;
            return new packstream_1.structure.Structure(DATE_TIME_WITH_ZONE_ID, [epochSecond, nano, timeZoneId]);
        },
        fromStructure: function (struct) {
            packstream_1.structure.verifyStructSize('DateTimeWithZoneId', DATE_TIME_WITH_ZONE_ID_STRUCT_SIZE, struct.size);
            var _a = __read(struct.fields, 3), epochSecond = _a[0], nano = _a[1], timeZoneId = _a[2];
            var localDateTime = (0, temporal_factory_1.epochSecondAndNanoToLocalDateTime)(epochSecond, nano);
            var result = new neo4j_driver_core_1.DateTime(localDateTime.year, localDateTime.month, localDateTime.day, localDateTime.hour, localDateTime.minute, localDateTime.second, localDateTime.nanosecond, null, timeZoneId);
            return convertIntegerPropsIfNeeded(result, disableLosslessIntegers, useBigInt);
        }
    });
}
/**
 * Creates the DateTime with Offset Transformer
 * @param {Object} param
 * @param {boolean} param.disableLosslessIntegers Disables lossless integers
 * @param {boolean} param.useBigInt Uses BigInt instead of number or Integer
 * @returns {TypeTransformer}
 */
function createDateTimeWithOffsetTransformer(_a) {
    var disableLosslessIntegers = _a.disableLosslessIntegers, useBigInt = _a.useBigInt;
    return new transformer_1.TypeTransformer({
        signature: DATE_TIME_WITH_ZONE_OFFSET,
        isTypeInstance: function (object) { return (0, neo4j_driver_core_1.isDateTime)(object) && object.timeZoneId == null; },
        toStructure: function (value) {
            var epochSecond = localDateTimeToEpochSecond(value.year, value.month, value.day, value.hour, value.minute, value.second, value.nanosecond);
            var nano = (0, neo4j_driver_core_1.int)(value.nanosecond);
            var timeZoneOffsetSeconds = (0, neo4j_driver_core_1.int)(value.timeZoneOffsetSeconds);
            return new packstream_1.structure.Structure(DATE_TIME_WITH_ZONE_OFFSET, [epochSecond, nano, timeZoneOffsetSeconds]);
        },
        fromStructure: function (struct) {
            packstream_1.structure.verifyStructSize('DateTimeWithZoneOffset', DATE_TIME_WITH_ZONE_OFFSET_STRUCT_SIZE, struct.size);
            var _a = __read(struct.fields, 3), epochSecond = _a[0], nano = _a[1], timeZoneOffsetSeconds = _a[2];
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
exports.default = __assign(__assign({}, bolt_protocol_v1_transformer_1.default), { createPoint2DTransformer: createPoint2DTransformer, createPoint3DTransformer: createPoint3DTransformer, createDurationTransformer: createDurationTransformer, createLocalTimeTransformer: createLocalTimeTransformer, createTimeTransformer: createTimeTransformer, createDateTransformer: createDateTransformer, createLocalDateTimeTransformer: createLocalDateTimeTransformer, createDateTimeWithZoneIdTransformer: createDateTimeWithZoneIdTransformer, createDateTimeWithOffsetTransformer: createDateTimeWithOffsetTransformer });
