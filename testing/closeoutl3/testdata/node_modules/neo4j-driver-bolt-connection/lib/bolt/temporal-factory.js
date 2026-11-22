"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.epochDayToDate = epochDayToDate;
exports.nanoOfDayToLocalTime = nanoOfDayToLocalTime;
exports.epochSecondAndNanoToLocalDateTime = epochSecondAndNanoToLocalDateTime;
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
var neo4j_driver_core_1 = require("neo4j-driver-core");
var _a = neo4j_driver_core_1.internal.temporalUtil, DAYS_0000_TO_1970 = _a.DAYS_0000_TO_1970, DAYS_PER_400_YEAR_CYCLE = _a.DAYS_PER_400_YEAR_CYCLE, NANOS_PER_HOUR = _a.NANOS_PER_HOUR, NANOS_PER_MINUTE = _a.NANOS_PER_MINUTE, NANOS_PER_SECOND = _a.NANOS_PER_SECOND, SECONDS_PER_DAY = _a.SECONDS_PER_DAY, floorDiv = _a.floorDiv, floorMod = _a.floorMod;
/**
 * Converts given epoch day to a local date.
 * @param {Integer|number|string} epochDay the epoch day to convert.
 * @return {Date} the date representing the epoch day in years, months and days.
 */
function epochDayToDate(epochDay) {
    epochDay = (0, neo4j_driver_core_1.int)(epochDay);
    var zeroDay = epochDay.add(DAYS_0000_TO_1970).subtract(60);
    var adjust = (0, neo4j_driver_core_1.int)(0);
    if (zeroDay.lessThan(0)) {
        var adjustCycles = zeroDay
            .add(1)
            .div(DAYS_PER_400_YEAR_CYCLE)
            .subtract(1);
        adjust = adjustCycles.multiply(400);
        zeroDay = zeroDay.add(adjustCycles.multiply(-DAYS_PER_400_YEAR_CYCLE));
    }
    var year = zeroDay
        .multiply(400)
        .add(591)
        .div(DAYS_PER_400_YEAR_CYCLE);
    var dayOfYearEst = zeroDay.subtract(year
        .multiply(365)
        .add(year.div(4))
        .subtract(year.div(100))
        .add(year.div(400)));
    if (dayOfYearEst.lessThan(0)) {
        year = year.subtract(1);
        dayOfYearEst = zeroDay.subtract(year
            .multiply(365)
            .add(year.div(4))
            .subtract(year.div(100))
            .add(year.div(400)));
    }
    year = year.add(adjust);
    var marchDayOfYear = dayOfYearEst;
    var marchMonth = marchDayOfYear
        .multiply(5)
        .add(2)
        .div(153);
    var month = marchMonth
        .add(2)
        .modulo(12)
        .add(1);
    var day = marchDayOfYear
        .subtract(marchMonth
        .multiply(306)
        .add(5)
        .div(10))
        .add(1);
    year = year.add(marchMonth.div(10));
    return new neo4j_driver_core_1.Date(year, month, day);
}
/**
 * Converts nanoseconds of the day into local time.
 * @param {Integer|number|string} nanoOfDay the nanoseconds of the day to convert.
 * @return {LocalTime} the local time representing given nanoseconds of the day.
 */
function nanoOfDayToLocalTime(nanoOfDay) {
    nanoOfDay = (0, neo4j_driver_core_1.int)(nanoOfDay);
    var hour = nanoOfDay.div(NANOS_PER_HOUR);
    nanoOfDay = nanoOfDay.subtract(hour.multiply(NANOS_PER_HOUR));
    var minute = nanoOfDay.div(NANOS_PER_MINUTE);
    nanoOfDay = nanoOfDay.subtract(minute.multiply(NANOS_PER_MINUTE));
    var second = nanoOfDay.div(NANOS_PER_SECOND);
    var nanosecond = nanoOfDay.subtract(second.multiply(NANOS_PER_SECOND));
    return new neo4j_driver_core_1.LocalTime(hour, minute, second, nanosecond);
}
/**
 * Converts given epoch second and nanosecond adjustment into a local date time object.
 * @param {Integer|number|string} epochSecond the epoch second to use.
 * @param {Integer|number|string} nano the nanosecond to use.
 * @return {LocalDateTime} the local date time representing given epoch second and nano.
 */
function epochSecondAndNanoToLocalDateTime(epochSecond, nano) {
    var epochDay = floorDiv(epochSecond, SECONDS_PER_DAY);
    var secondsOfDay = floorMod(epochSecond, SECONDS_PER_DAY);
    var nanoOfDay = secondsOfDay.multiply(NANOS_PER_SECOND).add(nano);
    var localDate = epochDayToDate(epochDay);
    var localTime = nanoOfDayToLocalTime(nanoOfDay);
    return new neo4j_driver_core_1.LocalDateTime(localDate.year, localDate.month, localDate.day, localTime.hour, localTime.minute, localTime.second, localTime.nanosecond);
}
