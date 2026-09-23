package cypher

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func formatCypherValueString(value interface{}) string {
	switch typed := value.(type) {
	case CypherDate:
		return typed.String()
	case *CypherDate:
		return typed.String()
	case CypherLocalTime:
		return typed.String()
	case *CypherLocalTime:
		return typed.String()
	case CypherTime:
		return typed.String()
	case *CypherTime:
		return typed.String()
	case CypherLocalDateTime:
		return typed.String()
	case *CypherLocalDateTime:
		return typed.String()
	case CypherDateTime:
		return typed.String()
	case *CypherDateTime:
		return typed.String()
	case time.Time:
		return formatZonedDateTimeString(typed, "")
	case *time.Time:
		return formatZonedDateTimeString(*typed, "")
	case CypherDuration:
		return typed.String()
	case *CypherDuration:
		return typed.String()
	default:
		return fmt.Sprint(value)
	}
}

func formatZonedDateTimeString(value time.Time, zoneID string) string {
	if zoneID == "" && strings.Contains(value.Location().String(), "/") {
		zoneID = value.Location().String()
	}
	return formatTemporalDateTime(value, true, zoneID)
}

// compareTemporalValues applies Neo4j temporal equality without depending on
// Go location-pointer identity or the incidental representation of durations.
func compareTemporalValues(left, right interface{}) (bool, bool) {
	leftKind, leftParts, leftOK := temporalEqualityParts(left)
	rightKind, rightParts, rightOK := temporalEqualityParts(right)
	if !leftOK && !rightOK {
		return false, false
	}
	if !leftOK || !rightOK || leftKind != rightKind {
		return false, true
	}
	return reflect.DeepEqual(leftParts, rightParts), true
}

func compareTemporalOrdering(left, right interface{}) (int, bool) {
	leftKind, leftTime, leftOK := temporalOrderParts(left)
	rightKind, rightTime, rightOK := temporalOrderParts(right)
	if !leftOK || !rightOK || leftKind != rightKind {
		return 0, false
	}
	switch leftKind {
	case "date":
		return compareCivilParts(leftTime, rightTime, false), true
	case "localtime":
		return compareInt64(clockNanos(leftTime), clockNanos(rightTime)), true
	case "time":
		_, leftOffset := leftTime.Zone()
		_, rightOffset := rightTime.Zone()
		leftUTC := clockNanos(leftTime) - int64(leftOffset)*1_000_000_000
		rightUTC := clockNanos(rightTime) - int64(rightOffset)*1_000_000_000
		if comparison := compareInt64(leftUTC, rightUTC); comparison != 0 {
			return comparison, true
		}
		return compareInt64(int64(leftOffset), int64(rightOffset)), true
	case "localdatetime":
		return compareCivilParts(leftTime, rightTime, true), true
	case "datetime":
		if comparison := compareInt64(leftTime.Unix(), rightTime.Unix()); comparison != 0 {
			return comparison, true
		}
		if comparison := compareInt64(int64(leftTime.Nanosecond()), int64(rightTime.Nanosecond())); comparison != 0 {
			return comparison, true
		}
		_, leftOffset := leftTime.Zone()
		_, rightOffset := rightTime.Zone()
		return compareInt64(int64(leftOffset), int64(rightOffset)), true
	default:
		return 0, false
	}
}

func temporalOrderParts(value interface{}) (string, time.Time, bool) {
	switch typed := value.(type) {
	case CypherDate:
		return "date", typed.Time, true
	case *CypherDate:
		if typed != nil {
			return "date", typed.Time, true
		}
	case CypherLocalTime:
		return "localtime", typed.Time, true
	case *CypherLocalTime:
		if typed != nil {
			return "localtime", typed.Time, true
		}
	case CypherTime:
		return "time", typed.Time, true
	case *CypherTime:
		if typed != nil {
			return "time", typed.Time, true
		}
	case CypherLocalDateTime:
		return "localdatetime", typed.Time, true
	case *CypherLocalDateTime:
		if typed != nil {
			return "localdatetime", typed.Time, true
		}
	case CypherDateTime:
		return "datetime", typed.Time, true
	case *CypherDateTime:
		if typed != nil {
			return "datetime", typed.Time, true
		}
	case time.Time:
		return "datetime", typed, true
	case *time.Time:
		if typed != nil {
			return "datetime", *typed, true
		}
	}
	return "", time.Time{}, false
}

func clockNanos(value time.Time) int64 {
	return int64(value.Hour())*3_600_000_000_000 + int64(value.Minute())*60_000_000_000 + int64(value.Second())*1_000_000_000 + int64(value.Nanosecond())
}

func compareCivilParts(left, right time.Time, includeClock bool) int {
	leftParts := [...]int{left.Year(), int(left.Month()), left.Day(), left.Hour(), left.Minute(), left.Second(), left.Nanosecond()}
	rightParts := [...]int{right.Year(), int(right.Month()), right.Day(), right.Hour(), right.Minute(), right.Second(), right.Nanosecond()}
	limit := 3
	if includeClock {
		limit = len(leftParts)
	}
	for index := 0; index < limit; index++ {
		if leftParts[index] < rightParts[index] {
			return -1
		}
		if leftParts[index] > rightParts[index] {
			return 1
		}
	}
	return 0
}

func compareInt64(left, right int64) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

func temporalEqualityParts(value interface{}) (string, []interface{}, bool) {
	switch typed := value.(type) {
	case CypherDate:
		return dateEqualityParts(typed.Time)
	case *CypherDate:
		if typed != nil {
			return dateEqualityParts(typed.Time)
		}
	case CypherLocalTime:
		return clockEqualityParts("localtime", typed.Time, "")
	case *CypherLocalTime:
		if typed != nil {
			return clockEqualityParts("localtime", typed.Time, "")
		}
	case CypherTime:
		return clockEqualityParts("time", typed.Time, temporalZoneIdentity(typed.Time, ""))
	case *CypherTime:
		if typed != nil {
			return clockEqualityParts("time", typed.Time, temporalZoneIdentity(typed.Time, ""))
		}
	case CypherLocalDateTime:
		return localDateTimeEqualityParts(typed.Time)
	case *CypherLocalDateTime:
		if typed != nil {
			return localDateTimeEqualityParts(typed.Time)
		}
	case CypherDateTime:
		return zonedDateTimeEqualityParts(typed.Time, typed.ZoneID)
	case *CypherDateTime:
		if typed != nil {
			return zonedDateTimeEqualityParts(typed.Time, typed.ZoneID)
		}
	case time.Time:
		return zonedDateTimeEqualityParts(typed, "")
	case *time.Time:
		if typed != nil {
			return zonedDateTimeEqualityParts(*typed, "")
		}
	case CypherDuration:
		return durationEqualityParts(&typed)
	case *CypherDuration:
		if typed != nil {
			return durationEqualityParts(typed)
		}
	}
	return "", nil, false
}

func dateEqualityParts(value time.Time) (string, []interface{}, bool) {
	return "date", []interface{}{value.Year(), value.Month(), value.Day()}, true
}

func clockEqualityParts(kind string, value time.Time, zone string) (string, []interface{}, bool) {
	return kind, []interface{}{value.Hour(), value.Minute(), value.Second(), value.Nanosecond(), zone}, true
}

func localDateTimeEqualityParts(value time.Time) (string, []interface{}, bool) {
	return "localdatetime", []interface{}{value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond()}, true
}

func zonedDateTimeEqualityParts(value time.Time, zoneID string) (string, []interface{}, bool) {
	return "datetime", []interface{}{value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond(), temporalZoneIdentity(value, zoneID)}, true
}

func durationEqualityParts(value *CypherDuration) (string, []interface{}, bool) {
	return "duration", []interface{}{value.Years*12 + value.Months, value.Days, value.Hours*3_600 + value.Minutes*60 + value.Seconds, value.Nanos}, true
}

func temporalZoneIdentity(value time.Time, zoneID string) string {
	if zoneID == "" {
		zoneID = value.Location().String()
	}
	if strings.Contains(zoneID, "/") {
		return "named:" + zoneID
	}
	_, offset := value.Zone()
	return "offset:" + strconv.Itoa(offset)
}

// evaluateTemporalProperty reads a Cypher temporal field from its typed value.
// The second result identifies temporal values so callers do not reinterpret
// unsupported fields as map properties; the third reports a supported field.
func evaluateTemporalProperty(value interface{}, property string) (interface{}, bool, bool) {
	var temporal time.Time
	var zoneID string
	hasDate, hasClock, hasZone, hasInstant := false, false, false, false

	switch typed := value.(type) {
	case CypherDate:
		temporal, hasDate = typed.Time, true
	case *CypherDate:
		if typed == nil {
			return nil, true, true
		}
		temporal, hasDate = typed.Time, true
	case CypherLocalTime:
		temporal, hasClock = typed.Time, true
	case *CypherLocalTime:
		if typed == nil {
			return nil, true, true
		}
		temporal, hasClock = typed.Time, true
	case CypherTime:
		temporal, hasClock, hasZone = typed.Time, true, true
	case *CypherTime:
		if typed == nil {
			return nil, true, true
		}
		temporal, hasClock, hasZone = typed.Time, true, true
	case CypherLocalDateTime:
		temporal, hasDate, hasClock = typed.Time, true, true
	case *CypherLocalDateTime:
		if typed == nil {
			return nil, true, true
		}
		temporal, hasDate, hasClock = typed.Time, true, true
	case CypherDateTime:
		temporal, zoneID = typed.Time, typed.ZoneID
		hasDate, hasClock, hasZone, hasInstant = true, true, true, true
	case *CypherDateTime:
		if typed == nil {
			return nil, true, true
		}
		temporal, zoneID = typed.Time, typed.ZoneID
		hasDate, hasClock, hasZone, hasInstant = true, true, true, true
	case time.Time:
		temporal = typed
		hasDate, hasClock, hasZone, hasInstant = true, true, true, true
	case *time.Time:
		if typed == nil {
			return nil, true, true
		}
		temporal = *typed
		hasDate, hasClock, hasZone, hasInstant = true, true, true, true
	case CypherDuration:
		return evaluateDurationProperty(&typed, property)
	case *CypherDuration:
		if typed == nil {
			return nil, true, true
		}
		return evaluateDurationProperty(typed, property)
	default:
		return nil, false, false
	}

	field := strings.ToLower(property)
	if hasDate {
		isoYear, isoWeek := temporal.ISOWeek()
		quarter := (int(temporal.Month())-1)/3 + 1
		quarterStart := time.Date(temporal.Year(), time.Month((quarter-1)*3+1), 1, 0, 0, 0, 0, temporal.Location())
		switch field {
		case "year":
			return int64(temporal.Year()), true, true
		case "quarter":
			return int64(quarter), true, true
		case "month":
			return int64(temporal.Month()), true, true
		case "week":
			return int64(isoWeek), true, true
		case "weekyear":
			return int64(isoYear), true, true
		case "day":
			return int64(temporal.Day()), true, true
		case "ordinalday":
			return int64(temporal.YearDay()), true, true
		case "weekday", "dayofweek":
			weekday := temporal.Weekday()
			if weekday == time.Sunday {
				return int64(7), true, true
			}
			return int64(weekday), true, true
		case "dayofquarter":
			return int64(temporal.YearDay() - quarterStart.YearDay() + 1), true, true
		}
	}
	if hasClock {
		switch field {
		case "hour":
			return int64(temporal.Hour()), true, true
		case "minute":
			return int64(temporal.Minute()), true, true
		case "second":
			return int64(temporal.Second()), true, true
		case "millisecond":
			return int64(temporal.Nanosecond() / 1_000_000), true, true
		case "microsecond":
			return int64(temporal.Nanosecond() / 1_000), true, true
		case "nanosecond":
			return int64(temporal.Nanosecond()), true, true
		}
	}
	if hasZone {
		_, offsetSeconds := temporal.Zone()
		switch field {
		case "timezone":
			if zoneID != "" {
				return zoneID, true, true
			}
			location := temporal.Location().String()
			if location == "UTC" || location == "Local" || location == "" {
				return formatTemporalOffset(offsetSeconds), true, true
			}
			return location, true, true
		case "offset":
			return formatTemporalOffset(offsetSeconds), true, true
		case "offsetminutes":
			return int64(offsetSeconds / 60), true, true
		case "offsetseconds":
			return int64(offsetSeconds), true, true
		}
	}
	if hasInstant {
		switch field {
		case "epochseconds":
			return temporal.UnixMilli() / 1_000, true, true
		case "epochmillis":
			return temporal.UnixMilli(), true, true
		}
	}
	return nil, true, false
}

func evaluateDurationProperty(duration *CypherDuration, property string) (interface{}, bool, bool) {
	months := duration.Years*12 + duration.Months
	seconds := duration.Hours*3_600 + duration.Minutes*60 + duration.Seconds
	switch strings.ToLower(property) {
	case "years":
		return months / 12, true, true
	case "quarters":
		return months / 3, true, true
	case "months":
		return months, true, true
	case "weeks":
		return duration.Days / 7, true, true
	case "days":
		return duration.Days, true, true
	case "hours":
		return seconds / 3_600, true, true
	case "minutes":
		return seconds / 60, true, true
	case "seconds":
		return seconds, true, true
	case "milliseconds":
		return seconds*1_000 + duration.Nanos/1_000_000, true, true
	case "microseconds":
		return seconds*1_000_000 + duration.Nanos/1_000, true, true
	case "nanoseconds":
		return seconds*1_000_000_000 + duration.Nanos, true, true
	case "quartersofyear":
		return (months / 3) % 4, true, true
	case "monthsofquarter":
		return months % 3, true, true
	case "monthsofyear":
		return months % 12, true, true
	case "daysofweek":
		return duration.Days % 7, true, true
	case "minutesofhour":
		return (seconds / 60) % 60, true, true
	case "secondsofminute":
		return seconds % 60, true, true
	case "millisecondsofsecond":
		return duration.Nanos / 1_000_000, true, true
	case "microsecondsofsecond":
		return duration.Nanos / 1_000, true, true
	case "nanosecondsofsecond":
		return duration.Nanos, true, true
	default:
		return nil, true, false
	}
}
