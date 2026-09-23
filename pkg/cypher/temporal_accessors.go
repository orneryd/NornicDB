package cypher

import (
	"strings"
	"time"
)

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
