package cypher

import (
	"strings"
	"time"
)

func truncateTemporalValue(kind, unit string, value interface{}, fields map[string]interface{}) (interface{}, bool) {
	base, zoneID, hasDate, hasClock, zoned := temporalTruncationParts(value)
	needsDate := kind == "date" || kind == "localdatetime" || kind == "datetime"
	needsClock := kind == "localtime" || kind == "time"
	if needsDate && !hasDate || needsClock && !hasClock {
		return nil, true
	}
	if needsDate && !hasClock {
		base = time.Date(base.Year(), base.Month(), base.Day(), 0, 0, 0, 0, base.Location())
		hasClock = true
	}

	location := base.Location()
	if location == nil || (!zoned && (kind == "datetime" || kind == "time")) {
		location = time.UTC
		zoneID = ""
	}
	if timezone, exists := fields["timezone"]; exists && (kind == "datetime" || kind == "time") {
		locationName, valid := timezone.(string)
		if !valid {
			return nil, true
		}
		var locationOK bool
		location, zoneID, locationOK = truncationLocation(locationName)
		if !locationOK {
			return nil, true
		}
	}
	base = time.Date(base.Year(), base.Month(), base.Day(), base.Hour(), base.Minute(), base.Second(), base.Nanosecond(), location)
	truncated, valid := truncateTimeAtUnit(base, strings.ToLower(unit))
	if !valid {
		return nil, true
	}
	truncated = applyTruncationFields(truncated, fields)

	switch kind {
	case "date":
		return CypherDate{Time: time.Date(truncated.Year(), truncated.Month(), truncated.Day(), 0, 0, 0, 0, time.UTC)}, true
	case "localdatetime":
		return CypherLocalDateTime{Time: time.Date(truncated.Year(), truncated.Month(), truncated.Day(), truncated.Hour(), truncated.Minute(), truncated.Second(), truncated.Nanosecond(), time.UTC)}, true
	case "datetime":
		if zoneID != "" {
			return CypherDateTime{Time: truncated, ZoneID: zoneID}, true
		}
		return CypherDateTime{Time: truncated}, true
	case "localtime":
		return CypherLocalTime{Time: time.Date(1970, 1, 1, truncated.Hour(), truncated.Minute(), truncated.Second(), truncated.Nanosecond(), time.UTC)}, true
	case "time":
		return CypherTime{Time: time.Date(1970, 1, 1, truncated.Hour(), truncated.Minute(), truncated.Second(), truncated.Nanosecond(), location)}, true
	default:
		return nil, false
	}
}

func temporalTruncationParts(value interface{}) (time.Time, string, bool, bool, bool) {
	switch typed := value.(type) {
	case CypherDate:
		return typed.Time, "", true, false, false
	case CypherLocalTime:
		return typed.Time, "", false, true, false
	case CypherTime:
		return typed.Time, "", false, true, true
	case CypherLocalDateTime:
		return typed.Time, "", true, true, false
	case CypherDateTime:
		return typed.Time, typed.ZoneID, true, true, true
	case time.Time:
		zoneID := ""
		if strings.Contains(typed.Location().String(), "/") {
			zoneID = typed.Location().String()
		}
		return typed, zoneID, true, true, true
	case *CypherDate:
		if typed != nil {
			return temporalTruncationParts(*typed)
		}
	case *CypherLocalTime:
		if typed != nil {
			return temporalTruncationParts(*typed)
		}
	case *CypherTime:
		if typed != nil {
			return temporalTruncationParts(*typed)
		}
	case *CypherLocalDateTime:
		if typed != nil {
			return temporalTruncationParts(*typed)
		}
	case *CypherDateTime:
		if typed != nil {
			return temporalTruncationParts(*typed)
		}
	case *time.Time:
		if typed != nil {
			return temporalTruncationParts(*typed)
		}
	}
	return time.Time{}, "", false, false, false
}

func truncationLocation(name string) (*time.Location, string, bool) {
	if offset, ok := parseTemporalOffset(name); ok {
		return time.FixedZone(name, offset), "", true
	}
	location, err := time.LoadLocation(name)
	return location, name, err == nil
}

func truncateTimeAtUnit(value time.Time, unit string) (time.Time, bool) {
	year, month, day := value.Date()
	hour, minute, second, nanos := value.Hour(), value.Minute(), value.Second(), value.Nanosecond()
	switch unit {
	case "millennium":
		year, month, day, hour, minute, second, nanos = year/1_000*1_000, 1, 1, 0, 0, 0, 0
	case "century":
		year, month, day, hour, minute, second, nanos = year/100*100, 1, 1, 0, 0, 0, 0
	case "decade":
		year, month, day, hour, minute, second, nanos = year/10*10, 1, 1, 0, 0, 0, 0
	case "year":
		month, day, hour, minute, second, nanos = 1, 1, 0, 0, 0, 0
	case "weekyear":
		isoYear, _ := value.ISOWeek()
		jan4 := time.Date(isoYear, 1, 4, 0, 0, 0, 0, value.Location())
		return jan4.AddDate(0, 0, -(int(jan4.Weekday())+6)%7), true
	case "quarter":
		month, day, hour, minute, second, nanos = time.Month((int(month)-1)/3*3+1), 1, 0, 0, 0, 0
	case "month":
		day, hour, minute, second, nanos = 1, 0, 0, 0, 0
	case "week":
		weekday := (int(value.Weekday()) + 6) % 7
		return time.Date(year, month, day, 0, 0, 0, 0, value.Location()).AddDate(0, 0, -weekday), true
	case "day":
		hour, minute, second, nanos = 0, 0, 0, 0
	case "hour":
		minute, second, nanos = 0, 0, 0
	case "minute":
		second, nanos = 0, 0
	case "second":
		nanos = 0
	case "millisecond":
		nanos = nanos / 1_000_000 * 1_000_000
	case "microsecond":
		nanos = nanos / 1_000 * 1_000
	case "nanosecond":
	default:
		return time.Time{}, false
	}
	return time.Date(year, month, day, hour, minute, second, nanos, value.Location()), true
}

func applyTruncationFields(value time.Time, fields map[string]interface{}) time.Time {
	year, month, day := value.Date()
	hour, minute, second, nanos := value.Hour(), value.Minute(), value.Second(), value.Nanosecond()
	if replacement, ok := temporalOptionalInt(fields, "day"); ok {
		day = int(replacement)
	}
	if replacement, ok := temporalOptionalInt(fields, "dayOfWeek"); ok {
		current := int(value.Weekday())
		if current == 0 {
			current = 7
		}
		value = value.AddDate(0, 0, int(replacement)-current)
		year, month, day = value.Date()
	}
	if replacement, ok := temporalOptionalInt(fields, "nanosecond"); ok {
		nanos += int(replacement)
	}
	return time.Date(year, month, day, hour, minute, second, nanos, value.Location())
}
