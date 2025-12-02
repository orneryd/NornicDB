// Package date provides APOC date/time functions.
//
// This package implements all apoc.date.* functions for working with
// dates, times, and durations in Cypher queries.
package date

import (
	"time"
)

// Parse parses a date string with a format.
//
// Example:
//   apoc.date.parse('2024-01-15', 'yyyy-MM-dd') => timestamp
func Parse(dateStr, format string) int64 {
	// Convert Java format to Go format
	goFormat := convertFormat(format)
	t, err := time.Parse(goFormat, dateStr)
	if err != nil {
		return 0
	}
	return t.Unix()
}

// Format formats a timestamp as a string.
//
// Example:
//   apoc.date.format(1705276800, 'yyyy-MM-dd') => '2024-01-15'
func Format(timestamp int64, format string) string {
	t := time.Unix(timestamp, 0)
	goFormat := convertFormat(format)
	return t.Format(goFormat)
}

// CurrentTimestamp returns the current Unix timestamp in seconds.
//
// Example:
//   apoc.date.currentTimestamp() => 1705276800
func CurrentTimestamp() int64 {
	return time.Now().Unix()
}

// Field extracts a field from a timestamp.
//
// Example:
//   apoc.date.field(timestamp, 'year') => 2024
func Field(timestamp int64, field string) int {
	t := time.Unix(timestamp, 0)
	
	switch field {
	case "year":
		return t.Year()
	case "month":
		return int(t.Month())
	case "day":
		return t.Day()
	case "hour":
		return t.Hour()
	case "minute":
		return t.Minute()
	case "second":
		return t.Second()
	case "dayOfWeek":
		return int(t.Weekday())
	case "dayOfYear":
		return t.YearDay()
	case "weekOfYear":
		_, week := t.ISOWeek()
		return week
	}
	
	return 0
}

// Fields extracts all fields from a timestamp.
//
// Example:
//   apoc.date.fields(timestamp) 
//   => {year:2024, month:1, day:15, ...}
func Fields(timestamp int64) map[string]int {
	t := time.Unix(timestamp, 0)
	_, week := t.ISOWeek()
	
	return map[string]int{
		"year":       t.Year(),
		"month":      int(t.Month()),
		"day":        t.Day(),
		"hour":       t.Hour(),
		"minute":     t.Minute(),
		"second":     t.Second(),
		"dayOfWeek":  int(t.Weekday()),
		"dayOfYear":  t.YearDay(),
		"weekOfYear": week,
	}
}

// Add adds a duration to a timestamp.
//
// Example:
//   apoc.date.add(timestamp, 1, 'days') => timestamp + 1 day
func Add(timestamp int64, amount int, unit string) int64 {
	t := time.Unix(timestamp, 0)
	duration := getDuration(amount, unit)
	return t.Add(duration).Unix()
}

// Convert converts between time units.
//
// Example:
//   apoc.date.convert(3600, 'seconds', 'hours') => 1
func Convert(value int64, fromUnit, toUnit string) int64 {
	// Convert to seconds first
	seconds := convertToSeconds(value, fromUnit)
	// Convert from seconds to target unit
	return convertFromSeconds(seconds, toUnit)
}

// ConvertFormat converts a date string from one format to another.
//
// Example:
//   apoc.date.convertFormat('2024-01-15', 'yyyy-MM-dd', 'dd/MM/yyyy')
//   => '15/01/2024'
func ConvertFormat(dateStr, fromFormat, toFormat string) string {
	timestamp := Parse(dateStr, fromFormat)
	return Format(timestamp, toFormat)
}

// FromISO8601 parses an ISO 8601 date string.
//
// Example:
//   apoc.date.fromISO8601('2024-01-15T10:30:00Z') => timestamp
func FromISO8601(dateStr string) int64 {
	t, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return 0
	}
	return t.Unix()
}

// ToISO8601 formats a timestamp as ISO 8601.
//
// Example:
//   apoc.date.toISO8601(timestamp) => '2024-01-15T10:30:00Z'
func ToISO8601(timestamp int64) string {
	t := time.Unix(timestamp, 0).UTC()
	return t.Format(time.RFC3339)
}

// ToYears converts a duration to years (approximate).
//
// Example:
//   apoc.date.toYears(31536000) => 1 (365 days)
func ToYears(seconds int64) float64 {
	return float64(seconds) / (365.25 * 24 * 3600)
}

// SystemTimezone returns the system timezone.
//
// Example:
//   apoc.date.systemTimezone() => 'America/New_York'
func SystemTimezone() string {
	zone, _ := time.Now().Zone()
	return zone
}

// ParseAsZonedDateTime parses a date with timezone.
//
// Example:
//   apoc.date.parseAsZonedDateTime('2024-01-15T10:30:00-05:00')
func ParseAsZonedDateTime(dateStr, format string) int64 {
	return Parse(dateStr, format)
}

// ToUnixTime converts to Unix timestamp (same as currentTimestamp).
//
// Example:
//   apoc.date.toUnixTime(date) => timestamp
func ToUnixTime(t time.Time) int64 {
	return t.Unix()
}

// FromUnixTime converts from Unix timestamp.
//
// Example:
//   apoc.date.fromUnixTime(1705276800) => time object
func FromUnixTime(timestamp int64) time.Time {
	return time.Unix(timestamp, 0)
}

// Helper functions

func convertFormat(javaFormat string) string {
	// Convert common Java date format patterns to Go format
	replacements := map[string]string{
		"yyyy": "2006",
		"yy":   "06",
		"MM":   "01",
		"M":    "1",
		"dd":   "02",
		"d":    "2",
		"HH":   "15",
		"H":    "15",
		"mm":   "04",
		"m":    "4",
		"ss":   "05",
		"s":    "5",
		"SSS":  "000",
		"Z":    "Z07:00",
		"z":    "MST",
	}
	
	result := javaFormat
	for java, golang := range replacements {
		result = replaceAll(result, java, golang)
	}
	
	return result
}

func replaceAll(s, old, new string) string {
	result := ""
	for i := 0; i < len(s); {
		if i+len(old) <= len(s) && s[i:i+len(old)] == old {
			result += new
			i += len(old)
		} else {
			result += string(s[i])
			i++
		}
	}
	return result
}

func getDuration(amount int, unit string) time.Duration {
	switch unit {
	case "ms", "millis", "milliseconds":
		return time.Duration(amount) * time.Millisecond
	case "s", "seconds":
		return time.Duration(amount) * time.Second
	case "m", "minutes":
		return time.Duration(amount) * time.Minute
	case "h", "hours":
		return time.Duration(amount) * time.Hour
	case "d", "days":
		return time.Duration(amount) * 24 * time.Hour
	case "w", "weeks":
		return time.Duration(amount) * 7 * 24 * time.Hour
	default:
		return 0
	}
}

func convertToSeconds(value int64, unit string) int64 {
	switch unit {
	case "ms", "millis", "milliseconds":
		return value / 1000
	case "s", "seconds":
		return value
	case "m", "minutes":
		return value * 60
	case "h", "hours":
		return value * 3600
	case "d", "days":
		return value * 86400
	case "w", "weeks":
		return value * 604800
	default:
		return value
	}
}

func convertFromSeconds(seconds int64, unit string) int64 {
	switch unit {
	case "ms", "millis", "milliseconds":
		return seconds * 1000
	case "s", "seconds":
		return seconds
	case "m", "minutes":
		return seconds / 60
	case "h", "hours":
		return seconds / 3600
	case "d", "days":
		return seconds / 86400
	case "w", "weeks":
		return seconds / 604800
	default:
		return seconds
	}
}
