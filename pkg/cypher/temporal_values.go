package cypher

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// CypherDate is a calendar date without a time or zone.
type CypherDate struct{ Time time.Time }

// CypherLocalTime is a wall-clock time without a zone.
type CypherLocalTime struct{ Time time.Time }

// CypherTime is a wall-clock time with a fixed offset.
type CypherTime struct{ Time time.Time }

// CypherLocalDateTime is a calendar date and wall-clock time without a zone.
type CypherLocalDateTime struct{ Time time.Time }

// CypherDateTime is a zoned instant. ZoneID is retained for named-zone wire encoding.
type CypherDateTime struct {
	Time   time.Time
	ZoneID string
}

func (v CypherDate) String() string          { return v.Time.Format("2006-01-02") }
func (v CypherLocalTime) String() string     { return formatTemporalClock(v.Time, false, "") }
func (v CypherTime) String() string          { return formatTemporalClock(v.Time, true, "") }
func (v CypherLocalDateTime) String() string { return formatTemporalDateTime(v.Time, false, "") }
func (v CypherDateTime) String() string      { return formatTemporalDateTime(v.Time, true, v.ZoneID) }

func (e *StorageExecutor) evaluateTemporalConstructor(ctxEval func(string) interface{}, expression string) (interface{}, bool) {
	name, argument, ok := parseFunctionCallWS(expression)
	if !ok {
		return nil, false
	}
	switch strings.ToLower(name) {
	case "date", "localtime", "time", "localdatetime", "datetime", "duration":
		value := ctxEval(strings.TrimSpace(argument))
		if text, isString := value.(string); isString {
			return parseTemporalText(strings.ToLower(name), text)
		}
		fields, isMap := toStringAnyMap(value)
		if !isMap {
			return nil, false
		}
		return buildTemporalValue(strings.ToLower(name), fields)
	case "datetime.fromepoch":
		arguments := e.splitFunctionArgs(argument)
		if len(arguments) != 2 {
			return nil, true
		}
		seconds, secondsOK := temporalInt(ctxEval(strings.TrimSpace(arguments[0])))
		nanos, nanosOK := temporalInt(ctxEval(strings.TrimSpace(arguments[1])))
		if !secondsOK || !nanosOK {
			return nil, true
		}
		return time.Unix(seconds, nanos).UTC(), true
	case "datetime.fromepochmillis":
		millis, valid := temporalInt(ctxEval(strings.TrimSpace(argument)))
		if !valid {
			return nil, true
		}
		return time.UnixMilli(millis).UTC(), true
	default:
		return nil, false
	}
}

func buildTemporalValue(kind string, fields map[string]interface{}) (interface{}, bool) {
	if kind == "duration" {
		return buildDurationFromFields(fields), true
	}
	date, valid := buildDateFromFields(fields)
	if !valid && kind != "localtime" && kind != "time" {
		return nil, true
	}
	if kind == "date" {
		return CypherDate{Time: date}, true
	}
	hour := temporalFieldInt(fields, "hour", 0)
	minute := temporalFieldInt(fields, "minute", 0)
	second := temporalFieldInt(fields, "second", 0)
	nanosecond := temporalFieldInt(fields, "nanosecond", 0) +
		temporalFieldInt(fields, "microsecond", 0)*1_000 +
		temporalFieldInt(fields, "millisecond", 0)*1_000_000
	if kind == "localtime" || kind == "time" {
		date = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	zone, zoneID, zoneOK := temporalLocation(fields, kind == "time" || kind == "datetime")
	if !zoneOK {
		return nil, true
	}
	value := time.Date(date.Year(), date.Month(), date.Day(), int(hour), int(minute), int(second), int(nanosecond), zone)
	switch kind {
	case "localtime":
		return CypherLocalTime{Time: value}, true
	case "time":
		return CypherTime{Time: value}, true
	case "localdatetime":
		return CypherLocalDateTime{Time: value}, true
	default:
		_ = zoneID
		return value, true
	}
}

func buildDateFromFields(fields map[string]interface{}) (time.Time, bool) {
	base, hasBase := temporalBaseDate(fields["date"])
	year, hasYear := temporalOptionalInt(fields, "year")
	if !hasYear && hasBase {
		year = int64(base.Year())
	}
	if !hasYear && !hasBase {
		return time.Time{}, false
	}
	if week, exists := temporalOptionalInt(fields, "week"); exists {
		day := int64(1)
		if hasBase {
			isoYear, _ := base.ISOWeek()
			if !hasYear {
				year = int64(isoYear)
			}
			day = int64(base.Weekday())
			if day == 0 {
				day = 7
			}
		}
		day = temporalFieldInt(fields, "dayOfWeek", day)
		jan4 := time.Date(int(year), 1, 4, 0, 0, 0, 0, time.UTC)
		monday := jan4.AddDate(0, 0, -(int(jan4.Weekday())+6)%7)
		return monday.AddDate(0, 0, (int(week)-1)*7+int(day)-1), true
	}
	if ordinal, exists := temporalOptionalInt(fields, "ordinalDay"); exists {
		return time.Date(int(year), 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(ordinal)-1), true
	}
	if quarter, exists := temporalOptionalInt(fields, "quarter"); exists {
		day := temporalFieldInt(fields, "dayOfQuarter", 1)
		return time.Date(int(year), time.Month((quarter-1)*3+1), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(day)-1), true
	}
	month, day := int64(1), int64(1)
	if hasBase {
		month, day = int64(base.Month()), int64(base.Day())
	}
	month = temporalFieldInt(fields, "month", month)
	day = temporalFieldInt(fields, "day", day)
	return time.Date(int(year), time.Month(month), int(day), 0, 0, 0, 0, time.UTC), true
}

func temporalBaseDate(value interface{}) (time.Time, bool) {
	switch value := value.(type) {
	case CypherDate:
		return value.Time, true
	case CypherLocalDateTime:
		return value.Time, true
	case CypherDateTime:
		return value.Time, true
	case time.Time:
		return value, true
	case string:
		parsed, err := time.Parse("2006-01-02", value)
		return parsed, err == nil
	default:
		return time.Time{}, false
	}
}

func temporalLocation(fields map[string]interface{}, zoned bool) (*time.Location, string, bool) {
	if !zoned {
		return time.UTC, "", true
	}
	zoneValue, exists := fields["timezone"]
	if !exists {
		return time.UTC, "", true
	}
	zoneID, ok := zoneValue.(string)
	if !ok {
		return nil, "", false
	}
	if offset, ok := parseTemporalOffset(zoneID); ok {
		return time.FixedZone(zoneID, offset), "", true
	}
	location, err := time.LoadLocation(zoneID)
	if err != nil {
		return nil, "", false
	}
	return location, zoneID, true
}

func parseTemporalOffset(value string) (int, bool) {
	if len(value) != 6 && len(value) != 9 {
		return 0, false
	}
	sign := 1
	if value[0] == '-' {
		sign = -1
	} else if value[0] != '+' {
		return 0, false
	}
	if value[3] != ':' || (len(value) == 9 && value[6] != ':') {
		return 0, false
	}
	hour, errHour := strconv.Atoi(value[1:3])
	minute, errMinute := strconv.Atoi(value[4:6])
	second := 0
	var errSecond error
	if len(value) == 9 {
		second, errSecond = strconv.Atoi(value[7:9])
	}
	if errHour != nil || errMinute != nil || errSecond != nil || minute > 59 || second > 59 {
		return 0, false
	}
	return sign * (hour*3600 + minute*60 + second), true
}

func temporalFieldInt(fields map[string]interface{}, name string, fallback int64) int64 {
	if value, exists := fields[name]; exists {
		if integer, ok := temporalInt(value); ok {
			return integer
		}
	}
	return fallback
}

func temporalOptionalInt(fields map[string]interface{}, name string) (int64, bool) {
	value, exists := fields[name]
	if !exists {
		return 0, false
	}
	integer, ok := temporalInt(value)
	return integer, ok
}

func temporalInt(value interface{}) (int64, bool) {
	switch number := value.(type) {
	case int64:
		return number, true
	case int:
		return int64(number), true
	case float64:
		return int64(number), number == math.Trunc(number)
	default:
		return 0, false
	}
}

func temporalFloat(fields map[string]interface{}, name string) float64 {
	switch number := fields[name].(type) {
	case int64:
		return float64(number)
	case int:
		return float64(number)
	case float64:
		return number
	default:
		return 0
	}
}

func buildDurationFromFields(fields map[string]interface{}) *CypherDuration {
	months := temporalFloat(fields, "years")*12 + temporalFloat(fields, "quarters")*3 + temporalFloat(fields, "months")
	wholeMonths, fractionalMonths := math.Modf(months)
	days := temporalFloat(fields, "weeks")*7 + temporalFloat(fields, "days") + fractionalMonths*30.436875
	wholeDays, fractionalDays := math.Modf(days)
	seconds := fractionalDays*86400 + temporalFloat(fields, "hours")*3600 + temporalFloat(fields, "minutes")*60 + temporalFloat(fields, "seconds")
	seconds += temporalFloat(fields, "milliseconds") / 1_000
	seconds += temporalFloat(fields, "microseconds") / 1_000_000
	seconds += temporalFloat(fields, "nanoseconds") / 1_000_000_000
	wholeSeconds, fraction := math.Modf(seconds)
	nanos := int64(math.Round(fraction * 1_000_000_000))
	secondsInt := int64(wholeSeconds)
	if nanos == 1_000_000_000 {
		secondsInt++
		nanos = 0
	}
	extraDays := secondsInt / 86_400
	secondsInt %= 86_400
	hours := secondsInt / 3_600
	secondsInt %= 3_600
	minutes := secondsInt / 60
	secondsInt %= 60
	totalMonths := int64(wholeMonths)
	return &CypherDuration{
		Years:   totalMonths / 12,
		Months:  totalMonths % 12,
		Days:    int64(wholeDays) + extraDays,
		Hours:   hours,
		Minutes: minutes,
		Seconds: secondsInt,
		Nanos:   nanos,
	}
}

func formatTemporalClock(value time.Time, zoned bool, zoneID string) string {
	format := "15:04"
	if value.Second() != 0 || value.Nanosecond() != 0 {
		format = "15:04:05"
		if value.Nanosecond() != 0 {
			format += ".999999999"
		}
	}
	result := value.Format(format)
	if zoned {
		_, offset := value.Zone()
		result += formatTemporalOffset(offset)
	}
	if zoneID != "" {
		result += "[" + zoneID + "]"
	}
	return result
}

func formatTemporalDateTime(value time.Time, zoned bool, zoneID string) string {
	result := value.Format("2006-01-02T") + formatTemporalClock(value, zoned, "")
	if zoneID != "" {
		result += "[" + zoneID + "]"
	}
	return result
}

func formatTemporalOffset(offset int) string {
	if offset == 0 {
		return "Z"
	}
	sign := '+'
	if offset < 0 {
		sign = '-'
		offset = -offset
	}
	hours := offset / 3600
	minutes := (offset % 3600) / 60
	seconds := offset % 60
	if seconds == 0 {
		return fmt.Sprintf("%c%02d:%02d", sign, hours, minutes)
	}
	return fmt.Sprintf("%c%02d:%02d:%02d", sign, hours, minutes, seconds)
}
