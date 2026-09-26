package cypher

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/vmihailenco/msgpack/v5"
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

func (CypherDate) TemporalPropertyKind() string          { return "date" }
func (CypherLocalTime) TemporalPropertyKind() string     { return "local-time" }
func (CypherTime) TemporalPropertyKind() string          { return "time" }
func (CypherLocalDateTime) TemporalPropertyKind() string { return "local-date-time" }
func (CypherDateTime) TemporalPropertyKind() string      { return "zoned-date-time" }

func (v CypherDate) TemporalTime() time.Time          { return v.Time }
func (v CypherLocalTime) TemporalTime() time.Time     { return v.Time }
func (v CypherTime) TemporalTime() time.Time          { return v.Time }
func (v CypherLocalDateTime) TemporalTime() time.Time { return v.Time }
func (v CypherDateTime) TemporalTime() time.Time      { return v.Time }

func init() {
	registerTemporalTimeExtension(41, CypherDate{}, func(value CypherDate) time.Time { return value.Time }, func(value time.Time) CypherDate { return CypherDate{Time: value} })
	registerTemporalTimeExtension(42, CypherLocalTime{}, func(value CypherLocalTime) time.Time { return value.Time }, func(value time.Time) CypherLocalTime { return CypherLocalTime{Time: value} })
	registerTemporalTimeExtension(43, CypherTime{}, func(value CypherTime) time.Time { return value.Time }, func(value time.Time) CypherTime { return CypherTime{Time: value} })
	registerTemporalTimeExtension(44, CypherLocalDateTime{}, func(value CypherLocalDateTime) time.Time { return value.Time }, func(value time.Time) CypherLocalDateTime { return CypherLocalDateTime{Time: value} })
	msgpack.RegisterExtEncoder(46, CypherDateTime{}, encodeCypherDateTime)
	msgpack.RegisterExtDecoder(46, CypherDateTime{}, decodeCypherDateTime)
	msgpack.RegisterExt(45, (*CypherDuration)(nil))
}

func registerTemporalTimeExtension[T any](id int8, prototype T, extract func(T) time.Time, construct func(time.Time) T) {
	msgpack.RegisterExtEncoder(id, prototype, func(_ *msgpack.Encoder, value reflect.Value) ([]byte, error) {
		return marshalTemporalTime(extract(value.Interface().(T))), nil
	})
	msgpack.RegisterExtDecoder(id, prototype, func(decoder *msgpack.Decoder, value reflect.Value, length int) error {
		data := make([]byte, length)
		if err := decoder.ReadFull(data); err != nil {
			return err
		}
		decoded, err := unmarshalTemporalTime(data)
		if err != nil {
			return err
		}
		value.Set(reflect.ValueOf(construct(decoded)))
		return nil
	})
}

func (v *CypherDate) MarshalMsgpack() ([]byte, error)      { return marshalTemporalTime(v.Time), nil }
func (v *CypherLocalTime) MarshalMsgpack() ([]byte, error) { return marshalTemporalTime(v.Time), nil }
func (v *CypherTime) MarshalMsgpack() ([]byte, error)      { return marshalTemporalTime(v.Time), nil }
func (v *CypherLocalDateTime) MarshalMsgpack() ([]byte, error) {
	return marshalTemporalTime(v.Time), nil
}

func (v *CypherDate) UnmarshalMsgpack(data []byte) error {
	value, err := unmarshalTemporalTime(data)
	v.Time = value
	return err
}
func (v *CypherLocalTime) UnmarshalMsgpack(data []byte) error {
	value, err := unmarshalTemporalTime(data)
	v.Time = value
	return err
}
func (v *CypherTime) UnmarshalMsgpack(data []byte) error {
	value, err := unmarshalTemporalTime(data)
	v.Time = value
	return err
}
func (v *CypherLocalDateTime) UnmarshalMsgpack(data []byte) error {
	value, err := unmarshalTemporalTime(data)
	v.Time = value
	return err
}

func marshalTemporalTime(value time.Time) []byte {
	data := make([]byte, 16)
	binary.BigEndian.PutUint64(data[0:8], uint64(value.Unix()))
	binary.BigEndian.PutUint32(data[8:12], uint32(value.Nanosecond()))
	_, offset := value.Zone()
	binary.BigEndian.PutUint32(data[12:16], uint32(int32(offset)))
	return data
}

func unmarshalTemporalTime(data []byte) (time.Time, error) {
	if len(data) != 16 {
		return time.Time{}, fmt.Errorf("invalid temporal property payload length %d", len(data))
	}
	seconds := int64(binary.BigEndian.Uint64(data[0:8]))
	nanos := int64(binary.BigEndian.Uint32(data[8:12]))
	offset := int(int32(binary.BigEndian.Uint32(data[12:16])))
	return time.Unix(seconds, nanos).In(time.FixedZone("", offset)), nil
}

func encodeCypherDateTime(_ *msgpack.Encoder, value reflect.Value) ([]byte, error) {
	timestamp := value.Interface().(CypherDateTime)
	zoneID := timestamp.ZoneID
	data := make([]byte, 16+len(zoneID))
	binary.BigEndian.PutUint64(data[0:8], uint64(timestamp.Time.Unix()))
	binary.BigEndian.PutUint32(data[8:12], uint32(timestamp.Time.Nanosecond()))
	_, offset := timestamp.Time.Zone()
	binary.BigEndian.PutUint32(data[12:16], uint32(int32(offset)))
	copy(data[16:], zoneID)
	return data, nil
}

func decodeCypherDateTime(decoder *msgpack.Decoder, value reflect.Value, length int) error {
	if length < 16 {
		return fmt.Errorf("invalid CypherDateTime payload length %d", length)
	}
	data := make([]byte, length)
	if err := decoder.ReadFull(data); err != nil {
		return err
	}
	seconds := int64(binary.BigEndian.Uint64(data[0:8]))
	nanos := int64(binary.BigEndian.Uint32(data[8:12]))
	offset := int(int32(binary.BigEndian.Uint32(data[12:16])))
	zoneID := string(data[16:])
	location := time.FixedZone("", offset)
	if zoneID != "" {
		loaded, ok := loadPinnedTemporalLocation(zoneID)
		if !ok {
			return fmt.Errorf("unknown CypherDateTime zone %q", zoneID)
		}
		location = loaded
	}
	value.Set(reflect.ValueOf(CypherDateTime{
		Time:   time.Unix(seconds, nanos).In(location),
		ZoneID: zoneID,
	}))
	return nil
}

func (e *StorageExecutor) evaluateTemporalConstructor(ctxEval func(string) interface{}, expression string) (interface{}, bool) {
	name, argument, ok := parseFunctionCallWS(expression)
	if !ok {
		return nil, false
	}
	kind := strings.ToLower(name)
	if kind == "duration.between" || kind == "duration.inmonths" || kind == "duration.indays" || kind == "duration.inseconds" {
		arguments := e.splitFunctionArgs(argument)
		if len(arguments) != 2 {
			return nil, true
		}
		leftExpression := strings.TrimSpace(arguments[0])
		rightExpression := strings.TrimSpace(arguments[1])
		if strings.EqualFold(leftExpression, "null") || strings.EqualFold(rightExpression, "null") {
			return nil, true
		}
		if leftExpression == rightExpression {
			return &CypherDuration{}, true
		}
		left := ctxEval(leftExpression)
		right := ctxEval(rightExpression)
		if left == nil || right == nil {
			return nil, true
		}
		return durationBetweenTemporalValues(strings.TrimPrefix(kind, "duration."), left, right)
	}
	if strings.HasSuffix(kind, ".truncate") {
		kind = strings.TrimSuffix(kind, ".truncate")
		arguments := e.splitFunctionArgs(argument)
		if len(arguments) < 2 || len(arguments) > 3 {
			return nil, true
		}
		unit, unitOK := ctxEval(strings.TrimSpace(arguments[0])).(string)
		if !unitOK {
			return nil, true
		}
		value := ctxEval(strings.TrimSpace(arguments[1]))
		fields := map[string]interface{}{}
		if len(arguments) == 3 {
			var fieldsOK bool
			fields, fieldsOK = toStringAnyMap(ctxEval(strings.TrimSpace(arguments[2])))
			if !fieldsOK {
				return nil, true
			}
		}
		return truncateTemporalValue(kind, unit, value, fields)
	}
	if dot := strings.IndexByte(kind, '.'); dot > 0 {
		suffix := kind[dot+1:]
		if suffix == "transaction" || suffix == "statement" || suffix == "realtime" {
			kind = kind[:dot]
		}
	}
	switch kind {
	case "date", "localtime", "time", "localdatetime", "datetime", "duration":
		argument = strings.TrimSpace(argument)
		if argument == "" {
			now := time.Now()
			switch kind {
			case "date":
				return CypherDate{Time: time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)}, true
			case "localtime":
				return CypherLocalTime{Time: now}, true
			case "time":
				return CypherTime{Time: now}, true
			case "localdatetime":
				return CypherLocalDateTime{Time: now}, true
			case "datetime":
				return CypherDateTime{Time: now}, true
			default:
				return nil, true
			}
		}
		value := ctxEval(argument)
		if value == nil {
			return nil, true
		}
		if text, isString := value.(string); isString {
			parsed, valid := parseTemporalText(kind, text)
			if !valid {
				return nil, true
			}
			return parsed, true
		}
		if converted, valid := projectTemporalValue(kind, value); valid {
			return converted, true
		}
		fields, isMap := toStringAnyMap(value)
		if !isMap {
			return nil, true
		}
		built, _ := buildTemporalValue(kind, fields)
		return built, true
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
		return CypherDateTime{Time: time.Unix(seconds, nanos).UTC()}, true
	case "datetime.fromepochmillis":
		millis, valid := temporalInt(ctxEval(strings.TrimSpace(argument)))
		if !valid {
			return nil, true
		}
		return CypherDateTime{Time: time.UnixMilli(millis).UTC()}, true
	default:
		return nil, false
	}
}

func buildTemporalValue(kind string, fields map[string]interface{}) (interface{}, bool) {
	if kind == "duration" {
		return buildDurationFromFields(fields), true
	}
	dateFields := fields
	if source, exists := fields["datetime"]; exists {
		dateFields = cloneTemporalFields(fields)
		if _, hasDate := dateFields["date"]; !hasDate {
			dateFields["date"] = source
		}
	}
	date, valid := buildDateFromFields(dateFields)
	if !valid && kind != "localtime" && kind != "time" {
		return nil, true
	}
	if kind == "date" {
		return CypherDate{Time: date}, true
	}
	baseTime, baseZoned, hasBaseTime := temporalBaseTime(fields)
	hour, minute, second, nanosecond := int64(0), int64(0), int64(0), int64(0)
	if hasBaseTime {
		hour, minute, second, nanosecond = int64(baseTime.Hour()), int64(baseTime.Minute()), int64(baseTime.Second()), int64(baseTime.Nanosecond())
	}
	hour = temporalFieldInt(fields, "hour", hour)
	minute = temporalFieldInt(fields, "minute", minute)
	second = temporalFieldInt(fields, "second", second)
	if hasAnyTemporalField(fields, "nanosecond", "microsecond", "millisecond") {
		nanosecond = temporalFieldInt(fields, "nanosecond", 0) +
			temporalFieldInt(fields, "microsecond", 0)*1_000 +
			temporalFieldInt(fields, "millisecond", 0)*1_000_000
	}
	if kind == "localtime" || kind == "time" {
		date = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	zone, zoneID, zoneOK := temporalLocationForProjection(fields, kind == "time" || kind == "datetime", baseTime, baseZoned)
	if !zoneOK {
		return nil, true
	}
	if zoneID != "" {
		zone, zoneOK = loadTemporalLocationAt(zoneID, date)
		if !zoneOK {
			return nil, true
		}
	}
	if hasBaseTime && baseZoned && hasAnyTemporalField(fields, "timezone") && (kind == "time" || kind == "datetime") {
		source := time.Date(date.Year(), date.Month(), date.Day(), baseTime.Hour(), baseTime.Minute(), baseTime.Second(), baseTime.Nanosecond(), baseTime.Location())
		converted := source.In(zone)
		date = converted
		hour, minute, second, nanosecond = int64(converted.Hour()), int64(converted.Minute()), int64(converted.Second()), int64(converted.Nanosecond())
		hour = temporalFieldInt(fields, "hour", hour)
		minute = temporalFieldInt(fields, "minute", minute)
		second = temporalFieldInt(fields, "second", second)
		if hasAnyTemporalField(fields, "nanosecond", "microsecond", "millisecond") {
			nanosecond = temporalFieldInt(fields, "nanosecond", 0) + temporalFieldInt(fields, "microsecond", 0)*1_000 + temporalFieldInt(fields, "millisecond", 0)*1_000_000
		}
	}
	value := time.Date(date.Year(), date.Month(), date.Day(), int(hour), int(minute), int(second), int(nanosecond), zone)
	value = normalizeTemporalNamedZone(value)
	switch kind {
	case "localtime":
		return CypherLocalTime{Time: value}, true
	case "time":
		return CypherTime{Time: value}, true
	case "localdatetime":
		return CypherLocalDateTime{Time: value}, true
	case "datetime":
		return CypherDateTime{Time: value, ZoneID: zoneID}, true
	default:
		return value, true
	}
}

func projectTemporalValue(kind string, value interface{}) (interface{}, bool) {
	date, hasDate := temporalBaseDate(value)
	clock, zoned, hasTime := temporalTimeParts(value)
	switch kind {
	case "date":
		if hasDate {
			return CypherDate{Time: time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)}, true
		}
	case "localtime":
		if hasTime {
			return CypherLocalTime{Time: time.Date(1970, 1, 1, clock.Hour(), clock.Minute(), clock.Second(), clock.Nanosecond(), time.UTC)}, true
		}
	case "time":
		if hasTime {
			location := time.UTC
			if zoned {
				location = clock.Location()
			}
			return CypherTime{Time: time.Date(1970, 1, 1, clock.Hour(), clock.Minute(), clock.Second(), clock.Nanosecond(), location)}, true
		}
	case "localdatetime":
		if hasDate && hasTime {
			return CypherLocalDateTime{Time: time.Date(date.Year(), date.Month(), date.Day(), clock.Hour(), clock.Minute(), clock.Second(), clock.Nanosecond(), time.UTC)}, true
		}
	case "datetime":
		if hasDate && hasTime {
			location := time.UTC
			zoneID := ""
			if zoned {
				location = clock.Location()
			}
			if typed, ok := value.(CypherDateTime); ok {
				zoneID = typed.ZoneID
			}
			return CypherDateTime{Time: time.Date(date.Year(), date.Month(), date.Day(), clock.Hour(), clock.Minute(), clock.Second(), clock.Nanosecond(), location), ZoneID: zoneID}, true
		}
	}
	return nil, false
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
		day := int64(1)
		if hasBase {
			quarterStart := time.Date(base.Year(), time.Month((int(base.Month())-1)/3*3+1), 1, 0, 0, 0, 0, time.UTC)
			day = int64(base.Sub(quarterStart)/(24*time.Hour)) + 1
		}
		day = temporalFieldInt(fields, "dayOfQuarter", day)
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

func temporalBaseTime(fields map[string]interface{}) (time.Time, bool, bool) {
	if value, exists := fields["time"]; exists {
		return temporalTimeParts(value)
	}
	if value, exists := fields["datetime"]; exists {
		return temporalTimeParts(value)
	}
	return time.Time{}, false, false
}

func temporalTimeParts(value interface{}) (time.Time, bool, bool) {
	switch value := value.(type) {
	case CypherLocalTime:
		return value.Time, false, true
	case CypherTime:
		return value.Time, true, true
	case CypherLocalDateTime:
		return value.Time, false, true
	case CypherDateTime:
		return value.Time, true, true
	case time.Time:
		return value, true, true
	default:
		return time.Time{}, false, false
	}
}

func cloneTemporalFields(fields map[string]interface{}) map[string]interface{} {
	clone := make(map[string]interface{}, len(fields)+1)
	for key, value := range fields {
		clone[key] = value
	}
	return clone
}

func hasAnyTemporalField(fields map[string]interface{}, names ...string) bool {
	for _, name := range names {
		if _, exists := fields[name]; exists {
			return true
		}
	}
	return false
}

func temporalLocationForProjection(fields map[string]interface{}, zoned bool, base time.Time, baseZoned bool) (*time.Location, string, bool) {
	if !zoned {
		return time.UTC, "", true
	}
	if _, explicit := fields["timezone"]; explicit {
		return temporalLocation(fields, true)
	}
	if baseZoned {
		zoneID := ""
		if strings.Contains(base.Location().String(), "/") {
			zoneID = base.Location().String()
		}
		return base.Location(), zoneID, true
	}
	return time.UTC, "", true
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
	if len(value) == 5 || len(value) == 7 {
		sign := 1
		if value[0] == '-' {
			sign = -1
		} else if value[0] != '+' {
			return 0, false
		}
		hour, errHour := strconv.Atoi(value[1:3])
		minute, errMinute := strconv.Atoi(value[3:5])
		second := 0
		var errSecond error
		if len(value) == 7 {
			second, errSecond = strconv.Atoi(value[5:7])
		}
		if errHour != nil || errMinute != nil || errSecond != nil || minute > 59 || second > 59 {
			return 0, false
		}
		return sign * (hour*3600 + minute*60 + second), true
	}
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
	} else if nanos == -1_000_000_000 {
		secondsInt--
		nanos = 0
	}
	hours := secondsInt / 3_600
	secondsInt %= 3_600
	minutes := secondsInt / 60
	secondsInt %= 60
	totalMonths := int64(wholeMonths)
	return &CypherDuration{
		Years:   totalMonths / 12,
		Months:  totalMonths % 12,
		Days:    int64(wholeDays),
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

// temporalConstructorTypes names the value each temporal constructor builds,
// as Neo4j's errors name it.
var temporalConstructorTypes = map[string]string{
	"date":          "Date",
	"datetime":      "DateTime",
	"localdatetime": "LocalDateTime",
	"time":          "Time",
	"localtime":     "LocalTime",
	"duration":      "Duration",
}

// isTemporalConstructor reports date, datetime, localdatetime, time,
// localtime and duration.
func isTemporalConstructor(function string) bool {
	_, ok := temporalConstructorTypes[strings.ToLower(function)]
	return ok
}

// temporalConstructorError is the statement error of a temporal constructor
// that built no value from a non-null input, as Neo4j 5.26 reports it: a
// SyntaxError for text it can't parse, ExecutionFailed for a map with
// invalid fields, and ProcedureCallFailed for a value of another type. It is
// nil for a null input, which gives null.
func temporalConstructorError(function string, input interface{}) error {
	typeName := temporalConstructorTypes[strings.ToLower(function)]
	switch value := input.(type) {
	case nil:
		return nil
	case string:
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidArgument",
			fmt.Sprintf("Text cannot be parsed to a %s\n%q\n ^", typeName, value))
	case map[string]interface{}:
		return newSemanticError("Neo.DatabaseError.Statement.ExecutionFailed", "InvalidArgument",
			fmt.Sprintf("invalid %s value: %v", typeName, value))
	case bool:
		return temporalCallSignatureError(typeName, fmt.Sprintf("Boolean('%t')", value))
	case float32, float64:
		return temporalCallSignatureError(typeName, fmt.Sprintf("Double(%v)", value))
	}
	if integer, ok := cypherIntegerValue(input); ok {
		return temporalCallSignatureError(typeName, fmt.Sprintf("Long(%d)", integer))
	}
	return temporalCallSignatureError(typeName, fmt.Sprintf("%v", input))
}

// temporalCallSignatureError is Neo4j's error for a temporal constructor
// called with a value of a type it doesn't take.
func temporalCallSignatureError(typeName, provided string) error {
	return newSemanticError("Neo.ClientError.Procedure.ProcedureCallFailed", "InvalidArgument",
		fmt.Sprintf("Invalid call signature for %sFunction: Provided input was [%s]", typeName, provided))
}
