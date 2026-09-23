package cypher

import (
	"math"
	"strconv"
	"strings"
	"time"
)

// parseTemporalText parses the ISO forms accepted by Cypher temporal
// constructors. It scans slices of the input directly so the common numeric
// forms do not require regular-expression or token allocations.
func parseTemporalText(kind, text string) (interface{}, bool) {
	switch kind {
	case "date":
		value, ok := parseCypherDateText(text)
		return CypherDate{Time: value}, ok
	case "localtime":
		value, ok := parseCypherClockText(text)
		return CypherLocalTime{Time: value}, ok
	case "time":
		value, ok := parseCypherTimeText(text)
		return CypherTime{Time: value}, ok
	case "localdatetime":
		value, ok := parseCypherLocalDateTimeText(text)
		return CypherLocalDateTime{Time: value}, ok
	case "datetime":
		value, _, ok := parseCypherDateTimeText(text)
		return value, ok
	case "duration":
		value, ok := parseCypherDurationText(text)
		return value, ok
	default:
		return nil, false
	}
}

func parseCypherDateText(text string) (time.Time, bool) {
	if len(text) > 0 && (text[0] == '+' || text[0] == '-') {
		return parseExpandedCypherDateText(text)
	}
	if len(text) < 4 {
		return time.Time{}, false
	}
	year, ok := parseFixedDecimal(text, 0, 4)
	if !ok {
		return time.Time{}, false
	}
	if len(text) == 4 {
		return checkedDate(year, 1, 1)
	}
	if (len(text) == 8 && text[4] == '-') || (len(text) == 7 && text[4] != '-') {
		start := 5
		if text[4] != '-' {
			start = 4
		}
		ordinal, valid := parseFixedDecimal(text, start, 3)
		if valid {
			value := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, ordinal-1)
			return value, ordinal >= 1 && value.Year() == year
		}
	}
	if text[4] == '-' && len(text) >= 8 && text[5] == 'W' || text[4] == 'W' {
		weekStart := 6
		if text[4] == 'W' {
			weekStart = 5
		}
		week, valid := parseFixedDecimal(text, weekStart, 2)
		if !valid {
			return time.Time{}, false
		}
		day := 1
		next := weekStart + 2
		if next < len(text) {
			if text[next] == '-' {
				next++
			}
			if next+1 != len(text) {
				return time.Time{}, false
			}
			day, valid = parseFixedDecimal(text, next, 1)
			if !valid {
				return time.Time{}, false
			}
		}
		return isoWeekDate(year, week, day)
	}
	monthStart := 4
	if text[4] == '-' {
		monthStart++
	}
	month, valid := parseFixedDecimal(text, monthStart, 2)
	if !valid {
		return time.Time{}, false
	}
	next := monthStart + 2
	if next == len(text) {
		return checkedDate(year, month, 1)
	}
	if text[next] == '-' {
		next++
	}
	day, valid := parseFixedDecimal(text, next, 2)
	if !valid || next+2 != len(text) {
		return time.Time{}, false
	}
	return checkedDate(year, month, day)
}

func parseExpandedCypherDateText(text string) (time.Time, bool) {
	separator := 1
	for separator < len(text) && text[separator] >= '0' && text[separator] <= '9' {
		separator++
	}
	if separator < 5 || separator+6 != len(text) || text[separator] != '-' || text[separator+3] != '-' {
		return time.Time{}, false
	}
	year, err := strconv.Atoi(text[:separator])
	month, monthOK := parseFixedDecimal(text, separator+1, 2)
	day, dayOK := parseFixedDecimal(text, separator+4, 2)
	if err != nil || !monthOK || !dayOK {
		return time.Time{}, false
	}
	return checkedDate(year, month, day)
}

func checkedDate(year, month, day int) (time.Time, bool) {
	value := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	return value, value.Year() == year && int(value.Month()) == month && value.Day() == day
}

func isoWeekDate(year, week, day int) (time.Time, bool) {
	if week < 1 || week > 53 || day < 1 || day > 7 {
		return time.Time{}, false
	}
	jan4 := time.Date(year, 1, 4, 0, 0, 0, 0, time.UTC)
	monday := jan4.AddDate(0, 0, -(int(jan4.Weekday())+6)%7)
	value := monday.AddDate(0, 0, (week-1)*7+day-1)
	actualYear, actualWeek := value.ISOWeek()
	return value, actualYear == year && actualWeek == week
}

func parseCypherClockText(text string) (time.Time, bool) {
	hour, minute, second, nanos, ok := parseClockComponents(text)
	if !ok {
		return time.Time{}, false
	}
	return time.Date(1970, 1, 1, hour, minute, second, nanos, time.UTC), true
}

func parseCypherTimeText(text string) (time.Time, bool) {
	clock, offset, ok := splitClockOffset(text)
	if !ok {
		hour, minute, second, nanos, clockOK := parseClockComponents(text)
		if !clockOK {
			return time.Time{}, false
		}
		return time.Date(1970, 1, 1, hour, minute, second, nanos, time.UTC), true
	}
	hour, minute, second, nanos, ok := parseClockComponents(clock)
	if !ok {
		return time.Time{}, false
	}
	return time.Date(1970, 1, 1, hour, minute, second, nanos, time.FixedZone("", offset)), true
}

func parseCypherLocalDateTimeText(text string) (time.Time, bool) {
	separator := strings.IndexByte(text, 'T')
	if separator < 0 {
		date, ok := parseCypherDateText(text)
		if !ok {
			return time.Time{}, false
		}
		return time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC), true
	}
	date, ok := parseCypherDateText(text[:separator])
	if !ok {
		return time.Time{}, false
	}
	hour, minute, second, nanos, ok := parseClockComponents(text[separator+1:])
	if !ok {
		return time.Time{}, false
	}
	return time.Date(date.Year(), date.Month(), date.Day(), hour, minute, second, nanos, time.UTC), true
}

func parseCypherDateTimeText(text string) (time.Time, string, bool) {
	zoneID := ""
	if len(text) > 0 && text[len(text)-1] == ']' {
		open := strings.LastIndexByte(text, '[')
		if open < 0 || open == len(text)-1 {
			return time.Time{}, "", false
		}
		zoneID = text[open+1 : len(text)-1]
		text = text[:open]
	}
	separator := strings.IndexByte(text, 'T')
	if separator < 0 {
		return time.Time{}, "", false
	}
	date, ok := parseCypherDateText(text[:separator])
	if !ok {
		return time.Time{}, "", false
	}
	clockText := text[separator+1:]
	clock, offset, hasOffset := splitClockOffset(clockText)
	if zoneID != "" && !hasOffset {
		clock = clockText
	}
	hour, minute, second, nanos, ok := parseClockComponents(clock)
	if !ok {
		return time.Time{}, "", false
	}
	location := time.FixedZone("", offset)
	if zoneID != "" {
		location, ok = loadTemporalLocation(zoneID)
		if !ok {
			return time.Time{}, "", false
		}
	}
	return time.Date(date.Year(), date.Month(), date.Day(), hour, minute, second, nanos, location), zoneID, true
}

func loadTemporalLocation(zoneID string) (*time.Location, bool) {
	location, err := time.LoadLocation(zoneID)
	return location, err == nil
}

func splitClockOffset(text string) (string, int, bool) {
	if len(text) > 0 && text[len(text)-1] == 'Z' {
		return text[:len(text)-1], 0, true
	}
	for index := 1; index < len(text); index++ {
		if text[index] != '+' && text[index] != '-' {
			continue
		}
		offset, ok := parseFlexibleTemporalOffset(text[index:])
		return text[:index], offset, ok
	}
	return text, 0, false
}

func parseFlexibleTemporalOffset(text string) (int, bool) {
	if len(text) < 3 || (text[0] != '+' && text[0] != '-') {
		return 0, false
	}
	sign := 1
	if text[0] == '-' {
		sign = -1
	}
	hour, ok := parseFixedDecimal(text, 1, 2)
	if !ok || hour > 18 {
		return 0, false
	}
	minute, second := 0, 0
	switch len(text) {
	case 3:
	case 5:
		minute, ok = parseFixedDecimal(text, 3, 2)
	case 6:
		ok = text[3] == ':'
		if ok {
			minute, ok = parseFixedDecimal(text, 4, 2)
		}
	case 7:
		minute, ok = parseFixedDecimal(text, 3, 2)
		if ok {
			second, ok = parseFixedDecimal(text, 5, 2)
		}
	case 9:
		ok = text[3] == ':' && text[6] == ':'
		if ok {
			minute, ok = parseFixedDecimal(text, 4, 2)
		}
		if ok {
			second, ok = parseFixedDecimal(text, 7, 2)
		}
	default:
		return 0, false
	}
	if !ok || minute > 59 || second > 59 || (hour == 18 && (minute != 0 || second != 0)) {
		return 0, false
	}
	return sign * (hour*3600 + minute*60 + second), true
}

func parseClockComponents(text string) (hour, minute, second, nanos int, ok bool) {
	fraction := ""
	if dot := strings.IndexByte(text, '.'); dot >= 0 {
		fraction = text[dot+1:]
		text = text[:dot]
		if len(fraction) == 0 || len(fraction) > 9 {
			return 0, 0, 0, 0, false
		}
	}
	colon := strings.IndexByte(text, ':') >= 0
	switch {
	case colon && len(text) == 2:
		hour, ok = parseFixedDecimal(text, 0, 2)
	case colon && len(text) == 5 && text[2] == ':':
		hour, ok = parseFixedDecimal(text, 0, 2)
		if ok {
			minute, ok = parseFixedDecimal(text, 3, 2)
		}
	case colon && len(text) == 8 && text[2] == ':' && text[5] == ':':
		hour, ok = parseFixedDecimal(text, 0, 2)
		if ok {
			minute, ok = parseFixedDecimal(text, 3, 2)
		}
		if ok {
			second, ok = parseFixedDecimal(text, 6, 2)
		}
	case !colon && len(text) == 2:
		hour, ok = parseFixedDecimal(text, 0, 2)
	case !colon && len(text) == 4:
		hour, ok = parseFixedDecimal(text, 0, 2)
		if ok {
			minute, ok = parseFixedDecimal(text, 2, 2)
		}
	case !colon && len(text) == 6:
		hour, ok = parseFixedDecimal(text, 0, 2)
		if ok {
			minute, ok = parseFixedDecimal(text, 2, 2)
		}
		if ok {
			second, ok = parseFixedDecimal(text, 4, 2)
		}
	default:
		return 0, 0, 0, 0, false
	}
	if !ok || hour > 23 || minute > 59 || second > 59 {
		return 0, 0, 0, 0, false
	}
	for index := 0; index < len(fraction); index++ {
		if fraction[index] < '0' || fraction[index] > '9' {
			return 0, 0, 0, 0, false
		}
		nanos = nanos*10 + int(fraction[index]-'0')
	}
	for index := len(fraction); index < 9; index++ {
		nanos *= 10
	}
	return hour, minute, second, nanos, true
}

func parseFixedDecimal(text string, start, width int) (int, bool) {
	if start < 0 || width <= 0 || start+width > len(text) {
		return 0, false
	}
	value := 0
	for index := start; index < start+width; index++ {
		if text[index] < '0' || text[index] > '9' {
			return 0, false
		}
		value = value*10 + int(text[index]-'0')
	}
	return value, true
}

func parseCypherDurationText(text string) (*CypherDuration, bool) {
	if len(text) < 2 || text[0] != 'P' {
		return nil, false
	}
	if len(text) >= 11 && text[5] == '-' && text[8] == '-' {
		return parseAlternativeDurationText(text)
	}
	var years, months, weeks, days, hours, minutes, seconds float64
	inTime := false
	for index := 1; index < len(text); {
		if text[index] == 'T' {
			if inTime {
				return nil, false
			}
			inTime = true
			index++
			continue
		}
		start := index
		if text[index] == '+' || text[index] == '-' {
			index++
		}
		digitsStart := index
		for index < len(text) && ((text[index] >= '0' && text[index] <= '9') || text[index] == '.') {
			index++
		}
		if digitsStart == index || index == len(text) {
			return nil, false
		}
		number, err := strconv.ParseFloat(text[start:index], 64)
		if err != nil || math.IsNaN(number) || math.IsInf(number, 0) {
			return nil, false
		}
		designator := text[index]
		index++
		switch designator {
		case 'Y':
			if inTime {
				return nil, false
			}
			years = number
		case 'M':
			if inTime {
				minutes = number
			} else {
				months = number
			}
		case 'W':
			if inTime {
				return nil, false
			}
			weeks = number
		case 'D':
			if inTime {
				return nil, false
			}
			days = number
		case 'H':
			if !inTime {
				return nil, false
			}
			hours = number
		case 'S':
			if !inTime {
				return nil, false
			}
			seconds = number
		default:
			return nil, false
		}
	}
	return normalizeDuration(years, months, weeks, days, hours, minutes, seconds), true
}

func parseAlternativeDurationText(text string) (*CypherDuration, bool) {
	separator := strings.IndexByte(text, 'T')
	if separator < 0 || separator+1 >= len(text) {
		return nil, false
	}
	years, errYear := strconv.ParseFloat(text[1:5], 64)
	months, errMonth := strconv.ParseFloat(text[6:8], 64)
	days, errDay := strconv.ParseFloat(text[9:separator], 64)
	clock := text[separator+1:]
	if len(clock) < 8 || clock[2] != ':' || clock[5] != ':' {
		return nil, false
	}
	hours, errHour := strconv.ParseFloat(clock[:2], 64)
	minutes, errMinute := strconv.ParseFloat(clock[3:5], 64)
	seconds, errSecond := strconv.ParseFloat(clock[6:], 64)
	if errYear != nil || errMonth != nil || errDay != nil || errHour != nil || errMinute != nil || errSecond != nil {
		return nil, false
	}
	return normalizeDuration(years, months, 0, days, hours, minutes, seconds), true
}

func normalizeDuration(years, months, weeks, days, hours, minutes, seconds float64) *CypherDuration {
	months += years * 12
	wholeMonths, fractionalMonths := math.Modf(months)
	days += weeks*7 + fractionalMonths*30.436875
	wholeDays, fractionalDays := math.Modf(days)
	seconds += minutes*60 + hours*3600 + fractionalDays*86400
	wholeSeconds, fractionalSeconds := math.Modf(seconds)
	nanos := int64(math.Round(fractionalSeconds * 1_000_000_000))
	secondsInt := int64(wholeSeconds)
	if nanos == 1_000_000_000 {
		secondsInt++
		nanos = 0
	} else if nanos == -1_000_000_000 {
		secondsInt--
		nanos = 0
	}
	hour := secondsInt / 3_600
	secondsInt %= 3_600
	minute := secondsInt / 60
	return &CypherDuration{
		Years:   int64(wholeMonths) / 12,
		Months:  int64(wholeMonths) % 12,
		Days:    int64(wholeDays),
		Hours:   hour,
		Minutes: minute,
		Seconds: secondsInt % 60,
		Nanos:   nanos,
	}
}
