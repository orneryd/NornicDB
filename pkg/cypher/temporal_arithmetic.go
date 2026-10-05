package cypher

import (
	"math"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
)

func addTemporalValues(left, right interface{}) (interface{}, bool) {
	if leftDuration, ok := asCypherDuration(left); ok {
		if rightDuration, ok := asCypherDuration(right); ok {
			return combineDurations(leftDuration, rightDuration, 1), true
		}
		if result, ok := applyDurationToTemporal(right, leftDuration, 1); ok {
			return result, true
		}
	}
	if rightDuration, ok := asCypherDuration(right); ok {
		if result, ok := applyDurationToTemporal(left, rightDuration, 1); ok {
			return result, true
		}
	}
	return nil, false
}

func subtractTemporalValues(left, right interface{}) (interface{}, bool) {
	rightDuration, ok := asCypherDuration(right)
	if !ok {
		return nil, false
	}
	if leftDuration, ok := asCypherDuration(left); ok {
		return combineDurations(leftDuration, rightDuration, -1), true
	}
	return applyDurationToTemporal(left, rightDuration, -1)
}

// scaleTemporalDuration is a duration value times factor (a division passes
// 1 / divisor). handled is false when value isn't a duration. A result that
// doesn't fit a duration has no value: durationScaleError reports it.
func scaleTemporalDuration(value interface{}, factor float64) (interface{}, bool) {
	duration, ok := asCypherDuration(value)
	if !ok || math.IsNaN(factor) || math.IsInf(factor, 0) {
		return nil, false
	}
	months, days, seconds, nanos := durationGroups(duration)
	scaledMonths, scaledDays, scaledSeconds := float64(months)*factor, float64(days)*factor, float64(seconds)*factor
	if !durationFits(scaledMonths, scaledDays, scaledSeconds) {
		return nil, true
	}
	return approximateDuration(scaledMonths, scaledDays, scaledSeconds, float64(nanos)*factor), true
}

// durationFits reports whether a duration of months, days and seconds has a
// length Neo4j can hold: each part, and their sum with a month as 2,629,746
// seconds and a day as 86,400, within a 64-bit count of seconds.
func durationFits(months, days, seconds float64) bool {
	const limit = float64(math.MaxInt64)
	monthSeconds, daySeconds := months*2_629_746, days*86_400
	for _, part := range [...]float64{monthSeconds, daySeconds, seconds, monthSeconds + daySeconds, monthSeconds + daySeconds + seconds} {
		if math.Abs(part) >= limit {
			return false
		}
	}
	return true
}

// durationScaleError is Neo4j's ArgumentError for a duration multiplied or
// divided by a number whose result doesn't fit a duration
// (duration('PT1H') * 9007199254740993), or nil.
func durationScaleError(op byte, left, right interface{}) error {
	if op != '*' && op != '/' {
		return nil
	}
	duration, durationOK := asCypherDuration(left)
	operand := right
	if !durationOK && op == '*' {
		duration, durationOK = asCypherDuration(right)
		operand = left
	}
	number, numberOK := toFloat64(operand)
	if !durationOK || !numberOK || math.IsNaN(number) || math.IsInf(number, 0) {
		return nil
	}
	factor := number
	if op == '/' {
		if number == 0 {
			return nil
		}
		factor = 1 / number
	}
	months, days, seconds, _ := durationGroups(duration)
	if durationFits(float64(months)*factor, float64(days)*factor, float64(seconds)*factor) {
		return nil
	}
	return localizedStatusError("Neo.ClientError.Statement.ArgumentError", "NumberOutOfRange",
		localization.CypherCoreDurationArithmeticOverflow(duration.String(), string(op), neo4jValueRepr(operand)))
}

func asCypherDuration(value interface{}) (*CypherDuration, bool) {
	switch typed := value.(type) {
	case *CypherDuration:
		return typed, typed != nil
	case CypherDuration:
		copy := typed
		return &copy, true
	default:
		return nil, false
	}
}

func durationGroups(duration *CypherDuration) (months, days, seconds, nanos int64) {
	return duration.Years*12 + duration.Months,
		duration.Days,
		duration.Hours*3_600 + duration.Minutes*60 + duration.Seconds,
		duration.Nanos
}

func combineDurations(left, right *CypherDuration, sign int64) *CypherDuration {
	leftMonths, leftDays, leftSeconds, leftNanos := durationGroups(left)
	rightMonths, rightDays, rightSeconds, rightNanos := durationGroups(right)
	return durationFromGroups(
		leftMonths+sign*rightMonths,
		leftDays+sign*rightDays,
		leftSeconds+sign*rightSeconds,
		leftNanos+sign*rightNanos,
	)
}

func approximateDuration(months, days, seconds, nanos float64) *CypherDuration {
	wholeMonths := math.Trunc(months)
	days += (months - wholeMonths) * 30.436875
	wholeDays := math.Trunc(days)
	seconds += (days - wholeDays) * 86_400
	wholeSeconds := math.Trunc(seconds)
	nanos += (seconds - wholeSeconds) * 1_000_000_000
	return durationFromGroups(int64(wholeMonths), int64(wholeDays), int64(wholeSeconds), int64(nanos))
}

func durationFromGroups(months, days, seconds, nanos int64) *CypherDuration {
	seconds += nanos / 1_000_000_000
	nanos %= 1_000_000_000
	if nanos < 0 {
		seconds--
		nanos += 1_000_000_000
	}
	return &CypherDuration{
		Years:   months / 12,
		Months:  months % 12,
		Days:    days,
		Hours:   seconds / 3_600,
		Minutes: (seconds % 3_600) / 60,
		Seconds: seconds % 60,
		Nanos:   nanos,
	}
}

func applyDurationToTemporal(value interface{}, duration *CypherDuration, sign int64) (interface{}, bool) {
	months, days, seconds, nanos := durationGroups(duration)
	months *= sign
	days *= sign
	seconds *= sign
	nanos *= sign
	clockDelta := time.Duration(seconds)*time.Second + time.Duration(nanos)*time.Nanosecond

	switch typed := value.(type) {
	case CypherDate:
		result := addCalendarDuration(typed.Time, months, days+seconds/86_400)
		return CypherDate{Time: time.Date(result.Year(), result.Month(), result.Day(), 0, 0, 0, 0, time.UTC)}, true
	case *CypherDate:
		if typed != nil {
			return applyDurationToTemporal(*typed, duration, sign)
		}
	case CypherLocalTime:
		return CypherLocalTime{Time: typed.Time.Add(clockDelta)}, true
	case *CypherLocalTime:
		if typed != nil {
			return applyDurationToTemporal(*typed, duration, sign)
		}
	case CypherTime:
		return CypherTime{Time: typed.Time.Add(clockDelta)}, true
	case *CypherTime:
		if typed != nil {
			return applyDurationToTemporal(*typed, duration, sign)
		}
	case CypherLocalDateTime:
		return CypherLocalDateTime{Time: addCalendarDuration(typed.Time, months, days).Add(clockDelta)}, true
	case *CypherLocalDateTime:
		if typed != nil {
			return applyDurationToTemporal(*typed, duration, sign)
		}
	case CypherDateTime:
		typed.Time = addCalendarDuration(typed.Time, months, days).Add(clockDelta)
		return typed, true
	case *CypherDateTime:
		if typed != nil {
			return applyDurationToTemporal(*typed, duration, sign)
		}
	case time.Time:
		return addCalendarDuration(typed, months, days).Add(clockDelta), true
	case *time.Time:
		if typed != nil {
			return addCalendarDuration(*typed, months, days).Add(clockDelta), true
		}
	}
	return nil, false
}

// addCalendarDuration follows java.time's plusMonths clamp behavior before
// applying the independent Cypher day group.
func addCalendarDuration(value time.Time, months, days int64) time.Time {
	monthIndex := int64(value.Year())*12 + int64(value.Month()-1) + months
	year := monthIndex / 12
	month := monthIndex % 12
	if month < 0 {
		month += 12
		year--
	}
	targetMonth := time.Month(month + 1)
	day := value.Day()
	lastDay := time.Date(int(year), targetMonth+1, 0, value.Hour(), value.Minute(), value.Second(), value.Nanosecond(), value.Location()).Day()
	if day > lastDay {
		day = lastDay
	}
	return time.Date(int(year), targetMonth, day, value.Hour(), value.Minute(), value.Second(), value.Nanosecond(), value.Location()).AddDate(0, 0, int(days))
}
