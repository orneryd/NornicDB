package cypher

import "time"

type temporalBetweenParts struct {
	value             time.Time
	hasDate, hasClock bool
	zoned             bool
}

func durationBetweenTemporalValues(mode string, left, right interface{}) (interface{}, bool) {
	from, fromOK := temporalBetweenPartsOf(left)
	to, toOK := temporalBetweenPartsOf(right)
	if !fromOK || !toOK {
		return nil, true
	}
	originalDates := from.hasDate && to.hasDate
	from, to = alignTemporalBetweenParts(from, to)

	switch mode {
	case "inmonths":
		if !from.hasDate || !to.hasDate {
			return &CypherDuration{}, true
		}
		return durationFromGroups(monthsBetween(from.value, to.value), 0, 0, 0), true
	case "indays":
		if !from.hasDate || !to.hasDate {
			return &CypherDuration{}, true
		}
		return durationFromGroups(0, daysBetween(from.value, to.value), 0, 0), true
	case "inseconds":
		seconds, nanos := secondsBetween(from, to)
		return durationFromGroups(0, 0, seconds, nanos), true
	case "between":
		months, days := int64(0), int64(0)
		cursor := from
		if originalDates {
			months = monthsBetween(cursor.value, to.value)
			cursor.value = addCalendarDuration(cursor.value, months, 0)
			days = daysBetween(cursor.value, to.value)
			cursor.value = cursor.value.AddDate(0, 0, int(days))
		}
		seconds, nanos := secondsBetween(cursor, to)
		return durationFromGroups(months, days, seconds, nanos), true
	default:
		return nil, false
	}
}

func temporalBetweenPartsOf(value interface{}) (temporalBetweenParts, bool) {
	temporal, _, hasDate, hasClock, zoned := temporalTruncationParts(value)
	if !hasDate && !hasClock {
		return temporalBetweenParts{}, false
	}
	return temporalBetweenParts{value: temporal, hasDate: hasDate, hasClock: hasClock, zoned: zoned}, true
}

func alignTemporalBetweenParts(from, to temporalBetweenParts) (temporalBetweenParts, temporalBetweenParts) {
	if !from.hasClock && from.hasDate {
		from.value = withTemporalClock(from.value, 0, 0, 0, 0)
		from.hasClock = true
	}
	if !to.hasClock && to.hasDate {
		to.value = withTemporalClock(to.value, 0, 0, 0, 0)
		to.hasClock = true
	}
	if from.hasDate && !to.hasDate {
		to.value = withTemporalDate(to.value, from.value.Year(), from.value.Month(), from.value.Day())
		to.hasDate = true
	} else if to.hasDate && !from.hasDate {
		from.value = withTemporalDate(from.value, to.value.Year(), to.value.Month(), to.value.Day())
		from.hasDate = true
	}
	if from.zoned && !to.zoned {
		to.value = withTemporalLocation(to.value, from.value.Location())
		to.zoned = true
	} else if to.zoned && !from.zoned {
		from.value = withTemporalLocation(from.value, to.value.Location())
		from.zoned = true
	} else if from.zoned && to.zoned {
		to.value = to.value.In(from.value.Location())
	}
	return from, to
}

func withTemporalClock(value time.Time, hour, minute, second, nanos int) time.Time {
	return time.Date(value.Year(), value.Month(), value.Day(), hour, minute, second, nanos, value.Location())
}

func withTemporalDate(value time.Time, year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, value.Hour(), value.Minute(), value.Second(), value.Nanosecond(), value.Location())
}

func withTemporalLocation(value time.Time, location *time.Location) time.Time {
	return time.Date(value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond(), location)
}

func monthsBetween(from, to time.Time) int64 {
	months := int64(to.Year()-from.Year())*12 + int64(to.Month()-from.Month())
	candidate := addCalendarDuration(from, months, 0)
	comparison := compareCivilParts(candidate, to, true)
	if months > 0 && comparison > 0 {
		months--
	} else if months < 0 && comparison < 0 {
		months++
	}
	return months
}

func daysBetween(from, to time.Time) int64 {
	days := civilEpochDay(to.Year(), int(to.Month()), to.Day()) - civilEpochDay(from.Year(), int(from.Month()), from.Day())
	fromClock := clockNanos(from)
	toClock := clockNanos(to)
	if days > 0 && toClock < fromClock {
		days--
	} else if days < 0 && toClock > fromClock {
		days++
	}
	return days
}

func secondsBetween(from, to temporalBetweenParts) (int64, int64) {
	seconds := temporalAbsoluteSeconds(to) - temporalAbsoluteSeconds(from)
	nanos := int64(to.value.Nanosecond() - from.value.Nanosecond())
	if nanos < 0 {
		seconds--
		nanos += 1_000_000_000
	}
	return seconds, nanos
}

func temporalAbsoluteSeconds(value temporalBetweenParts) int64 {
	seconds := int64(value.value.Hour()*3_600 + value.value.Minute()*60 + value.value.Second())
	if value.hasDate {
		seconds += civilEpochDay(value.value.Year(), int(value.value.Month()), value.value.Day()) * 86_400
	}
	if value.zoned {
		_, offset := value.value.Zone()
		seconds -= int64(offset)
	}
	return seconds
}

func civilEpochDay(year, month, day int) int64 {
	y := int64(year)
	m := int64(month)
	if m <= 2 {
		y--
	}
	era := floorDivide(y, 400)
	yearOfEra := y - era*400
	adjustedMonth := m - 3
	if adjustedMonth < 0 {
		adjustedMonth += 12
	}
	dayOfYear := (153*adjustedMonth+2)/5 + int64(day) - 1
	dayOfEra := yearOfEra*365 + yearOfEra/4 - yearOfEra/100 + dayOfYear
	return era*146097 + dayOfEra - 719468
}

func floorDivide(value, divisor int64) int64 {
	quotient := value / divisor
	if value%divisor < 0 {
		quotient--
	}
	return quotient
}
