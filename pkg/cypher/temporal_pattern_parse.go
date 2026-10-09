package cypher

import (
	"strings"
	"time"
)

// patternField is a calendar or clock field a pattern reads.
type patternField uint8

const (
	fieldEra patternField = iota
	fieldYearOfEra
	fieldYear
	fieldWeekBasedYear
	fieldDayOfYear
	fieldMonth
	fieldDayOfMonth
	fieldModifiedJulianDay
	fieldQuarter
	fieldWeekOfWeekBasedYear
	fieldWeekOfMonth
	fieldDayOfWeek // ISO: Monday 1 … Sunday 7
	fieldUSDayOfWeek
	fieldAlignedWeekOfMonth
	fieldAmPm
	fieldDayPeriod // index into javaDayPeriods
	fieldClockHourOfAmPm
	fieldHourOfAmPm
	fieldClockHourOfDay
	fieldHourOfDay
	fieldMinute
	fieldSecond
	fieldNano
	fieldMilliOfDay
	fieldNanoOfDay
	patternFieldCount
)

// numericPatternFields are the fields the numeric pattern letters read (e
// and c only as numbers, at one or two letters).
var numericPatternFields = map[byte]patternField{
	'y': fieldYearOfEra, 'u': fieldYear, 'Y': fieldWeekBasedYear, 'D': fieldDayOfYear, 'M': fieldMonth, 'L': fieldMonth,
	'd': fieldDayOfMonth, 'g': fieldModifiedJulianDay, 'Q': fieldQuarter, 'q': fieldQuarter, 'w': fieldWeekOfWeekBasedYear,
	'W': fieldWeekOfMonth, 'e': fieldUSDayOfWeek, 'c': fieldUSDayOfWeek, 'F': fieldAlignedWeekOfMonth,
	'h': fieldClockHourOfAmPm, 'K': fieldHourOfAmPm, 'k': fieldClockHourOfDay, 'H': fieldHourOfDay, 'm': fieldMinute,
	's': fieldSecond, 'S': fieldNano, 'n': fieldNano, 'A': fieldMilliOfDay, 'N': fieldNanoOfDay,
}

// patternParse is the state of reading text with a pattern: the fields read
// so far, and the zone and offset.
type patternParse struct {
	text     string
	values   [patternFieldCount]int64
	has      [patternFieldCount]bool
	zoneID   string
	location *time.Location
	offset   int
	hasZone  bool
	hasOff   bool
}

// set records a field; reading it twice with different values fails.
func (state *patternParse) set(field patternField, value int64) bool {
	if state.has[field] && state.values[field] != value {
		return false
	}
	state.values[field], state.has[field] = value, true
	return true
}

// parseItems reads items from text at pos to the end of items; an optional
// section that doesn't match reads nothing.
func (state *patternParse) parseItems(items []patternItem, pos int) (int, bool) {
	for index := 0; index < len(items); index++ {
		item := items[index]
		switch item.kind {
		case patternLiteral:
			if !strings.HasPrefix(state.text[pos:], item.text) {
				return pos, false
			}
			pos += len(item.text)
		case patternOptionalStart:
			saved := *state
			if next, ok := state.parseItems(items[index+1:item.end], pos); ok {
				pos = next
			} else {
				*state = saved
			}
			index = item.end
		case patternFieldItem:
			next, ok := state.parsePadded(item, pos)
			if !ok {
				return pos, false
			}
			pos = next
		}
	}
	return pos, true
}

// parsePadded reads a field, which fills exactly its pad width, leading
// spaces first, when it has one.
func (state *patternParse) parsePadded(item patternItem, pos int) (int, bool) {
	if item.pad == 0 {
		return state.parseField(item, pos)
	}
	end := pos + item.pad
	if end > len(state.text) {
		return pos, false
	}
	for pos < end && state.text[pos] == ' ' {
		pos++
	}
	full := state.text
	state.text = full[:end]
	next, ok := state.parseField(item, pos)
	state.text = full
	return next, ok && next == end
}

func (state *patternParse) parseField(item patternItem, pos int) (int, bool) {
	if spec, numeric := item.numeric(); numeric {
		value, next, ok := parseJavaNumber(state.text, pos, spec, item.subsequent)
		if !ok {
			return pos, false
		}
		if spec.reduced {
			value += 2000
		}
		if spec.fraction {
			for digits := item.count; digits < 9; digits++ {
				value *= 10
			}
		}
		return next, state.set(numericPatternFields[item.letter], value)
	}
	text := state.text[pos:]
	switch item.letter {
	case 'G':
		form := min(max(item.count, 3), 5) - 3
		return state.parseText(text, pos, fieldEra, 0, javaEraNames[0][form], javaEraNames[1][form])
	case 'M', 'L':
		names := make([]string, len(javaMonthNames))
		for index, name := range javaMonthNames {
			names[index] = javaTextForm(name, item.count)
		}
		return state.parseText(text, pos, fieldMonth, 1, names...)
	case 'Q', 'q':
		names := make([]string, 4)
		for index := range names {
			switch item.count {
			case 3:
				names[index] = "Q" + string(rune('1'+index))
			case 4:
				names[index] = javaQuarterNames[index]
			default:
				names[index] = string(rune('1' + index))
			}
		}
		return state.parseText(text, pos, fieldQuarter, 1, names...)
	case 'E', 'e', 'c':
		names := make([]string, len(javaWeekdayNames))
		for index, name := range javaWeekdayNames {
			names[index] = javaTextForm(name, max(item.count, 3))
		}
		return state.parseText(text, pos, fieldDayOfWeek, 1, names...)
	case 'a':
		return state.parseText(text, pos, fieldAmPm, 0, "AM", "PM")
	case 'B':
		names := make([]string, len(javaDayPeriods))
		for index, period := range javaDayPeriods {
			names[index] = period.full
			if item.count == 5 {
				names[index] = period.narrow
			}
		}
		return state.parseText(text, pos, fieldDayPeriod, 0, names...)
	case 'V':
		return state.parseZoneID(pos, nil)
	case 'z', 'v':
		if item.count == 4 {
			return state.parseZoneID(pos, javaZoneLongNameZones)
		}
		return state.parseZoneID(pos, javaZoneShortNameZones)
	case 'O':
		if !strings.HasPrefix(text, "GMT") {
			return pos, false
		}
		seconds, next, ok := parseLocalizedOffset(state.text, pos+3, item.count == 4)
		if !ok {
			return pos, false
		}
		return next, state.setOffset(seconds)
	}
	if item.letter == 'Z' && item.count == 4 {
		if !strings.HasPrefix(text, "GMT") {
			return pos, false
		}
		seconds, next, ok := parseLocalizedOffset(state.text, pos+3, true)
		if !ok {
			return pos, false
		}
		return next, state.setOffset(seconds)
	}
	seconds, next, ok := parsePatternOffset(state.text, pos, item.letter, item.count)
	if !ok {
		return pos, false
	}
	return next, state.setOffset(seconds)
}

// parseText reads the longest of names at pos as the field's value: the
// index of the name plus base.
func (state *patternParse) parseText(text string, pos int, field patternField, base int64, names ...string) (int, bool) {
	best := -1
	for index, name := range names {
		if strings.HasPrefix(text, name) && (best < 0 || len(name) > len(names[best])) {
			best = index
		}
	}
	if best < 0 {
		return pos, false
	}
	return pos + len(names[best]), state.set(field, int64(best)+base)
}

func (state *patternParse) setOffset(seconds int) bool {
	if state.hasOff && state.offset != seconds {
		return false
	}
	state.offset, state.hasOff = seconds, true
	return true
}

// parseZoneID reads a zone: an offset (+01:00, Z), UTC / GMT / UT with an
// offset (a zone of its own, prefixedOffsetZone), the longest known zone ID,
// or the longest of names (Java's English zone names, which stand for the
// zone they map to).
func (state *patternParse) parseZoneID(pos int, names map[string]string) (int, bool) {
	text := state.text[pos:]
	if strings.HasPrefix(text, "+") || strings.HasPrefix(text, "-") {
		seconds, next, ok := parseOffsetID(state.text, pos)
		return next, ok && state.setOffset(seconds)
	}
	if zoneID, _, length, ok := prefixedOffsetZone(text); ok {
		location, _ := loadTemporalLocation(zoneID)
		state.zoneID, state.location, state.hasZone = zoneID, location, true
		return pos + length, true
	}
	best := ""
	for end := len(text); end > 0 && best == ""; end-- {
		if _, known := javaZoneNames[text[:end]]; known {
			best = text[:end]
		}
	}
	zoneID := best
	if best == "" && strings.HasPrefix(text, "Z") {
		return pos + 1, state.setOffset(0)
	}
	for name, zone := range names {
		// A name wins over a zone ID of its length: CET is Europe/Paris.
		if len(name) >= len(best) && len(name) > 0 && strings.HasPrefix(text, name) {
			best, zoneID = name, zone
		}
	}
	if best == "" {
		return pos, false
	}
	location, ok := loadTemporalLocation(zoneID)
	if !ok {
		return pos, false
	}
	// A later zone replaces an earlier one, as in Java.
	state.zoneID, state.location, state.hasZone = zoneID, location, true
	return pos + len(best), true
}

// parseJavaNumber reads a number as Java strictly reads one of spec: at
// least min and at most max digits, leaving subsequent digits to the
// fixed-width numbers after it; a - only where the sign style allows one; a
// + only where the number is wider than its minimum and its style shows it.
func parseJavaNumber(text string, pos int, spec numericSpec, subsequent int) (int64, int, bool) {
	start := pos
	negative, positive := false, false
	if pos < len(text) && !spec.reduced && !spec.fraction {
		switch text[pos] {
		case '+':
			if spec.sign != signExceedsPad {
				return 0, start, false
			}
			positive = true
			pos++
		case '-':
			if spec.sign == signNotNegative {
				return 0, start, false
			}
			negative = true
			pos++
		}
	}
	digitsStart := pos
	limit := spec.max + subsequent
	end := digitsStart
	for end < len(text) && end-digitsStart < limit && text[end] >= '0' && text[end] <= '9' {
		end++
	}
	if subsequent > 0 {
		end = digitsStart + max(spec.min, end-digitsStart-subsequent)
		if end > len(text) {
			return 0, start, false
		}
	}
	length := end - digitsStart
	if length < spec.min || length > 18 {
		return 0, start, false
	}
	var value int64
	for index := digitsStart; index < end; index++ {
		if text[index] < '0' || text[index] > '9' {
			return 0, start, false
		}
		value = value*10 + int64(text[index]-'0')
	}
	if spec.sign == signExceedsPad {
		if positive && length <= spec.min || !positive && length > spec.min {
			return 0, start, false
		}
	}
	if negative {
		if value == 0 {
			return 0, start, false
		}
		value = -value
	}
	return value, end, true
}

// parseOffsetID reads +hh, +hh:mm or +hh:mm:ss (or the same without colons).
func parseOffsetID(text string, pos int) (int, int, bool) {
	if pos >= len(text) || text[pos] != '+' && text[pos] != '-' {
		return 0, pos, false
	}
	sign := 1
	if text[pos] == '-' {
		sign = -1
	}
	hours, next, ok := readFixedDigits(text, pos+1, 2)
	if !ok {
		return 0, pos, false
	}
	total := hours * 3600
	for unit := 60; unit >= 1; unit /= 60 {
		at := next
		if at < len(text) && text[at] == ':' {
			at++
		}
		part, after, ok := readFixedDigits(text, at, 2)
		if !ok || part > 59 {
			break
		}
		total += part * unit
		next = after
	}
	if hours > 18 {
		return 0, pos, false
	}
	return sign * total, next, true
}

// parseLocalizedOffset reads the part of GMT+1 / GMT+01:00 after GMT: none
// for zero, else a sign, the hours (one or two digits, or exactly two when
// full) and optional :mm and :ss.
func parseLocalizedOffset(text string, pos int, full bool) (int, int, bool) {
	if pos >= len(text) || text[pos] != '+' && text[pos] != '-' {
		return 0, pos, true
	}
	sign := 1
	if text[pos] == '-' {
		sign = -1
	}
	hours, next, ok := readFixedDigits(text, pos+1, 2)
	if !ok && !full {
		hours, next, ok = readFixedDigits(text, pos+1, 1)
	}
	if !ok {
		return 0, pos, false
	}
	total := hours * 3600
	for unit := 60; unit >= 1; unit /= 60 {
		if next >= len(text) || text[next] != ':' {
			break
		}
		part, after, ok := readFixedDigits(text, next+1, 2)
		if !ok || part > 59 {
			return 0, pos, false
		}
		total += part * unit
		next = after
	}
	return sign * total, next, true
}

// parsePatternOffset reads an X / x / Z offset: the zero text, or a sign,
// two-digit hours and the minutes and seconds the form takes.
func parsePatternOffset(text string, pos int, letter byte, count int) (int, int, bool) {
	minutes, separator, zero := patternOffsetForm(letter, count)
	if strings.HasPrefix(text[pos:], zero) && (zero == "Z" || len(text)-pos == len(zero) || !isDigitByte(text[pos+len(zero)])) {
		return 0, pos + len(zero), true
	}
	if pos >= len(text) || text[pos] != '+' && text[pos] != '-' {
		return 0, pos, false
	}
	sign := 1
	if text[pos] == '-' {
		sign = -1
	}
	hours, next, ok := readFixedDigits(text, pos+1, 2)
	if !ok {
		return 0, pos, false
	}
	total := hours * 3600
	for part := 0; part < 2; part++ {
		if part == 1 && count < 4 {
			break
		}
		at := next
		if separator != "" {
			if !strings.HasPrefix(text[at:], separator) {
				if part == 0 && minutes {
					return 0, pos, false
				}
				break
			}
			at += len(separator)
		}
		value, after, ok := readFixedDigits(text, at, 2)
		if !ok || value > 59 {
			if part == 0 && minutes {
				return 0, pos, false
			}
			break
		}
		total += value * []int{60, 1}[part]
		next = after
	}
	return sign * total, next, true
}

func readFixedDigits(text string, pos, width int) (int, int, bool) {
	if pos+width > len(text) {
		return 0, pos, false
	}
	value := 0
	for index := pos; index < pos+width; index++ {
		if text[index] < '0' || text[index] > '9' {
			return 0, pos, false
		}
		value = value*10 + int(text[index]-'0')
	}
	return value, pos + width, true
}

// resolvedPattern is what a pattern read: a date, a time of day, and a zone
// or offset, each when the fields resolve to one.
type resolvedPattern struct {
	date                 time.Time
	hour, minute, second int
	nano                 int
	hasDate, hasTime     bool
}

// resolve combines the fields read into a date and a time of day, as Java's
// SMART resolver does, and checks every field the resolution didn't use
// against the result (Mon for a Tuesday fails); ok is false for an invalid or
// contradictory value. A date or time the fields don't make is absent, not
// an error.
func (state *patternParse) resolve() (resolvedPattern, bool) {
	var result resolvedPattern
	var used [patternFieldCount]bool
	v, has := state.values, state.has
	// Year: a year of era is the year in the Common Era unless an era says BC.
	year, hasYear := v[fieldYear], has[fieldYear]
	if has[fieldYearOfEra] {
		if v[fieldYearOfEra] < 1 || has[fieldEra] && (v[fieldEra] < 0 || v[fieldEra] > 1) {
			return result, false
		}
		fromEra := v[fieldYearOfEra]
		if has[fieldEra] && v[fieldEra] == 0 {
			fromEra = 1 - fromEra
		}
		if hasYear && year != fromEra {
			return result, false
		}
		year, hasYear = fromEra, true
	}
	use := func(fields ...patternField) {
		for _, field := range fields {
			used[field] = true
		}
	}
	yearUsed := true
	switch {
	case has[fieldModifiedJulianDay]:
		result.date, result.hasDate = time.Unix((v[fieldModifiedJulianDay]-40_587)*86_400, 0).UTC(), true
		use(fieldModifiedJulianDay)
		yearUsed = false
	case hasYear && has[fieldMonth] && has[fieldDayOfMonth]:
		month, day := v[fieldMonth], v[fieldDayOfMonth]
		if month < 1 || month > 12 || day < 1 || day > 31 {
			return result, false
		}
		// A day past the month's end is its last day.
		first := time.Date(int(year), time.Month(month), 1, 0, 0, 0, 0, time.UTC)
		result.date, result.hasDate = first.AddDate(0, 0, min(int(day), first.AddDate(0, 1, -1).Day())-1), true
		use(fieldMonth, fieldDayOfMonth)
	case hasYear && has[fieldDayOfYear]:
		first := time.Date(int(year), 1, 1, 0, 0, 0, 0, time.UTC)
		day := v[fieldDayOfYear]
		if day < 1 || int(day) > first.AddDate(1, 0, -1).YearDay() {
			return result, false
		}
		result.date, result.hasDate = first.AddDate(0, 0, int(day)-1), true
		use(fieldDayOfYear)
	case hasYear && has[fieldMonth] && has[fieldAlignedWeekOfMonth] && has[fieldDayOfWeek]:
		month, week, weekday := v[fieldMonth], v[fieldAlignedWeekOfMonth], v[fieldDayOfWeek]
		if month < 1 || month > 12 || week < 1 || week > 5 || weekday < 1 || weekday > 7 {
			return result, false
		}
		// The day may fall in the next month, as SMART resolution allows.
		date := time.Date(int(year), time.Month(month), 1+int(week-1)*7, 0, 0, 0, 0, time.UTC)
		result.date, result.hasDate = date.AddDate(0, 0, floorModInt(int(weekday)%7-int(date.Weekday()), 7)), true
		use(fieldMonth, fieldAlignedWeekOfMonth, fieldDayOfWeek)
	case has[fieldWeekBasedYear] && has[fieldWeekOfWeekBasedYear] && (has[fieldUSDayOfWeek] || has[fieldDayOfWeek]):
		weekday, weekdayField, ok := state.usWeekday()
		if !ok {
			return result, false
		}
		january := time.Date(int(v[fieldWeekBasedYear]), 1, 1, 0, 0, 0, 0, time.UTC)
		date := january.AddDate(0, 0, 1-usDayOfWeek(january)+int(v[fieldWeekOfWeekBasedYear]-1)*7+weekday-1)
		if y, w := usWeekBasedYear(date); int64(y) != v[fieldWeekBasedYear] || int64(w) != v[fieldWeekOfWeekBasedYear] {
			return result, false
		}
		result.date, result.hasDate = date, true
		use(fieldWeekBasedYear, fieldWeekOfWeekBasedYear, weekdayField)
		yearUsed = false
	case hasYear && has[fieldMonth] && has[fieldWeekOfMonth] && (has[fieldUSDayOfWeek] || has[fieldDayOfWeek]):
		weekday, weekdayField, ok := state.usWeekday()
		month := v[fieldMonth]
		if !ok || month < 1 || month > 12 {
			return result, false
		}
		// The week must be one of the month's; its day may fall in the month
		// before or after, as SMART resolution allows.
		first := time.Date(int(year), time.Month(month), 1, 0, 0, 0, 0, time.UTC)
		last := first.AddDate(0, 1, -1)
		if week := v[fieldWeekOfMonth]; week < 1 || int(week) > usWeekOf(last.Day(), usDayOfWeek(last)) {
			return result, false
		}
		result.date, result.hasDate = first.AddDate(0, 0, 1-usDayOfWeek(first)+int(v[fieldWeekOfMonth]-1)*7+weekday-1), true
		use(fieldMonth, fieldWeekOfMonth, weekdayField)
	default:
		yearUsed = false
	}
	if yearUsed {
		use(fieldYear, fieldYearOfEra)
		if has[fieldYearOfEra] {
			// The era made the year of era a year; with u alone it is checked.
			use(fieldEra)
		}
	}

	excessDays := 0
	if ok := state.resolveTime(&result, &excessDays, &used); !ok {
		return result, false
	}
	if result.hasDate {
		if !state.crossCheckDate(result.date, &used) {
			return result, false
		}
		result.date = result.date.AddDate(0, 0, excessDays)
	}
	return result, true
}

// usWeekday is the US day of the week (Sunday 1) of the e / c or E field
// read, and which of the two it is.
func (state *patternParse) usWeekday() (int, patternField, bool) {
	if state.has[fieldUSDayOfWeek] {
		weekday := state.values[fieldUSDayOfWeek]
		return int(weekday), fieldUSDayOfWeek, weekday >= 1 && weekday <= 7
	}
	weekday := state.values[fieldDayOfWeek]
	return int(weekday)%7 + 1, fieldDayOfWeek, weekday >= 1 && weekday <= 7
}

// resolveTime makes the time of day: hour of day from H, k, a with h / K, or
// B with h / K; then minute, second and nanosecond, each defaulting to zero
// only when nothing smaller was read; or the milli / nano of the day. The
// fields it uses are marked in used.
func (state *patternParse) resolveTime(result *resolvedPattern, excessDays *int, used *[patternFieldCount]bool) bool {
	v, has := state.values, state.has
	hour, hasHour := v[fieldHourOfDay], has[fieldHourOfDay]
	hourFields := []patternField{fieldHourOfDay}
	if has[fieldClockHourOfDay] {
		// SMART resolution takes 0 as well as 1 to 24 (and 0 to 12 below).
		clock := v[fieldClockHourOfDay]
		if clock < 0 || clock > 24 {
			return false
		}
		if hasHour && hour != clock%24 {
			return false
		}
		hour, hasHour = clock%24, true
		hourFields = append(hourFields, fieldClockHourOfDay)
	}
	hourOfAmPm, hasHourOfAmPm := v[fieldHourOfAmPm], has[fieldHourOfAmPm]
	if has[fieldClockHourOfAmPm] {
		clock := v[fieldClockHourOfAmPm]
		if clock < 0 || clock > 12 {
			return false
		}
		if hasHourOfAmPm && hourOfAmPm != clock%12 {
			return false
		}
		hourOfAmPm, hasHourOfAmPm = clock%12, true
	}
	if hasHourOfAmPm && (hourOfAmPm < 0 || hourOfAmPm > 11) {
		return false
	}
	if hasHourOfAmPm && !hasHour {
		switch {
		case has[fieldAmPm]:
			hour, hasHour = v[fieldAmPm]*12+hourOfAmPm, true
			hourFields = append(hourFields, fieldAmPm, fieldHourOfAmPm, fieldClockHourOfAmPm)
		case has[fieldDayPeriod]:
			period := javaDayPeriods[v[fieldDayPeriod]]
			minute := int(v[fieldMinute])
			for _, candidate := range []int64{hourOfAmPm, hourOfAmPm + 12} {
				if at := int(candidate)*60 + minute; at >= period.from && at <= period.to {
					hour, hasHour = candidate, true
					break
				}
			}
			if !hasHour {
				return false
			}
			hourFields = append(hourFields, fieldDayPeriod, fieldHourOfAmPm, fieldClockHourOfAmPm)
		}
	}
	minute, second, nano := v[fieldMinute], v[fieldSecond], v[fieldNano]
	switch {
	case hasHour:
		if !has[fieldMinute] && (has[fieldSecond] || has[fieldNano]) || has[fieldMinute] && !has[fieldSecond] && has[fieldNano] {
			return true
		}
		if minute < 0 || minute > 59 || second < 0 || second > 59 || nano < 0 || nano > 999_999_999 {
			return false
		}
		if hour == 24 && minute == 0 && second == 0 && nano == 0 {
			hour, *excessDays = 0, 1
		}
		if hour < 0 || hour > 23 {
			return false
		}
		hourFields = append(hourFields, fieldMinute, fieldSecond, fieldNano)
	case has[fieldNanoOfDay] || has[fieldMilliOfDay]:
		ofDay, field := v[fieldNanoOfDay], fieldNanoOfDay
		if !has[fieldNanoOfDay] {
			ofDay, field = v[fieldMilliOfDay]*1_000_000, fieldMilliOfDay
		}
		if ofDay < 0 || ofDay >= 86_400_000_000_000 {
			return false
		}
		hour, minute, second = ofDay/3_600_000_000_000, ofDay/60_000_000_000%60, ofDay/1_000_000_000%60
		if !has[fieldNano] {
			nano = ofDay % 1_000_000_000
		}
		hourFields = []patternField{field}
	default:
		return true
	}
	for _, field := range hourFields {
		used[field] = true
	}
	result.hour, result.minute, result.second, result.nano, result.hasTime = int(hour), int(minute), int(second), int(nano), true
	return state.crossCheckTime(*result, used)
}

// crossCheckDate checks every date field read that resolving didn't use
// against the resolved date.
func (state *patternParse) crossCheckDate(date time.Time, used *[patternFieldCount]bool) bool {
	weekBasedYear, week := usWeekBasedYear(date)
	era, yearOfEra := int64(1), int64(date.Year())
	if date.Year() <= 0 {
		era, yearOfEra = 0, 1-yearOfEra
	}
	expected := map[patternField]int64{
		fieldEra: era, fieldYear: int64(date.Year()), fieldYearOfEra: yearOfEra, fieldDayOfYear: int64(date.YearDay()),
		fieldMonth: int64(date.Month()), fieldDayOfMonth: int64(date.Day()), fieldModifiedJulianDay: modifiedJulianDay(date),
		fieldQuarter: int64((int(date.Month()) + 2) / 3), fieldWeekBasedYear: int64(weekBasedYear),
		fieldWeekOfWeekBasedYear: int64(week), fieldWeekOfMonth: int64(usWeekOf(date.Day(), usDayOfWeek(date))),
		fieldDayOfWeek: int64((int(date.Weekday())+6)%7 + 1), fieldUSDayOfWeek: int64(usDayOfWeek(date)),
		fieldAlignedWeekOfMonth: int64((date.Day()-1)/7 + 1),
	}
	return state.crossCheck(expected, used)
}

// crossCheckTime checks the clock fields read that resolving didn't use
// against the resolved time.
func (state *patternParse) crossCheckTime(result resolvedPattern, used *[patternFieldCount]bool) bool {
	hour := int64(result.hour)
	ofDay := hour*3_600_000_000_000 + int64(result.minute)*60_000_000_000 + int64(result.second)*1_000_000_000 + int64(result.nano)
	expected := map[patternField]int64{
		fieldAmPm: hour / 12, fieldHourOfAmPm: hour % 12, fieldClockHourOfAmPm: (hour+11)%12 + 1, fieldHourOfDay: hour,
		fieldClockHourOfDay: (hour+23)%24 + 1, fieldMilliOfDay: ofDay / 1_000_000, fieldNanoOfDay: ofDay,
	}
	if state.has[fieldDayPeriod] && !used[fieldDayPeriod] {
		period := javaDayPeriods[state.values[fieldDayPeriod]]
		if at := int(hour)*60 + result.minute; at < period.from || at > period.to {
			return false
		}
	}
	return state.crossCheck(expected, used)
}

func (state *patternParse) crossCheck(expected map[patternField]int64, used *[patternFieldCount]bool) bool {
	for field, value := range expected {
		if state.has[field] && !used[field] && state.values[field] != value {
			return false
		}
	}
	return true
}

// parseTemporalPattern builds a value of kind (date, localtime, time,
// localdatetime, datetime) from text read with a Java pattern; ok is false
// when the pattern is invalid, doesn't match the whole text, or the text
// lacks what kind needs: a date for the date kinds, a time of day for the
// time kinds. A datetime without a time is at midnight and, like a time,
// without a zone is in UTC.
func parseTemporalPattern(kind, text, pattern string) (interface{}, bool) {
	items, ok := compileTemporalPattern(pattern)
	if !ok {
		return nil, false
	}
	state := &patternParse{text: text}
	end, ok := state.parseItems(items, 0)
	if !ok || end != len(text) {
		return nil, false
	}
	resolved, ok := state.resolve()
	if !ok {
		return nil, false
	}
	needsDate := kind == "date" || kind == "localdatetime" || kind == "datetime"
	needsTime := kind == "localtime" || kind == "time"
	if needsDate && !resolved.hasDate || needsTime && !resolved.hasTime {
		return nil, false
	}
	// An offset beyond 18 hours, which a pattern offset may read, is no offset.
	if state.offset < -64_800 || state.offset > 64_800 {
		state.hasOff, state.offset = false, 0
	}
	d := resolved.date
	clock := func(location *time.Location) time.Time {
		return time.Date(d.Year(), d.Month(), d.Day(), resolved.hour, resolved.minute, resolved.second, resolved.nano, location)
	}
	switch kind {
	case "date":
		return CypherDate{Time: d}, true
	case "localtime":
		d = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		return CypherLocalTime{Time: clock(time.UTC)}, true
	case "time":
		d = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		location := time.UTC
		if state.hasOff {
			location = time.FixedZone("", state.offset)
		}
		return CypherTime{Time: clock(location)}, true
	case "localdatetime":
		return CypherLocalDateTime{Time: clock(time.UTC)}, true
	}
	if state.hasZone {
		value := clock(state.location)
		if state.hasOff {
			if _, offset := value.Zone(); offset != state.offset {
				// An offset the zone has at another instant of the same wall
				// clock (a repeated hour) picks that instant.
				if alternative := value.Add(time.Duration(offset-state.offset) * time.Second); alternative.Hour() == value.Hour() {
					if _, other := alternative.Zone(); other == state.offset {
						value = alternative
					}
				}
			}
		}
		return CypherDateTime{Time: value, ZoneID: state.zoneID}, true
	}
	return CypherDateTime{Time: clock(time.FixedZone("", state.offset))}, true
}
