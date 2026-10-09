package cypher

import (
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Temporal patterns: Java's DateTimeFormatter pattern language, in the
// English (en-US) locale, as Neo4j's format() prints temporal values with it
// and its temporal constructors read text with it (date('18.11.1986',
// 'dd.MM.yyyy')). One compiled form serves both directions. Reading is
// strict and case-sensitive and resolves as Java's SMART resolver does: a
// day past the end of its month is the month's last day, 24:00 is the next
// midnight, and a field the resolved value contradicts (Mon for a Tuesday)
// fails. Weeks are US weeks: Sunday first, week 1 holds 1 January.

type patternItemKind uint8

const (
	patternLiteral patternItemKind = iota
	patternFieldItem
	patternOptionalStart
	patternOptionalEnd
)

// patternItem is one element of a compiled pattern: literal text, a field
// (a run of one letter), or an optional section's bounds. A field's pad is
// the width a preceding run of p pads it to; an optional start's end is the
// index of its end item; subsequent is the digits a variable-width number
// leaves to the fixed-width numbers that follow it with nothing between
// (yyyyMMdd reads 19861118).
type patternItem struct {
	kind       patternItemKind
	text       string
	letter     byte
	count      int
	pad        int
	end        int
	subsequent int
}

// patternLetterCounts are the run lengths Java accepts for each pattern
// letter; a letter not listed is reserved (an invalid pattern).
var patternLetterCounts = map[byte][]int{
	'G': {1, 2, 3, 4, 5}, 'u': nil, 'y': nil, 'Y': nil, 'D': {1, 2, 3}, 'M': {1, 2, 3, 4, 5}, 'L': {1, 2, 3, 4, 5},
	'd': {1, 2}, 'g': nil, 'Q': {1, 2, 3, 4, 5}, 'q': {1, 2, 3, 4, 5}, 'w': {1, 2}, 'W': {1}, 'E': {1, 2, 3, 4, 5},
	'e': {1, 2, 3, 4, 5}, 'c': {1, 3, 4, 5}, 'F': {1}, 'a': {1}, 'B': {1, 4, 5}, 'h': {1, 2}, 'K': {1, 2},
	'k': {1, 2}, 'H': {1, 2}, 'm': {1, 2}, 's': {1, 2}, 'S': {1, 2, 3, 4, 5, 6, 7, 8, 9}, 'A': nil, 'n': nil,
	'N': nil, 'V': {2}, 'v': {1, 4}, 'z': {1, 2, 3, 4}, 'O': {1, 4}, 'X': {1, 2, 3, 4, 5}, 'x': {1, 2, 3, 4, 5},
	'Z': {1, 2, 3, 4, 5},
}

func validPatternLetter(letter byte, count int) bool {
	counts, known := patternLetterCounts[letter]
	if !known {
		return false
	}
	if counts == nil {
		return count <= 19
	}
	for _, allowed := range counts {
		if allowed == count {
			return true
		}
	}
	return false
}

// compileTemporalPattern compiles a Java DateTimeFormatter pattern; ok is
// false for a pattern Java rejects: a reserved letter or character ({, }, #),
// a run of a length its letter doesn't take, an unterminated quote, a ] with
// no [, or a pad (p) not followed by a field. An open [ closes at the end.
func compileTemporalPattern(pattern string) (items []patternItem, ok bool) {
	var open []int
	pad := 0
	active := -1
	for index := 0; index < len(pattern); {
		c := pattern[index]
		switch {
		case isASCIILetter(c):
			end := index + 1
			for end < len(pattern) && pattern[end] == c {
				end++
			}
			count := end - index
			index = end
			if c == 'p' {
				if end >= len(pattern) || !isASCIILetter(pattern[end]) {
					return nil, false
				}
				pad = count
				continue
			}
			if !validPatternLetter(c, count) {
				return nil, false
			}
			item := patternItem{kind: patternFieldItem, letter: c, count: count, pad: pad}
			spec, numeric := item.numeric()
			switch {
			case numeric && pad == 0 && active >= 0 && spec.fixedWidth():
				items[active].subsequent += spec.max
			case numeric && pad == 0:
				active = len(items)
			default:
				active = -1
			}
			pad = 0
			items = append(items, item)
			continue
		case c == '\'':
			end := index + 1
			var literal strings.Builder
			for {
				if end >= len(pattern) {
					return nil, false
				}
				if pattern[end] == '\'' {
					if end+1 < len(pattern) && pattern[end+1] == '\'' {
						literal.WriteByte('\'')
						end += 2
						continue
					}
					break
				}
				literal.WriteByte(pattern[end])
				end++
			}
			text := literal.String()
			if end == index+1 {
				text = "'"
			}
			items = append(items, patternItem{kind: patternLiteral, text: text})
			index = end + 1
		case c == '[':
			open = append(open, len(items))
			items = append(items, patternItem{kind: patternOptionalStart})
			index++
		case c == ']':
			if len(open) == 0 {
				return nil, false
			}
			items[open[len(open)-1]].end = len(items)
			open = open[:len(open)-1]
			items = append(items, patternItem{kind: patternOptionalEnd})
			index++
		case c == '{' || c == '}' || c == '#':
			return nil, false
		default:
			_, size := utf8.DecodeRuneInString(pattern[index:])
			items = append(items, patternItem{kind: patternLiteral, text: pattern[index : index+size]})
			index += size
		}
		active = -1
	}
	for len(open) > 0 {
		items[open[len(open)-1]].end = len(items)
		open = open[:len(open)-1]
		items = append(items, patternItem{kind: patternOptionalEnd})
	}
	return items, true
}

type signStyle uint8

const (
	signNormal signStyle = iota
	signNotNegative
	signExceedsPad
)

// numericSpec is how a numeric field prints and reads: its digit widths, its
// sign, and whether it is a two-digit year (2000-2099) or a fraction of a
// second.
type numericSpec struct {
	min, max int
	sign     signStyle
	reduced  bool
	fraction bool
}

func (spec numericSpec) fixedWidth() bool {
	return spec.min == spec.max && spec.sign != signNormal && spec.sign != signExceedsPad
}

// numeric is a field's numeric form, as Java's pattern parser builds it;
// ok is false for a text or zone field.
func (item patternItem) numeric() (numericSpec, bool) {
	count := item.count
	switch item.letter {
	case 'u', 'y', 'Y':
		switch {
		case count == 2:
			return numericSpec{min: 2, max: 2, sign: signNotNegative, reduced: true}, true
		case count < 4:
			return numericSpec{min: count, max: 19, sign: signNormal}, true
		default:
			return numericSpec{min: count, max: 19, sign: signExceedsPad}, true
		}
	case 'M', 'L', 'Q', 'q', 'd', 'h', 'K', 'k', 'H', 'm', 's':
		if count == 1 {
			return numericSpec{min: 1, max: 19, sign: signNormal}, true
		}
		if count == 2 {
			return numericSpec{min: 2, max: 2, sign: signNotNegative}, true
		}
	case 'D':
		switch count {
		case 1:
			return numericSpec{min: 1, max: 19, sign: signNormal}, true
		case 2:
			return numericSpec{min: 2, max: 3, sign: signNotNegative}, true
		default:
			return numericSpec{min: 3, max: 3, sign: signNotNegative}, true
		}
	case 'w':
		return numericSpec{min: count, max: 2, sign: signNotNegative}, true
	case 'W':
		return numericSpec{min: 1, max: 1, sign: signNotNegative}, true
	case 'e', 'c':
		if count <= 2 {
			return numericSpec{min: count, max: count, sign: signNotNegative}, true
		}
	case 'F':
		return numericSpec{min: 1, max: 19, sign: signNormal}, true
	case 'g':
		return numericSpec{min: count, max: 19, sign: signNormal}, true
	case 'A', 'n', 'N':
		return numericSpec{min: count, max: 19, sign: signNotNegative}, true
	case 'S':
		return numericSpec{min: count, max: count, sign: signNotNegative, fraction: true}, true
	}
	return numericSpec{}, false
}

// formatJavaNumber prints value as Java prints a number of spec: zero-padded
// to the minimum width, a - for a negative value, and, for a sign that
// shows when the pad is exceeded, a + for a value wider than the minimum
// (yyyy prints 12021 as +12021). A two-digit year prints its last two digits.
func formatJavaNumber(value int64, spec numericSpec) string {
	if spec.reduced {
		value %= 100
		if value < 0 {
			value = -value
		}
		return padDigits(strconv.FormatInt(value, 10), 2)
	}
	negative := value < 0
	digits := strconv.FormatInt(value, 10)
	if negative {
		digits = digits[1:]
	}
	padded := padDigits(digits, spec.min)
	switch {
	case negative:
		return "-" + padded
	case spec.sign == signExceedsPad && len(digits) > spec.min:
		return "+" + padded
	}
	return padded
}

func padDigits(digits string, width int) string {
	if len(digits) >= width {
		return digits
	}
	return strings.Repeat("0", width-len(digits)) + digits
}

var (
	javaMonthNames   = [...]string{"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"}
	javaWeekdayNames = [...]string{"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"}
	javaQuarterNames = [...]string{"1st quarter", "2nd quarter", "3rd quarter", "4th quarter"}
	javaEraNames     = [...][3]string{{"BC", "Before Christ", "B"}, {"AD", "Anno Domini", "A"}}
)

// javaTextForms are the texts of a field with values 1..len(names) at a
// run length: 3 is the short form, 4 the full name, 5 the narrow one.
func javaTextForm(name string, count int) string {
	switch count {
	case 4:
		return name
	case 5:
		return name[:1]
	}
	return name[:3]
}

// javaDayPeriod is Java's English flexible day period (B) of a minute of the
// day, as its full and narrow text: midnight and noon at those minutes, and
// otherwise morning (before noon), afternoon, evening (from 18:00) and night
// (from 21:00).
func javaDayPeriod(minuteOfDay int) (full, narrow string) {
	switch {
	case minuteOfDay == 0:
		return "midnight", "mi"
	case minuteOfDay == 720:
		return "noon", "n"
	case minuteOfDay < 720:
		return "in the morning", "in the morning"
	case minuteOfDay < 1080:
		return "in the afternoon", "in the afternoon"
	case minuteOfDay < 1260:
		return "in the evening", "in the evening"
	}
	return "at night", "at night"
}

// javaDayPeriods are the day periods a B field reads, with the minutes of the
// day each covers.
var javaDayPeriods = []struct {
	full, narrow string
	from, to     int
}{
	{"midnight", "mi", 0, 0}, {"noon", "n", 720, 720}, {"in the morning", "in the morning", 0, 719},
	{"in the afternoon", "in the afternoon", 720, 1079}, {"in the evening", "in the evening", 1080, 1259},
	{"at night", "at night", 1260, 1439},
}

// US weeks: Sunday is the first day, and week 1 is the week holding 1
// January (or the 1st of the month for a week of the month).
func usDayOfWeek(date time.Time) int { return int(date.Weekday()) + 1 }

func usWeekOf(day, dayOfWeek int) int {
	weekStart := floorModInt(day-dayOfWeek, 7)
	offset := -weekStart
	if weekStart+1 > 1 {
		offset = 7 - weekStart
	}
	return (7 + offset + day - 1) / 7
}

// usWeekBasedYear is a date's US week-based year and week: the week holding
// the next 1 January is that year's week 1.
func usWeekBasedYear(date time.Time) (year, week int) {
	if endOfWeek := date.AddDate(0, 0, 7-usDayOfWeek(date)); endOfWeek.Year() > date.Year() {
		return endOfWeek.Year(), 1
	}
	return date.Year(), usWeekOf(date.YearDay(), usDayOfWeek(date))
}

func floorModInt(value, modulus int) int {
	result := value % modulus
	if result < 0 {
		result += modulus
	}
	return result
}

// modifiedJulianDay of a date: days since 1858-11-17.
func modifiedJulianDay(date time.Time) int64 {
	return time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC).Unix()/86_400 + 40_587
}

// patternTemporal is a temporal value as a pattern sees it: the fields it has
// and its wall clock in its zone.
type patternTemporal struct {
	value                      time.Time
	date, clock, offset, zoned bool
	zoneID, typeName           string
}

// patternTemporalOf is a temporal value's pattern view; ok is false for a
// value that isn't a date, time or datetime.
func patternTemporalOf(value interface{}) (patternTemporal, bool) {
	switch typed := value.(type) {
	case CypherDate:
		return patternTemporal{value: typed.Time, date: true, typeName: "DATE"}, true
	case CypherLocalTime:
		return patternTemporal{value: typed.Time, clock: true, typeName: "LOCAL TIME"}, true
	case CypherTime:
		return patternTemporal{value: typed.Time, clock: true, offset: true, typeName: "ZONED TIME"}, true
	case CypherLocalDateTime:
		return patternTemporal{value: typed.Time, date: true, clock: true, typeName: "LOCAL DATETIME"}, true
	case CypherDateTime:
		return patternTemporal{value: typed.Time, date: true, clock: true, offset: true, zoned: true, zoneID: typed.ZoneID, typeName: "ZONED DATETIME"}, true
	case time.Time:
		return patternTemporal{value: typed, date: true, clock: true, offset: true, zoned: true, typeName: "ZONED DATETIME"}, true
	}
	return patternTemporal{}, false
}

// formatTemporalPattern prints a temporal value with a compiled pattern; ok
// is false when the pattern names a field the value doesn't have outside an
// optional section (an optional section with one prints nothing), or a
// padded field is wider than its pad.
func formatTemporalPattern(items []patternItem, value patternTemporal) (string, bool) {
	var out strings.Builder
	for index := 0; index < len(items); index++ {
		item := items[index]
		switch item.kind {
		case patternLiteral:
			out.WriteString(item.text)
		case patternOptionalStart:
			section, ok := formatTemporalPattern(items[index+1:item.end], value)
			if ok {
				out.WriteString(section)
			}
			index = item.end
		case patternFieldItem:
			text, ok := formatPatternField(item, value)
			if !ok {
				return "", false
			}
			if item.pad > 0 {
				width := utf8.RuneCountInString(text)
				if width > item.pad {
					return "", false
				}
				text = strings.Repeat(" ", item.pad-width) + text
			}
			out.WriteString(text)
		}
	}
	return out.String(), true
}

// patternFieldNeeds is what a value must have for a letter to print it.
func patternFieldNeeds(letter byte) (date, clock, offset, zone bool) {
	switch letter {
	case 'G', 'u', 'y', 'Y', 'D', 'M', 'L', 'd', 'g', 'Q', 'q', 'w', 'W', 'E', 'e', 'c', 'F':
		return true, false, false, false
	case 'a', 'B', 'h', 'K', 'k', 'H', 'm', 's', 'S', 'A', 'n', 'N':
		return false, true, false, false
	case 'O', 'X', 'x', 'Z':
		return false, false, true, false
	}
	return false, false, false, true
}

func formatPatternField(item patternItem, value patternTemporal) (string, bool) {
	date, clock, offset, zone := patternFieldNeeds(item.letter)
	if date && !value.date || clock && !value.clock || offset && !value.offset || zone && !value.zoned {
		return "", false
	}
	t := value.value
	count := item.count
	number := func(field int64) (string, bool) {
		spec, _ := item.numeric()
		if spec.fraction {
			return padDigits(strconv.Itoa(t.Nanosecond()), 9)[:count], true
		}
		return formatJavaNumber(field, spec), true
	}
	nanoOfDay := int64(t.Hour())*3_600_000_000_000 + int64(t.Minute())*60_000_000_000 + int64(t.Second())*1_000_000_000 + int64(t.Nanosecond())
	switch item.letter {
	case 'G':
		era := 1
		if t.Year() <= 0 {
			era = 0
		}
		return javaEraNames[era][min(max(count, 3), 5)-3], true
	case 'u':
		return number(int64(t.Year()))
	case 'y':
		year := int64(t.Year())
		if year <= 0 {
			year = 1 - year
		}
		return number(year)
	case 'Y':
		year, _ := usWeekBasedYear(t)
		return number(int64(year))
	case 'D':
		return number(int64(t.YearDay()))
	case 'M', 'L':
		if count >= 3 {
			return javaTextForm(javaMonthNames[t.Month()-1], count), true
		}
		return number(int64(t.Month()))
	case 'd':
		return number(int64(t.Day()))
	case 'g':
		return number(modifiedJulianDay(t))
	case 'Q', 'q':
		quarter := (int(t.Month()) + 2) / 3
		switch count {
		case 3:
			return "Q" + strconv.Itoa(quarter), true
		case 4:
			return javaQuarterNames[quarter-1], true
		case 5:
			return strconv.Itoa(quarter), true
		}
		return number(int64(quarter))
	case 'w':
		_, week := usWeekBasedYear(t)
		return number(int64(week))
	case 'W':
		return number(int64(usWeekOf(t.Day(), usDayOfWeek(t))))
	case 'E', 'e', 'c':
		if item.letter != 'E' && count <= 2 {
			return number(int64(usDayOfWeek(t)))
		}
		return javaTextForm(javaWeekdayNames[(int(t.Weekday())+6)%7], max(count, 3)), true
	case 'F':
		return number(int64((t.Day()-1)/7 + 1))
	case 'a':
		if t.Hour() < 12 {
			return "AM", true
		}
		return "PM", true
	case 'B':
		full, narrow := javaDayPeriod(t.Hour()*60 + t.Minute())
		if count == 5 {
			return narrow, true
		}
		return full, true
	case 'h':
		return number(int64((t.Hour()+11)%12 + 1))
	case 'K':
		return number(int64(t.Hour() % 12))
	case 'k':
		return number(int64((t.Hour()+23)%24 + 1))
	case 'H':
		return number(int64(t.Hour()))
	case 'm':
		return number(int64(t.Minute()))
	case 's':
		return number(int64(t.Second()))
	case 'S':
		return number(0)
	case 'A':
		return number(nanoOfDay / 1_000_000)
	case 'n':
		return number(int64(t.Nanosecond()))
	case 'N':
		return number(nanoOfDay)
	}
	_, seconds := t.Zone()
	switch item.letter {
	case 'V':
		return value.zoneText(seconds), true
	case 'z', 'v':
		if value.zoneID == "" {
			return value.zoneText(seconds), true
		}
		names, known := javaZoneNames[value.zoneID]
		var name string
		switch {
		case item.letter == 'v' && count == 4:
			name = names.genericLong
		case item.letter == 'v':
			name = names.genericShort
		case count == 4 && t.IsDST():
			name = names.dstLong
		case count == 4:
			name = names.stdLong
		case t.IsDST():
			name = names.dstShort
		default:
			name = names.stdShort
		}
		if !known || name == "" {
			return localizedGMTOffset(seconds, true), true
		}
		return name, true
	case 'O':
		return localizedGMTOffset(seconds, count == 4), true
	}
	// X, x and Z; ZZZZ is the localized offset.
	if item.letter == 'Z' && count == 4 {
		return localizedGMTOffset(seconds, true), true
	}
	return formatPatternOffset(item.letter, count, seconds), true
}

// javaZoneNameSet is a zone's English names: specific short and full names
// in standard and daylight time, and the generic short and full names.
type javaZoneNameSet struct {
	stdShort, dstShort, stdLong, dstLong, genericShort, genericLong string
}

// zoneText is a value's zone ID (VV), or its offset's ID when it has no
// named zone: Z, +01:00, -05:30.
func (value patternTemporal) zoneText(seconds int) string {
	if value.zoneID != "" {
		return value.zoneID
	}
	if seconds == 0 {
		return "Z"
	}
	return offsetID(seconds, true)
}

// offsetID writes an offset as +hh:mm, with :ss when it has seconds.
func offsetID(seconds int, colon bool) string {
	sign := "+"
	if seconds < 0 {
		sign, seconds = "-", -seconds
	}
	separator := ""
	if colon {
		separator = ":"
	}
	text := sign + padDigits(strconv.Itoa(seconds/3600), 2) + separator + padDigits(strconv.Itoa(seconds%3600/60), 2)
	if seconds%60 != 0 {
		text += separator + padDigits(strconv.Itoa(seconds%60), 2)
	}
	return text
}

// localizedGMTOffset is Java's localized offset: GMT for zero, else GMT+1 /
// GMT+5:30 (short) or GMT+01:00 (full), with seconds when the offset has
// them.
func localizedGMTOffset(seconds int, full bool) string {
	if seconds == 0 {
		return "GMT"
	}
	if full {
		return "GMT" + offsetID(seconds, true)
	}
	sign := "+"
	if seconds < 0 {
		sign, seconds = "-", -seconds
	}
	text := "GMT" + sign + strconv.Itoa(seconds/3600)
	if seconds%3600 != 0 {
		text += ":" + padDigits(strconv.Itoa(seconds%3600/60), 2)
		if seconds%60 != 0 {
			text += ":" + padDigits(strconv.Itoa(seconds%60), 2)
		}
	}
	return text
}

// patternOffsetForm is an X / x / Z field's offset form: whether minutes
// always print (otherwise only when non-zero), the separator, and the text of
// a zero offset. Seconds print, when non-zero, at four letters or more.
func patternOffsetForm(letter byte, count int) (minutes bool, separator, zero string) {
	switch count {
	case 1:
		minutes, separator = false, ""
	case 2, 4:
		minutes, separator = true, ""
	default:
		minutes, separator = true, ":"
	}
	switch letter {
	case 'X':
		zero = "Z"
	case 'x':
		zero = "+00" + map[int]string{1: "", 2: "00", 3: ":00", 4: "00", 5: ":00"}[count]
	case 'Z':
		minutes, separator, zero = true, "", "+0000"
		if count == 5 {
			separator, zero = ":", "Z"
		}
	}
	return minutes, separator, zero
}

func formatPatternOffset(letter byte, count, seconds int) string {
	minutes, separator, zero := patternOffsetForm(letter, count)
	if seconds == 0 {
		return zero
	}
	sign := "+"
	if seconds < 0 {
		sign, seconds = "-", -seconds
	}
	text := sign + padDigits(strconv.Itoa(seconds/3600), 2)
	if minutes || seconds%3600 != 0 {
		text += separator + padDigits(strconv.Itoa(seconds%3600/60), 2)
	}
	if seconds%60 != 0 && count >= 4 {
		text += separator + padDigits(strconv.Itoa(seconds%60), 2)
	}
	return text
}
