package cypher

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// APOC named formats share the Java-pattern compiler; scalar inputs follow
// APOC's Java text contract instead of Cypher format()'s printf extension.
var nativeAPOCNamedPatterns = map[string]string{
	"basic_date":                        "yyyyMMdd",
	"basic_date_time":                   "yyyyMMdd'T'HHmmss.SSSZ",
	"basic_date_time_no_millis":         "yyyyMMdd'T'HHmmssZ",
	"basic_ordinal_date":                "yyyyDDD",
	"basic_ordinal_date_time":           "yyyyDDD'T'HHmmss.SSSZ",
	"basic_ordinal_date_time_no_millis": "yyyyDDD'T'HHmmssZ",
	"basic_time":                        "HHmmss.SSSZ",
	"basic_time_no_millis":              "HHmmssZ",
	"basic_t_time":                      "'T'HHmmss.SSSZ",
	"basic_t_time_no_millis":            "'T'HHmmssZ",
	"basic_week_date":                   "xxxx'W'wwe",
	"basic_week_date_time":              "xxxx'W'wwe'T'HHmmss.SSSZ",
	"basic_week_date_time_no_millis":    "xxxx'W'wwe'T'HHmmssZ",
	"date":                              "yyyy-MM-dd",
	"date_hour":                         "yyyy-MM-dd'T'HH",
	"date_hour_minute":                  "yyyy-MM-dd'T'HH:mm",
	"date_hour_minute_second":           "yyyy-MM-dd'T'HH:mm:ss",
	"date_hour_minute_second_fraction":  "yyyy-MM-dd'T'HH:mm:ss.SSS",
	"date_hour_minute_second_millis":    "yyyy-MM-dd'T'HH:mm:ss.SSS",
	"date_time":                         "yyyy-MM-dd'T'HH:mm:ss.SSSZZ",
	"date_time_no_millis":               "yyy-MM-dd'T'HH:mm:ssZZ",
	"hour":                              "HH",
	"hour_minute":                       "HH:mm",
	"hour_minute_second":                "HH:mm:ss",
	"hour_minute_second_fraction":       "HH:mm:ss.SSS",
	"hour_minute_second_millis":         "HH:mm:ss.SSS",
	"ordinal_date":                      "yyyy-DDD",
	"ordinal_date_time":                 "yyyy-DDD'T'HH:mm:ss.SSSZZ",
	"ordinal_date_time_no_millis":       "yyyy-DDD'T'HH:mm:ssZZ",
	"time":                              "HH:mm:ss.SSSZZ",
	"time_no_millis":                    "HH:mm:ssZZ",
	"t_time":                            "'T'HH:mm:ss.SSSZZ",
	"t_time_no_millis":                  "'T'HH:mm:ssZZ",
	"week_date":                         "xxxx-'W'ww-e",
	"week_date_time":                    "xxxx-'W'ww-e'T'HH:mm:ss.SSSZZ",
	"week_date_time_no_millis":          "xxxx-'W'ww-e'T'HH:mm:ssZZ",
	"weekyear":                          "xxxx",
	"weekyear_week":                     "xxxx-'W'ww",
	"weekyear_week_day":                 "xxxx-'W'ww-e",
	"year":                              "yyyy",
	"year_month":                        "yyyy-MM",
	"year_month_day":                    "yyyy-MM-dd",
}

func nativeAPOCTemporalFormat(ctx context.Context, args []interface{}) (interface{}, error) {
	const name = "apoc.temporal.format"
	value := args[0]
	pattern := "yyyy-MM-dd"
	if len(args) > 1 {
		var ok bool
		pattern, ok = args[1].(string)
		if !ok {
			return nil, invalidFunctionArgument(name, args[1])
		}
	}
	lower := lowerASCII(pattern)
	namedISO := strings.HasPrefix(lower, "iso_") || lower == "basic_iso_date" || lower == "rfc_1123_date_time"
	if resolved, ok := nativeAPOCNamedPatterns[lower]; ok {
		pattern = resolved
	}
	if namedISO {
		var known bool
		pattern, known = nativeAPOCISOBasePattern(lower)
		if !known {
			return nil, nativeAPOCCallFailure(name, args)
		}
	}
	items, valid := compileTemporalPattern(pattern)
	if !valid || value == nil {
		return nil, nativeAPOCCallFailure(name, value)
	}
	if temporal, ok := patternTemporalOf(value); ok {
		if namedISO {
			if text, valid := nativeAPOCISOFormat(lower, temporal); valid {
				return text, nil
			}
			return nil, nativeAPOCCallFailure(name, value)
		}
		text, valid := formatTemporalPattern(items, temporal)
		if !valid {
			return nil, nativeAPOCCallFailure(name, value)
		}
		return text, nil
	}
	if duration, ok := value.(*CypherDuration); ok {
		if namedISO {
			switch lower {
			case "iso_date_time", "iso_local_date_time":
				pattern = "yyyy-MM-dd'T'HH:mm:ssI"
			case "iso_time", "iso_local_time":
				pattern = "HH:mm:ssI"
			}
		}
		return nativeAPOCDurationFormat(duration, pattern), nil
	}
	return nativeAPOCJavaText(valueWalk(ctx), value), nil
}

func nativeAPOCISOBasePattern(name string) (string, bool) {
	switch name {
	case "basic_iso_date":
		return "yyyyMMdd", true
	case "iso_date", "iso_local_date", "iso_offset_date":
		return "yyyy-MM-dd", true
	case "iso_date_time", "iso_local_date_time", "iso_offset_date_time", "iso_zoned_date_time", "iso_instant":
		return "yyyy-MM-dd'T'HH:mm:ss", true
	case "iso_time", "iso_local_time", "iso_offset_time":
		return "HH:mm:ss", true
	case "iso_ordinal_date":
		return "yyyy-DDD", true
	case "iso_week_date":
		return "YYYY-'W'ww-e", true
	case "rfc_1123_date_time":
		return "EEE, d MMM yyyy HH:mm:ss Z", true
	}
	return "", false
}

func nativeAPOCISOFormat(name string, t patternTemporal) (string, bool) {
	switch name {
	case "iso_local_date_time":
		if t.date && t.clock {
			return formatTemporalDateTime(t.value, false, "", true), true
		}
	case "iso_date_time", "iso_zoned_date_time", "iso_offset_date_time":
		if !t.date || !t.clock || (name == "iso_offset_date_time" && !t.offset) || (name == "iso_zoned_date_time" && !t.zoned) {
			return "", false
		}
		zone := ""
		if name != "iso_offset_date_time" {
			zone = t.zoneID
		}
		return formatTemporalDateTime(t.value, t.offset, zone, true), true
	case "iso_instant":
		if t.date && t.clock && t.offset {
			return t.value.UTC().Format(time.RFC3339Nano), true
		}
	case "iso_local_time":
		if t.clock {
			return formatTemporalClock(t.value, false, "", true), true
		}
	case "iso_time", "iso_offset_time":
		if t.clock && (name != "iso_offset_time" || t.offset) {
			return formatTemporalClock(t.value, t.offset, "", true), true
		}
	case "iso_date", "iso_local_date", "iso_offset_date", "basic_iso_date":
		if !t.date || (name == "iso_offset_date" && !t.offset) {
			return "", false
		}
		pattern := "yyyy-MM-dd"
		if name == "basic_iso_date" {
			pattern = "yyyyMMdd"
		}
		if t.offset && name != "iso_local_date" {
			if name == "basic_iso_date" {
				pattern += "XX"
			} else {
				pattern += "XXX"
			}
		}
		items, _ := compileTemporalPattern(pattern)
		return formatTemporalPattern(items, t)
	case "iso_ordinal_date", "iso_week_date":
		if !t.date {
			return "", false
		}
		text := fmt.Sprintf("%04d-%03d", t.value.Year(), t.value.YearDay())
		if name == "iso_week_date" {
			year, week := t.value.ISOWeek()
			day := int(t.value.Weekday())
			if day == 0 {
				day = 7
			}
			text = fmt.Sprintf("%04d-W%02d-%d", year, week, day)
		}
		if t.offset {
			items, _ := compileTemporalPattern("XXX")
			offset, ok := formatTemporalPattern(items, t)
			if !ok {
				return "", false
			}
			text += offset
		}
		return text, true
	case "rfc_1123_date_time":
		if !t.date || !t.clock || !t.offset {
			return "", false
		}
		zone := t.value.Format("-0700")
		if _, offset := t.value.Zone(); offset == 0 {
			zone = "GMT"
		}
		return t.value.Format("Mon, 2 Jan 2006 15:04:05 ") + zone, true
	}
	return "", false
}

// APOC's DurationFormatUtil uses fixed normalized fields, independently of
// which other fields the pattern includes. Its S/n operation pads the raw
// nanosecond integer before truncating, unlike Cypher format(duration,...).
func nativeAPOCDurationFormat(duration *CypherDuration, pattern string) string {
	months, days, seconds, nanos := durationGroups(duration)
	seconds += nanos / 1_000_000_000
	nanos %= 1_000_000_000
	if nanos < 0 {
		seconds--
		nanos += 1_000_000_000
	}
	var out strings.Builder
	quoted := false
	for i := 0; i < len(pattern); {
		c := pattern[i]
		if c == '\'' {
			quoted = !quoted
			i++
			continue
		}
		if quoted {
			out.WriteByte(c)
			i++
			continue
		}
		j := i + 1
		for j < len(pattern) && pattern[j] == c {
			j++
		}
		width := j - i
		var value int64
		field := true
		fraction := false
		switch c {
		case 'y', 'Y', 'u':
			value = months / 12
		case 'M', 'L':
			value = months % 12
		case 'q', 'Q':
			value = (months % 12) / 3
		case 'd', 'D':
			value = days
		case 'w', 'W':
			value = days / 7
		case 'H', 'h', 'k', 'K':
			value = seconds / 3600
		case 'm':
			value = (seconds / 60) % 60
		case 's':
			value = seconds % 60
		case 'S', 'n':
			value = nanos
			fraction = true
		case 'A':
			value = seconds*1000 + nanos/1_000_000
		case 'N':
			if width < 2 {
				field = false
			} else {
				value = seconds*1_000_000_000 + nanos
			}
		case 'I':
			text := strings.TrimRight(fmt.Sprintf("%d", nanos), "0")
			if text != "" {
				out.WriteByte('.')
				out.WriteString(text)
			}
			i = j
			continue
		default:
			field = false
		}
		if field {
			text := fmt.Sprintf("%0*d", width, value)
			if fraction && len(text) > width {
				text = text[:width]
			}
			out.WriteString(text)
		} else {
			out.WriteString(pattern[i:j])
		}
		i = j
	}
	return out.String()
}

func nativeAPOCJavaText(work *materializedRowCheckpoint, value interface{}) string {
	if work.visit() {
		return ""
	}
	if value == nil {
		return "null"
	}
	if object, ok := value.(map[string]interface{}); ok {
		keys := make([]string, 0, len(object))
		for key := range object {
			keys = append(keys, key)
		}
		if work.sortKeys(keys) {
			return ""
		}
		parts := make([]string, len(keys))
		for i, key := range keys {
			parts[i] = key + "=" + nativeAPOCJavaText(work, object[key])
		}
		return "{" + strings.Join(parts, ", ") + "}"
	}
	if list, ok := toInterfaceSlice(value); ok {
		parts := make([]string, len(list))
		for i, v := range list {
			parts[i] = nativeAPOCJavaText(work, v)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
	return formatCypherValueString(value)
}
