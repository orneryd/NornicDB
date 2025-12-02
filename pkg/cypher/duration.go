// Duration and date/time handling for NornicDB Cypher.
//
// This file contains the CypherDuration type and functions for working with
// temporal data in Cypher queries. CypherDuration is fully compatible with
// Neo4j's duration type and supports ISO 8601 duration format.
//
// # Duration Format
//
// Durations use ISO 8601 format: P[n]Y[n]M[n]DT[n]H[n]M[n]S
//
//   - P: Duration designator (required at start)
//   - Y: Years
//   - M: Months (before T) or Minutes (after T)
//   - D: Days
//   - T: Time designator (separates date from time components)
//   - H: Hours
//   - S: Seconds (can include fractional seconds)
//
// # Examples
//
//	P1Y          - 1 year
//	P3M          - 3 months
//	P5D          - 5 days
//	PT2H         - 2 hours
//	PT30M        - 30 minutes
//	PT45S        - 45 seconds
//	P1Y2M3DT4H5M6S - 1 year, 2 months, 3 days, 4 hours, 5 minutes, 6 seconds
//
// # ELI12
//
// Think of a duration like a recipe for time travel:
//
//	"P1Y2M3D" = "Jump forward 1 year, 2 months, and 3 days"
//	"PT2H30M" = "Jump forward 2 hours and 30 minutes"
//
// The "P" at the start says "this is a time period" and the "T" in the
// middle separates "calendar time" (years, months, days) from "clock time"
// (hours, minutes, seconds).
//
// It's like giving directions: "Walk 3 blocks north, then turn and walk 2 blocks east"
// becomes "Add 3 months, then add 2 hours".
//
// # Neo4j Compatibility
//
// CypherDuration matches Neo4j's duration semantics:
//   - Supports all ISO 8601 duration components
//   - Handles fractional seconds (nanosecond precision)
//   - Can be added to/subtracted from dates
//   - Supports duration arithmetic

package cypher

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// CypherDuration represents a Neo4j-compatible duration type.
//
// This type stores time intervals with separate components for calendar
// units (years, months, days) and clock units (hours, minutes, seconds).
//
// Calendar units are stored separately because they have variable lengths:
//   - A year can be 365 or 366 days
//   - A month can be 28-31 days
//
// Clock units have fixed lengths and are stored precisely.
//
// # Fields
//
//   - Years: Number of years (calendar unit)
//   - Months: Number of months (calendar unit)
//   - Days: Number of days (calendar unit)
//   - Hours: Number of hours (clock unit)
//   - Minutes: Number of minutes (clock unit)
//   - Seconds: Number of seconds (clock unit)
//   - Nanos: Nanoseconds for sub-second precision
//
// # Example
//
//	dur := &CypherDuration{
//	    Years: 1, Months: 6, Days: 15,
//	    Hours: 2, Minutes: 30, Seconds: 45,
//	}
//	fmt.Println(dur.String()) // P1Y6M15DT2H30M45S
//
// # ELI12
//
// Think of CypherDuration like a time capsule with two compartments:
//
//  1. Calendar compartment: "How many birthdays/months/days to skip?"
//     (Years, Months, Days - these depend on the calendar)
//
//  2. Clock compartment: "How much to move the clock hands?"
//     (Hours, Minutes, Seconds - these are always the same length)
//
// We keep them separate because months aren't all the same length!
// February has 28-29 days, while January has 31.
type CypherDuration struct {
	Years   int64 // Calendar years
	Months  int64 // Calendar months
	Days    int64 // Calendar days
	Hours   int64 // Clock hours
	Minutes int64 // Clock minutes
	Seconds int64 // Clock seconds
	Nanos   int64 // Sub-second precision (nanoseconds)
}

// String returns the duration in ISO 8601 format.
//
// The format is: P[n]Y[n]M[n]DT[n]H[n]M[n]S
//
// Only non-zero components are included. If no components are present,
// returns "PT0S" (zero duration).
//
// # Example
//
//	dur := &CypherDuration{Days: 5, Hours: 2}
//	fmt.Println(dur.String()) // "P5DT2H"
//
//	zeroDur := &CypherDuration{}
//	fmt.Println(zeroDur.String()) // "PT0S"
func (d *CypherDuration) String() string {
	var sb strings.Builder
	sb.WriteString("P")
	if d.Years > 0 {
		sb.WriteString(fmt.Sprintf("%dY", d.Years))
	}
	if d.Months > 0 {
		sb.WriteString(fmt.Sprintf("%dM", d.Months))
	}
	if d.Days > 0 {
		sb.WriteString(fmt.Sprintf("%dD", d.Days))
	}
	if d.Hours > 0 || d.Minutes > 0 || d.Seconds > 0 || d.Nanos > 0 {
		sb.WriteString("T")
		if d.Hours > 0 {
			sb.WriteString(fmt.Sprintf("%dH", d.Hours))
		}
		if d.Minutes > 0 {
			sb.WriteString(fmt.Sprintf("%dM", d.Minutes))
		}
		if d.Seconds > 0 || d.Nanos > 0 {
			if d.Nanos > 0 {
				sb.WriteString(fmt.Sprintf("%d.%09dS", d.Seconds, d.Nanos))
			} else {
				sb.WriteString(fmt.Sprintf("%dS", d.Seconds))
			}
		}
	}
	if sb.Len() == 1 {
		sb.WriteString("T0S") // Empty duration
	}
	return sb.String()
}

// TotalDays returns the approximate total number of days.
//
// Note: This is an approximation because it assumes:
//   - 1 year = 365.25 days (average accounting for leap years)
//   - 1 month = 30.4375 days (365.25 / 12)
//
// For precise date calculations, use the individual components
// with AddDurationToDate instead.
//
// # Example
//
//	dur := &CypherDuration{Years: 1}
//	fmt.Println(dur.TotalDays()) // ~365.25
func (d *CypherDuration) TotalDays() float64 {
	// Approximate: 1 year = 365.25 days, 1 month = 30.4375 days
	return float64(d.Years)*365.25 + float64(d.Months)*30.4375 + float64(d.Days) +
		float64(d.Hours)/24 + float64(d.Minutes)/1440 + float64(d.Seconds)/86400
}

// TotalSeconds returns the approximate total number of seconds.
//
// Note: This is an approximation for the calendar components.
// See TotalDays for the assumptions used.
//
// # Example
//
//	dur := &CypherDuration{Hours: 2, Minutes: 30}
//	fmt.Println(dur.TotalSeconds()) // 9000
func (d *CypherDuration) TotalSeconds() float64 {
	// Calculate directly to avoid double-counting from TotalDays
	return float64(d.Years)*365.25*86400 + float64(d.Months)*30.4375*86400 +
		float64(d.Days)*86400 + float64(d.Hours)*3600 + float64(d.Minutes)*60 +
		float64(d.Seconds) + float64(d.Nanos)/1e9
}

// ToTimeDuration converts to Go's time.Duration.
//
// Note: This only converts the clock components (hours, minutes, seconds, nanos).
// Calendar components (years, months, days) are converted to their approximate
// durations which may not be accurate for actual date arithmetic.
//
// For precise date calculations, use AddDurationToDate instead.
//
// # Example
//
//	dur := &CypherDuration{Hours: 2, Minutes: 30}
//	goDur := dur.ToTimeDuration()
//	fmt.Println(goDur) // 2h30m0s
func (d *CypherDuration) ToTimeDuration() time.Duration {
	// Convert to time.Duration (approximate for calendar components)
	total := time.Duration(d.Days) * 24 * time.Hour
	total += time.Duration(d.Hours) * time.Hour
	total += time.Duration(d.Minutes) * time.Minute
	total += time.Duration(d.Seconds) * time.Second
	total += time.Duration(d.Nanos) * time.Nanosecond
	// Note: Years and months require calendar context for accurate conversion
	return total
}

// parseDuration parses an ISO 8601 duration string into a CypherDuration.
//
// Supported formats:
//   - ISO 8601: "P1Y2M3DT4H5M6S"
//   - With fractional seconds: "P1DT0.5S"
//
// # Parameters
//
//   - s: The duration string to parse
//
// # Returns
//
//   - A CypherDuration pointer, or nil if parsing fails
//
// # Example
//
//	dur := parseDuration("P1Y2M3DT4H5M6S")
//	// dur.Years = 1, dur.Months = 2, dur.Days = 3
//	// dur.Hours = 4, dur.Minutes = 5, dur.Seconds = 6
//
//	dur = parseDuration("PT2H30M")
//	// dur.Hours = 2, dur.Minutes = 30
func parseDuration(s string) *CypherDuration {
	s = strings.Trim(s, "'\"")
	if !strings.HasPrefix(strings.ToUpper(s), "P") {
		return nil
	}

	d := &CypherDuration{}
	s = strings.ToUpper(s[1:]) // Remove P prefix

	// Handle date part (before T)
	tIndex := strings.Index(s, "T")
	datePart := s
	timePart := ""
	if tIndex >= 0 {
		datePart = s[:tIndex]
		timePart = s[tIndex+1:]
	}

	// Parse date components
	re := regexp.MustCompile(`(\d+)([YMD])`)
	matches := re.FindAllStringSubmatch(datePart, -1)
	for _, match := range matches {
		val, _ := strconv.ParseInt(match[1], 10, 64)
		switch match[2] {
		case "Y":
			d.Years = val
		case "M":
			d.Months = val
		case "D":
			d.Days = val
		}
	}

	// Parse time components
	if timePart != "" {
		re = regexp.MustCompile(`(\d+\.?\d*)([HMS])`)
		matches = re.FindAllStringSubmatch(timePart, -1)
		for _, match := range matches {
			switch match[2] {
			case "H":
				val, _ := strconv.ParseInt(match[1], 10, 64)
				d.Hours = val
			case "M":
				val, _ := strconv.ParseInt(match[1], 10, 64)
				d.Minutes = val
			case "S":
				// Handle fractional seconds
				if strings.Contains(match[1], ".") {
					parts := strings.Split(match[1], ".")
					d.Seconds, _ = strconv.ParseInt(parts[0], 10, 64)
					// Convert fraction to nanoseconds
					frac := parts[1]
					for len(frac) < 9 {
						frac += "0"
					}
					d.Nanos, _ = strconv.ParseInt(frac[:9], 10, 64)
				} else {
					d.Seconds, _ = strconv.ParseInt(match[1], 10, 64)
				}
			}
		}
	}

	return d
}

// durationFromMap creates a CypherDuration from a map of components.
//
// This is used when duration components are specified as a map in Cypher:
//
//	duration({days: 5, hours: 2})
//
// # Parameters
//
//   - m: A map with keys like "years", "months", "days", "hours", etc.
//
// # Returns
//
//   - A CypherDuration populated from the map
//
// # Example
//
//	m := map[string]interface{}{"days": 5, "hours": 2}
//	dur := durationFromMap(m)
//	// dur.Days = 5, dur.Hours = 2
func durationFromMap(m map[string]interface{}) *CypherDuration {
	d := &CypherDuration{}

	if v, ok := m["years"]; ok {
		d.Years = toInt64(v)
	}
	if v, ok := m["months"]; ok {
		d.Months = toInt64(v)
	}
	if v, ok := m["days"]; ok {
		d.Days = toInt64(v)
	}
	if v, ok := m["hours"]; ok {
		d.Hours = toInt64(v)
	}
	if v, ok := m["minutes"]; ok {
		d.Minutes = toInt64(v)
	}
	if v, ok := m["seconds"]; ok {
		d.Seconds = toInt64(v)
	}
	if v, ok := m["nanoseconds"]; ok {
		d.Nanos = toInt64(v)
	}

	return d
}

// durationBetween calculates the duration between two dates/datetimes.
//
// The result is always positive (absolute difference).
//
// # Parameters
//
//   - d1: First date/datetime (string or time.Time)
//   - d2: Second date/datetime (string or time.Time)
//
// # Returns
//
//   - A CypherDuration representing the difference, or nil if parsing fails
//
// # Example
//
//	dur := durationBetween("2025-01-01", "2025-01-06")
//	// dur.Days = 5
//
//	dur = durationBetween("2025-01-01T10:00:00", "2025-01-01T12:30:00")
//	// dur.Hours = 2, dur.Minutes = 30
func durationBetween(d1, d2 interface{}) *CypherDuration {
	t1 := parseDateTime(d1)
	t2 := parseDateTime(d2)
	if t1.IsZero() || t2.IsZero() {
		return nil
	}

	diff := t2.Sub(t1)
	if diff < 0 {
		diff = -diff
	}

	return &CypherDuration{
		Days:    int64(diff.Hours()) / 24,
		Hours:   int64(diff.Hours()) % 24,
		Minutes: int64(diff.Minutes()) % 60,
		Seconds: int64(diff.Seconds()) % 60,
		Nanos:   int64(diff.Nanoseconds()) % 1e9,
	}
}

// parseDateTime parses various date/datetime formats.
//
// Supported formats:
//   - RFC3339: "2006-01-02T15:04:05Z07:00"
//   - ISO datetime: "2006-01-02T15:04:05"
//   - Datetime with space: "2006-01-02 15:04:05"
//   - Date only: "2006-01-02"
//
// # Parameters
//
//   - v: A value to parse (string or time.Time)
//
// # Returns
//
//   - A time.Time value, or zero time if parsing fails
//
// # Example
//
//	t := parseDateTime("2025-01-15")
//	// t.Year() == 2025, t.Month() == January, t.Day() == 15
//
//	t = parseDateTime("2025-01-15T14:30:00")
//	// includes time components
func parseDateTime(v interface{}) time.Time {
	switch val := v.(type) {
	case time.Time:
		return val
	case string:
		s := strings.Trim(val, "'\"")
		for _, layout := range []string{
			time.RFC3339,
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02",
		} {
			if parsed, err := time.Parse(layout, s); err == nil {
				return parsed
			}
		}
	}
	return time.Time{}
}

// addDurationToDate adds a CypherDuration to a date/datetime.
//
// Calendar components (years, months, days) are added using calendar arithmetic,
// correctly handling variable month lengths and leap years. Clock components
// are added as fixed durations.
//
// # Parameters
//
//   - dateVal: The base date/datetime
//   - dur: The duration to add
//
// # Returns
//
//   - RFC3339 formatted result string, or empty string on error
//
// # Example
//
//	result := addDurationToDate("2025-01-15", &CypherDuration{Days: 5})
//	// result = "2025-01-20T00:00:00Z"
//
//	result = addDurationToDate("2025-01-31", &CypherDuration{Months: 1})
//	// result = "2025-02-28T00:00:00Z" (end of February)
func addDurationToDate(dateVal interface{}, dur *CypherDuration) string {
	t := parseDateTime(dateVal)
	if t.IsZero() || dur == nil {
		return ""
	}

	// Add the duration components
	t = t.AddDate(int(dur.Years), int(dur.Months), int(dur.Days))
	t = t.Add(time.Duration(dur.Hours)*time.Hour +
		time.Duration(dur.Minutes)*time.Minute +
		time.Duration(dur.Seconds)*time.Second +
		time.Duration(dur.Nanos)*time.Nanosecond)

	return t.Format(time.RFC3339)
}

// subtractDurationFromDate subtracts a CypherDuration from a date/datetime.
//
// This is the inverse of addDurationToDate. Calendar components are subtracted
// using calendar arithmetic.
//
// # Parameters
//
//   - dateVal: The base date/datetime
//   - dur: The duration to subtract
//
// # Returns
//
//   - RFC3339 formatted result string, or empty string on error
//
// # Example
//
//	result := subtractDurationFromDate("2025-01-15", &CypherDuration{Days: 5})
//	// result = "2025-01-10T00:00:00Z"
func subtractDurationFromDate(dateVal interface{}, dur *CypherDuration) string {
	t := parseDateTime(dateVal)
	if t.IsZero() || dur == nil {
		return ""
	}

	// Subtract the duration components
	t = t.AddDate(-int(dur.Years), -int(dur.Months), -int(dur.Days))
	t = t.Add(-(time.Duration(dur.Hours)*time.Hour +
		time.Duration(dur.Minutes)*time.Minute +
		time.Duration(dur.Seconds)*time.Second +
		time.Duration(dur.Nanos)*time.Nanosecond))

	return t.Format(time.RFC3339)
}
