package cypher

import (
	"fmt"
	"strings"
	"time"
)

// temporalDateForm is one of the ways a map names a date: calendar (month,
// day), week (week, dayOfWeek), quarter (quarter, dayOfQuarter) or ordinal
// (ordinalDay).
type temporalDateForm struct {
	name   string
	fields []string
}

// temporalDateForms lists the date forms in Neo4j's precedence: a map that
// mixes forms is the first form's date, and a field of a later form is a
// TypeError ("Cannot assign month to week date.").
var temporalDateForms = []temporalDateForm{
	{name: "week", fields: []string{"week", "dayOfWeek"}},
	{name: "calendar", fields: []string{"month", "day"}},
	{name: "quarter", fields: []string{"quarter", "dayOfQuarter"}},
	{name: "ordinal", fields: []string{"ordinalDay"}},
}

// temporalFieldRange is a field's valid values and the name and range text
// Neo4j's error uses ("Invalid value for MonthOfYear (valid values 1 - 12):
// 13").
type temporalFieldRange struct {
	field    string
	name     string
	min, max int64
	valid    string
}

var temporalDateFieldRanges = []temporalFieldRange{
	{"year", "Year", -999_999_999, 999_999_999, "-999999999 - 999999999"},
	{"month", "MonthOfYear", 1, 12, "1 - 12"},
	{"week", "WeekOfWeekBasedYear", 1, 53, "1 - 52/53"},
	{"dayOfWeek", "DayOfWeek", 1, 7, "1 - 7"},
	{"quarter", "QuarterOfYear", 1, 4, "1 - 4"},
	{"dayOfQuarter", "DayOfQuarter", 1, 92, "1 - 90/92"},
	{"ordinalDay", "DayOfYear", 1, 366, "1 - 365/366"},
	{"day", "DayOfMonth", 1, 31, "1 - 28/31"},
}

var temporalTimeFieldRanges = []temporalFieldRange{
	{"hour", "HourOfDay", 0, 23, "0 - 23"},
	{"minute", "MinuteOfHour", 0, 59, "0 - 59"},
	{"second", "SecondOfMinute", 0, 59, "0 - 59"},
}

// temporalFieldsError is the error Neo4j raises for a temporal
// constructor's map (date, localtime, time, localdatetime, datetime), or nil
// when the map names a valid value. The rules, as Neo4j 5.26:
//   - a map mixing date forms is a TypeError (temporalDateForms);
//   - without a base value (date / time / datetime keys), year and hour are
//     required, and each field needs the coarser ones of its form (day needs
//     month, dayOfWeek week, second minute, a sub-second second); a
//     date-time with time fields needs a whole date;
//   - each field has its range (MonthOfYear 1 - 12, HourOfDay 0 - 23, …),
//     and a sub-second's range is what the coarser sub-seconds leave;
//   - a day must exist in its month, and day 366 in its year.
//
// A field a base value supplies is not checked here: the builder clamps it
// (a base 2020-05-31 with month 2 is 2020-02-29).
func temporalFieldsError(kind string, fields map[string]interface{}) error {
	hasDate := kind == "date" || kind == "localdatetime" || kind == "datetime"
	hasTime := kind == "localtime" || kind == "time" || kind == "localdatetime" || kind == "datetime"
	_, baseDate := fields["date"]
	_, baseDateTime := fields["datetime"]
	_, baseTime := fields["time"]
	baseDate = baseDate || baseDateTime
	baseTime = baseTime || baseDateTime
	timeFields := hasAnyTemporalField(fields, "hour", "minute", "second", "millisecond", "microsecond", "nanosecond")

	if hasDate {
		if err := temporalDateFormError(fields); err != nil {
			return err
		}
		if !baseDate {
			if err := temporalDatePresenceError(kind, fields, timeFields); err != nil {
				return err
			}
		}
	}
	if hasTime && !baseTime && (timeFields || kind == "localtime" || kind == "time") {
		if err := temporalTimePresenceError(fields); err != nil {
			return err
		}
	}
	if hasDate {
		if err := temporalRangeError(fields, temporalDateFieldRanges); err != nil {
			return err
		}
		if err := temporalDateValidityError(fields); err != nil {
			return err
		}
	}
	if hasTime {
		if err := temporalRangeError(fields, temporalTimeFieldRanges); err != nil {
			return err
		}
		return temporalSubsecondError(fields)
	}
	return nil
}

// temporalDateFormError is the TypeError for a map that mixes date forms.
func temporalDateFormError(fields map[string]interface{}) error {
	primary := -1
	for index, form := range temporalDateForms {
		if !hasAnyTemporalField(fields, form.fields...) {
			continue
		}
		if primary < 0 {
			primary = index
			continue
		}
		for _, field := range form.fields {
			if _, exists := fields[field]; exists {
				return newSemanticError("Neo.ClientError.Statement.TypeError", "InvalidArgument",
					fmt.Sprintf("Cannot assign %s to %s date.", field, temporalDateForms[primary].name))
			}
		}
	}
	return nil
}

func temporalDatePresenceError(kind string, fields map[string]interface{}, timeFields bool) error {
	if _, exists := fields["year"]; !exists {
		return temporalArgumentError("year must be specified")
	}
	for _, requirement := range [][2]string{{"day", "month"}, {"dayOfWeek", "week"}, {"dayOfQuarter", "quarter"}} {
		_, finer := fields[requirement[0]]
		_, coarser := fields[requirement[1]]
		if finer && !coarser {
			return temporalArgumentError(requirement[0] + " cannot be specified without " + requirement[1])
		}
	}
	if kind == "date" || !timeFields {
		return nil
	}
	// A date-time with a time of day needs a whole date.
	switch {
	case hasAnyTemporalField(fields, "week"):
		if _, exists := fields["dayOfWeek"]; !exists {
			return temporalArgumentError("dayOfWeek must be specified")
		}
	case hasAnyTemporalField(fields, "quarter"):
		if _, exists := fields["dayOfQuarter"]; !exists {
			return temporalArgumentError("dayOfQuarter must be specified")
		}
	case hasAnyTemporalField(fields, "ordinalDay"):
	default:
		if _, exists := fields["month"]; !exists {
			return temporalArgumentError("month must be specified")
		}
		if _, exists := fields["day"]; !exists {
			return temporalArgumentError("day must be specified")
		}
	}
	return nil
}

func temporalTimePresenceError(fields map[string]interface{}) error {
	_, hour := fields["hour"]
	_, minute := fields["minute"]
	_, second := fields["second"]
	subsecond := hasAnyTemporalField(fields, "millisecond", "microsecond", "nanosecond")
	switch {
	case !hour:
		return temporalArgumentError("hour must be specified")
	case second && !minute:
		return temporalArgumentError("second cannot be specified without minute")
	case subsecond && !minute:
		return temporalArgumentError("subsecond cannot be specified without minute")
	case subsecond && !second:
		return temporalArgumentError("subsecond cannot be specified without second")
	}
	return nil
}

func temporalRangeError(fields map[string]interface{}, ranges []temporalFieldRange) error {
	for _, fieldRange := range ranges {
		value, exists := temporalOptionalInt(fields, fieldRange.field)
		if exists && (value < fieldRange.min || value > fieldRange.max) {
			return temporalArgumentError(fmt.Sprintf("Invalid value for %s (valid values %s): %d", fieldRange.name, fieldRange.valid, value))
		}
	}
	return nil
}

// temporalDateValidityError checks that an explicit day exists in its month
// and an explicit ordinal day 366 in its year, with Neo4j's messages.
func temporalDateValidityError(fields map[string]interface{}) error {
	year, hasYear := temporalOptionalInt(fields, "year")
	if !hasYear {
		if base, ok := temporalBaseDate(fields["date"]); ok {
			year = int64(base.Year())
		} else if base, ok := temporalBaseDate(fields["datetime"]); ok {
			year = int64(base.Year())
		}
	}
	leap := isLeapYear(year)
	if ordinal, exists := temporalOptionalInt(fields, "ordinalDay"); exists && ordinal == 366 && !leap {
		return temporalArgumentError(fmt.Sprintf("Invalid date 'DayOfYear 366' as '%d' is not a leap year", year))
	}
	day, hasDay := temporalOptionalInt(fields, "day")
	month, hasMonth := temporalOptionalInt(fields, "month")
	if !hasDay || !hasMonth {
		return nil
	}
	if day <= int64(daysInMonth(year, time.Month(month))) {
		return nil
	}
	if month == 2 && day == 29 {
		return temporalArgumentError(fmt.Sprintf("Invalid date 'February 29' as '%d' is not a leap year", year))
	}
	return temporalArgumentError(fmt.Sprintf("Invalid date '%s %d'", strings.ToUpper(time.Month(month).String()), day))
}

// temporalSubsecondError checks millisecond, microsecond and nanosecond:
// each is below the next coarser sub-second given (a nanosecond with a
// microsecond is 0 - 999).
func temporalSubsecondError(fields map[string]interface{}) error {
	_, hasMillisecond := fields["millisecond"]
	_, hasMicrosecond := fields["microsecond"]
	limits := []struct {
		field, name string
		max         int64
	}{
		{"millisecond", "Millisecond", 999},
		{"microsecond", "Microsecond", 999_999},
		{"nanosecond", "Nanosecond", 999_999_999},
	}
	if hasMillisecond {
		limits[1].max, limits[2].max = 999, 999_999
	}
	if hasMicrosecond {
		limits[2].max = 999
	}
	for _, limit := range limits {
		if value, exists := temporalOptionalInt(fields, limit.field); exists && (value < 0 || value > limit.max) {
			return temporalArgumentError(fmt.Sprintf("Invalid value for %s: %d", limit.name, value))
		}
	}
	return nil
}

func temporalArgumentError(message string) error {
	return newSemanticError("Neo.ClientError.Statement.ArgumentError", "InvalidArgument", message)
}

func isLeapYear(year int64) bool {
	return year%4 == 0 && (year%100 != 0 || year%400 == 0)
}

func daysInMonth(year int64, month time.Month) int {
	return time.Date(int(year), month+1, 0, 0, 0, 0, 0, time.UTC).Day()
}
