// Package temporal provides APOC temporal/datetime functions.
//
// This package implements all apoc.temporal.* functions for working
// with dates, times, and durations.
package temporal

import (
	"fmt"
	"time"
)

// Format formats a time value.
//
// Example:
//
//	apoc.temporal.format(time.Now(), 'yyyy-MM-dd') => '2024-01-15'
func Format(t time.Time, format string) string {
	// Convert Java SimpleDateFormat to Go format
	goFormat := convertFormat(format)
	return t.Format(goFormat)
}

// convertFormat converts Java date format to Go format.
func convertFormat(javaFormat string) string {
	// Simplified conversion
	// For production, implement full Java SimpleDateFormat conversion
	replacements := map[string]string{
		"yyyy": "2006",
		"MM":   "01",
		"dd":   "02",
		"HH":   "15",
		"mm":   "04",
		"ss":   "05",
	}

	result := javaFormat
	for java, golang := range replacements {
		result = replaceAll(result, java, golang)
	}

	return result
}

func replaceAll(s, old, new string) string {
	// Simple string replacement
	for i := 0; i < len(s); i++ {
		if i+len(old) <= len(s) && s[i:i+len(old)] == old {
			s = s[:i] + new + s[i+len(old):]
			i += len(new) - 1
		}
	}
	return s
}

// Parse parses a time string.
//
// Example:
//
//	apoc.temporal.parse('2024-01-15', 'yyyy-MM-dd') => time
func Parse(value, format string) (time.Time, error) {
	goFormat := convertFormat(format)
	return time.Parse(goFormat, value)
}

// FormatDuration formats a duration.
//
// Example:
//
//	apoc.temporal.formatDuration(duration, 'HH:mm:ss') => '01:30:45'
func FormatDuration(d time.Duration, format string) string {
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
}

// ToEpochMillis converts time to epoch milliseconds.
//
// Example:
//
//	apoc.temporal.toEpochMillis(time.Now()) => 1705276800000
func ToEpochMillis(t time.Time) int64 {
	return t.UnixMilli()
}

// FromEpochMillis converts epoch milliseconds to time.
//
// Example:
//
//	apoc.temporal.fromEpochMillis(1705276800000) => time
func FromEpochMillis(millis int64) time.Time {
	return time.UnixMilli(millis)
}

// Add adds duration to time.
//
// Example:
//
//	apoc.temporal.add(time.Now(), 1, 'days') => time + 1 day
func Add(t time.Time, amount int, unit string) time.Time {
	switch unit {
	case "years", "year":
		return t.AddDate(amount, 0, 0)
	case "months", "month":
		return t.AddDate(0, amount, 0)
	case "days", "day":
		return t.AddDate(0, 0, amount)
	case "hours", "hour":
		return t.Add(time.Duration(amount) * time.Hour)
	case "minutes", "minute":
		return t.Add(time.Duration(amount) * time.Minute)
	case "seconds", "second":
		return t.Add(time.Duration(amount) * time.Second)
	default:
		return t
	}
}

// Subtract subtracts duration from time.
//
// Example:
//
//	apoc.temporal.subtract(time.Now(), 1, 'days') => time - 1 day
func Subtract(t time.Time, amount int, unit string) time.Time {
	return Add(t, -amount, unit)
}

// Difference calculates difference between two times.
//
// Example:
//
//	apoc.temporal.difference(time1, time2, 'days') => difference in days
func Difference(t1, t2 time.Time, unit string) int64 {
	diff := t2.Sub(t1)

	switch unit {
	case "years", "year":
		return int64(diff.Hours() / 24 / 365)
	case "months", "month":
		return int64(diff.Hours() / 24 / 30)
	case "days", "day":
		return int64(diff.Hours() / 24)
	case "hours", "hour":
		return int64(diff.Hours())
	case "minutes", "minute":
		return int64(diff.Minutes())
	case "seconds", "second":
		return int64(diff.Seconds())
	default:
		return 0
	}
}

// StartOf returns start of time unit.
//
// Example:
//
//	apoc.temporal.startOf(time.Now(), 'day') => start of day
func StartOf(t time.Time, unit string) time.Time {
	switch unit {
	case "year":
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
	case "month":
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	case "day":
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case "hour":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	case "minute":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
	default:
		return t
	}
}

// EndOf returns end of time unit.
//
// Example:
//
//	apoc.temporal.endOf(time.Now(), 'day') => end of day
func EndOf(t time.Time, unit string) time.Time {
	start := StartOf(t, unit)
	return Add(start, 1, unit).Add(-time.Nanosecond)
}

// IsBetween checks if time is between two times.
//
// Example:
//
//	apoc.temporal.isBetween(time, start, end) => true/false
func IsBetween(t, start, end time.Time) bool {
	return (t.After(start) || t.Equal(start)) && (t.Before(end) || t.Equal(end))
}

// IsWeekend checks if time is on weekend.
//
// Example:
//
//	apoc.temporal.isWeekend(time.Now()) => true/false
func IsWeekend(t time.Time) bool {
	weekday := t.Weekday()
	return weekday == time.Saturday || weekday == time.Sunday
}

// IsWeekday checks if time is on weekday.
//
// Example:
//
//	apoc.temporal.isWeekday(time.Now()) => true/false
func IsWeekday(t time.Time) bool {
	return !IsWeekend(t)
}

// DayOfWeek returns day of week (0-6, Sunday=0).
//
// Example:
//
//	apoc.temporal.dayOfWeek(time.Now()) => 1 (Monday)
func DayOfWeek(t time.Time) int {
	return int(t.Weekday())
}

// DayOfYear returns day of year (1-366).
//
// Example:
//
//	apoc.temporal.dayOfYear(time.Now()) => 15
func DayOfYear(t time.Time) int {
	return t.YearDay()
}

// WeekOfYear returns week of year.
//
// Example:
//
//	apoc.temporal.weekOfYear(time.Now()) => 3
func WeekOfYear(t time.Time) int {
	_, week := t.ISOWeek()
	return week
}

// Quarter returns quarter of year (1-4).
//
// Example:
//
//	apoc.temporal.quarter(time.Now()) => 1
func Quarter(t time.Time) int {
	return (int(t.Month()) + 2) / 3
}

// IsLeapYear checks if year is a leap year.
//
// Example:
//
//	apoc.temporal.isLeapYear(2024) => true
func IsLeapYear(year int) bool {
	return year%4 == 0 && (year%100 != 0 || year%400 == 0)
}

// DaysInMonth returns number of days in month.
//
// Example:
//
//	apoc.temporal.daysInMonth(2024, 2) => 29
func DaysInMonth(year, month int) int {
	t := time.Date(year, time.Month(month+1), 0, 0, 0, 0, 0, time.UTC)
	return t.Day()
}

// Age calculates age from birthdate.
//
// Example:
//
//	apoc.temporal.age(birthdate) => age in years
func Age(birthdate time.Time) int {
	now := time.Now()
	age := now.Year() - birthdate.Year()

	if now.Month() < birthdate.Month() ||
		(now.Month() == birthdate.Month() && now.Day() < birthdate.Day()) {
		age--
	}

	return age
}

// Duration creates a duration.
//
// Example:
//
//	apoc.temporal.duration(1, 'hours') => 1 hour duration
func Duration(amount int, unit string) time.Duration {
	switch unit {
	case "hours", "hour":
		return time.Duration(amount) * time.Hour
	case "minutes", "minute":
		return time.Duration(amount) * time.Minute
	case "seconds", "second":
		return time.Duration(amount) * time.Second
	case "milliseconds", "millisecond":
		return time.Duration(amount) * time.Millisecond
	default:
		return 0
	}
}

// Truncate truncates time to unit.
//
// Example:
//
//	apoc.temporal.truncate(time.Now(), 'hour') => time truncated to hour
func Truncate(t time.Time, unit string) time.Time {
	return StartOf(t, unit)
}

// Round rounds time to nearest unit.
//
// Example:
//
//	apoc.temporal.round(time.Now(), 'hour') => time rounded to nearest hour
func Round(t time.Time, unit string) time.Time {
	start := StartOf(t, unit)
	end := Add(start, 1, unit)

	if t.Sub(start) < end.Sub(t) {
		return start
	}
	return end
}

// Timezone converts time to timezone.
//
// Example:
//
//	apoc.temporal.timezone(time.Now(), 'America/New_York') => time in NY timezone
func Timezone(t time.Time, tz string) (time.Time, error) {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return t, err
	}
	return t.In(loc), nil
}

// ToUTC converts time to UTC.
//
// Example:
//
//	apoc.temporal.toUTC(time.Now()) => time in UTC
func ToUTC(t time.Time) time.Time {
	return t.UTC()
}

// ToLocal converts time to local timezone.
//
// Example:
//
//	apoc.temporal.toLocal(time.Now()) => time in local timezone
func ToLocal(t time.Time) time.Time {
	return t.Local()
}
