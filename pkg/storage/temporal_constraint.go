package storage

import (
	"fmt"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
)

type temporalInterval struct {
	start  time.Time
	end    time.Time
	hasEnd bool
	nodeID NodeID
}

// CoerceTemporalTime reads a point in time from a temporal property or
// argument, in UTC: a time.Time, a Cypher temporal value (anything with
// TemporalTime(), so datetime(), date() and localdatetime() values alike), an
// ISO 8601 string (RFC 3339, or a date or local date-time read as UTC), or
// Unix seconds. It is the one reading used by the TEMPORAL NO OVERLAP
// constraint and the db.temporal procedures, so a value the constraint
// accepts is one the procedures read the same way.
func CoerceTemporalTime(value interface{}) (time.Time, bool) {
	switch v := value.(type) {
	case time.Time:
		return v.UTC(), true
	case *time.Time:
		if v == nil {
			return time.Time{}, false
		}
		return v.UTC(), true
	case string:
		return parseTemporalString(v)
	case int64:
		return time.Unix(v, 0).UTC(), true
	case int:
		return time.Unix(int64(v), 0).UTC(), true
	case float64:
		return time.Unix(int64(v), 0).UTC(), true
	default:
		if temporal, ok := value.(interface{ TemporalTime() time.Time }); ok {
			return temporal.TemporalTime().UTC(), true
		}
		if s, ok := value.(fmt.Stringer); ok {
			return parseTemporalString(s.String())
		}
	}
	return time.Time{}, false
}

func parseTemporalString(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}

	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, layout := range layouts {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func intervalsOverlap(a temporalInterval, b temporalInterval) bool {
	return TemporalIntervalsOverlap(a.start, a.end, a.hasEnd, b.start, b.end, b.hasEnd)
}

// ReadTemporalBounds reads a half-open epoch. A nil start is unbounded past;
// a nil end is unbounded future. At least one bound must be present. Non-nil
// bounds must be valid temporal values; two finite bounds must increase.
// A zero start in the returned representation denotes unbounded past only.
func ReadTemporalBounds(startValue, endValue interface{}) (start, end time.Time, hasEnd, ok bool) {
	hasStart := startValue != nil && startValue != (*time.Time)(nil)
	hasEnd = endValue != nil && endValue != (*time.Time)(nil)
	if !hasStart && !hasEnd {
		return start, end, false, false
	}
	if hasStart {
		start, ok = CoerceTemporalTime(startValue)
		if !ok || start.IsZero() {
			return start, end, false, false
		}
	}
	if hasEnd {
		end, ok = CoerceTemporalTime(endValue)
		if !ok || end.IsZero() {
			return start, end, false, false
		}
		hasEnd = true
	}
	return start, end, hasEnd, validTemporalBounds(start, end, hasEnd)
}

// validTemporalBounds also protects direct overlap callers from treating an
// empty/invalid representation as an all-time claim.
func validTemporalBounds(start, end time.Time, hasEnd bool) bool {
	if start.IsZero() {
		return hasEnd && !end.IsZero()
	}
	return !hasEnd || (!end.IsZero() && start.Before(end))
}

// TemporalIntervalsOverlap compares half-open epochs. A zero start denotes
// unbounded past and hasEnd=false denotes unbounded future. Callers admitting
// external values validate them with ReadTemporalBounds first.
func TemporalIntervalsOverlap(aStart, aEnd time.Time, aHasEnd bool, bStart, bEnd time.Time, bHasEnd bool) bool {
	if !validTemporalBounds(aStart, aEnd, aHasEnd) || !validTemporalBounds(bStart, bEnd, bHasEnd) {
		return false
	}
	if bHasEnd && !aStart.IsZero() && !aStart.Before(bEnd) {
		return false
	}
	if aHasEnd && !bStart.IsZero() && !bStart.Before(aEnd) {
		return false
	}
	return true
}

// temporalBoundsViolation keeps bounds errors consistent across DDL, direct
// writes and transaction preparation. Invalid supplied starts retain their
// existing specific error; symmetric shape errors name both properties.
func temporalBoundsViolation(c Constraint, props map[string]interface{}) error {
	spec, _ := splitTemporalKeySpec(c.Properties)
	message := localization.StorageValidationTemporalBoundsInvalid(spec.startProp, spec.endProp)
	if value := props[spec.startProp]; value != nil {
		if start, ok := CoerceTemporalTime(value); !ok || start.IsZero() {
			message = localization.StorageValidationTemporalStartInvalid(spec.startProp)
		}
	}
	return newLocalizedConstraintViolation(ConstraintTemporal, c.Label, c.Properties, message, nil)
}
