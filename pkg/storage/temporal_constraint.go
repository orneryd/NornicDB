package storage

import (
	"fmt"
	"strings"
	"time"
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

// TemporalIntervalsOverlap reports whether the half-open validity intervals
// [aStart, aEnd) and [bStart, bEnd) overlap; an interval without an end is
// open-ended. An interval with a zero start overlaps nothing. The TEMPORAL
// NO OVERLAP constraint and db.temporal.assertNoOverlap both decide overlap
// with it.
func TemporalIntervalsOverlap(aStart, aEnd time.Time, aHasEnd bool, bStart, bEnd time.Time, bHasEnd bool) bool {
	if aStart.IsZero() || bStart.IsZero() {
		return false
	}
	if bHasEnd && !aStart.Before(bEnd) {
		return false
	}
	if aHasEnd && !bStart.Before(aEnd) {
		return false
	}
	return true
}
