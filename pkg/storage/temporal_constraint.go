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

func coerceTemporalTime(value interface{}) (time.Time, bool) {
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
	if a.start.IsZero() || b.start.IsZero() {
		return false
	}
	if b.hasEnd && !a.start.Before(b.end) {
		return false
	}
	if a.hasEnd && !b.start.Before(a.end) {
		return false
	}
	return true
}
