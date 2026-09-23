package cypher

import (
	"strings"
	"time"
	_ "time/tzdata"
)

const (
	neo4jStockholmHistoricalOffset = 53*60 + 28
	neo4jStockholmTransitionYear   = 1893
	neo4jStockholmTransitionMonth  = time.April
	neo4jStockholmTransitionDay    = 1
)

var neo4jStockholmHistoricalLocation = time.FixedZone("Europe/Stockholm", neo4jStockholmHistoricalOffset)

// loadTemporalLocationAt resolves a named zone using Neo4j's Java-time
// semantics. The Java tzdb used by Neo4j starts Europe/Stockholm at +00:53:28
// and transitions to +01:00 on 1893-04-01. Some Unix tzdata builds instead
// expose an earlier local-mean-time offset of +01:12:12, so using the host
// zone database directly makes pre-1893 Cypher values OS-dependent.
func loadTemporalLocationAt(zoneID string, date time.Time) (*time.Location, bool) {
	if zoneID == "Europe/Stockholm" && stockholmUsesNeo4jHistoricalOffset(date) {
		return neo4jStockholmHistoricalLocation, true
	}
	return loadTemporalLocation(zoneID)
}

func stockholmUsesNeo4jHistoricalOffset(date time.Time) bool {
	if date.Year() != neo4jStockholmTransitionYear {
		return date.Year() < neo4jStockholmTransitionYear
	}
	if date.Month() != neo4jStockholmTransitionMonth {
		return date.Month() < neo4jStockholmTransitionMonth
	}
	return date.Day() < neo4jStockholmTransitionDay
}

// normalizeTemporalNamedZone reapplies named-zone rules after calendar
// arithmetic or projection. Fixed historical locations retain their zone ID,
// allowing values that cross the compatibility transition to switch back to
// the regular IANA rule set.
func normalizeTemporalNamedZone(value time.Time) time.Time {
	zoneID := value.Location().String()
	if !strings.Contains(zoneID, "/") {
		return value
	}
	location, ok := loadTemporalLocationAt(zoneID, value)
	if !ok || location == value.Location() {
		return value
	}
	return time.Date(value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond(), location)
}
