package cypher

import (
	"archive/zip"
	"bytes"
	_ "embed"
	"io"
	"strings"
	"sync"
	"time"
)

//go:embed temporal_zoneinfo.zip
var temporalZoneinfoArchive []byte

var (
	temporalZoneFilesOnce sync.Once
	temporalZoneFiles     map[string]*zip.File
	// temporalLocationCache holds the zones loaded from the embedded tzdb
	// archive. Only zones found in the archive are stored, so it holds at most
	// the archive's zone count, which is below its limit: in practice it never
	// clears.
	temporalLocationCache = newBoundedCache[string, *time.Location](4096)
)

// loadTemporalLocationAt resolves a named zone using Neo4j's Java-time
// semantics. The date is retained in the signature because callers resolve a
// zone while constructing a calendar value; all dates now use the same pinned
// server tzdb instead of host-specific historical rules.
func loadTemporalLocationAt(zoneID string, _ time.Time) (*time.Location, bool) {
	return loadTemporalLocation(zoneID)
}

func loadPinnedTemporalLocation(zoneID string) (*time.Location, bool) {
	if zoneID == "UTC" {
		return time.UTC, true
	}
	if id, offset, length, ok := prefixedOffsetZone(zoneID); ok && length == len(zoneID) && id == zoneID {
		return time.FixedZone(zoneID, offset), true
	}
	if cached, ok := temporalLocationCache.get(zoneID); ok {
		return cached, true
	}
	temporalZoneFilesOnce.Do(func() {
		temporalZoneFiles = make(map[string]*zip.File)
		archive, err := zip.NewReader(bytes.NewReader(temporalZoneinfoArchive), int64(len(temporalZoneinfoArchive)))
		if err != nil {
			return
		}
		for _, file := range archive.File {
			if !file.FileInfo().IsDir() {
				temporalZoneFiles[strings.TrimPrefix(file.Name, "./")] = file
			}
		}
	})
	file := temporalZoneFiles[zoneID]
	if file == nil {
		return nil, false
	}
	reader, err := file.Open()
	if err != nil {
		return nil, false
	}
	data, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		return nil, false
	}
	location, err := time.LoadLocationFromTZData(zoneID, data)
	if err != nil {
		return nil, false
	}
	temporalLocationCache.put(zoneID, location)
	return location, true
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

// prefixedOffsetZone reads, at the start of text, a zone that is UTC, GMT or
// UT followed by an offset written +hh:mm or +hh:mm:ss (GMT-05:30), which
// Neo4j takes as a zone of its own: the zone's ID (just the prefix for a
// zero offset: UTC+00:00 is UTC), its offset and the text's length.
func prefixedOffsetZone(text string) (zoneID string, offset, length int, ok bool) {
	for _, prefix := range []string{"UTC", "GMT", "UT"} {
		if !strings.HasPrefix(text, prefix) || len(text) < len(prefix)+6 || text[len(prefix)] != '+' && text[len(prefix)] != '-' {
			continue
		}
		end := len(prefix) + 6
		if len(text) >= end+3 && text[end] == ':' && isDigitByte(text[end+1]) && isDigitByte(text[end+2]) {
			end += 3
		}
		if offset, ok = parseTemporalOffset(text[len(prefix):end]); !ok || offset < -64_800 || offset > 64_800 {
			return "", 0, 0, false
		}
		if offset == 0 {
			return prefix, 0, end, true
		}
		return prefix + offsetID(offset, true), offset, end, true
	}
	return "", 0, 0, false
}
