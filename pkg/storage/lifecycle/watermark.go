package lifecycle

import (
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// ComputeSafeFloor returns a monotonic safe floor from runtime and retention bounds.
func ComputeSafeFloor(oldestReaderVersion, ttlBoundVersion, maxVersionsBoundVersion, previousFloor storage.MVCCVersion) storage.MVCCVersion {
	safeFloor := minVersion(oldestReaderVersion, ttlBoundVersion, maxVersionsBoundVersion)
	return monotonicMax(previousFloor, safeFloor)
}

// TTLBoundVersion returns the version bound for TTL-based retention.
func TTLBoundVersion(ttl time.Duration) storage.MVCCVersion {
	if ttl <= 0 {
		return maxVersion()
	}
	return storage.MVCCVersion{CommitTimestamp: time.Now().UTC().Add(-ttl), CommitSequence: ^uint64(0)}
}

func minVersion(versions ...storage.MVCCVersion) storage.MVCCVersion {
	best := maxVersion()
	for _, version := range versions {
		if version.IsZero() {
			continue
		}
		if version.Compare(best) < 0 {
			best = version
		}
	}
	return best
}

func monotonicMax(a, b storage.MVCCVersion) storage.MVCCVersion {
	if a.IsZero() {
		return b
	}
	if b.IsZero() {
		return a
	}
	if a.Compare(b) >= 0 {
		return a
	}
	return b
}

func maxVersion() storage.MVCCVersion {
	return storage.MVCCVersion{CommitTimestamp: time.Unix(0, math.MaxInt64).UTC(), CommitSequence: ^uint64(0)}
}
