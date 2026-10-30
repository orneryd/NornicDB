// Package search - version.go provides semver-style format version comparison for index files.
// Follows the Qdrant approach: index files store a version string (e.g. "1.0.0"); on load we
// compare with the current format version and reject old, unknown, or newer (written by a
// newer application) index files so the caller can rebuild from storage.

package search

import (
	"strconv"
	"strings"
)

// parseSemver parses a "major.minor.patch" version string into components.
// Returns ok=false for empty, invalid, or non-numeric parts.
func parseSemver(s string) (major, minor, patch int, ok bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, 0, 0, false
	}
	parts := strings.SplitN(s, ".", 3)
	if len(parts) < 3 {
		return 0, 0, 0, false
	}
	major, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil || major < 0 {
		return 0, 0, 0, false
	}
	minor, err = strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil || minor < 0 {
		return 0, 0, 0, false
	}
	patch, err = strconv.Atoi(strings.TrimSpace(parts[2]))
	if err != nil || patch < 0 {
		return 0, 0, 0, false
	}
	return major, minor, patch, true
}

// compareSemver compares two semver strings. Returns -1 if a < b, 0 if a == b, 1 if a > b.
// If a is invalid (e.g. old format or corrupt), returns -1 so we treat it as old and reject.
// If b is invalid, returns 0 (should not happen for current version constant).
func compareSemver(a, b string) int {
	ma, na, pa, okA := parseSemver(a)
	mb, nb, pb, okB := parseSemver(b)
	if !okA {
		return -1 // stored version invalid → treat as old
	}
	if !okB {
		return 0
	}
	if ma != mb {
		if ma < mb {
			return -1
		}
		return 1
	}
	if na != nb {
		if na < nb {
			return -1
		}
		return 1
	}
	if pa != pb {
		if pa < pb {
			return -1
		}
		return 1
	}
	return 0
}

// searchIndexVersionCompatible returns true if the stored version is exactly the current
// format version (load is allowed). If stored is newer, it logs and returns false (Qdrant-style:
// "upgrade application"). If stored is older or invalid, returns false without logging.
func searchIndexVersionCompatible(stored, current string, indexName string) bool {
	cmp := compareSemver(stored, current)
	switch cmp {
	case 0:
		return true
	case 1:
		logSearchPrintf("📇 %s index was written by a newer version (%s > %s); please upgrade the application, skipping load", indexName, stored, current)
		return false
	default:
		return false // old or invalid
	}
}
