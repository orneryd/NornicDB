package storage

import "strings"

// PhysicalLocationKey is the shared location identity for transaction admission
// and native graph provenance. Preserve the shipped Fabric key strings exactly.
// These keys are internal: element IDs and errors use catalog qualifiers.
func PhysicalLocationKey(uri, database string, remote bool) string {
	if !remote {
		return "local:" + strings.TrimSpace(database)
	}
	return "remote:" + strings.TrimSpace(uri) + "|" + strings.TrimSpace(database)
}

