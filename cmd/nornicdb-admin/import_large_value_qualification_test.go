//go:build largequalification

package main

import "testing"

// This public round trip crosses the old 128 MiB importer limit. Its measured
// multi-gigabyte peak belongs in explicit large-value qualification, not the
// default package suite. Run with -tags largequalification -run LargeValueCapacity.
func TestAdminCLIImportFullLargeValueCapacity(t *testing.T) {
	checkAdminImportCapacityRoundTrip(t, 136<<20, false)
}
