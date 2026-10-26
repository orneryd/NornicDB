package replication

import "github.com/orneryd/nornicdb/pkg/storage"

// Test-only access preserves the discovery/identity oracle; production cannot
// obtain a raw reader. Calls and assertions remain in their original tests.
func capabilityForTest[T any](engine any) (T, bool) {
	result, err := storage.ReadWithCapability(engine, func(reader T) (T, error) { return reader, nil }, nil)
	return result, err == nil
}
