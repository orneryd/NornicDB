package storage

import "strings"

// ListNamespaces returns the set of database namespaces currently present in this
// Badger engine, derived from the cached per-namespace node/edge counters.
//
// Namespaces are returned without the trailing ':' (e.g., "nornic", "db2").
func (b *BadgerEngine) ListNamespaces() []string {
	if !b.work.Enter() {
		return nil
	}
	// Keep both counter maps alive until discovery finishes. Checking that the
	// engine is open alone does not stop Close from releasing them mid-read.
	defer b.work.Leave()

	seen := make(map[string]struct{})

	b.namespaceCountsMu.RLock()
	for prefix := range b.namespaceNodeCounts {
		if name := strings.TrimSuffix(prefix, ":"); name != "" {
			seen[name] = struct{}{}
		}
	}
	for prefix := range b.namespaceEdgeCounts {
		if name := strings.TrimSuffix(prefix, ":"); name != "" {
			seen[name] = struct{}{}
		}
	}
	b.namespaceCountsMu.RUnlock()

	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	return out
}
