package storage

import "testing"

// BenchmarkPublicationStage isolates the per-commit scheduler bookkeeping
// from Badger, index maintenance and notification delivery.
func BenchmarkPublicationStage(b *testing.B) {
	var stage publicationStage
	keys := []any{NodeID("db:n"), uniqueConstraintLockKey{label: "L", property: "u", value: "v"}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ts := uint64(i + 1)
		pub := commitPublication{keys: keys, run: func() {}}
		stage.register(ts, &pub)
		stage.apply(ts)
	}
}
