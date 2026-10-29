package storage

// Context-owner and envelope-metadata convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// These are the compiled sidecar producer routes and the two delivery owners:
// registered listeners and bound worker completion. Measure the accepted-proof
// read inside real public delivery, after wrappers and durable admission.
func TestCapturedPublicationDeliveryAllocationPopulation(t *testing.T) {
	for _, route := range []string{"native", "wal", "namespace", "namespace-wal", "composite"} {
		for _, bound := range []bool{false, true} {
			name := route + "/unbound"
			if bound {
				name = route + "/bound"
			}
			t.Run(name, func(t *testing.T) {
				b := newSidecarTestBadger(t)
				node := publicationProofPayload(t, b)
				var engine Engine = b
				if route == "wal" || route == "namespace-wal" {
					wal, err := NewWAL("", &WALConfig{Dir: filepath.Join(t.TempDir(), "wal"), SyncMode: "immediate"})
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, wal.Close()) })
					engine = NewWALEngine(engine, wal)
				}
				if route == "namespace" || route == "namespace-wal" {
					engine = NewNamespacedEngine(engine, "test")
				}
				if route == "composite" {
					engine = NewCompositeEngine(map[string]Engine{"one": engine}, map[string]string{"one": "test"}, map[string]string{"one": "read_write"})
				}
				listeners, workers := 0, 0
				check := func(accepted *Node) {
					require.True(t, accepted.runtimeState().embeddingPublication.matches(b, accepted.ID, accepted.runtimeState().embeddingRevision))
					allocs := testing.AllocsPerRun(20, func() {
						same, err := b.embeddingRevisionIsCurrent(context.Background(), accepted.ID, accepted.runtimeState().embeddingRevision, accepted.runtimeState().embeddingPublication)
						if err != nil || !same {
							panic("accepted publication is not current")
						}
					})
					require.Zero(t, allocs, "accepted cached validation must borrow its callback on every delivery route")
				}
				b.OnNodeUpdated(func(accepted *Node) { listeners++; check(accepted) })
				if bound {
					node.SetEmbeddingAttempt(NewEmbeddingAttempt(1, 1, node.EmbeddingRevision(), func() (func(), bool) { return func() {}, true }, func(accepted *Node) { workers++; check(accepted) }))
				}
				require.NoError(t, engine.(EmbeddingSidecarUpdater).UpdateNodeEmbeddingSidecar(node))
				require.Equal(t, 1, listeners)
				if bound {
					require.Equal(t, 1, workers)
				} else {
					require.Zero(t, workers)
				}
			})
		}
	}
}
