package storage

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// Repair-route additions: storage-format-owners lane (Codex), 1 November 2026.
// Synchronous repairs are part of the public read that discovers them. They
// share the existing admission schedule with request-owned maintenance writes.
func TestResidualMaintenanceAdmissionCancellation(t *testing.T) {
	for _, route := range []string{"mark-clean", "consume-clean", "temporal-rebuild", "mvcc-rebuild", "temporal-write", "lifecycle-stripes", "lifecycle-suppression", "mvcc-prune", "label-drop", "edge-type-drop", "between-drop", "raw-drop-all", "raw-load", "between-repair", "label-scan-repair"} {
		t.Run(route, func(t *testing.T) {
			b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: route == "lifecycle-stripes", WriteBehindInterval: time.Hour}))
			require.NoError(t, err)
			var finalOutcome error
			fixtureowner.OwnWithCloseAssertion(t, b, func(err error) bool {
				return finalOutcome != nil && err == finalOutcome
			})
			var target sync.Locker = &b.db.commitGate
			var run func(context.Context) error
			switch route {
			case "mark-clean":
				run = b.MarkCleanShutdown
			case "consume-clean":
				run = func(ctx context.Context) error { _, err := b.ConsumeCleanShutdownMarker(ctx); return err }
			case "temporal-rebuild":
				target, run = &b.writeBarrier, b.RebuildTemporalIndexes
			case "mvcc-rebuild":
				target, run = &b.writeBarrier, b.RebuildMVCCHeads
			case "temporal-write":
				run = b.RebuildTemporalIndexes
			case "lifecycle-stripes":
				target, run = &b.writeBehind.order[2], b.RebuildTemporalIndexes
			case "lifecycle-suppression":
				target, run = &b.suppressionWriteMu, b.RebuildTemporalIndexes
			case "mvcc-prune":
				target = &b.mvccPruneMu
				run = func(ctx context.Context) error { _, err := b.PruneMVCCVersions(ctx, MVCCPruneOptions{}); return err }
			case "label-drop":
				run = func(ctx context.Context) error { _, err := b.rebuildLabelIndex(ctx); return err }
			case "edge-type-drop":
				run = func(ctx context.Context) error { _, err := b.rebuildEdgeTypeIndex(ctx); return err }
			case "between-drop":
				run = func(ctx context.Context) error {
					_, err := b.rebuildRelationshipIndexes(ctx, rebuildEdgeBetweenOnly)
					return err
				}
			case "raw-drop-all":
				run = func(ctx context.Context) error {
					if current, ok := any(b.db).(interface{ DropAll(context.Context) error }); ok {
						return current.DropAll(ctx)
					}
					return any(b.db).(interface{ DropAll() error }).DropAll()
				}
			case "raw-load":
				run = func(ctx context.Context) error {
					if current, ok := any(b.db).(interface {
						Load(context.Context, io.Reader, int) error
					}); ok {
						return current.Load(ctx, strings.NewReader(""), 1)
					}
					return any(b.db).(interface{ Load(io.Reader, int) error }).Load(strings.NewReader(""), 1)
				}
			case "between-repair":
				for _, id := range []NodeID{"test:a", "test:b"} {
					_, err := b.CreateNode(t.Context(), &Node{ID: id})
					require.NoError(t, err)
				}
				edge := &Edge{ID: "test:repair", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}
				require.NoError(t, b.CreateEdge(t.Context(), edge))
				require.NoError(t, b.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
					return b.deleteEdgeBetweenIndexesInTxn(txn, edge)
				}))
				run = func(ctx context.Context) error {
					_, err := b.GetEdgesBetween(ctx, edge.StartNode, edge.EndNode)
					return err
				}
			case "label-scan-repair":
				seedCancellationNodes(t, b, labelScanPointLookups+2)
				require.NoError(t, b.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
					return txn.Set(labelCountKey("test", "Doc"), []byte{1})
				}))
				run = func(ctx context.Context) error {
					return b.StreamNodesByLabelProjectedInScope(ctx, "test:", "Doc", nil, func(*Node) error { return nil })
				}
			}
			target.Lock()
			held := true
			defer func() {
				if held {
					target.Unlock()
				}
			}()
			waited := make(chan context.Context, 1)
			t.Cleanup(setAdmissionWaitObserver(func(owner any, ctx context.Context) {
				if owner == target {
					select {
					case waited <- ctx:
					default:
					}
				}
			}))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- run(ctx) }()
			var actual context.Context
			select {
			case actual = <-waited:
			case <-time.After(2 * time.Second):
				target.Unlock()
				held = false
				cancel()
				<-done
				t.Fatal("maintenance did not reach its actual admission owner")
			}
			cancel()
			returned := false
			var runErr error
			select {
			case runErr = <-done:
				returned = true
			case <-time.After(2 * time.Second):
			}
			target.Unlock()
			held = false
			if !returned {
				runErr = <-done
			}
			require.True(t, returned, "maintenance cancellation must return before peer admission is released")
			require.ErrorIs(t, runErr, context.Canceled)
			require.Same(t, ctx, actual)
			if route == "lifecycle-stripes" {
				available := b.writeBehind.order[0].TryLock()
				if available {
					b.writeBehind.order[0].Unlock()
				}
				require.True(t, available, "cancelled lifecycle acquisition must release its partial stripe holds")
			}
			if route == "label-scan-repair" {
				// The injected state is the malformed persisted count this read
				// repairs: retirement reports the value it cannot decode, and repeated
				// Close retains that outcome.
				finalOutcome = b.Close()
				require.EqualError(t, finalOutcome, "label-count final fold: derived count value malformed: decode derived count: expected 8 bytes, got 1")
			}
		})
	}
}

func TestResidualCleanMarkerKeepsGraphCachesWarm(t *testing.T) {
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	_, err = b.CreateNode(t.Context(), &Node{ID: "test:marker-peer"})
	require.NoError(t, err)
	_, err = b.GetNode(t.Context(), "test:marker-peer")
	require.NoError(t, err)
	b.nodeCacheMu.RLock()
	before := b.nodeCache["test:marker-peer"]
	b.nodeCacheMu.RUnlock()
	require.NotNil(t, before)
	require.NoError(t, b.MarkCleanShutdown(context.Background()))
	b.nodeCacheMu.RLock()
	after := b.nodeCache["test:marker-peer"]
	b.nodeCacheMu.RUnlock()
	require.Same(t, before, after, "a shutdown metadata marker must preserve unrelated graph cache bodies")
}
