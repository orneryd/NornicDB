package replication

// Batch 2 earlier-issue probe on the final build (research only, untracked).

import (
	"fmt"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// L519: the same logical ID in two databases must be journalled twice and
// reach a follower twice.
func TestZZEI2NamespaceScopedProjection(t *testing.T) {
	for _, requestID := range []bool{false, true} {
		for _, reopen := range []bool{false, true} {
			t.Run(fmt.Sprintf("requestID=%v/reopen=%v", requestID, reopen), func(t *testing.T) {
				base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
				fixture := setupTestAdapter(t, adapterFixtureOptions{backing: base})
				a := fixture.adapter
				var positions []uint64
				for i, namespace := range []string{"tenant_a", "tenant_b"} {
					if i == 1 && reopen {
						require.NoError(t, a.Close())
						var err error
						a, err = NewStorageAdapterWithWAL(base, fixture.walDir)
						require.NoError(t, err)
						t.Cleanup(func() { _ = a.Close() })
					}
					cmd := databaseScopedCommand(t, namespace, "import-row-42", "n")
					if !requestID {
						cmd.RequestID = ""
					}
					publication, err := a.ApplyCommand(t.Context(), cmd, WALPublished)
					require.NoError(t, err)
					positions = append(positions, publication.Position)
					_, err = storage.GetStoredNode(t.Context(), base, storage.NodeID(namespace+":n"))
					require.NoError(t, err, "the native owner accepted the distinct database commit")
				}
				require.NoError(t, a.FlushWAL())
				entries, err := a.GetWALEntries(0, 100)
				require.NoError(t, err)
				t.Logf("ZZEI2 L519 requestID=%v reopen=%v: publication positions=%v; journal records=%d", requestID, reopen, positions, len(entries))
				peerBase := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
				peer := setupTestAdapter(t, adapterFixtureOptions{backing: peerBase}).adapter
				_, err = NewWALApplier(peer).ApplyBatch(t.Context(), entries)
				require.NoError(t, err)
				_, errA := storage.GetStoredNode(t.Context(), peerBase, "tenant_a:n")
				_, errB := storage.GetStoredNode(t.Context(), peerBase, "tenant_b:n")
				t.Logf("ZZEI2 L519 follower lookups: tenant_a err=%v tenant_b err=%v", errA, errB)
				require.NoError(t, errA)
				require.NoError(t, errB, "a successful independent database commit must reach the replica")
				require.Len(t, entries, 2)
				require.Greater(t, positions[1], positions[0])
			})
		}
	}
}

// L519, prepared route: two databases, one logical ID.
func TestZZEI2PreparedNamespacedProjection(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	fixture := setupTestAdapter(t, adapterFixtureOptions{backing: base, standalone: true, timeout: time.Second})
	for _, namespace := range []string{"tenant_a", "tenant_b"} {
		tx, err := base.BeginTransaction(t.Context(), storage.OrdinaryBegin)
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace(namespace))
		_, err = tx.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(namespace + ":prepared")})
		require.NoError(t, err)
		prepared, err := tx.PrepareEntityCommit(t.Context(), storage.EntityCommitRequest{LogicalID: "import-row-42"})
		require.NoError(t, err)
		t.Cleanup(prepared.Abort)
		receipt, err := fixture.engine.SubmitEntityCommit(t.Context(), prepared, nil)
		require.NoError(t, err)
		require.NoError(t, receipt.Wait(t.Context()))
		_, err = storage.GetStoredNode(t.Context(), base, storage.NodeID(namespace+":prepared"))
		require.NoError(t, err)
	}
	require.NoError(t, fixture.adapter.FlushWAL())
	entries, err := fixture.adapter.GetWALEntries(0, 100)
	require.NoError(t, err)
	t.Logf("ZZEI2 L519 prepared route: journal records=%d", len(entries))
	require.Len(t, entries, 2, "two public prepared commits in different databases require two records")
}
