package storage_test

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Restore must refresh quota admission on the still-live manager, including
// when the failed DROP's final metadata is absent from its administrative backup.
func TestReview1406ManagedRestoreAfterDropFinalMetadata(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, quota := range []string{"count", "bytes"} {
			t.Run(fmt.Sprintf("buffered=%t/%s", buffered, quota), func(t *testing.T) {
				p := newReview1406Probe(t, buffered, quota)
				base, manager, tenant, peer := p.base, p.manager, p.tenant, p.peer
				stamp, body, originalBytes := p.stamp, p.body, p.originalBytes
				sentinel := errors.New("DROP final metadata sync unavailable")
				calls := 0
				storage.ReviewSetDropTailFailure(base, func(prefix string) error {
					require.Equal(t, "victim:", prefix)
					calls++
					if calls == 2 {
						return sentinel
					}
					return nil
				})
				// Native prefix deletion retains the catalog and its quota policy.
				// A journaled catalog DROP is instead irrevocably admitted and
				// remains offline across Restore (covered by the manager recovery
				// matrix); it cannot serve as a live-catalog quota fixture.
				_, _, dropErr := base.DeleteByPrefix("victim:")
				require.ErrorIs(t, dropErr, sentinel)
				require.Equal(t, 2, calls, "fault must follow atomic final metadata commit")
				exists, err := storage.ReviewRawEntityExists(base, storage.StoredEntityNodeKind, "victim:old000")
				require.NoError(t, err)
				require.False(t, exists)
				backup := filepath.Join(t.TempDir(), "after-final-metadata.bak")
				require.NoError(t, base.Backup(backup), "administrative backup remains allowed under fence")
				storage.ReviewSetDropTailFailure(base, nil)
				require.NoError(t, base.Restore(backup))
				// Keep the SAME manager and tenant wrapper; constructing a new manager
				// here would conceal stale admission state retained across Restore.
				nodes, err := base.NodeCountByPrefix("victim:")
				require.NoError(t, err)
				require.Zero(t, nodes)
				peerNodes, err := base.NodeCountByPrefix("peer:")
				require.NoError(t, err)
				require.Equal(t, int64(1), peerNodes)
				_, err = peer.GetNode("keep")
				require.NoError(t, err)
				_, err = peer.CreateNode(&storage.Node{ID: "after"})
				require.NoError(t, err)
				reported, _, _ := manager.GetStorageSize("victim")
				t.Logf("last-known size report after Restore=%d; admission, not this report, is the oracle", reported)
				_, err = tenant.CreateNode(body("new000"))
				require.NoError(t, err, "empty restored tenant must admit its first node under the retained quota")
				stored, err := base.GetStoredNode("victim:new000")
				require.NoError(t, err)
				require.Equal(t, body("victim:new000").Properties, stored.Properties)
				require.Equal(t, stamp, stored.CreatedAt)
				require.Equal(t, stamp, stored.UpdatedAt)
				actualBytes, _, _ := manager.GetStorageSize("victim")
				require.Equal(t, originalBytes, actualBytes, "equal-sized stored image must consume exactly the original measured quota")
				_, err = tenant.CreateNode(body("next00"))
				require.Error(t, err, "a second image must exceed the count or exact byte ceiling")
				exists, readErr := storage.ReviewRawEntityExists(base, storage.StoredEntityNodeKind, "victim:next00")
				require.NoError(t, readErr)
				require.False(t, exists, "rejected growth must not partially write")
				nodes, err = base.NodeCountByPrefix("victim:")
				require.NoError(t, err)
				require.Equal(t, int64(1), nodes)
			})
		}
	}
}

// A successful ordinary Restore can add stored images as well as remove them.
// Admission on the existing wrapper must reflect the restored image and retain
// its limit, rather than treating every invalidated namespace as empty.
func TestReview1406ManagedRestoreNonemptyAdmission(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, quota := range []string{"count", "bytes"} {
			t.Run(fmt.Sprintf("buffered=%t/%s", buffered, quota), func(t *testing.T) {
				p := newReview1406Probe(t, buffered, quota)
				base, manager, tenant, peer := p.base, p.manager, p.tenant, p.peer
				body, originalBytes := p.body, p.originalBytes
				backup := filepath.Join(t.TempDir(), "nonempty.bak")
				require.NoError(t, base.Backup(backup))
				require.NoError(t, tenant.DeleteNode("old000"))
				currentBytes, _, _ := manager.GetStorageSize("victim")
				require.Zero(t, currentBytes, "pre-Restore live admission has observed the deletion")
				require.NoError(t, base.Restore(backup))
				restored, err := tenant.GetNode("old000")
				require.NoError(t, err)
				require.Equal(t, body("old000").Properties, restored.Properties)
				nodes, err := base.NodeCountByPrefix("victim:")
				require.NoError(t, err)
				require.Equal(t, int64(1), nodes)
				_, err = tenant.CreateNode(body("next00"))
				require.Error(t, err, "restored existing image consumes the retained quota")
				exists, err := storage.ReviewRawEntityExists(base, storage.StoredEntityNodeKind, "victim:next00")
				require.NoError(t, err)
				require.False(t, exists, "rejected growth must not partially write")
				actualBytes, _, _ := manager.GetStorageSize("victim")
				require.Equal(t, originalBytes, actualBytes, "admission refreshes the actual restored image")
				_, err = peer.GetNode("keep")
				require.NoError(t, err)
				_, err = peer.CreateNode(&storage.Node{ID: "after"})
				require.NoError(t, err)
			})
		}
	}
}

// review1406Probe is the shared pre-Restore fixture for the two Review 1406
// managed Restore tests: live base engine + manager, victim/peer databases,
// a seeded victim node and peer sentinel, and a retained count-or-bytes quota.
// It owns all cleanup (engine after manager) and performs the same positive
// byte assertion both tests already made. Restore-triggering behavior stays
// with each caller.
type review1406Probe struct {
	base          *storage.BadgerEngine
	manager       *multidb.DatabaseManager
	tenant        storage.Engine
	peer          storage.Engine
	stamp         time.Time
	body          func(id storage.NodeID) *storage.Node
	originalBytes int64
}

func newReview1406Probe(t *testing.T, buffered bool, quota string) *review1406Probe {
	t.Helper()
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	manager, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "victim"}))
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "peer"}))
	tenant, err := manager.GetStorage("victim")
	require.NoError(t, err)
	peer, err := manager.GetStorage("peer")
	require.NoError(t, err)
	stamp := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).In(time.Local)
	body := func(id storage.NodeID) *storage.Node {
		return &storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"payload": strings.Repeat("x", 1024)}, CreatedAt: stamp, UpdatedAt: stamp}
	}
	require.NoError(t, tenant.BulkCreateNodes([]*storage.Node{body("old000")}))
	_, err = peer.CreateNode(&storage.Node{ID: "keep"})
	require.NoError(t, err)
	originalBytes, _, _ := manager.GetStorageSize("victim")
	require.Positive(t, originalBytes)
	limits := &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 1}}
	if quota == "bytes" {
		limits.Storage = multidb.StorageLimits{MaxBytes: originalBytes}
	}
	require.NoError(t, manager.SetDatabaseLimits(t.Context(), "victim", limits))
	return &review1406Probe{
		base:          base,
		manager:       manager,
		tenant:        tenant,
		peer:          peer,
		stamp:         stamp,
		body:          body,
		originalBytes: originalBytes,
	}
}
