package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// Graph-only fixtures declare the ordinary system catalog absent. Encountering
// one is a fixture error, never an identity transform of credential metadata.
func graphOnlyCatalogUpgradeV4() *CatalogUpgradeV4 {
	return &CatalogUpgradeV4{NodeID: "system:databases:metadata", Transform: func(context.Context, string) (string, bool, error) {
		return "", false, errors.New("unexpected database catalog in graph-only fixture")
	}}
}

func catalogV4Engine(t *testing.T, plan *CatalogUpgradeV4) (*BadgerEngine, string) {
	t.Helper()
	dir := t.TempDir()
	engine, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true}))
	require.NoError(t, err)
	// Construct the pinned V3 fixture after opening the test engine. These
	// bodies use the unchanged V3 codec; the tested arm must advance its marker.
	if plan != nil {
		copy := *plan
		engine.catalogUpgradeV4 = &copy
	}
	require.NoError(t, engine.writeSchemaVersion(storageVersionEdgeAdjacencyV3))
	engine.storageVersion = storageVersionEdgeAdjacencyV3
	return engine, dir
}
func catalogV4Snapshot(t *testing.T, b *BadgerEngine) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	require.NoError(t, b.db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			out[string(item.KeyCopy(nil))] = value
		}
		return nil
	}))
	return out
}
func catalogV4Run(b *BadgerEngine, ctx context.Context) error {
	lease, err := b.beginMaintenanceWrite(ctx, nil)
	if err != nil {
		return err
	}
	defer lease.finish()
	return b.migrateV3ToV4(ctx, false)
}
func catalogV4Node(t *testing.T, b *BadgerEngine, id NodeID) *Node {
	t.Helper()
	raw := catalogV4Snapshot(t, b)[string(nodeKey(id))]
	node, err := b.decodeNode(namespaceForNodeID(id), raw)
	require.NoError(t, err)
	return node
}
func catalogV4Seed(t *testing.T, b *BadgerEngine, id NodeID, data interface{}) {
	t.Helper()
	_, err := b.CreateNode(&Node{ID: id, Labels: []string{"_System", "_Metadata"}, Properties: map[string]interface{}{"data": data, "unknown_future": map[string]interface{}{"items": []interface{}{int64(17), "keep"}}, "server_id": "keep-id"}, CreatedAt: time.Unix(1700000000, 37).UTC(), UpdatedAt: time.Unix(1700000100, 41).UTC(), NamedEmbeddings: map[string][]float32{"named": {1, 2}}, ChunkEmbeddings: [][]float32{{3, 4}}, EmbedMeta: map[string]interface{}{"model": "keep"}})
	require.NoError(t, err)
}

func TestCatalogUpgradeV4AtomicPayloadAndVersion(t *testing.T) {
	id := NodeID("configured:exact-catalog")
	calls := 0
	ctxKey := struct{}{}
	ctx := context.WithValue(t.Context(), ctxKey, "original")
	plan := &CatalogUpgradeV4{NodeID: id, Transform: func(got context.Context, data string) (string, bool, error) {
		calls++
		require.Equal(t, "original", got.Value(ctxKey))
		require.Equal(t, `{"secret":"old"}`, data)
		return `{"secret":"encrypted"}`, true, nil
	}}
	b, dir := catalogV4Engine(t, plan)
	catalogV4Seed(t, b, id, `{"secret":"old"}`)
	catalogV4Seed(t, b, "tenant:databases:metadata", `{"secret":"old"}`)
	catalogV4Seed(t, b, "configured:decoy", `{"secret":"old"}`)
	// Settle the ordinary count-delta/readiness owners before taking a whole-KV
	// migration snapshot. The fixture uses the unchanged V3 native body codec;
	// its pinned V3 marker is installed after this normal startup preparation.
	require.NoError(t, b.writeSchemaVersion(storageVersionCurrent))
	require.NoError(t, b.Close())
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true, CatalogUpgradeV4: plan}))
	require.NoError(t, err)
	require.NoError(t, b.writeSchemaVersion(storageVersionEdgeAdjacencyV3))
	b.storageVersion = storageVersionEdgeAdjacencyV3
	ledger := &registryUsageRuntime{policy: "retained"}
	b.GetOrAttachManagedEntityRuntime("unrelated-ledger", ledger)
	before := catalogV4Snapshot(t, b)
	oldNode := catalogV4Node(t, b, id)
	// Prime point/body caches: settlement must not return the old catalog image.
	_, err = b.GetNode(id)
	require.NoError(t, err)
	require.NoError(t, catalogV4Run(b, ctx))
	require.Equal(t, 1, calls)
	require.EqualValues(t, 1, ledger.resets.Load())
	require.Equal(t, "retained", ledger.policy)
	version, err := b.readSchemaVersion()
	require.NoError(t, err)
	require.Equal(t, storageVersionCatalogV4, version)
	require.Equal(t, storageVersionCatalogV4, b.storageVersion)
	after := catalogV4Snapshot(t, b)
	expectedNode := oldNode
	expectedNode.Properties["data"] = `{"secret":"encrypted"}`
	require.Equal(t, expectedNode, catalogV4Node(t, b, id), "only catalog data changes; known fields and unknown properties survive")
	require.Len(t, after, len(before))
	for key, value := range before {
		if key != string(nodeKey(id)) && key != string(mvccSchemaVersionKey()) {
			require.Equal(t, value, after[key], fmt.Sprintf("unrelated key %x changed", key))
		}
	}
	current, err := b.GetNode(id)
	require.NoError(t, err)
	require.Equal(t, `{"secret":"encrypted"}`, current.Properties["data"])
	require.NoError(t, catalogV4Run(b, t.Context()))
	require.Equal(t, 1, calls, "V4 does not transform twice")
	require.Equal(t, after, catalogV4Snapshot(t, b))
	require.NoError(t, b.Close())
	reopened, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true}))
	require.NoError(t, err)
	require.Equal(t, after, catalogV4Snapshot(t, reopened), "reopen preserves the complete settled image")
}

func TestCatalogUpgradeV4RejectsWithoutWrites(t *testing.T) {
	for _, kind := range []string{"missing-plan", "missing-transform", "empty-id", "missing-data", "wrong-data-type", "corrupt-body", "rejected", "wrong-key", "canceled-before", "canceled-in-transform"} {
		t.Run(kind, func(t *testing.T) {
			id := NodeID("configured:catalog")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			sentinel := errors.New("synthetic transform rejected")
			plan := &CatalogUpgradeV4{NodeID: id, Transform: func(context.Context, string) (string, bool, error) {
				switch kind {
				case "rejected", "wrong-key":
					return "", false, sentinel
				case "canceled-in-transform":
					cancel()
				}
				return `{"new":1}`, true, nil
			}}
			if kind == "missing-plan" {
				plan = nil
			} else if kind == "missing-transform" {
				plan.Transform = nil
			} else if kind == "empty-id" {
				plan.NodeID = ""
			}
			b, _ := catalogV4Engine(t, plan)
			catalogV4Seed(t, b, id, `{"old":1}`)
			if kind == "wrong-data-type" {
				node, err := b.GetNode(id)
				require.NoError(t, err)
				node.Properties["data"] = int64(1)
				require.NoError(t, b.UpdateNode(NodeUpdateRequest{After: node}))
			}
			if kind == "missing-data" {
				node, err := b.GetNode(id)
				require.NoError(t, err)
				delete(node.Properties, "data")
				require.NoError(t, b.UpdateNode(NodeUpdateRequest{After: node}))
			}
			if kind == "corrupt-body" {
				require.NoError(t, b.db.Update(func(tx *badger.Txn) error { return tx.Set(nodeKey(id), []byte("corrupt")) }))
			}
			if kind == "canceled-before" {
				cancel()
			}
			before := catalogV4Snapshot(t, b)
			err := catalogV4Run(b, ctx)
			require.Error(t, err)
			if strings.HasPrefix(kind, "canceled") {
				require.ErrorIs(t, err, context.Canceled)
			}
			if kind == "rejected" || kind == "wrong-key" {
				require.ErrorIs(t, err, sentinel)
			}
			require.Equal(t, before, catalogV4Snapshot(t, b))
			require.Equal(t, storageVersionEdgeAdjacencyV3, b.storageVersion)
		})
	}
}

func TestCatalogUpgradeV4MissingAndUnchanged(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing=%v", missing), func(t *testing.T) {
			calls := 0
			id := NodeID("configured:catalog")
			b, _ := catalogV4Engine(t, &CatalogUpgradeV4{NodeID: id, Transform: func(_ context.Context, data string) (string, bool, error) {
				calls++
				return "ignored replacement", false, nil
			}})
			if !missing {
				catalogV4Seed(t, b, id, `{"old":1}`)
			}
			before := catalogV4Snapshot(t, b)
			require.NoError(t, catalogV4Run(b, t.Context()))
			after := catalogV4Snapshot(t, b)
			for key, value := range before {
				if key != string(mvccSchemaVersionKey()) {
					require.Equal(t, value, after[key])
				}
			}
			require.Len(t, after, len(before))
			require.Equal(t, map[bool]int{false: 1, true: 0}[missing], calls)
			version, err := b.readSchemaVersion()
			require.NoError(t, err)
			require.Equal(t, storageVersionCatalogV4, version)
		})
	}
}

func TestCatalogUpgradeV4PendingTransformHasNoPartialPublication(t *testing.T) {
	id := NodeID("configured:catalog")
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	b, _ := catalogV4Engine(t, &CatalogUpgradeV4{NodeID: id, Transform: func(_ context.Context, data string) (string, bool, error) {
		close(entered)
		<-release
		return `{"new":1}`, true, nil
	}})
	catalogV4Seed(t, b, id, `{"old":1}`)
	before := catalogV4Snapshot(t, b)
	done := make(chan error, 1)
	go func() {
		defer close(done)
		done <- catalogV4Run(b, t.Context())
	}()
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		<-done
	})
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("migration returned before entering transform: %v", err)
	}
	require.Equal(t, before, catalogV4Snapshot(t, b), "readers see neither replacement nor V4 before atomic commit")
	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-done)
	version, err := b.readSchemaVersion()
	require.NoError(t, err)
	require.Equal(t, storageVersionCatalogV4, version)
	require.Equal(t, `{"new":1}`, catalogV4Node(t, b, id).Properties["data"])
}

func TestCatalogUpgradeV4GrowthAndUnsupportedVectorImage(t *testing.T) {
	for _, vectors := range []bool{false, true} {
		t.Run(fmt.Sprintf("vectors=%v", vectors), func(t *testing.T) {
			id := NodeID("configured:catalog")
			b, _ := catalogV4Engine(t, &CatalogUpgradeV4{NodeID: id, Transform: func(context.Context, string) (string, bool, error) {
				return strings.Repeat("x", maxNodeSize+1), true, nil
			}})
			if vectors {
				catalogV4Seed(t, b, id, "old")
			} else {
				_, err := b.CreateNode(&Node{ID: id, Properties: map[string]interface{}{"data": "old"}})
				require.NoError(t, err)
			}
			before := catalogV4Snapshot(t, b)
			err := catalogV4Run(b, t.Context())
			if vectors {
				require.ErrorContains(t, err, "unsupported catalog V4 image")
				require.Equal(t, before, catalogV4Snapshot(t, b))
			} else {
				require.NoError(t, err)
				require.Equal(t, strings.Repeat("x", maxNodeSize+1), catalogV4Node(t, b, id).Properties["data"])
			}
		})
	}
}
