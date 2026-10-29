package replication

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestReplicatedEngineReadCapabilitiesRetainNamespace(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	engine := NewReplicatedEngine(storage.NewTracedEngine(base), nil, time.Second)
	tenant := storage.NewNamespacedEngine(engine, "tenant")
	_, err := base.CreateNode(&storage.Node{ID: "tenant:row", Labels: []string{"Record"}, Properties: map[string]interface{}{"id": int64(1)}})
	require.NoError(t, err)
	_, err = base.CreateNode(&storage.Node{ID: "other:row", Labels: []string{"Record"}, Properties: map[string]interface{}{"id": int64(99)}})
	require.NoError(t, err)
	reader, ok := capabilityForTest[storage.ProjectedLabelNodeReader](storage.NewTracedEngine(tenant))
	require.True(t, ok)
	var values []interface{}
	require.NoError(t, reader.StreamNodesByLabelProjected("Record", []string{"id"}, func(node *storage.Node) error { values = append(values, node.Properties["id"]); return nil }))
	require.Equal(t, []interface{}{int64(1)}, values)
	version, ok := engine.GraphMutationVersion()
	require.True(t, ok)
	baseVersion, baseOK := base.GraphMutationVersion()
	require.Equal(t, baseOK, ok)
	require.Equal(t, baseVersion, version)
	version, ok = engine.GraphMutationVersionInNamespace("tenant")
	require.True(t, ok)
	baseVersion, baseOK = base.GraphMutationVersionInNamespace("tenant")
	require.Equal(t, baseOK, ok)
	require.Equal(t, baseVersion, version)
	// Backup/restore must retain the write owner instead of bypassing a wrapper
	// that does not expose those operations.
	require.ErrorIs(t, engine.Backup(t.TempDir()+"/backup"), storage.ErrNotImplemented)
	require.ErrorIs(t, engine.Restore(t.TempDir()+"/backup"), storage.ErrNotImplemented)
}
