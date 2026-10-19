package storage

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

func TestNamespaceDropPreparationFencesAllLocalIngress(t *testing.T) {
	b := NewMemoryEngine()
	defer b.Close()
	for _, id := range []NodeID{"admit_a:seed", "admit_a:end", "admit_b:seed"} {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Admit"}})
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(&Edge{ID: "admit_a:edge", StartNode: "admit_a:seed", EndNode: "admit_a:end", Type: "ADMIT"}))
	schema := b.GetSchemaForNamespace("admit_a")
	old, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, old.SetNamespace("admit_a"))
	defer old.Rollback()
	_, err = old.CreateNode(&Node{ID: "admit_a:old"})
	require.NoError(t, err)
	indexKeys := b.collectNodeIndexKeys("admit_a:seed", []string{"Admit"})
	op := NewNamespaceDrop(nil)
	require.NoError(t, b.PrepareNamespaceDrop(context.Background(), "admit_a:", op))
	defer op.Abort()
	ingress := map[string]func() error{
		"native-create":    func() error { _, err := b.CreateNode(&Node{ID: "admit_a:new"}); return err },
		"native-delete":    func() error { return b.DeleteNode("admit_a:seed") },
		"bulk":             func() error { return b.BulkCreateNodes([]*Node{{ID: "admit_a:bulk"}}) },
		"transaction":      old.Commit,
		"node-suppression": func() error { return b.SetNodeManuallySuppressed("admit_a:seed", true) },
		"edge-suppression": func() error { return b.SetEdgeManuallySuppressed("admit_a:edge", true) },
		"access-meta": func() error {
			return b.PutAccessMeta("admit_a:seed", &knowledgepolicy.AccessMetaEntry{TargetID: "admit_a:seed"})
		},
		"index-catalog": func() error {
			return b.PutIndexEntryCatalog("admit_a:seed", &IndexEntryCatalog{TargetID: "admit_a:seed"})
		},
		"index-tombstone": func() error { return b.WriteIndexTombstones(indexKeys) },
		"schema":          func() error { return schema.AddUniqueConstraint("admission_unique", "Admit", "key") },
	}
	for name, write := range ingress {
		t.Run(name, func(t *testing.T) { require.ErrorIs(t, write(), ErrConflict) })
	}
	_, err = b.CreateNode(&Node{ID: "admit_b:peer"})
	require.NoError(t, err)
	require.False(t, op.Committed())
	op.Abort()
	_, err = b.GetNode("admit_a:seed")
	require.NoError(t, err)
	_, err = b.CreateNode(&Node{ID: "admit_a:after-abort"})
	require.NoError(t, err)
	// Preparation did not advance the logical lifetime or delete any data.
	fresh, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, fresh.SetNamespace("admit_a"))
	require.NoError(t, fresh.Commit())
}
