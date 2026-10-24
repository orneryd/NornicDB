package storage

import (
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

// A transaction's write admission decides every data write, the first schema
// change and every knowledge-policy change before it is made: while it
// refuses, the write fails with its error and the transaction holds nothing;
// once it admits, later writes don't ask again. Reads never ask.
func TestTransactionWriteAdmissionDecidesEveryWrite(t *testing.T) {
	engine := createTestBadgerEngine(t)
	begin := func(admit func() error) *BadgerTransaction {
		t.Helper()
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		require.NoError(t, tx.SetNamespace("adm"))
		tx.SetWriteAdmission(admit)
		return tx
	}
	seed := begin(nil)
	_, err := seed.CreateNode(&Node{ID: "adm:a", Labels: []string{"A"}})
	require.NoError(t, err)
	_, err = seed.CreateNode(&Node{ID: "adm:b", Labels: []string{"A"}})
	require.NoError(t, err)
	require.NoError(t, seed.CreateEdge(&Edge{ID: "adm:ab", StartNode: "adm:a", EndNode: "adm:b", Type: "R"}))
	require.NoError(t, seed.Commit())

	refused := errors.New("refused")
	asks := 0
	tx := begin(func() error { asks++; return refused })
	node, err := tx.GetNode("adm:a")
	require.NoError(t, err, "reads don't ask")
	require.Equal(t, []string{"A"}, node.Labels)
	_, err = tx.GetEdge("adm:ab")
	require.NoError(t, err)
	view, err := tx.KnowledgePolicySchema()
	require.NoError(t, err, "reading the knowledge-policy schema doesn't ask")
	require.Zero(t, asks)
	for name, write := range map[string]func() error{
		"CreateNode": func() error { _, err := tx.CreateNode(&Node{ID: "adm:c", Labels: []string{"A"}}); return err },
		"UpdateNode": func() error {
			return tx.UpdateNode(&Node{ID: "adm:a", Labels: []string{"A"}, Properties: map[string]interface{}{"v": int64(1)}})
		},
		"DeleteNode":          func() error { return tx.DeleteNode("adm:a") },
		"DeleteConnectedNode": func() error { return tx.DeleteConnectedNode("adm:a") },
		"CreateEdge": func() error {
			return tx.CreateEdge(&Edge{ID: "adm:ba", StartNode: "adm:b", EndNode: "adm:a", Type: "R"})
		},
		"UpdateEdge": func() error {
			return tx.UpdateEdge(&Edge{ID: "adm:ab", StartNode: "adm:a", EndNode: "adm:b", Type: "S"})
		},
		"DeleteEdge": func() error { return tx.DeleteEdge("adm:ab") },
		"BulkCreateEdges": func() error {
			return tx.BulkCreateEdges([]*Edge{{ID: "adm:ba", StartNode: "adm:b", EndNode: "adm:a", Type: "R"}})
		},
		"Schema":     func() error { _, err := tx.Schema(); return err },
		"AdmitWrite": tx.AdmitWrite,
	} {
		require.ErrorIs(t, write(), refused, name)
	}
	require.ErrorContains(t, view.CreatePromotionPolicy(knowledgepolicy.PromotionPolicyDef{
		Name: "refused", TargetLabels: []string{"A"}, Enabled: true,
	}), "refused")
	require.Equal(t, 11, asks)
	require.Zero(t, tx.OperationCount(), "a refused write changes nothing")
	require.False(t, tx.HasKnowledgePolicyChanges())
	_, err = tx.CreateNode(&Node{ID: "adm:c", Labels: []string{"A"}})
	require.ErrorIs(t, err, refused, "it asks again while it refuses")

	asks = 0
	tx = begin(func() error { asks++; return nil })
	_, err = tx.CreateNode(&Node{ID: "adm:c", Labels: []string{"A"}})
	require.NoError(t, err)
	require.NoError(t, tx.UpdateNode(&Node{ID: "adm:a", Labels: []string{"A"}, Properties: map[string]interface{}{"v": int64(1)}}))
	require.NoError(t, tx.CreateEdge(&Edge{ID: "adm:ca", StartNode: "adm:c", EndNode: "adm:a", Type: "R"}))
	require.NoError(t, tx.AdmitWrite())
	require.Equal(t, 1, asks, "once admitted, later writes don't ask")
	require.NoError(t, tx.Commit())
	_, err = engine.GetNode("adm:c")
	require.NoError(t, err)

	asks = 0
	tx = begin(func() error { asks++; return nil })
	view, err = tx.Schema()
	require.NoError(t, err, "a schema change is admitted")
	require.NoError(t, view.CreatePromotionPolicy(knowledgepolicy.PromotionPolicyDef{
		Name: "admitted", TargetLabels: []string{"A"}, Enabled: true,
	}))
	require.Equal(t, 1, asks)
	require.True(t, tx.HasKnowledgePolicyChanges())
}
