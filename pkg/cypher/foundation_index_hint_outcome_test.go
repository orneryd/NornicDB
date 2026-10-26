// Author: Lane #1644 (Codex). Public index-hint hydration must preserve an
// unrecovered point failure while ordinary missing candidates remain absent.
package cypher_test

import (
	"errors"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

type foundationHintReadFault struct {
	storage.Engine
	failure error
}

func (v foundationHintReadFault) GetNode(id storage.NodeID) (*storage.Node, error) {
	if v.failure != nil {
		return nil, v.failure
	}
	return v.Engine.GetNode(id)
}
func TestFoundationPublicIndexHintReadOutcome(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	issued := storage.NewNamespacedEngine(base, "tenant")
	_, err := issued.CreateNode(&storage.Node{ID: "n", Labels: []string{"Doc"}, Properties: map[string]any{"key": "value"}})
	require.NoError(t, err)
	schema := issued.GetSchema()
	require.NoError(t, schema.AddPropertyIndex("doc_key", "Doc", []string{"key"}))
	require.NoError(t, schema.PropertyIndexInsert("Doc", "key", "n", "value"))
	cause := errors.New("unrecovered hint point read")
	for _, tc := range []struct {
		name    string
		failure error
	}{{"failure", cause}, {"missing", storage.ErrNotFound}, {"healthy", nil}} {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := cypher.ApplyIndexHint(foundationHintReadFault{issued, tc.failure}, schema, cypher.IndexHint{Type: cypher.HintIndex, Variable: "n", Label: "Doc", Property: "key"}, "value")
			if tc.failure == cause {
				require.ErrorIs(t, err, cause)
				require.Empty(t, rows)
			} else {
				require.NoError(t, err)
				if tc.failure != nil {
					require.Empty(t, rows)
				} else {
					require.Len(t, rows, 1)
				}
			}
		})
	}
}
