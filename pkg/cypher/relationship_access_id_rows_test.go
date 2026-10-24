package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

// Keys derived from an incoming relationship are resolved at the access owner;
// predicates requiring that incoming row are applied by the existing row owner.
func TestRelationshipAccessProgramIDIncomingRows(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "id_incoming_rows"))
	_, err := e.Execute(context.Background(), "CREATE (:IDSeed {id:'a'})-[:RHS {p:'x'}]->(:IDSeed {id:'b'})", nil)
	require.NoError(t, err)
	for _, tc := range relationshipAccessGeneratedIncomingIDCases() {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
				result, err := e.Execute(context.Background(), mode+tc.query, nil)
				require.NoError(t, err, mode)
				if mode == "EXPLAIN " {
					require.Empty(t, result.Rows)
				} else {
					require.Equal(t, tc.rows, result.Rows, mode)
				}
				if mode != "" {
					require.NotNil(t, relationshipPlanOperator(result.Metadata["plan"].(*ExecutionPlan).Root, tc.operator), mode)
				}
			}
		})
	}
}
