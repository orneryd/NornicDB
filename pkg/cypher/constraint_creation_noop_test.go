package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestExistingConstraintCreationDoesNotRebuildPopulation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, store := newTestExecutor(t)
		for i := 0; i < 256; i++ {
			_, err := store.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(fmt.Sprintf("doc-%d", i)), Labels: []string{"ConstraintDoc"}, Properties: map[string]interface{}{"id": int64(i)}})
			require.NoError(t, err)
		}
		for _, ddl := range []string{
			"CREATE CONSTRAINT doc_id IF NOT EXISTS FOR (n:ConstraintDoc) REQUIRE n.id IS UNIQUE",
			"CREATE CONSTRAINT IF NOT EXISTS FOR (n:ConstraintDoc) REQUIRE n.id IS UNIQUE",
			"CREATE CONSTRAINT id_present IF NOT EXISTS FOR (n:ConstraintDoc) REQUIRE n.id IS NOT NULL",
			"CREATE CONSTRAINT id_type IF NOT EXISTS FOR (n:ConstraintDoc) REQUIRE n.id IS TYPED INTEGER",
		} {
			t.Run(ddl, func(t *testing.T) {
				_, err := exec.Execute(t.Context(), ddl, nil)
				require.NoError(t, err)
				before, owned := store.GetSchema().ConstraintPropertyIndex("doc_id")
				require.True(t, owned)
				var runErr error
				ordinary := testing.AllocsPerRun(2, func() { _, runErr = exec.Execute(t.Context(), "RETURN 1 AS n", nil) })
				require.NoError(t, runErr)
				repeated := testing.AllocsPerRun(2, func() { _, runErr = exec.Execute(t.Context(), ddl, nil) })
				require.NoError(t, runErr)
				t.Logf("ordinary %.0f, existing constraint %.0f allocations", ordinary, repeated)
				require.Less(t, repeated, ordinary+1024, "an existing constraint must not copy/validate/rebuild the 256 stored nodes")
				after, owned := store.GetSchema().ConstraintPropertyIndex("doc_id")
				require.True(t, owned)
				require.Same(t, before, after)
			})
		}
	})
}
