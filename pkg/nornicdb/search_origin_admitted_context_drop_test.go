package nornicdb

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The deterministic variant: a request context admitted before DROP INDEX is
// carried into a native DB write, or a Cypher SET, issued after the DROP. The
// storage write must inherit that admission, not read the clock again.
func TestSearchOriginAdmittedContextAfterDrop(t *testing.T) {
	for _, write := range []string{"CreateNodeWithID", "UpdateNode", "CypherSET"} {
		for _, recreate := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/recreate=%t", write, recreate), func(t *testing.T) {
				db, svc := openConsumerDB(t, false)
				ctx := context.Background()
				ns := db.defaultDatabaseName()
				store := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), ns)
				ddl := cypher.NewStorageExecutor(store)
				ddl.SetSearchService(svc)
				create := "CREATE VECTOR INDEX carriedvec FOR (n:Doc) ON (n.embedding)"
				_, err := ddl.Execute(ctx, create, nil)
				require.NoError(t, err)
				vector := make([]float64, svc.VectorIndexDimensions())
				vector[0] = 1
				if write != "CreateNodeWithID" {
					_, err = db.CreateNodeWithID(ctx, "doc", []string{"Doc"}, map[string]interface{}{"content": "carriedword"})
					require.NoError(t, err)
					settleQueuedRefreshes()
				}
				admitted, _ := storage.AdmitVectorWork(ctx)
				_, err = ddl.Execute(ctx, "DROP INDEX carriedvec", nil)
				require.NoError(t, err)
				if recreate {
					_, err = ddl.Execute(ctx, create, nil)
					require.NoError(t, err)
				}
				properties := map[string]interface{}{"content": "carriedword", "embedding": vector, "other": vector}
				switch write {
				case "CreateNodeWithID":
					_, err = db.CreateNodeWithID(admitted, "doc", []string{"Doc"}, properties)
				case "UpdateNode":
					_, err = db.UpdateNode(admitted, "doc", properties)
				case "CypherSET":
					mutation := cypher.NewStorageExecutor(store)
					mutation.SetSearchService(svc)
					_, err = mutation.Execute(admitted, "MATCH (n:Doc) WHERE n.content = 'carriedword' SET n.embedding = $v, n.other = $v", map[string]interface{}{"v": vector})
				}
				require.NoError(t, err)
				settleQueuedRefreshes()
				require.Zero(t, svc.CountPropertyVectorEntries("embedding"), "a write carrying a pre-DROP admission restored a retired entry")
				require.Equal(t, 1, svc.CountPropertyVectorEntries("other"))
			})
		}
	}
}
