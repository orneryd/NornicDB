package cypher

import (
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestFabricQueryStructureGraphNames(t *testing.T) {
	ctx := context.Background()
	base := newFabricCallRowsExecutor(t)
	manager := base.dbManager.(*testDatabaseManagerAdapter).manager
	driver := fabricCallRowsReference(t)
	for _, word := range fabricGrammarWords() {
		name := strings.ToLower(word)
		if manager.Exists(name) {
			continue
		}
		require.NoError(t, manager.CreateCompositeDatabase(t.Context(), name, []multidb.ConstituentRef{{Alias: "one", DatabaseName: "one", Type: "local", AccessMode: "read_write"}}))
		for _, normalization := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/normalization_%v", word, normalization), func(t *testing.T) {
				restore := func() {}
				if !normalization {
					restore = config.WithCypherQueryNormalizationDisabled()
				}
				defer restore()
				for _, quoted := range []bool{false, true} {
					t.Run(fmt.Sprintf("quoted_%v", quoted), func(t *testing.T) {
						written := name
						if quoted {
							written = "`" + name + "`"
						}
						if driver != nil {
							_, err := neo4j.ExecuteQuery(ctx, driver, "USE GRAPH "+written+" RETURN 7", nil, neo4j.EagerResultTransformer)
							var ref *neo4j.Neo4jError
							require.ErrorAs(t, err, &ref)
							if ref.Code == "Neo.ClientError.Statement.SyntaxError" && !quoted {
								t.Skipf("Neo4j rejects graph-name spelling: %v", ref)
							}
							require.Equal(t, "Neo.ClientError.Database.DatabaseNotFound", ref.Code)
							require.Contains(t, ref.Msg, "Graph not found: "+name)
						}
						query := "USE GRAPH " + written + " CALL { USE " + written + ".one MATCH (n:P) RETURN n.db AS k } RETURN k"
						for _, explicit := range []bool{false, true} {
							t.Run(fmt.Sprintf("explicit_%v", explicit), func(t *testing.T) {
								exec := base
								if explicit {
									store, err := manager.GetStorage(name)
									require.NoError(t, err)
									exec = NewStorageExecutor(store)
									exec.SetDatabaseManager(base.dbManager)
									_, err = exec.Execute(ctx, "BEGIN", nil)
									require.NoError(t, err)
									require.IsType(t, (*fabric.FabricTransaction)(nil), exec.txContext.tx)
								}
								result, err := exec.Execute(ctx, query, nil)
								if explicit {
									_, endErr := exec.Execute(ctx, "ROLLBACK", nil)
									require.NoError(t, endErr)
								}
								require.NoError(t, err, query)
								fabricStructureRows(t, result.Rows, `[["one"]]`)
							})
						}
					})
				}
			})
		}
	}
}
