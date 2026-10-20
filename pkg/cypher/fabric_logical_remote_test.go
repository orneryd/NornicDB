package cypher

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// Remote records must carry the same logical frame as local storage adapters.
// The oracle runs in a rolled-back transaction so its writes cannot seed the
// remote constituent. Unit CALL checks both returned cardinality and effects.
func TestFabricLogicalRemoteFrame(t *testing.T) {
	driver := fabricCallRowsReference(t)
	if driver == nil {
		t.Skip("NORNICDB_NEO4J_REFERENCE_URI is required for the remote constituent")
	}
	ctx := context.Background()
	for _, normalize := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			for _, tc := range []struct {
				name, query, want string
				invalid, unit     bool
			}{
				{"named", "WITH 11 AS x CALL (x) { <USE> WITH 1 AS y RETURN x AS v } RETURN v", "[[11]]", false, false},
				{"star", "WITH 11 AS x CALL (*) { <USE> WITH 1 AS y RETURN x AS v } RETURN v", "[[11]]", false, false},
				{"expression_label", "CALL { <USE> RETURN head([null]):A AS v } RETURN v", "[[null]]", false, false},
				{"scoped_expression_label", "WITH 11 AS x CALL (x) { <USE> RETURN (null):A AS v } RETURN x,v", "[[11,null]]", false, false},
				{"empty_scope", "WITH 11 AS x CALL () { <USE> RETURN x AS v } RETURN v", "", true, false},
				{"embedding", "WITH 7 AS embedding CALL (embedding) { <USE> WITH 1 AS y CREATE (:FabricFrameRemoteProjection) WITH embedding RETURN 1 AS k } RETURN k", "[[1]]", false, false},
				{"unit", "UNWIND [1,2] AS x CALL (x) { <USE> UNWIND [1,2] AS y CREATE (:FabricFrameRemoteUnit {x:x,y:y}) } RETURN x ORDER BY x", "[[1],[2]]", false, true},
				{"typed_import", "WITH [1] AS x CALL (x) { <USE> UNWIND [] AS unused RETURN toUpper(x) AS v } RETURN v", "", true, false},
				{"empty_admission", "UNWIND [] AS unused CALL { <USE> RETURN toUpper(CASE WHEN true THEN [1] ELSE ['a'] END) AS v } RETURN v", "", true, false},
				{"numeric_union", "<USE> RETURN 1 AS v UNION <USE> RETURN 1.0 AS v", "[[1]]", false, false},
				{"nested_union", "CALL { <USE> RETURN {k:[1]} AS v UNION <USE> RETURN {k:[1.0]} AS v } RETURN v", "[[{\"k\":[1]}]]", false, false},
			} {
				t.Run(fmt.Sprintf("normalization_%v/explicit_%v/%s", normalize, explicit, tc.name), func(t *testing.T) {
					restore := func() {}
					if !normalize {
						restore = config.WithCypherQueryNormalizationDisabled()
					}
					defer restore()
					cleanup := func() {
						_, err := neo4j.ExecuteQuery(ctx, driver, "MATCH (n) WHERE n:FabricFrameRemoteUnit OR n:FabricFrameRemoteProjection DETACH DELETE n", nil, neo4j.EagerResultTransformer)
						require.NoError(t, err)
					}
					cleanup()
					t.Cleanup(cleanup)
					session := driver.NewSession(ctx, neo4j.SessionConfig{})
					defer session.Close(ctx)
					ref, err := session.BeginTransaction(ctx)
					require.NoError(t, err)
					if tc.invalid {
						refResult, refErr := ref.Run(ctx, strings.ReplaceAll(tc.query, "<USE>", ""), nil)
						if refErr == nil {
							_, refErr = refResult.Collect(ctx)
						}
						var status *neo4j.Neo4jError
						require.ErrorAs(t, refErr, &status)
						require.Equal(t, "Neo.ClientError.Statement.SyntaxError", status.Code)
					} else {
						fabricCallRowsAssertReference(t, ref, strings.ReplaceAll(tc.query, "<USE>", ""), tc.want)
						if tc.unit {
							fabricCallRowsAssertReference(t, ref, "MATCH (n:FabricFrameRemoteUnit) RETURN count(n) AS c", "[[4]]")
						}
					}
					require.NoError(t, ref.Rollback(ctx))
					base := newFabricCallRowsExecutor(t)
					mgr := base.dbManager.(*testDatabaseManagerAdapter).manager
					require.NoError(t, mgr.AddConstituent("comp", multidb.ConstituentRef{Alias: "remote", DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")}))
					exec := fabricCallRowsCompositeExecutor(t, base)
					if explicit {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer func() { _, err := exec.Execute(ctx, "ROLLBACK", nil); require.NoError(t, err) }()
						_, ok := exec.txContext.tx.(*fabric.FabricTransaction)
						require.True(t, ok)
					}
					query := strings.ReplaceAll(tc.query, "<USE>", "USE comp.remote")
					result, err := exec.Execute(ctx, query, nil)
					if tc.invalid {
						require.Error(t, err)
						code, _ := nornicerrors.Neo4jStatus(err)
						require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
						return
					}
					require.NoError(t, err)
					fabricDispatchRows(t, result.Rows, tc.want)
					if tc.unit {
						count, err := exec.Execute(ctx, "USE comp.remote MATCH (n:FabricFrameRemoteUnit) RETURN count(n) AS c", nil)
						require.NoError(t, err)
						fabricDispatchRows(t, count.Rows, "[[4]]")
					}
				})
			}
		}
	}
}
