package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// The grammar's entire keyword vocabulary is data in each lexical container,
// including a parameter next to a real UNION. No word-specific exceptions.
func TestFabricQueryStructureGeneratedData(t *testing.T) {
	ctx := context.Background()
	base := newFabricCallRowsExecutor(t)
	driver := fabricCallRowsReference(t)
	for _, normalization := range []bool{true, false} {
		t.Run(fmt.Sprintf("normalization_%v", normalization), func(t *testing.T) {
			restore := func() {}
			if !normalization {
				restore = config.WithCypherQueryNormalizationDisabled()
			}
			defer restore()
			for _, word := range fabricGrammarWords() {
				for _, form := range []string{"parameter", "string", "quoted_name", "comment", "real_union"} {
					t.Run(word+"/"+form, func(t *testing.T) {
						key := strings.ToLower(word)
						params := map[string]interface{}{key: int64(7)}
						body := "RETURN $" + key + " AS k"
						want := "[[7]]"
						switch form {
						case "string":
							body = "RETURN '" + word + "' AS k"
							encoded, err := json.Marshal([][]interface{}{{word}})
							require.NoError(t, err)
							want = string(encoded)
						case "quoted_name":
							body = "WITH 7 AS `" + word + "` RETURN `" + word + "` AS k"
						case "comment":
							body = "RETURN 7 /* " + word + " CALL { } UNION USE */ AS k"
						case "real_union":
							body += " UNION ALL RETURN 8 AS k"
							want = "[[7],[8]]"
						}
						if driver != nil {
							ref, err := neo4j.ExecuteQuery(ctx, driver, "CALL { "+body+" } RETURN k", params, neo4j.EagerResultTransformer)
							require.NoError(t, err)
							rows := make([][]interface{}, 0, len(ref.Records))
							for _, record := range ref.Records {
								rows = append(rows, record.Values)
							}
							fabricStructureRows(t, rows, want)
						}
						for _, explicit := range []bool{false, true} {
							exec := base
							if explicit {
								exec = fabricCallRowsCompositeExecutor(t, base)
								_, err := exec.Execute(ctx, "BEGIN", nil)
								require.NoError(t, err)
							}
							result, err := exec.Execute(ctx, "USE comp CALL { USE comp.one "+body+" } RETURN k", params)
							if explicit {
								_, endErr := exec.Execute(ctx, "ROLLBACK", nil)
								require.NoError(t, endErr)
							}
							require.NoError(t, err, "explicit=%v", explicit)
							fabricStructureRows(t, result.Rows, want)
						}
					})
				}
			}
		})
	}
}
