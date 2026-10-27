package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestSolReview1483R3Lexical(t *testing.T) {
	base := newFabricCallRowsExecutor(t)
	for _, word := range strings.Fields("UNION ALL CALL USE RETURN WITH CREATE MERGE DELETE SET REMOVE MATCH OPTIONAL UNWIND WHERE ORDER BY SKIP LIMIT AS AND") {
		for _, form := range []string{"parameter", "quoted", "property", "comment"} {
			t.Run(word+"/"+form, func(t *testing.T) {
				expression := "$" + strings.ToLower(word)
				params := map[string]interface{}{strings.ToLower(word): int64(7)}
				switch form {
				case "quoted":
					expression = "`" + word + "`"
				case "property":
					expression = "({`" + word + "`:7}).`" + word + "`"
				case "comment":
					expression = "7 /* " + word + " { } */"
				}
				prefix := ""
				if form == "quoted" {
					prefix = "WITH 7 AS `" + word + "` "
				}
				query := "USE comp CALL /* USE UNION { } */ { USE comp.one " + prefix + "RETURN " + expression + " AS k } RETURN k"
				for _, explicit := range []bool{false, true} {
					exec := base
					if explicit {
						exec = fabricCallRowsCompositeExecutor(t, base)
						_, err := exec.Execute(context.Background(), "BEGIN", nil)
						require.NoError(t, err)
					}
					result, err := exec.Execute(context.Background(), query, params)
					if explicit {
						_, endErr := exec.Execute(context.Background(), "ROLLBACK", nil)
						require.NoError(t, endErr)
					}
					require.NoError(t, err, "explicit=%v", explicit)
					fabricStructureRows(t, result.Rows, "[[7]]")
				}
			})
		}
	}
}

// Token matching now accepts comments/whitespace between words. Verify that
// downstream CALL lookup modifier parsing preserves the same legal forms.
func TestSolReview1483R3ModifierTokens(t *testing.T) {
	ctx := context.Background()
	base := newFabricCallRowsExecutor(t)
	_, err := base.Execute(ctx, "USE comp.two CREATE (:R3Mod {k:1, v:1}), (:R3Mod {k:1, v:2}), (:R3Mod {k:1, v:3})", nil)
	require.NoError(t, err)
	driver := fabricCallRowsReference(t)
	for _, separator := range []string{" ", "  ", "\n", " /* UNION CALL RETURN */ "} {
		for _, direction := range []string{"ASC", "DESC"} {
			t.Run(fmt.Sprintf("%q/%s", separator, direction), func(t *testing.T) {
				order := "ORDER" + separator + "BY v " + direction + " LIMIT 1"
				query := "USE comp CALL { USE comp.one RETURN 1 AS k } CALL { WITH k USE comp.two MATCH (n:R3Mod) WHERE n.k = k RETURN n.v AS v " + order + " } RETURN v"
				reference := "CALL { RETURN 1 AS k } CALL { WITH k UNWIND [1,2,3] AS v RETURN v " + order + " } RETURN v"
				want := "[[1]]"
				if direction == "DESC" {
					want = "[[3]]"
				}
				if driver != nil {
					ref, err := neo4j.ExecuteQuery(ctx, driver, reference, nil, neo4j.EagerResultTransformer)
					require.NoError(t, err)
					rows := make([][]interface{}, 0, len(ref.Records))
					for _, r := range ref.Records {
						rows = append(rows, r.Values)
					}
					fabricStructureRows(t, rows, want)
				}
				for _, explicit := range []bool{false, true} {
					exec := base
					if explicit {
						exec = fabricCallRowsCompositeExecutor(t, base)
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					result, err := exec.Execute(ctx, query, nil)
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
}

func TestSolReview1483R3RawText(t *testing.T) {
	restore := config.WithCypherQueryNormalizationDisabled()
	defer restore()
	require.False(t, config.IsCypherQueryNormalizationEnabled(), "raw probe must bypass canonicalization")
	TestSolReview1483R3ModifierTokens(t)
}

func TestSolReview1483R3UnquotedNames(t *testing.T) {
	ctx := context.Background()
	base := newFabricCallRowsExecutor(t)
	driver := fabricCallRowsReference(t)
	if driver == nil {
		t.Skip("reviewer oracle probe requires --ref neo-ref")
	}
	for _, word := range []string{"union", "use", "return", "with", "create", "call"} {
		t.Run(word, func(t *testing.T) {
			query := "USE comp WITH 7 AS " + word + " WITH DISTINCT " + word + " CALL { WITH " + word + " USE comp.one RETURN " + word + " AS k } RETURN k"
			reference := strings.NewReplacer("USE comp.one ", "", "USE comp ", "").Replace(query)
			ref, err := neo4j.ExecuteQuery(ctx, driver, reference, nil, neo4j.EagerResultTransformer)
			require.NoError(t, err, "Neo4j accepts the unquoted name")
			rows := make([][]interface{}, 0, len(ref.Records))
			for _, r := range ref.Records {
				rows = append(rows, r.Values)
			}
			fabricStructureRows(t, rows, "[[7]]")
			for _, explicit := range []bool{false, true} {
				exec := base
				if explicit {
					exec = fabricCallRowsCompositeExecutor(t, base)
					_, err = exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				result, err := exec.Execute(ctx, query, nil)
				if explicit {
					_, endErr := exec.Execute(ctx, "ROLLBACK", nil)
					require.NoError(t, endErr)
				}
				require.NoError(t, err, "explicit=%v", explicit)
				fabricStructureRows(t, result.Rows, "[[7]]")
			}
		})
	}
}
