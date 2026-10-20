package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestReviewR7EmbeddingProjectionAfterMutation(t *testing.T) {
	exec := newFabricCallRowsExecutor(t)
	result, err := exec.Execute(context.Background(), "USE comp.one CREATE (embedding:ReviewEmbeddingName {k:7}) WITH embedding RETURN embedding.k AS k", nil)
	require.NoError(t, err)
	require.EqualValues(t, 7, result.Rows[0][0])
}

func TestFabricEmbeddingProjectionMutationMatrix(t *testing.T) {
	for _, normalization := range []bool{true, false} {
		for _, mode := range []string{"autocommit", "explicit", "native_autocommit", "native_explicit"} {
			t.Run(fmt.Sprintf("normalize_%v/%s", normalization, mode), func(t *testing.T) {
				if !normalization {
					defer config.WithCypherQueryNormalizationDisabled()()
				}
				exec := newFabricCallRowsExecutor(t)
				if mode == "explicit" {
					exec = fabricCallRowsCompositeExecutor(t, exec)
				}
				ctx := context.Background()
				for _, name := range []string{"embedding", "EMBEDDING", "EmBeDdInG", "`embedding`"} {
					for _, mutation := range []string{"CREATE (%[1]s:ProjectionMutation {k:7})", "MERGE (%[1]s:ProjectionMutation {k:7})", "INSERT (%[1]s:ProjectionMutation {k:7})", "NODETACH DELETE %[1]s", "SET %[1]s.k=7", "REMOVE %[1]s.unused", "DELETE %[1]s", "DETACH DELETE %[1]s", "FOREACH (i IN [1] | SET %[1]s.k=7)"} {
						if strings.HasPrefix(mode, "native_") && (strings.HasPrefix(mutation, "INSERT") || strings.HasPrefix(mutation, "NODETACH")) {
							continue
						}
						for _, alias := range []bool{false, true} {
							for _, following := range []string{"RETURN 7 AS k", "WITH * RETURN 7 AS k", "UNWIND [7] AS k RETURN k"} {
								t.Run(fmt.Sprintf("%s/%s/alias_%v/%s", name, mutation, alias, following), func(t *testing.T) {
									body := fmt.Sprintf(mutation, name)
									if mutation != "CREATE (%[1]s:ProjectionMutation {k:7})" && mutation != "MERGE (%[1]s:ProjectionMutation {k:7})" && mutation != "INSERT (%[1]s:ProjectionMutation {k:7})" {
										body = fmt.Sprintf("CREATE (%s:ProjectionMutation {k:7}) ", name) + body
									}
									projection := name
									if alias {
										projection += " AS retained"
									}
									query := body + " WITH " + projection + " " + following
									if !strings.HasPrefix(mode, "native_") {
										query = "USE comp.one " + query
									}
									if strings.Contains(mode, "explicit") {
										_, err := exec.Execute(ctx, "BEGIN", nil)
										require.NoError(t, err)
										defer exec.Execute(ctx, "ROLLBACK", nil)
									}
									result, err := exec.Execute(ctx, query, nil)
									require.NoError(t, err, query)
									require.NotEmpty(t, result.Rows)
									require.EqualValues(t, 7, result.Rows[0][0])
									if strings.Contains(mode, "explicit") {
										_, err = exec.Execute(ctx, "COMMIT", nil)
										require.NoError(t, err)
									}
								})
							}
						}
					}
				}
			})
		}
	}
}

func TestFabricEmbeddingProjectionAcrossFragments(t *testing.T) {
	exec := newFabricCallRowsExecutor(t)
	for _, query := range []string{
		"WITH 7 AS embedding CALL { WITH embedding USE comp.one CREATE (n:CrossFragmentEmbedding) WITH embedding RETURN embedding AS k } RETURN k",
		"WITH 7 AS EMBEDDING CALL { WITH EMBEDDING USE comp.one CREATE (n:CrossFragmentEmbedding) WITH EMBEDDING RETURN EMBEDDING AS k } RETURN k",
		"WITH 7 AS embedding USE comp.one CREATE (n:CrossFragmentEmbedding) WITH embedding RETURN embedding AS k",
	} {
		result, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err, query)
		require.NotEmpty(t, result.Rows)
		require.EqualValues(t, 7, result.Rows[0][0])
	}
}
