package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRetrievalWhereFunctionAliases(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "where_alias")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	service := search.NewService(store)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	service.EnableRelationshipSearch(nil)
	service.StartRelationshipIndex()
	exec.SetSearchService(service)
	_, err := exec.Execute(ctx, "CREATE (a:Doc {content:'alias transcript',kind:'a'})-[:CITES {reason:'alias transcript',kind:'edge'}]->(:Doc {content:'alias transcript',kind:'b'})", nil)
	require.NoError(t, err)
	require.NoError(t, service.BuildIndexes(ctx))
	withPluginFunctions(t, map[string]interface{}{
		"aliasfixture.local_time": func() bool { return true },
	})
	priorPredicate, priorValue := rowPredicateProgramsDisabled.Load(), rowValueProgramsDisabled.Load()
	t.Cleanup(func() {
		rowPredicateProgramsDisabled.Store(priorPredicate)
		rowValueProgramsDisabled.Store(priorValue)
	})
	for _, disabled := range []bool{false, true} {
		rowPredicateProgramsDisabled.Store(disabled)
		rowValueProgramsDisabled.Store(disabled)
		for _, procedure := range []string{"db.retrieve", "db.rretrieve", "db.retrieveRelationships", "db.retrieveGraph", "db.rretrieveGraph"} {
			t.Run(fmt.Sprintf("programsDisabled=%t/%s", disabled, procedure), func(t *testing.T) {
				query := "CALL " + procedure + "($request)"
				read := func(predicate string) (*ExecuteResult, error) {
					return exec.Execute(ctx, query, map[string]interface{}{
						"request":    map[string]interface{}{"query": "alias", "limit": int64(10), "where": predicate},
						"local_time": true, "spelling": "local_time()",
					})
				}
				want := 2
				property := "property_exists(node, kind)"
				if strings.HasSuffix(procedure, "Relationships") {
					want, property = 1, "property_exists(relationship, kind)"
				} else if strings.HasSuffix(procedure, "Graph") {
					want = 3
					property = "CASE WHEN node IS NULL THEN property_exists(relationship, kind) ELSE property_exists(node, kind) END"
				}
				require.Eventually(t, func() bool {
					result, err := read("")
					return err == nil && len(result.Rows) == want
				}, 5*time.Second, 20*time.Millisecond, "fixture must expose every candidate kind")
				for _, predicate := range []string{
					"ceiling (1.2) = 2", "LN(1) = 0",
					"local_time('12:00') = localtime('12:00')",
					"local_datetime('2020-01-02T03:04') = localdatetime('2020-01-02T03:04')",
					"zoned_time('12:00+01:00') = time('12:00+01:00')",
					"zoned_datetime('2020-01-02T03:04Z') = datetime('2020-01-02T03:04Z')",
					"local_time() IS NOT NULL", "local_datetime() IS NOT NULL",
					"zoned_time() IS NOT NULL", "zoned_datetime() IS NOT NULL",
					"duration_between(date('2020-01-01'),date('2020-01-02')) IS NOT NULL",
					"path_length(null) IS NULL", property,
					"'local_time()' = $spelling", "$local_time", "aliasfixture.local_time()",
				} {
					for _, negate := range []bool{false, true} {
						text, count := predicate, want
						if negate {
							text, count = "NOT ("+predicate+")", 0
						}
						if disabled {
							text = "CASE WHEN " + text + " THEN true ELSE false END"
						}
						for round := 0; round < 2; round++ {
							result, err := read(text)
							require.NoError(t, err, text)
							require.Len(t, result.Rows, count, text)
						}
					}
				}
			})
		}
	}
}
