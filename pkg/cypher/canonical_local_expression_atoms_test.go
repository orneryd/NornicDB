package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A compound expression over a statement-local expression (a subscript, an
// arithmetic result or a property access over a comprehension or reduce)
// keeps its static type whether or not the request carries parameters. Both
// signature readers (procedure arguments and function arguments) must reject
// it before anything is published, in every mode and position.
var canonicalLocalExpressionAtomFacts = []struct{ name, query string }{
	{"call subscript", "CALL db.resampleIndex([x IN [1] | x][0]) RETURN 1 AS done"},
	{"call arithmetic", "CALL db.resampleIndex(reduce(s = 0, x IN [1] | s + x) + 1) RETURN 1 AS done"},
	{"call property access", "CALL db.resampleIndex([x IN [1] | x][0].k) RETURN 1 AS done"},
	{"function subscript", "RETURN labels([x IN [1] | x][0]) AS bad"},
	{"function arithmetic", "RETURN labels(reduce(s = 0, x IN [1] | s + x) + 1) AS bad"},
}

var canonicalLocalExpressionAtomParams = []struct {
	name   string
	params map[string]interface{}
}{
	{"nil", nil},
	{"params", map[string]interface{}{"unused": int64(1)}},
}

type canonicalAtomExecutor func(t *testing.T) (*StorageExecutor, context.Context)

func checkCanonicalLocalExpressionAtoms(t *testing.T, newExecutor canonicalAtomExecutor, prefix string, positions []string) {
	for _, fact := range canonicalLocalExpressionAtomFacts {
		t.Run(fact.name, func(t *testing.T) {
			exec, ctx := newExecutor(t)
			baseline := prefix + fact.query
			_, err := exec.Execute(ctx, baseline, nil)
			require.Error(t, err, baseline)
			wantCode, wantDetail, wantMessage := boundRuleError(err)
			for _, position := range positions {
				for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
					for _, params := range canonicalLocalExpressionAtomParams {
						query := mode + prefix + position + fact.query
						t.Run(mode+position+params.name, func(t *testing.T) {
							exec, ctx := newExecutor(t)
							result, err := exec.Execute(ctx, query, params.params)
							code, detail, message := boundRuleError(err)
							t.Logf("LOCAL_EXPRESSION_ATOM_LEAF query=%q params=%s code=%q detail=%q message=%q result=%t", query, params.name, code, detail, message, result != nil)
							require.Error(t, err, query)
							require.Nil(t, result, query)
							require.Equal(t, wantCode, code, "%s vs %s", query, baseline)
							require.Equal(t, wantDetail, detail, "%s vs %s", query, baseline)
							require.Equal(t, wantMessage, message, "%s vs %s", query, baseline)
						})
					}
				}
			}
		})
	}
}

func TestCanonicalLocalExpressionAtomsWithParameters(t *testing.T) {
	positions := []string{"", "MATCH (n) ", "UNWIND [] AS ignored ", "UNWIND [0] AS ignored ", "CALL db.labels() YIELD label AS first "}
	checkCanonicalLocalExpressionAtoms(t, newUnitExecutor, "", positions)
}

// Fabric installs the request parameters before semantic admission, so its
// USE entry admits these statements with parameters present.
func TestCanonicalLocalExpressionAtomsFabricUse(t *testing.T) {
	newComposite := func(t *testing.T) (*StorageExecutor, context.Context) {
		t.Helper()
		base := newTestMemoryEngine(t)
		manager, err := multidb.NewDatabaseManager(t.Context(), base, nil)
		require.NoError(t, err)
		require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "leaf"}))
		require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "cmp", []multidb.ConstituentRef{
			{Alias: "a", DatabaseName: "leaf", Type: "local", AccessMode: "read_write"},
		}))
		exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"))
		exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
		return exec, context.Background()
	}
	positions := []string{"", "UNWIND [] AS ignored ", "UNWIND [0] AS ignored "}
	checkCanonicalLocalExpressionAtoms(t, newComposite, "USE cmp.a ", positions)
}
