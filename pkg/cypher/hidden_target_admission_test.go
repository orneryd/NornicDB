package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// hiddenTargetForms place a selection where execution may not reach it, may
// fail before it, or may never plan it. {name} is the selected graph.
var hiddenTargetForms = []string{
	"USE {name} UNWIND [] AS u RETURN u",
	"UNWIND [] AS u CALL { USE {name} RETURN 1 AS x } RETURN x",
	"UNWIND [1/0] AS u CALL { USE {name} RETURN 1 AS x } RETURN x",
	"UNWIND [1] AS u WITH u WHERE u > 1 CALL { USE {name} RETURN 1 AS x } RETURN x",
	"MATCH (n:NoSuchLabel) CALL { USE {name} RETURN 1 AS x } RETURN x",
	"UNWIND [] AS u CALL (u) { USE {name} RETURN u AS x } RETURN x",
	"UNWIND [] AS u OPTIONAL CALL { USE {name} RETURN 1 AS x } RETURN x",
	"UNWIND [] AS u CALL { WITH u USE {name} RETURN u AS x } RETURN x",
	"UNWIND [] AS u CALL { USE {name} CREATE (n) } RETURN u",
	"UNWIND [] AS u CALL { CALL { USE {name} RETURN 1 AS x } RETURN x } RETURN x",
	"CALL { RETURN 1 AS x UNION ALL USE {name} RETURN 2 AS x } RETURN x",
	"UNWIND [] AS u CALL { USE {name} RETURN 1 AS x } RETURN x, $missing AS m",
	"UNWIND [] AS u CALL { USE {name} RETURN 1 AS x } RETRUN x",
	"UNWIND [] AS u CALL { USE {name} RETURN 1 AS x } RETURN nope",
	"EXPLAIN UNWIND [] AS u CALL { USE {name} RETURN 1 AS x } RETURN x",
	"PROFILE UNWIND [] AS u CALL { USE {name} RETURN 1 AS x } RETURN x",
	"UNWIND [] AS u CALL { USE graph.byName('{name}') RETURN 1 AS x } RETURN x",
	"UNWIND [1] AS u CALL { USE graph.byName('{name}') RETURN 1 AS x } RETURN x",
	"UNWIND ['{name}'] AS g CALL (g) { USE graph.byName(g) RETURN 1 AS x } RETURN x",
}

// answerOf is everything a client sees of a statement, with the name it asked
// about removed.
func answerOf(result *ExecuteResult, err error, names ...string) string {
	var text string
	if err != nil {
		text = "error: " + err.Error()
	} else {
		encoded, marshalErr := json.Marshal(map[string]any{"columns": result.Columns, "rows": result.Rows})
		if marshalErr != nil {
			return "unencodable: " + marshalErr.Error()
		}
		text = string(encoded)
	}
	for _, name := range names {
		text = strings.ReplaceAll(text, name, "NAME")
	}
	return text
}

// TestHiddenTargetAnswersAsAbsentWhereverItIsSelected compares a graph the
// principal may not access with one that does not exist, when the statement
// runs alone, inside an explicit transaction, and after a principal who may
// access the graph has warmed the plan and result caches.
func TestHiddenTargetAnswersAsAbsentWhereverItIsSelected(t *testing.T) {
	inComposite := newCompositeFixture(t)
	insideComposite := WithDatabasePermissionResolver(context.Background(), "pcomp", func(database, permission string) bool {
		return database == "pcomp" || database == "pcomp.other" || database == "pother"
	})

	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "private"}))
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "hidden_composite", []multidb.ConstituentRef{
		{Alias: "ro", DatabaseName: "private", Type: "local", AccessMode: "read"},
	}))
	plain := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	plain.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	outside := WithDatabasePermissionResolver(context.Background(), "test", func(database, permission string) bool {
		return database == "test" || database == "nornic"
	})

	for _, scenario := range []struct {
		name            string
		exec            *StorageExecutor
		principal       context.Context
		hidden, missing string
	}{
		{"constituent from its composite", inComposite, insideComposite, "pcomp.third", "pcomp.fifth"},
		{"composite from another database", plain, outside, "hidden_composite", "absent_composite"},
		{"constituent of a hidden composite", plain, outside, "hidden_composite.ro", "absent_composite.ro"},
		{"database from another database", plain, outside, "private", "missing"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			names := []string{scenario.hidden, scenario.missing}
			run := func(ctx context.Context, form, name string, params map[string]any) string {
				result, err := scenario.exec.Execute(ctx, strings.ReplaceAll(form, "{name}", name), params)
				return answerOf(result, err, names...)
			}
			for _, form := range hiddenTargetForms {
				t.Run(form, func(t *testing.T) {
					require.Equal(t, run(scenario.principal, form, scenario.missing, nil), run(scenario.principal, form, scenario.hidden, nil), "alone")
					if !strings.Contains(form, "CREATE") {
						// A principal with access plans and caches the hidden name first.
						_ = run(context.Background(), form, scenario.hidden, nil)
						require.Equal(t, run(scenario.principal, form, scenario.missing, nil), run(scenario.principal, form, scenario.hidden, nil), "after warming")
					}
					inTransaction := func(name string) string {
						if _, err := scenario.exec.Execute(scenario.principal, "BEGIN", nil); err != nil {
							return fmt.Sprintf("begin: %v", err)
						}
						answer := run(scenario.principal, form, name, nil)
						_, _ = scenario.exec.Execute(scenario.principal, "ROLLBACK", nil)
						return answer
					}
					require.Equal(t, inTransaction(scenario.missing), inTransaction(scenario.hidden), "in a transaction")
				})
			}
		})
	}

	// Admission must not refuse what the principal may run, and a principal who
	// may access a name still learns it is absent.
	result, err := inComposite.Execute(insideComposite, "UNWIND [] AS u CALL { USE pcomp.other RETURN 1 AS x } RETURN x", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"x"}, result.Columns)
	require.Empty(t, result.Rows)
	result, err = inComposite.Execute(insideComposite, "UNWIND [1] AS u CALL { USE pcomp.other RETURN 1 AS x } RETURN x", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	_, err = inComposite.Execute(context.Background(), "UNWIND [] AS u CALL { USE pcomp.absent RETURN 1 AS x } RETURN x", nil)
	require.Error(t, err)
	var denied *PermissionDeniedError
	require.NotErrorAs(t, err, &denied)

	// What a statement needs of a graph it may see is decided up front too.
	readOnly := WithDatabasePermissionResolver(context.Background(), "pcomp", func(database, permission string) bool {
		return permission != "write" && (database == "pcomp" || database == "pcomp.other" || database == "pother")
	})
	_, err = inComposite.Execute(readOnly, "UNWIND [] AS u CALL { USE pcomp.other CREATE (n) } RETURN u", nil)
	require.ErrorAs(t, err, &denied)
	require.Equal(t, "write", denied.Permission)
}
