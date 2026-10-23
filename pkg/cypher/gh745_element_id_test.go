package cypher

// gh745_element_id_test.go — regression tests for #745: element ids must
// identify the entity's own database on every route. Section 3: an entity
// returned through a composite subquery keeps its constituent's identity.

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGh745_CompositeSubqueryElementIDNamesConstituent(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("pcomp_other"))
	require.NoError(t, mgr.CreateCompositeDatabase("pcomp", []multidb.ConstituentRef{
		{Alias: "other", DatabaseName: "pcomp_other", Type: "local", AccessMode: "read_write"},
	}))

	cmpStore, err := mgr.GetStorage("pcomp")
	require.NoError(t, err)
	exec := NewStorageExecutor(cmpStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	ctx := context.Background()

	// Seed one node in the constituent.
	_, err = exec.Execute(ctx, "CALL { USE pcomp.other CREATE (n:EI {k: 1}) RETURN count(n) AS c } RETURN c", nil)
	require.NoError(t, err)

	// The node id inside the subquery names the constituent.
	direct, err := exec.Execute(ctx, "CALL { USE pcomp.other MATCH (n:EI) RETURN elementId(n) AS e } RETURN e", nil)
	require.NoError(t, err)
	require.Len(t, direct.Rows, 1)
	directID := gh745ColumnString(t, direct, "e")
	require.Contains(t, directID, ":pcomp_other:")

	// The entity returned through the subquery and projected outside it keeps
	// the same constituent identity (#745 §3): elementId(n) after CALL { USE }
	// must not name the default database.
	result, err := exec.Execute(ctx, "CALL { USE pcomp.other MATCH (n:EI) RETURN n } RETURN elementId(n) AS e", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	projectedID := gh745ColumnString(t, result, "e")
	require.Equal(t, directID, projectedID)
	require.Contains(t, projectedID, ":pcomp_other:")

	// And the id round-trips: WHERE elementId(n) = <that id> finds the node.
	roundTrip, err := exec.Execute(ctx,
		"CALL { USE pcomp.other MATCH (n:EI) WHERE elementId(n) = $e RETURN n.k AS k } RETURN k",
		map[string]interface{}{"e": projectedID})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, roundTrip.Rows)
}

// gh745ColumnString reads a string cell of the first row by column name,
// tolerating fabric's combined input+inner columns on composite applies.
func gh745ColumnString(t *testing.T, result *ExecuteResult, column string) string {
	t.Helper()
	for i, name := range result.Columns {
		if !strings.EqualFold(strings.TrimSpace(name), column) {
			continue
		}
		if len(result.Rows) > 0 && i < len(result.Rows[0]) {
			if value, ok := result.Rows[0][i].(string); ok {
				return value
			}
		}
	}
	t.Fatalf("column %q not found as a string in %#v", column, result)
	return ""
}
