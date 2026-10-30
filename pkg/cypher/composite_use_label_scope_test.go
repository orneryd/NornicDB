package cypher

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// A statement routed to a composite constituent (USE composite.alias) checks
// its variable scopes as one database does: a label test on an expression
// ((null):A) is a label test, not a reference to a variable A, while a name
// the statement never defines is still rejected.
func TestCompositeUseRouteLabelTestScopes(t *testing.T) {
	ctx := context.Background()
	manager, err := multidb.NewDatabaseManager(ctx, newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	defer manager.Close()
	require.NoError(t, manager.CreateDatabase(ctx, multidb.DatabaseCreateRequest{Name: "lbl_shard"}))
	require.NoError(t, manager.CreateCompositeDatabase(ctx, "lbl_cmp", []multidb.ConstituentRef{{Alias: "sh", DatabaseName: "lbl_shard", Type: "local", AccessMode: "read_write"}}))
	composite, err := manager.GetStorage("lbl_cmp")
	require.NoError(t, err)
	exec := NewStorageExecutor(composite)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})

	for _, tc := range []struct{ query, rows string }{
		{"USE lbl_cmp.sh RETURN (null):A AS v", `[[null]]`},
		{"USE lbl_cmp.sh UNWIND [1,2] AS x RETURN x, (null):A AS v", `[[1,null],[2,null]]`},
		{"USE lbl_cmp.sh RETURN (null):A|B AS v", `[[null]]`},
		{"CALL { USE lbl_cmp.sh RETURN (null):A AS v } RETURN v", `[[null]]`},
	} {
		result, err := exec.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.query)
		rows, err := json.Marshal(result.Rows)
		require.NoError(t, err)
		require.JSONEq(t, tc.rows, string(rows), tc.query)
	}
	_, err = exec.Execute(ctx, "EXPLAIN USE lbl_cmp.sh RETURN (null):A AS v", nil)
	require.NoError(t, err, "EXPLAIN checks the same scopes")

	for _, query := range []string{
		"USE lbl_cmp.sh RETURN missing AS v",
		"USE lbl_cmp.sh RETURN missing:A AS v",
		"EXPLAIN USE lbl_cmp.sh RETURN missing:A|B AS v",
		"PROFILE USE lbl_cmp.sh RETURN missing:A AS v",
	} {
		_, err = exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), "Variable `missing` not defined", query)
	}
	// Inside CALL { USE ... } the member database rejects the name when it
	// runs the subquery; its message is its own.
	_, err = exec.Execute(ctx, "CALL { USE lbl_cmp.sh RETURN missing:A AS v } RETURN v", nil)
	require.Error(t, err)
}
