package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The knowledge-policy procedures are kp.* (#1078). Each earlier nornicdb.*
// name runs the same procedure with the same signature, and SHOW PROCEDURES
// lists it as deprecated by its kp.* name.
func TestKnowledgePolicyProcedureAliases(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "kpalias"))
	ctx := context.Background()
	aliases := map[string]string{
		"nornicdb.decay.info":                    "kp.decayInfo",
		"nornicdb.knowledgepolicy.info":          "kp.info",
		"nornicdb.knowledgepolicy.profiles":      "kp.profiles",
		"nornicdb.knowledgepolicy.policies":      "kp.policies",
		"nornicdb.knowledgepolicy.resolve":       "kp.resolve",
		"nornicdb.knowledgepolicy.deindexStatus": "kp.deindexStatus",
	}
	for alias, name := range aliases {
		current, ok := globalProcedureRegistry.Get(name)
		require.True(t, ok, name)
		deprecated, ok := globalProcedureRegistry.Get(alias)
		require.True(t, ok, alias)
		require.Equal(t, strings.TrimPrefix(current.Spec.Signature, name), strings.TrimPrefix(deprecated.Spec.Signature, alias), alias)
		require.Equal(t, current.Spec.Returns, deprecated.Spec.Returns, alias)
		require.Equal(t, current.Spec.Mode, deprecated.Spec.Mode, alias)

		want, wantErr := exec.Execute(ctx, "CALL "+name+"()", nil)
		got, gotErr := exec.Execute(ctx, "CALL "+alias+"()", nil)
		if wantErr != nil {
			// The error names the procedure as called.
			require.EqualError(t, gotErr, strings.Replace(wantErr.Error(), "`"+name+"`", "`"+alias+"`", 1), alias)
			continue
		}
		require.NoError(t, gotErr, alias)
		require.Equal(t, want.Columns, got.Columns, alias)
		require.Equal(t, want.Rows, got.Rows, alias)
	}
	_, err := exec.Execute(ctx, "CALL nornicdb.knowledgepolicy.resolve()", nil)
	require.ErrorContains(t, err, "kp.resolve requires entityId, labels, or edgeType")

	show, err := exec.Execute(ctx, "SHOW PROCEDURES YIELD *", nil)
	require.NoError(t, err)
	column := func(name string) int {
		for index, candidate := range show.Columns {
			if candidate == name {
				return index
			}
		}
		t.Fatalf("SHOW PROCEDURES has no %s column", name)
		return -1
	}
	nameColumn, isDeprecatedColumn, deprecatedByColumn := column("name"), column("isDeprecated"), column("deprecatedBy")
	listed := map[string][2]interface{}{}
	for _, row := range show.Rows {
		listed[row[nameColumn].(string)] = [2]interface{}{row[isDeprecatedColumn], row[deprecatedByColumn]}
	}
	for alias, name := range aliases {
		require.Equal(t, [2]interface{}{true, name}, listed[alias], alias)
		require.Equal(t, [2]interface{}{false, nil}, listed[name], name)
	}
	for _, name := range []string{"kp.suppress", "kp.unsuppress"} {
		require.Equal(t, [2]interface{}{false, nil}, listed[name], name)
	}
}
