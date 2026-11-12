package cypher

import (
	"context"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/stretchr/testify/require"
)

func TestProfileEvaluationOwnsItsInputMetadata(t *testing.T) {
	builtInCypherFunctions["testing.profile_input_clean"] = struct{}{}
	t.Cleanup(func() { delete(builtInCypherFunctions, "testing.profile_input_clean") })
	// This stateless test-owned function observes the public execution while
	// it is in progress, rather than merely checking restored maps afterward.
	cypherfn.Register("testing.profile_input_clean", func(ctx context.Context, _ cypherfn.Context, _ []string) (interface{}, error) {
		for _, shared := range []map[string]interface{}{cypher25RowValues, parameterRowValues(ctx)} {
			if _, present := shared[rowStatementContextKey]; present {
				return false, nil
			}
			if _, present := shared[rowExpressionOriginKey]; present {
				return false, nil
			}
		}
		return true, nil
	}, cypherfn.OpaqueReads)
	forEachParser(t, func(t *testing.T) {
		for _, prefix := range []string{"", "CYPHER 25 "} {
			for _, params := range []map[string]interface{}{nil, {"unused": int64(7)}} {
				exec, ctx := newProfileRuntimeTestExecutor(t, "CREATE (:Seed)")
				result, err := exec.Execute(ctx, prefix+"PROFILE RETURN testing.profile_input_clean() AS clean", params)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{true}}, result.Rows)
			}
		}
	})
}
