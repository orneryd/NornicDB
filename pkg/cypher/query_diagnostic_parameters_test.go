package cypher

import (
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func TestQueryDiagnosticParametersUsePreparedCredentialSpansWithoutEffects(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "existing_diagnostic", []multidb.ConstituentRef{{Alias: "seed", DatabaseName: "nornic", Type: "local", AccessMode: "read"}}))
	const secret = "synthetic-prepared-password-58371"
	for _, query := range []string{
		"CREATE COMPOSITE DATABASE untouched_diagnostic ALIAS r FOR DATABASE remote AT 'https://example.invalid' USER service PASSWORD '" + secret + "' ACCESS read",
		"CREATE COMPOSITE DATABASE existing_diagnostic IF NOT EXISTS ALIAS r FOR DATABASE remote AT 'https://example.invalid' USER service PASSWORD '" + secret + "' ACCESS read",
		"ALTER COMPOSITE DATABASE existing_diagnostic ADD ALIAS r FOR DATABASE remote AT 'https://example.invalid' USER service PASSWORD '" + secret + "' ACCESS read",
		"ALTER COMPOSITE DATABASE existing_diagnostic ADD ALIAS r FOR DATABASE remote AT 'https://example.invalid' USER service PASSWORD 'synthetic\\u002Dprepared-password-58371' ACCESS read",
	} {
		ctx := withStatementCredentials(t.Context(), query)
		set := statementCredentials(ctx)
		for _, span := range set.Spans {
			require.False(t, span.Password, "tokenization alone does not assign the password role")
		}
		params := map[string]any{"ordinary": secret, "safe": "visible", "count": int64(7)}
		want := map[string]any{"ordinary": secret, "safe": "visible", "count": int64(7)}
		safe := QueryDiagnosticParameters(ctx, query, params)
		require.Equal(t, "<REDACTED>", safe["ordinary"])
		require.Equal(t, "visible", safe["safe"])
		require.Equal(t, int64(7), safe["count"])
		require.True(t, reflect.DeepEqual(want, params))
		found := false
		for _, span := range set.Spans {
			found = found || span.Password && span.Value == secret
		}
		require.True(t, found, "the original context's credential set receives the actual reader's decoded password")
		require.Equal(t, query, set.Source.Original)
		require.False(t, manager.Exists("untouched_diagnostic"))
		refs, err := manager.GetCompositeConstituents("existing_diagnostic")
		require.NoError(t, err)
		require.Len(t, refs, 1)
		require.Equal(t, "seed", refs[0].Alias)
	}
	var params map[string]any
	require.Nil(t, QueryDiagnosticParameters(t.Context(), "RETURN 1", params))
}
