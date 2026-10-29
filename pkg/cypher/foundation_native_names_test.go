package cypher

import (
	"github.com/orneryd/nornicdb/internal/contextslot"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func TestFoundationNativeDropAliasIdentity(t *testing.T) {
	for _, alias := range []string{"ordinary", " a,b ", "a\x60b"} {
		t.Run(alias, func(t *testing.T) {
			manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "drop_names", []multidb.ConstituentRef{
				{Alias: alias, DatabaseName: "nornic", Type: "local", AccessMode: "read_write"},
				{Alias: "kept", DatabaseName: "nornic", Type: "local", AccessMode: "read_write"},
			}))
			executor := NewStorageExecutor(newTestMemoryEngine(t))
			executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			written := "\x60" + strings.ReplaceAll(alias, "\x60", "\x60\x60") + "\x60"
			result, err := executor.Execute(t.Context(), "ALTER COMPOSITE DATABASE drop_names DROP ALIAS "+written, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"drop_names", "DROP", alias}}, result.Rows)
			refs, err := manager.GetCompositeConstituents("drop_names")
			require.NoError(t, err)
			require.Len(t, refs, 1)
			require.Equal(t, "kept", refs[0].Alias)
		})
	}
}

func TestFoundationNativeQuotedNames(t *testing.T) {
	t.Run("public constituent and credential consumers", func(t *testing.T) {
		config := multidb.DefaultConfig()
		config.RemoteCredentialEncryptionKey = "foundation-native-name-key"
		manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), config)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, manager.Close()) })
		executor := NewStorageExecutor(newTestMemoryEngine(t))
		executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
		ctx := contextslot.Ensure[cyphertext.CredentialSet](t.Context())
		const query = "CREATE COMPOSITE DATABASE native_names ALIAS \x60a\x60\x60b\x60 FOR DATABASE \x60d\x60\x60b\x60 AT 'http://example.invalid' USER \x60u\x60\x60b\x60 PASSWORD \x60p\x60\x60b\x60"
		_, err = executor.Execute(ctx, query, nil)
		require.NoError(t, err)
		refs, err := manager.GetCompositeConstituents("native_names")
		require.NoError(t, err)
		require.Len(t, refs, 1)
		require.Equal(t, "a\x60b", refs[0].Alias)
		require.Equal(t, "d\x60b", refs[0].DatabaseName)
		// An encrypted stored registration intentionally carries no raw USER or
		// PASSWORD field. Catalog identity stays public; the explicit USER is
		// decoded only by the manager-owned display projection.
		require.NotEmpty(t, refs[0].ConnectionV1, "one encrypted descriptor owns saved credentials")
		require.Empty(t, refs[0].User)
		require.Empty(t, refs[0].Password)
		display, err := manager.RemoteConstituentForDisplay(refs[0])
		require.NoError(t, err)
		require.Equal(t, "u\x60b", display.User, "explicit USER remains the public display identity")
		require.Empty(t, display.Password)
		require.Equal(t, "user_password", display.AuthMode)
		found := false
		for _, span := range contextslot.Get[cyphertext.CredentialSet](ctx).Spans {
			if span.Password && span.Raw == "\x60p\x60\x60b\x60" {
				require.Equal(t, "p\x60b", span.Value)
				require.Equal(t, span.Raw, query[span.Start:span.End])
				found = true
			}
		}
		require.True(t, found, "actual native value reader must retain the decoded credential")
	})
	t.Run("public value identity", func(t *testing.T) {
		executor := NewStorageExecutor(newTestMemoryEngine(t))
		for _, name := range []string{"a\x60b", "\x60a\x60"} {
			written := "\x60"
			for _, r := range name {
				written += string(r)
				if r == '\x60' {
					written += "\x60"
				}
			}
			written += "\x60"
			query := "UNWIND [7] AS " + written + " RETURN " + written + " AS value"
			t.Run(query, func(t *testing.T) {
				result, err := executor.Execute(t.Context(), query, nil)
				require.NoError(t, err)
				require.Equal(t, []string{"value"}, result.Columns)
				require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
			})
		}
	})
	t.Run("malformed quote stays invalid", func(t *testing.T) {
		executor := NewStorageExecutor(newTestMemoryEngine(t))
		_, err := executor.Execute(t.Context(), "CREATE COMPOSITE DATABASE malformed ALIAS \x60a\x60b\x60 FOR DATABASE nornic", nil)
		require.Error(t, err, "the quote closes after a; b plus the unmatched final delimiter is not one symbolic name")
	})
}
