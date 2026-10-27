package cypher

import (
	"encoding/json"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCompositeNativeDoubledBacktickValues(t *testing.T) {
	previous := config.GetParserType()
	config.SetParserType("nornic")
	t.Cleanup(func() { config.SetParserType(previous) })
	for _, operation := range []string{"CREATE", "ALTER"} {
		t.Run(operation, func(t *testing.T) {
			var resolved []multidb.ConstituentRef
			cfg := multidb.DefaultConfig()
			cfg.RemoteCredentialEncryptionKey = "synthetic-escaped-values-key"
			cfg.RemoteEngineFactory = func(ref multidb.ConstituentRef, _ string) (storage.Engine, error) {
				resolved = append(resolved, ref)
				return newTestMemoryEngine(t), nil
			}
			inner := newTestMemoryEngine(t)
			manager, err := multidb.NewDatabaseManager(t.Context(), inner, cfg)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			engine, err := manager.GetStorage("nornic")
			require.NoError(t, err)
			executor := NewStorageExecutor(engine)
			executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			prefix := "CREATE COMPOSITE DATABASE escaped_values "
			if operation == "ALTER" {
				_, err = executor.Execute(t.Context(), prefix+"ALIAS seed FOR DATABASE nornic", nil)
				require.NoError(t, err)
				prefix = "ALTER COMPOSITE DATABASE escaped_values ADD "
			}
			clause := "ALIAS `r``one` FOR DATABASE `db``one` AT 'http://127.0.0.1:1' USER `us``er` PASSWORD `pa``ss` TYPE remote ACCESS read"
			_, err = executor.Execute(t.Context(), prefix+clause, nil)
			require.NoError(t, err)
			refs, err := manager.GetCompositeConstituents("escaped_values")
			require.NoError(t, err)
			stored := refs[len(refs)-1]
			require.Equal(t, "r`one", stored.Alias)
			require.Equal(t, "db`one", stored.DatabaseName)
			require.NotEmpty(t, stored.ConnectionV1, "one encrypted descriptor owns saved credentials")
			require.Empty(t, stored.User)
			require.Empty(t, stored.Password)
			require.Empty(t, stored.URI)
			display, err := manager.RemoteConstituentForDisplay(stored)
			require.NoError(t, err)
			require.Equal(t, "us`er", display.User, "explicit USER remains the public display identity")
			_, err = manager.GetStorage("escaped_values.r`one")
			require.NoError(t, err)
			require.Len(t, resolved, 1)
			require.Equal(t, "r`one", resolved[0].Alias)
			require.Equal(t, "db`one", resolved[0].DatabaseName)
			require.Equal(t, "us`er", resolved[0].User)
			require.Equal(t, "pa`ss", resolved[0].Password)
			system, err := manager.GetStorage("system")
			require.NoError(t, err)
			metadata, err := system.GetNode("databases:metadata")
			require.NoError(t, err)
			encoded, err := json.Marshal(metadata.Properties)
			require.NoError(t, err)
			require.NotContains(t, string(encoded), "pa`ss")
			require.NotContains(t, string(encoded), "us`er")
			for _, bad := range []string{"`bad`tail`", "`unclosed", "`bad`tail", "``"} {
				before, err := manager.GetCompositeConstituents("escaped_values")
				require.NoError(t, err)
				_, err = executor.Execute(t.Context(), "ALTER COMPOSITE DATABASE escaped_values ADD ALIAS "+bad+" FOR DATABASE nornic", nil)
				require.Error(t, err, bad)
				after, err := manager.GetCompositeConstituents("escaped_values")
				require.NoError(t, err)
				require.Equal(t, before, after, "malformed names must not mutate catalog")
			}
			_, err = executor.Execute(t.Context(), "ALTER COMPOSITE DATABASE escaped_values ADD ALIAS normal FOR DATABASE nornic", nil)
			require.NoError(t, err)
		})
	}
}

func TestAdminQuotedIdentifierPreservesCanonicalAdmission(t *testing.T) {
	for _, tc := range []struct {
		written, want string
		invalid       bool
	}{
		{"plain.name", "plain.name", false},
		{" `ordinary` ", "ordinary", false},
		{"`a``b`", "a`b", false},
		{"``", "", false},
		{"`a`b`", "", true},
	} {
		t.Run(tc.written, func(t *testing.T) {
			got, err := unquoteBacktickIdentifier(tc.written)
			if tc.invalid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
