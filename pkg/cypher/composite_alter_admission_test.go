package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func TestCompositeAlterPublicAdmission(t *testing.T) {
	const secret = "synthetic-alter-admission-385247"
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			cfg := multidb.DefaultConfig()
			cfg.RemoteCredentialEncryptionKey = "synthetic-admission-key"
			manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), cfg)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			engine, err := manager.GetStorage("nornic")
			require.NoError(t, err)
			executor := NewStorageExecutor(engine)
			executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			ctx := context.Background()
			_, err = executor.Execute(ctx, "CREATE COMPOSITE DATABASE alter_admission ALIAS seed FOR DATABASE nornic", nil)
			require.NoError(t, err)
			_, err = executor.Execute(ctx, "ALTER COMPOSITE DATABASE alter_admission ADD ALIAS second FOR DATABASE nornic", nil)
			require.NoError(t, err)
			endpoint := "http://admin:" + secret + "@127.0.0.1:1"
			query := "ALTER COMPOSITE DATABASE alter_admission ADD ALIAS remote FOR DATABASE nornic AT '" + endpoint + "' TYPE remote ACCESS read"
			result, err := executor.Execute(ctx, query, nil)
			if parser == "antlr" {
				require.Error(t, err)
				require.NotContains(t, err.Error(), secret)
			} else {
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
				require.NotContains(t, result.Rows[0][5], secret)
				refs, err := manager.GetCompositeConstituents("alter_admission")
				require.NoError(t, err)
				require.Len(t, refs, 3)
				require.Equal(t, "remote", refs[2].Alias)
				for _, explain := range []struct {
					query   string
					columns []string
				}{
					{"CREATE COMPOSITE DATABASE explain_admission ALIAS r FOR DATABASE nornic AT '" + endpoint + "' TYPE remote ACCESS read", []string{"name"}},
					{"ALTER COMPOSITE DATABASE alter_admission ADD ALIAS explained FOR DATABASE nornic AT '" + endpoint + "' TYPE remote ACCESS read", []string{"composite_database", "action", "alias", "database", "type", "uri", "secret_ref", "auth_mode", "user"}},
				} {
					plan, err := executor.Execute(ctx, "EXPLAIN "+explain.query, nil)
					require.NoError(t, err)
					require.Equal(t, explain.columns, plan.Columns)
					require.Empty(t, plan.Rows)
					require.NotContains(t, fmt.Sprint(plan), secret)
				}
				require.False(t, manager.Exists("explain_admission"))
				refs, err = manager.GetCompositeConstituents("alter_admission")
				require.NoError(t, err)
				require.Len(t, refs, 3)
			}
			for _, suffix := range []string{" '" + endpoint + "'", " AT 'http://127.0.0.1:1' USER admin PASSWORD PASSWORD '" + secret + "'"} {
				for _, prefix := range []string{"", "EXPLAIN "} {
					_, err = executor.Execute(ctx, prefix+"ALTER COMPOSITE DATABASE alter_admission ADD ALIAS malformed FOR DATABASE nornic"+suffix, nil)
					require.Error(t, err)
					require.False(t, strings.Contains(err.Error(), secret), "diagnostic disclosed synthetic password")
				}
			}
			refs, err := manager.GetCompositeConstituents("alter_admission")
			require.NoError(t, err)
			for _, ref := range refs {
				require.NotEqual(t, "malformed", ref.Alias)
			}
		})
	}
}

func TestCompositePreparedCommandExplain(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			engine, err := manager.GetStorage("nornic")
			require.NoError(t, err)
			executor := NewStorageExecutor(engine)
			executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			ctx := context.Background()
			create := "CREATE COMPOSITE DATABASE prepared_composite ALIAS seed FOR DATABASE nornic"
			add := "ALTER COMPOSITE DATABASE prepared_composite ADD ALIAS second FOR DATABASE nornic"
			drop := "ALTER COMPOSITE DATABASE prepared_composite DROP ALIAS second"
			for _, entry := range []struct {
				query   string
				columns []string
				count   int
			}{
				{create, []string{"name"}, 0},
				{add, []string{"composite_database", "action", "alias", "database", "type", "uri", "secret_ref", "auth_mode", "user"}, 1},
				{drop, []string{"composite_database", "action", "alias"}, 2},
			} {
				result, err := executor.Execute(ctx, "EXPLAIN "+entry.query, nil)
				require.NoError(t, err)
				require.Equal(t, entry.columns, result.Columns)
				require.Empty(t, result.Rows)
				if entry.count == 0 {
					require.False(t, manager.Exists("prepared_composite"))
				} else {
					refs, err := manager.GetCompositeConstituents("prepared_composite")
					require.NoError(t, err)
					require.Len(t, refs, entry.count)
				}
				columns, err := executor.StatementResultColumnsContext(ctx, entry.query, RawSource)
				require.NoError(t, err)
				require.Equal(t, entry.columns, columns)
				result, err = executor.Execute(ctx, entry.query, nil)
				require.NoError(t, err)
				require.Equal(t, entry.columns, result.Columns)
				require.Len(t, result.Rows, 1)
			}
			refs, err := manager.GetCompositeConstituents("prepared_composite")
			require.NoError(t, err)
			require.Len(t, refs, 1)
			for _, query := range []string{"CREATE COMPOSITE DATABASE empty_plan", "ALTER COMPOSITE DATABASE prepared_composite ADD ALIAS", "ALTER COMPOSITE DATABASE prepared_composite DROP ALIAS"} {
				_, err := executor.Execute(ctx, "EXPLAIN "+query, nil)
				require.Error(t, err)
				_, err = executor.Execute(ctx, query, nil)
				require.Error(t, err)
			}
			require.False(t, manager.Exists("empty_plan"))
			// Existing IF NOT EXISTS remains a no-op before constituent parsing.
			if parser == "nornic" {
				result, err := executor.Execute(ctx, "CREATE COMPOSITE DATABASE prepared_composite IF NOT EXISTS invalid_suffix", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"prepared_composite"}}, result.Rows)
				require.Nil(t, result.Administration)
			}
		})
	}
}
