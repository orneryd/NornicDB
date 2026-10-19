//go:build rule73_legacy_heimdall_probe

// Target overlay: PR #1466 head f8f83a00, before #1364 changed the Heimdall
// Stats and asynchronous-invoker interfaces to carry the caller context.
package server

import (
	"context"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/stretchr/testify/require"
)

func rule73LegacyHeimdallContext(defaultDatabase string, databases []string, read, write bool) context.Context {
	selections := make(map[string]string, len(databases))
	for _, database := range databases {
		selections[database] = database
	}
	ctx := auth.WithRequestDatabaseScope(context.Background(), auth.NewRequestDatabaseScope(defaultDatabase, selections))
	return auth.WithRequestEntitlementResolver(ctx, func(_ string, permission auth.Permission) bool {
		return permission == "access" || permission == auth.PermRead && read || permission == auth.PermWrite && write
	})
}

// Temporary Rule 73 probe for the split release head before #1364. It proves
// all three Heimdall data actions need the same mandatory admission boundary.
func TestRule73ProbeLegacyHeimdallActions(t *testing.T) {
	server, _ := setupTestServer(t)
	require.NoError(t, server.dbManager.CreateDatabase("rule73_named"))
	router := newHeimdallDBRouter(server.db, server.dbManager, nil)
	for _, database := range []string{"nornic", "rule73_named"} {
		_, err := router.Query(context.Background(), database, "CREATE (:Rule73Secret {value: 'secret'})", nil)
		require.NoError(t, err)
	}

	profiles := []struct {
		name    string
		ctx     context.Context
		allowed map[string]bool
	}{
		{"default-only", rule73LegacyHeimdallContext("nornic", []string{"nornic"}, true, false), map[string]bool{"nornic": true}},
		{"named-only", rule73LegacyHeimdallContext("rule73_named", []string{"rule73_named"}, true, false), map[string]bool{"rule73_named": true}},
		{"write-only", rule73LegacyHeimdallContext("nornic", []string{"nornic", "rule73_named"}, false, true), nil},
		{"no-access", rule73LegacyHeimdallContext("nornic", []string{"nornic", "rule73_named"}, false, false), nil},
		{"admin", rule73LegacyHeimdallContext("nornic", []string{"nornic", "rule73_named"}, true, true), map[string]bool{"nornic": true, "rule73_named": true}},
		{"auth-off", context.Background(), map[string]bool{"nornic": true, "rule73_named": true}},
	}
	for _, profile := range profiles {
		for _, database := range []string{"nornic", "rule73_named"} {
			t.Run(profile.name+"/query/"+database, func(t *testing.T) {
				_, err := router.Query(profile.ctx, database, "MATCH (n:Rule73Secret) RETURN n.value AS value", nil)
				if profile.allowed[database] {
					require.NoError(t, err)
				} else {
					require.Error(t, err, "denied Heimdall Query reached database data")
				}
			})
			t.Run(profile.name+"/discover/"+database, func(t *testing.T) {
				_, err := router.Discover(profile.ctx, database, "secret", nil, 5, 0)
				if profile.allowed[database] {
					require.NoError(t, err)
				} else {
					require.Error(t, err, "denied Heimdall Discover reached database data")
				}
			})
		}
	}
}

func TestRule73ProbeLegacyHeimdallStatsAndAsyncCarryNoPrincipal(t *testing.T) {
	server, _ := setupTestServer(t)
	t.Run("stats", func(t *testing.T) {
		routerType := reflect.TypeOf(newHeimdallDBRouter(server.db, server.dbManager, nil))
		stats, ok := routerType.MethodByName("Stats")
		require.True(t, ok)
		require.Equal(t, 3, stats.Type.NumIn(), "Stats has no context parameter, so no principal can be admitted")
	})
	t.Run("async", func(t *testing.T) {
		async, ok := reflect.TypeOf((*heimdall.LiveHeimdallInvoker)(nil)).MethodByName("InvokeActionAsync")
		require.True(t, ok)
		require.Equal(t, 4, async.Type.NumIn(), "InvokeActionAsync has no context parameter, so it discards the caller principal")
	})
}
