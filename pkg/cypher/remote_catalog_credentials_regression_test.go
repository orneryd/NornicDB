package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRemoteQueryCredentialPublic(t *testing.T) {
	const secret = "synthetic-query-public-492715 /+&=?"
	aliases := []string{"password", "api_key", "X-Amz-Signature", "X-Goog-Signature", "AWSAccessKeyId", "clientSecret"}
	for _, key := range observability.DefaultRedactKeys {
		if alias, ok := strings.CutPrefix(key, "url_query_"); ok {
			aliases = append(aliases, alias)
		}
	}
	var query strings.Builder
	query.WriteString("region=keep%20spelling")
	for _, alias := range aliases {
		for range 2 {
			query.WriteString("&" + url.QueryEscape(alias) + "=" + url.QueryEscape(secret))
		}
	}
	inner := storage.NewMemoryEngine()
	config := multidb.DefaultConfig()
	config.RemoteCredentialEncryptionKey = "synthetic-public-query-key-582461"
	calls := 0
	config.RemoteEngineFactory = func(ref multidb.ConstituentRef, token string) (storage.Engine, error) {
		calls++
		endpoint, err := url.Parse(ref.URI)
		require.NoError(t, err)
		for _, alias := range aliases {
			require.Equal(t, []string{secret, secret}, endpoint.Query()[alias])
		}
		require.Contains(t, endpoint.RawQuery, "region=keep%20spelling")
		return storage.NewMemoryEngine(), nil
	}
	manager, err := multidb.NewDatabaseManager(inner, config)
	require.NoError(t, err)
	defer manager.Close()
	require.NoError(t, manager.CreateCompositeDatabase("probe", []multidb.ConstituentRef{{Alias: "r", DatabaseName: "remote", Type: "remote", AccessMode: "read", URI: "http://example.test/?" + query.String()}}))
	system, err := manager.GetStorage("system")
	require.NoError(t, err)
	node, err := system.GetNode("databases:metadata")
	require.NoError(t, err)
	data, err := json.Marshal(node.Properties)
	require.NoError(t, err)
	for _, value := range []string{secret, url.QueryEscape(secret)} {
		require.NotContains(t, string(data), value)
	}
	_, err = manager.GetStorageWithAuth("probe.r", "synthetic-forwarded-token-385162")
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	engine, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	executor := NewStorageExecutor(engine)
	executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	for _, statement := range []string{"SHOW CONSTITUENTS FOR COMPOSITE DATABASE probe", "SHOW ALIASES"} {
		result, err := executor.Execute(context.Background(), statement, nil)
		require.NoError(t, err)
		data, err := json.Marshal(result)
		require.NoError(t, err)
		for _, value := range []string{secret, url.QueryEscape(secret), "enc:v1:"} {
			require.NotContains(t, string(data), value)
		}
	}
}

func TestManagedRemoteCatalogWinnerPublic(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Contains(t, r.URL.Path, "/winner/")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"results":[{"columns":["marker"],"data":[{"row":["winner"],"meta":[null]}]}],"errors":[]}`)
	}))
	defer remote.Close()
	config := multidb.DefaultConfig()
	var selected []string
	config.RemoteEngineFactory = func(ref multidb.ConstituentRef, token string) (storage.Engine, error) {
		selected = append(selected, ref.DatabaseName)
		return storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, AuthToken: token})
	}
	manager, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), config)
	require.NoError(t, err)
	defer manager.Close()
	require.NoError(t, manager.CreateCompositeDatabase("probe", []multidb.ConstituentRef{
		{Alias: "R", DatabaseName: "loser", Type: "remote", AccessMode: "read", URI: remote.URL},
		{Alias: "r", DatabaseName: "winner", Type: "remote", AccessMode: "read", URI: remote.URL},
	}))
	for _, name := range []string{"probe.R", "probe.r", "probe"} {
		_, err := manager.GetStorage(name)
		require.NoError(t, err)
	}
	catalog := fabric.NewCatalog()
	require.NoError(t, catalog.PopulateFromManager(manager))
	location, err := catalog.Resolve("probe.R")
	require.NoError(t, err)
	locationRemote := location.(*fabric.LocationRemote)
	require.Equal(t, "winner", locationRemote.DBName)
	_, err = locationRemote.ResolveEngine("")
	require.NoError(t, err)
	engine, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	executor := NewStorageExecutor(engine)
	executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	result, err := executor.Execute(context.Background(), "USE probe CALL { USE probe.R RETURN 'winner' AS marker } RETURN marker", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"winner"}}, result.Rows)
	for _, database := range selected {
		require.Equal(t, "winner", database)
	}
}
