package cypher

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func TestCredentialUserinfoCatalogDisplayPublic(t *testing.T) {
	const secret = "synthetic/token-user-871429"
	cfg := multidb.DefaultConfig()
	cfg.RemoteCredentialEncryptionKey = "synthetic-userinfo-display-key-317"
	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), cfg)
	require.NoError(t, err)
	fixtureowner.Own(t, manager)
	endpoint := &url.URL{Scheme: "http", Host: "example.invalid", User: url.UserPassword(secret, "")}
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "probe", []multidb.ConstituentRef{{Alias: "r", Type: "remote", AccessMode: "read", DatabaseName: "remote", URI: endpoint.String()}}))
	engine, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	executor := NewStorageExecutor(engine)
	executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	result, err := executor.Execute(context.Background(), "SHOW CONSTITUENTS FOR COMPOSITE DATABASE probe", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	display := fmt.Sprint(result.Rows)
	for _, value := range []string{secret, url.QueryEscape(secret), "enc:v1:", "enc%3Av1"} {
		if strings.Contains(display, value) {
			t.Error("public catalog display contains URL credential material; capture withheld")
		}
	}
	require.Equal(t, "<REDACTED>", result.Rows[0][7])
}
