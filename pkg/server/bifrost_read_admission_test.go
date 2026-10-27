package server

import (
	"github.com/orneryd/nornicdb/pkg/multidb"

	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/stretchr/testify/require"
)

func TestBifrostGlobalFeedRequiresAllDatabaseReads(t *testing.T) {
	server, users := setupTestServer(t)
	require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "feed_named"}))
	ctx := context.Background()
	for _, spec := range []struct {
		name      string
		databases []string
		read      bool
	}{
		{"feed-default", []string{"nornic"}, true},
		{"feed-named", []string{"feed_named"}, true},
		{"feed-write-only", []string{"nornic", "feed_named"}, false},
		{"feed-reader", []string{"nornic", "feed_named", "system"}, true},
	} {
		require.NoError(t, server.roleStore.CreateRole(ctx, spec.name))
		require.NoError(t, server.roleEntitlementsStore.Set(ctx, spec.name, []string{"read", "write"}))
		require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, spec.name, spec.databases))
		for _, database := range []string{"nornic", "feed_named", "system"} {
			require.NoError(t, server.privilegesStore.SavePrivilege(ctx, spec.name, database, spec.read, true))
		}
		_, err := users.CreateUser(spec.name, "password123", []auth.Role{auth.Role(spec.name)})
		require.NoError(t, err)
	}
	server.config.Features.HeimdallEnabled = true
	router := newHeimdallDBRouter(server.db, server.dbManager, nil)
	handler := heimdall.NewHandler(&heimdall.Manager{}, heimdall.Config{Enabled: true, BifrostEnabled: true, Model: "test"}, router, &heimdallMetricsReader{})
	server.setHeimdallHandler(handler)
	network := httptest.NewServer(server.buildRouter())
	defer network.Close()
	connect := func(name string) (*http.Response, context.CancelFunc) {
		requestContext, cancel := context.WithTimeout(ctx, 5*time.Second)
		request, err := http.NewRequestWithContext(requestContext, http.MethodGet, network.URL+"/api/bifrost/events", nil)
		require.NoError(t, err)
		request.Header.Set("Authorization", "Bearer "+getAuthToken(t, users, name))
		response, err := network.Client().Do(request)
		require.NoError(t, err)
		return response, cancel
	}
	for _, name := range []string{"feed-default", "feed-named", "feed-write-only"} {
		t.Run(name, func(t *testing.T) {
			response, cancel := connect(name)
			defer cancel()
			defer response.Body.Close()
			require.Equal(t, http.StatusForbidden, response.StatusCode, "global database-result feed accepted a limited reader")
		})
	}

	response, cancel := connect("feed-reader")
	defer cancel()
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	events := make(chan string, 8)
	go func() {
		scanner := bufio.NewScanner(response.Body)
		for scanner.Scan() {
			if strings.Contains(scanner.Text(), "event-marker") {
				events <- scanner.Text()
			}
		}
	}()
	require.NoError(t, handler.Bifrost().SendNotification("info", "result", "before-event-marker"))
	select {
	case <-events:
	case <-time.After(time.Second):
		t.Fatal("allowed global feed did not deliver a result")
	}
	for _, database := range []string{"feed_named", "system"} {
		require.NoError(t, server.privilegesStore.SavePrivilege(ctx, "feed-reader", database, false, true))
		require.NoError(t, handler.Bifrost().SendNotification("info", "result", "after-event-marker"))
		select {
		case event := <-events:
			t.Fatalf("global result delivered after database read revocation: %s", event)
		case <-time.After(150 * time.Millisecond):
		}
		require.NoError(t, server.privilegesStore.SavePrivilege(ctx, "feed-reader", database, true, true))
		require.NoError(t, handler.Bifrost().SendNotification("info", "result", "restored-event-marker"))
		select {
		case <-events:
		case <-time.After(time.Second):
			t.Fatal("restored read permission did not resume the global feed")
		}
	}
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, "feed-reader", []string{"nornic"}))
	require.NoError(t, handler.Bifrost().SendNotification("info", "result", "allowlist-revoked-event-marker"))
	select {
	case event := <-events:
		t.Fatalf("global result delivered after database selection revocation: %s", event)
	case <-time.After(150 * time.Millisecond):
	}
}
