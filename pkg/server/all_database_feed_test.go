package server

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestAllDatabaseFeedIncludesSystemAdmission(t *testing.T) {
	s, users := setupTestServer(t)
	ctx := context.Background()
	const role = "review-feed-reader"
	require.NoError(t, s.roleStore.CreateRole(ctx, role))
	require.NoError(t, s.roleEntitlementsStore.Set(ctx, role, []string{"read"}))
	require.NoError(t, s.allowlistStore.SaveRoleDatabases(ctx, role, []string{"nornic", "system"}))
	require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "nornic", true, false))
	_, err := users.CreateUser(role, "password123", []auth.Role{auth.Role(role)})
	require.NoError(t, err)
	token := "Bearer " + getAuthToken(t, users, role)
	system, err := s.dbManager.GetStorage("system")
	require.NoError(t, err)
	_, err = system.CreateNode(&storage.Node{ID: "feed-system-sentinel", Labels: []string{"ReviewSystemFeed"}, Properties: map[string]any{"value": "review-system-feed-secret"}})
	require.NoError(t, err)
	s.config.Features.HeimdallEnabled = true
	router := newHeimdallDBRouter(s.db, s.dbManager, nil)
	handler := heimdall.NewHandler(&heimdall.Manager{}, heimdall.Config{Enabled: true, BifrostEnabled: true, Model: "test"}, router, &heimdallMetricsReader{})
	s.setHeimdallHandler(handler)
	network := httptest.NewServer(s.buildRouter())
	defer network.Close()
	heimdall.RegisterBuiltinAction(heimdall.ActionFunc{
		Name: "review_system_feed_probe", Category: "testing",
		Handler: func(action heimdall.ActionContext) (*heimdall.ActionResult, error) {
			stats, err := action.Database.Stats(action.Context, "system")
			return &heimdall.ActionResult{Success: err == nil, Message: fmt.Sprintf("review-system-feed-count=%d", stats.NodeCount)}, err
		},
	})
	invoker := heimdall.NewLiveHeimdallInvoker(heimdall.GetSubsystemManager(), nil, handler.Bifrost(), router, nil)
	adminRequest := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(context.WithValue(ctx, contextKeyClaims, &auth.JWTClaims{Roles: []string{string(auth.RoleAdmin)}}))
	adminContext := s.withBifrostRBAC(adminRequest).Context()
	for index, readable := range []bool{false, true, false} {
		require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "system", readable, false))
		control := makeRequest(t, s, http.MethodGet, "/db/system", nil, token)
		if !readable {
			require.Equal(t, http.StatusForbidden, control.Code, control.Body.String())
		}
		requestContext, cancel := context.WithTimeout(ctx, 5*time.Second)
		request, err := http.NewRequestWithContext(requestContext, http.MethodGet, network.URL+"/api/bifrost/events", nil)
		require.NoError(t, err)
		request.Header.Set("Authorization", token)
		response, err := network.Client().Do(request)
		require.NoError(t, err)
		t.Logf("phase %d system read=%v: /db/system=%d; events=%d", index, readable, control.Code, response.StatusCode)
		if response.StatusCode == http.StatusOK {
			messages := make(chan string, 1)
			go func() {
				scanner := bufio.NewScanner(response.Body)
				for scanner.Scan() {
					if strings.Contains(scanner.Text(), "review-system-feed-count=") {
						messages <- scanner.Text()
						return
					}
				}
			}()
			invoker.InvokeActionAsync(adminContext, "review_system_feed_probe", nil)
			select {
			case message := <-messages:
				t.Logf("phase %d received %s", index, message)
				if !readable {
					t.Errorf("system database statistics delivered to reader whose system privilege is false")
				}
			case <-time.After(time.Second):
				if readable {
					t.Error("system-readable control did not receive its allowed result")
				}
			}
		} else if readable {
			t.Errorf("system-readable control refused with HTTP %d", response.StatusCode)
		}
		response.Body.Close()
		cancel()
	}
}
