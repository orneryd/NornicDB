package heimdall

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	core "github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/stretchr/testify/require"
)

type nor281SchemaRouter struct{ memoryDBRouter }

func (*nor281SchemaRouter) Query(_ context.Context, _ string, query string, _ map[string]interface{}) ([]map[string]interface{}, error) {
	if strings.Contains(query, "db.labels()") {
		return []map[string]interface{}{{"label": "Person"}}, nil
	}
	return nil, nil
}

// Establish the real bundled registration boundary separately from the
// controlled exact-name action used to isolate the model cleanup defect.
// A schema-backed static suggestion keeps this case independent of inference.
func TestNOR281BundledAutocompleteActionReachesPublicHandler(t *testing.T) {
	core.ResetSubsystemManagerForTests()
	manager := core.GetSubsystemManager()
	t.Cleanup(func() {
		_ = manager.StopAll()
		core.ResetSubsystemManagerForTests()
	})
	cfg := core.DefaultConfig()
	router := &nor281SchemaRouter{}
	manager.SetContext(core.SubsystemHost{Config: cfg, Database: router, Bifrost: &core.NoOpBifrost{}})
	require.NoError(t, manager.RegisterPlugin(&WatcherPlugin{}, "", true))
	_, canonical := manager.GetAction("heimdall_watcher_autocomplete_suggest")
	require.True(t, canonical, "real Watcher plugin registers its namespaced autocomplete action")
	control, err := core.ExecuteAction("heimdall_watcher_autocomplete_suggest", core.ActionContext{
		Context: t.Context(), Database: router, Params: map[string]interface{}{"query": "MATCH ("},
	})
	require.NoError(t, err)
	require.True(t, control.Success)
	require.Equal(t, "MATCH (n:Person) RETURN n LIMIT 25", control.Data["suggestion"])
	_, legacy := manager.GetAction("heimdall_autocomplete_suggest")
	t.Logf("bundled canonical action registered=%v; handler's unqualified action registered=%v", canonical, legacy)

	handler := core.NewHandler(&core.Manager{}, cfg, router, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/bifrost/autocomplete", strings.NewReader(`{"query":"MATCH ("}`)))
	t.Logf("actual bundled route status=%d body=%s", response.Code, response.Body.String())
	require.Equal(t, http.StatusOK, response.Code, "the real registered schema action must be reachable through its documented autocomplete endpoint: %s", response.Body.String())
}
