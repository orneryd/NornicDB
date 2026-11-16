package heimdall

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// integerArgumentDBRouter records the numeric parameters the built-in actions
// derive from their tool-call arguments.
type integerArgumentDBRouter struct {
	queryParams map[string]interface{}
	discovered  bool
	limit       int
	depth       int
}

func (r *integerArgumentDBRouter) DefaultDatabaseName() string { return "default" }

func (r *integerArgumentDBRouter) ResolveDatabase(nameOrAlias string) (string, error) {
	return "default", nil
}

func (r *integerArgumentDBRouter) ListDatabases() []string { return []string{"default"} }

func (r *integerArgumentDBRouter) Query(ctx context.Context, database string, cypher string, params map[string]interface{}) ([]map[string]interface{}, error) {
	r.queryParams = params
	return []map[string]interface{}{{"label": "Memory", "count": int64(1)}}, nil
}

func (r *integerArgumentDBRouter) Stats(ctx context.Context, database string) (heimdall.DatabaseStats, error) {
	return heimdall.DatabaseStats{}, nil
}

func (r *integerArgumentDBRouter) Discover(ctx context.Context, database string, query string, nodeTypes []string, limit int, depth int) (*heimdall.DiscoverResult, error) {
	r.discovered, r.limit, r.depth = true, limit, depth
	return &heimdall.DiscoverResult{}, nil
}

// TestWatcherPluginIntegerToolArgumentsDriveLimits proves the built-in actions
// read the numeric tool-call arguments the assistant dispatches: a JSON INTEGER
// now arrives as int64, so the plugin must use the canonical numeric reader
// instead of a float64 type assertion that silently falls back to its default.
func TestWatcherPluginIntegerToolArgumentsDriveLimits(t *testing.T) {
	router := &integerArgumentDBRouter{}
	plugin := &WatcherPlugin{}
	require.NoError(t, plugin.Initialize(heimdall.SubsystemContext{SubsystemHost: heimdall.SubsystemHost{
		Config:   heimdall.Config{Model: "test-model"},
		Database: router,
	}}))
	require.NoError(t, plugin.Start())

	repoMap, err := plugin.actionRepoMap(heimdall.ActionContext{
		Context:  context.Background(),
		Params:   map[string]interface{}{"limit": int64(40)},
		Database: router,
	})
	require.NoError(t, err)
	require.True(t, repoMap.Success, repoMap.Message)
	assert.Equal(t, 40, router.queryParams["limit"])

	discover, err := plugin.actionDiscover(heimdall.ActionContext{
		Context:  context.Background(),
		Params:   map[string]interface{}{"query": "ledger", "limit": int64(30), "depth": int64(3)},
		Database: router,
	})
	require.NoError(t, err)
	require.True(t, discover.Success, discover.Message)
	require.True(t, router.discovered, "discover must run")
	assert.Equal(t, 30, router.limit)
	assert.Equal(t, 3, router.depth)
}
