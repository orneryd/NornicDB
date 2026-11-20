package heimdall

import (
	"testing"

	core "github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestV152RCCBundledAutocompleteActionName is a minimal owner-boundary
// reproduction. Registration's canonical control passes; the exact name used
// by Handler.handleAutocomplete at handler.go:470 is absent and fails here.
func TestV152RCCBundledAutocompleteActionName(t *testing.T) {
	core.ResetSubsystemManagerForTests()
	manager := core.GetSubsystemManager()
	t.Cleanup(func() {
		_ = manager.StopAll()
		core.ResetSubsystemManagerForTests()
	})
	manager.SetContext(core.SubsystemHost{Config: core.DefaultConfig(), Bifrost: &core.NoOpBifrost{}})
	require.NoError(t, manager.RegisterPlugin(&WatcherPlugin{}, "", true))
	_, canonical := manager.GetAction("heimdall_watcher_autocomplete_suggest")
	_, requested := manager.GetAction("heimdall_autocomplete_suggest")
	t.Logf("RCC_HEIMDALL_ACTION registration=pkg/heimdall/plugin.go:1001-1011 handler=pkg/heimdall/handler.go:470 canonical=%v requested=%v", canonical, requested)
	require.True(t, canonical, "canonical registration control")
	assert.True(t, requested, "the exact action requested by the public handler must be registered")
}
