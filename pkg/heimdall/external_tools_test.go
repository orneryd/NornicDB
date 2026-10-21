package heimdall

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

type externalTestPlugin struct {
	*MockHeimdallPlugin
	actions map[string]ActionFunc
}

func (p *externalTestPlugin) Actions() map[string]ActionFunc { return p.actions }

func TestExternalActionsFIFOAndCollisions(t *testing.T) {
	m := &SubsystemManager{plugins: make(map[string]*LoadedHeimdallPlugin), actions: make(map[string]ActionFunc)}
	require.Empty(t, m.ExternalActions())
	first := ActionFunc{External: true, Description: "first", Handler: func(ActionContext) (*ActionResult, error) { return &ActionResult{Success: true, Message: "first"}, nil }}
	require.NoError(t, m.RegisterPlugin(&externalTestPlugin{NewMockPlugin("a"), map[string]ActionFunc{
		"z":         {External: true, Description: "last in first plugin"},
		"b_c":       first,
		"hidden":    {Description: "internal"},
		"b_private": {Description: "private first"},
	}}, "", true))
	// Both plugin/action pairs produce heimdall_a_b_c. The first wins even if
	// the later action opts out of public exposure.
	require.NoError(t, m.RegisterPlugin(&externalTestPlugin{NewMockPlugin("a_b"), map[string]ActionFunc{
		"c":       {Description: "replacement"},
		"a":       {External: true, Description: "second plugin"},
		"private": {External: true, Description: "must remain private"},
	}}, "", true))
	for i := 0; i < 20; i++ {
		actions := m.ExternalActions()
		require.Len(t, actions, 3)
		require.Equal(t, []string{"heimdall_a_b_c", "heimdall_a_z", "heimdall_a_b_a"}, []string{actions[0].Name, actions[1].Name, actions[2].Name})
		require.Equal(t, "first", actions[0].Description)
		result, err := actions[0].Handler(ActionContext{Context: context.Background()})
		require.NoError(t, err)
		require.Equal(t, &ActionResult{Success: true, Message: "first"}, result)
	}
	hidden, ok := m.GetAction("heimdall_a_hidden")
	require.True(t, ok)
	require.False(t, hidden.External)
	require.Equal(t, "internal", hidden.Description)
}

func TestExternalFlagJSONContract(t *testing.T) {
	var action ActionFunc
	require.NoError(t, json.Unmarshal([]byte(`{"external":true}`), &action))
	require.True(t, action.External)
	var internal ActionFunc
	require.NoError(t, json.Unmarshal([]byte(`{}`), &internal))
	require.False(t, internal.External)
}
