package heimdall

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLocalModelDefaultsUseThePackagedManifest(t *testing.T) {
	var previous LocalModelDefaults
	require.NoError(t, json.Unmarshal(packagedModelJSON, &previous))
	require.Equal(t, previous, loadLocalModelDefaults(packagedModelJSON))
	require.Equal(t, previous.Name, DefaultLocalModel())
	require.Positive(t, previous.NativeContextTokens)
	require.Panics(t, func() { loadLocalModelDefaults([]byte(`{"native_context_tokens":`)) }, "invalid packaged configuration remains a startup failure")
}
