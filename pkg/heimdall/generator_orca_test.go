package heimdall

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewOrcaGeneratorRequiresGenericAPIKey(t *testing.T) {
	_, err := newOpenAIGenerator(Config{Provider: "orca"})
	require.ErrorContains(t, err, "NORNICDB_HEIMDALL_API_KEY")
}

func TestNewManagerSelectsOrcaProvider(t *testing.T) {
	manager, err := NewManager(Config{Enabled: true, Provider: "orca", APIKey: "test-key"})
	require.NoError(t, err)
	require.Equal(t, "orca:orcarouter/auto", manager.ModelPath())
	require.IsType(t, &openAIGenerator{}, manager.generator)
}
