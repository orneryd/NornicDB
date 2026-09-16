package heimdall

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewOrcaGeneratorDefaults(t *testing.T) {
	generator, err := newOrcaGenerator(Config{APIKey: "test-key"})
	require.NoError(t, err)

	orca, ok := generator.(*orcaGenerator)
	require.True(t, ok)
	require.Equal(t, "https://api.orcarouter.ai", orca.baseURL)
	require.Equal(t, "orcarouter/auto", orca.model)
	require.Equal(t, "test-key", orca.apiKey)
	require.Equal(t, "orca:orcarouter/auto", orca.ModelPath())
}

func TestNewOrcaGeneratorPreservesOverrides(t *testing.T) {
	generator, err := newOrcaGenerator(Config{
		APIURL: "https://gateway.example/v1/",
		APIKey: "custom-key",
		Model:  "anthropic/claude-sonnet-4.6",
	})
	require.NoError(t, err)

	orca := generator.(*orcaGenerator)
	require.Equal(t, "https://gateway.example", orca.baseURL)
	require.Equal(t, "anthropic/claude-sonnet-4.6", orca.model)
	require.Equal(t, "custom-key", orca.apiKey)
}

func TestNewOrcaGeneratorRequiresGenericAPIKey(t *testing.T) {
	_, err := newOrcaGenerator(Config{})
	require.ErrorContains(t, err, "NORNICDB_HEIMDALL_API_KEY")
}

func TestNewManagerSelectsOrcaProvider(t *testing.T) {
	manager, err := NewManager(Config{Enabled: true, Provider: "orca", APIKey: "test-key"})
	require.NoError(t, err)
	require.Equal(t, "orca:orcarouter/auto", manager.ModelPath())
	require.IsType(t, &orcaGenerator{}, manager.generator)
}
