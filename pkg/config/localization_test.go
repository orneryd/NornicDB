package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

func TestLocalizationConfigurationPrecedence(t *testing.T) {
	require.Equal(t, localization.AutoLanguage, LoadDefaults().Localization.Language)

	path := filepath.Join(t.TempDir(), "nornicdb.yaml")
	require.NoError(t, os.WriteFile(path, []byte("localization:\n  language: fr-FR\n"), 0o600))

	t.Setenv(localization.EnvLanguage, "es-ES")
	configured, err := LoadFromFile(path)
	require.NoError(t, err)
	require.Equal(t, "es-ES", configured.Localization.Language)
}

func TestLocalizationConfigurationRejectsMalformedLanguage(t *testing.T) {
	configured := LoadDefaults()
	configured.Localization.Language = "not_a_locale_@"
	require.ErrorContains(t, configured.Validate(), "localization.language")

	t.Setenv(localization.EnvLanguage, "not_a_locale_@")
	require.ErrorContains(t, ApplyEnvVars(LoadDefaults()), localization.EnvLanguage)
}

func TestLocalizationConfigurationAcceptsAutoAndPOSIXForm(t *testing.T) {
	configured := LoadDefaults()
	t.Setenv(localization.EnvLanguage, "en_US.UTF-8")
	require.NoError(t, ApplyEnvVars(configured))
	require.Equal(t, "en_US.UTF-8", configured.Localization.Language)

	t.Setenv(localization.EnvLanguage, "auto")
	require.NoError(t, ApplyEnvVars(configured))
	require.Equal(t, localization.AutoLanguage, configured.Localization.Language)
}
