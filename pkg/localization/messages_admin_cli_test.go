package localization

import (
	"testing"

	"github.com/nicksnyder/go-i18n/v2/i18n"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

func TestAdminCLICatalogRendering(t *testing.T) {
	paths := []string{
		"catalog/active.admincli.en-US.yaml",
		"catalog/active.admincli.es-ES.yaml",
		"catalog/active.admincli.en-XA.yaml",
	}
	require.NoError(t, validateCatalogFiles(catalogFS, paths))
	bundle := i18n.NewBundle(language.AmericanEnglish)
	bundle.RegisterUnmarshalFunc("yaml", yaml.Unmarshal)
	for _, path := range paths {
		_, err := bundle.LoadMessageFileFS(catalogFS, path)
		require.NoError(t, err)
	}

	message := AdminCLIHelpCommandLong("nornicdb-admin")
	english, err := i18n.NewLocalizer(bundle, "en-US").Localize(&i18n.LocalizeConfig{MessageID: string(message.ID), TemplateData: message.Data})
	require.NoError(t, err)
	require.Equal(t, message.Fallback, english)

	spanish, err := i18n.NewLocalizer(bundle, "es-ES").Localize(&i18n.LocalizeConfig{MessageID: string(message.ID), TemplateData: message.Data})
	require.NoError(t, err)
	require.Equal(t, "La ayuda proporciona información sobre cualquier comando de la aplicación.\nEscriba nornicdb-admin help [ruta al comando] para obtener todos los detalles.", spanish)

	pseudoMessage := AdminCLIExactArgs(1, 2)
	pseudo, err := i18n.NewLocalizer(bundle, "en-XA").Localize(&i18n.LocalizeConfig{MessageID: string(pseudoMessage.ID), TemplateData: pseudoMessage.Data})
	require.NoError(t, err)
	require.Equal(t, "[!! accepts 1 arg(s), received 2 !!]", pseudo)
}
