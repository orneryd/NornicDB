package localization

import (
	"testing"

	"github.com/nicksnyder/go-i18n/v2/i18n"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

func TestCypherMatchingCatalogRendering(t *testing.T) {
	paths := []string{
		"catalog/active.cyphermatching.en-US.yaml",
		"catalog/active.cyphermatching.es-ES.yaml",
		"catalog/active.cyphermatching.en-XA.yaml",
	}
	require.NoError(t, validateCatalogFiles(catalogFS, paths))

	bundle := i18n.NewBundle(language.AmericanEnglish)
	bundle.RegisterUnmarshalFunc("yaml", yaml.Unmarshal)
	for _, path := range paths {
		_, err := bundle.LoadMessageFileFS(catalogFS, path)
		require.NoError(t, err)
	}

	message := CypherMatchingTraversalPatternInvalid("(a)-[r")
	config := &i18n.LocalizeConfig{MessageID: string(message.ID), TemplateData: message.Data}

	spanish, err := i18n.NewLocalizer(bundle, "es-ES").Localize(config)
	require.NoError(t, err)
	require.Equal(t, "patrón de recorrido no válido: (a)-[r", spanish)

	pseudo, err := i18n.NewLocalizer(bundle, "en-XA").Localize(config)
	require.NoError(t, err)
	require.Equal(t, "[!! invalid traversal pattern: (a)-[r !!]", pseudo)
}
