package localization

import (
	"testing"

	"github.com/nicksnyder/go-i18n/v2/i18n"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

func TestCypherInvariantsCatalogRendering(t *testing.T) {
	paths := []string{
		"catalog/active.cypherinvariants.en-US.yaml",
		"catalog/active.cypherinvariants.es-ES.yaml",
		"catalog/active.cypherinvariants.en-XA.yaml",
	}
	require.NoError(t, validateCatalogFiles(catalogFS, paths))

	bundle := i18n.NewBundle(language.AmericanEnglish)
	bundle.RegisterUnmarshalFunc("yaml", yaml.Unmarshal)
	for _, path := range paths {
		_, err := bundle.LoadMessageFileFS(catalogFS, path)
		require.NoError(t, err)
	}

	message := CypherInvariantsPipelineCreateFailed(assertiveError("create failed"))
	config := &i18n.LocalizeConfig{MessageID: string(message.ID), TemplateData: message.Data}

	spanish, err := i18n.NewLocalizer(bundle, "es-ES").Localize(config)
	require.NoError(t, err)
	require.Equal(t, "CREATE en la canalización produjo un error: create failed", spanish)

	pseudo, err := i18n.NewLocalizer(bundle, "en-XA").Localize(config)
	require.NoError(t, err)
	require.Equal(t, "[!! pipeline CREATE failed: create failed !!]", pseudo)
}

type assertiveError string

func (e assertiveError) Error() string { return string(e) }
