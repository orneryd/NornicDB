package localization

import (
	"testing"

	"github.com/nicksnyder/go-i18n/v2/i18n"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

// apoc.custom's messages render in every catalog as their fallback text does
// (#1066).
func TestCypherProceduresCustomMessages(t *testing.T) {
	paths := []string{"catalog/active.cypherprocedures.en-US.yaml", "catalog/active.cypherprocedures.es-ES.yaml", "catalog/active.cypherprocedures.en-XA.yaml"}
	require.NoError(t, validateCatalogFiles(catalogFS, paths))
	bundle := i18n.NewBundle(language.AmericanEnglish)
	bundle.RegisterUnmarshalFunc("yaml", yaml.Unmarshal)
	for _, path := range paths {
		_, err := bundle.LoadMessageFileFS(catalogFS, path)
		require.NoError(t, err)
	}
	for _, message := range []Message{CypherProceduresCustomSignatureInvalid("p("), CypherProceduresCustomArgumentType("apoc.custom.installProcedure", "mode", "STRING")} {
		for locale, want := range map[string]string{"en-US": message.Fallback, "en-XA": "[!! " + message.Fallback + " !!]"} {
			got, err := i18n.NewLocalizer(bundle, locale).Localize(&i18n.LocalizeConfig{MessageID: string(message.ID), TemplateData: message.Data})
			require.NoError(t, err)
			require.Equal(t, want, got, locale)
		}
		spanish, err := i18n.NewLocalizer(bundle, "es-ES").Localize(&i18n.LocalizeConfig{MessageID: string(message.ID), TemplateData: message.Data})
		require.NoError(t, err)
		require.NotEqual(t, message.Fallback, spanish)
	}
}
