package localization

import (
	"testing"

	"github.com/nicksnyder/go-i18n/v2/i18n"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

// Relationship search's messages render in every catalog as their fallback
// text does (#1065).
func TestSearchRelationshipsMessages(t *testing.T) {
	paths := []string{"catalog/active.searchrelationships.en-US.yaml", "catalog/active.searchrelationships.es-ES.yaml", "catalog/active.searchrelationships.en-XA.yaml"}
	require.NoError(t, validateCatalogFiles(catalogFS, paths))
	bundle := i18n.NewBundle(language.AmericanEnglish)
	bundle.RegisterUnmarshalFunc("yaml", yaml.Unmarshal)
	for _, path := range paths {
		_, err := bundle.LoadMessageFileFS(catalogFS, path)
		require.NoError(t, err)
	}
	render := func(locale string, message Message) string {
		text, err := i18n.NewLocalizer(bundle, locale).Localize(&i18n.LocalizeConfig{MessageID: string(message.ID), TemplateData: message.Data})
		require.NoError(t, err, message.ID)
		return text
	}
	for _, message := range []Message{SearchRelationshipsDisabled(), SearchRelationshipsContinuation()} {
		require.Equal(t, message.Fallback, render("en-US", message))
		require.Equal(t, "[!! "+message.Fallback+" !!]", render("en-XA", message))
		require.NotEqual(t, message.Fallback, render("es-ES", message), message.ID)
	}
}

// The relationship and graph search procedures' listing descriptions render
// in every catalog (#1065).
func TestSearchRelationshipsProcedureMetadata(t *testing.T) {
	bundle := i18n.NewBundle(language.AmericanEnglish)
	bundle.RegisterUnmarshalFunc("yaml", yaml.Unmarshal)
	for _, locale := range []string{"en-US", "es-ES", "en-XA"} {
		_, err := bundle.LoadMessageFileFS(catalogFS, "catalog/active.cypherproceduremetadata."+locale+".yaml")
		require.NoError(t, err)
	}
	for _, message := range []Message{CypherProcedureMetadataDbRetrieveRelationships(), CypherProcedureMetadataDbRetrieveGraph(), CypherProcedureMetadataDbRretrieveGraph()} {
		for locale, check := range map[string]func(string){
			"en-US": func(text string) { require.Equal(t, message.Fallback, text) },
			"en-XA": func(text string) { require.Equal(t, "[!! "+message.Fallback+" !!]", text) },
			"es-ES": func(text string) { require.NotEqual(t, message.Fallback, text) },
		} {
			text, err := i18n.NewLocalizer(bundle, locale).Localize(&i18n.LocalizeConfig{MessageID: string(message.ID)})
			require.NoError(t, err, locale, message.ID)
			check(text)
		}
	}
}
