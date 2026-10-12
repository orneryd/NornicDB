package localization

import (
	"testing"

	"github.com/nicksnyder/go-i18n/v2/i18n"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

// The visibility procedures' descriptions and errors render in every
// catalog as their fallback text does (#1064).
func TestVisibilityMessages(t *testing.T) {
	var paths []string
	for _, domain := range []string{"cypherproceduremetadata", "cypherknowledgepolicy"} {
		for _, locale := range []string{"en-US", "es-ES", "en-XA"} {
			paths = append(paths, "catalog/active."+domain+"."+locale+".yaml")
		}
	}
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
	for _, message := range []Message{
		CypherProcedureMetadataKpSuppress(), CypherProcedureMetadataKpUnsuppress(),
		CypherKnowledgePolicyVisibilityEntityRequired("kp.suppress"), CypherKnowledgePolicyVisibilityUnsupported("kp.unsuppress"),
	} {
		require.Equal(t, message.Fallback, render("en-US", message))
		require.Equal(t, "[!! "+message.Fallback+" !!]", render("en-XA", message))
		require.NotEqual(t, message.Fallback, render("es-ES", message), message.ID)
	}
}
