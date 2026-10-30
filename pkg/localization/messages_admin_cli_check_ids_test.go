package localization

import (
	"testing"

	"github.com/nicksnyder/go-i18n/v2/i18n"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

// check-ids' messages render in every catalog as their fallback text does
// (#907, I29).
func TestAdminCLICheckIDsMessages(t *testing.T) {
	paths := []string{"catalog/active.admincli.en-US.yaml", "catalog/active.admincli.es-ES.yaml", "catalog/active.admincli.en-XA.yaml"}
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
	for _, message := range []Message{
		AdminCLICheckIDsShort(), AdminCLIRepairFlag(), AdminCLICheckIDsSummary(2, 3), AdminCLICheckIDsMismatchedSummary(4, 5),
		AdminCLICheckIDsNode("nornic:a"), AdminCLICheckIDsRelationship("nornic:r"), AdminCLIRepairIDsSummary(1, 2, 3, 4),
	} {
		require.Equal(t, message.Fallback, render("en-US", message))
		require.Equal(t, "[!! "+message.Fallback+" !!]", render("en-XA", message))
		require.NotEqual(t, message.Fallback, render("es-ES", message), message.ID)
	}
	require.Equal(t, "2 nodos y 3 relaciones no tienen entrada en el diccionario de IDs", render("es-ES", AdminCLICheckIDsSummary(2, 3)))
}
