package localization

import (
	"testing"

	"github.com/nicksnyder/go-i18n/v2/i18n"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

// The catalogs render a query with too many chunks as its fallback text does
// (#907).
func TestSearchQueryChunksExceededRendering(t *testing.T) {
	paths := []string{"catalog/active.search.en-US.yaml", "catalog/active.search.es-ES.yaml", "catalog/active.search.en-XA.yaml"}
	require.NoError(t, validateCatalogFiles(catalogFS, paths))
	bundle := i18n.NewBundle(language.AmericanEnglish)
	bundle.RegisterUnmarshalFunc("yaml", yaml.Unmarshal)
	for _, path := range paths {
		_, err := bundle.LoadMessageFileFS(catalogFS, path)
		require.NoError(t, err)
	}
	message := SearchQueryChunksExceeded(45, 32)
	for locale, want := range map[string]string{
		"en-US": message.Fallback,
		"es-ES": "la consulta de búsqueda tiene 45 fragmentos; se buscan como máximo 32 (NORNICDB_SEARCH_QUERY_MAX_CHUNKS)",
		"en-XA": "[!! " + message.Fallback + " !!]",
	} {
		got, err := i18n.NewLocalizer(bundle, locale).Localize(&i18n.LocalizeConfig{MessageID: string(message.ID), TemplateData: message.Data})
		require.NoError(t, err)
		require.Equal(t, want, got, locale)
	}
}
