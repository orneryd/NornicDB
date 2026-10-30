package localization

import (
	"testing"

	"github.com/nicksnyder/go-i18n/v2/i18n"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

// The catalogs render the query-chunk messages as their fallback texts do
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
	for _, check := range []struct {
		message Message
		spanish string
	}{
		{SearchQueryChunksExceeded(45, 32), "la consulta de búsqueda tiene 45 fragmentos; se buscan como máximo 32 (NORNICDB_SEARCH_QUERY_MAX_CHUNKS)"},
		{SearchQueryChunkDimensions(2, 3, 2), "el embedding del fragmento de consulta 2 tiene 3 dimensiones; el del primer fragmento tiene 2"},
	} {
		for locale, want := range map[string]string{
			"en-US": check.message.Fallback,
			"es-ES": check.spanish,
			"en-XA": "[!! " + check.message.Fallback + " !!]",
		} {
			got, err := i18n.NewLocalizer(bundle, locale).Localize(&i18n.LocalizeConfig{MessageID: string(check.message.ID), TemplateData: check.message.Data})
			require.NoError(t, err)
			require.Equal(t, want, got, locale)
		}
	}
}
