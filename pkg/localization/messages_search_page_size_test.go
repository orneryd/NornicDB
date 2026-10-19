package localization

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestSearchPageSizeExceededLocalization(t *testing.T) {
	manager, err := NewManager(nil, nil)
	require.NoError(t, err)
	message := SearchPageSizeExceeded(750)
	for _, test := range []struct {
		locale string
		want   string
	}{
		{"en-US", "page_size_exceeded: search continuation n exceeds max_page_size=750"},
		{"en-XA", "[!! page_size_exceeded: search continuation n exceeds max_page_size=750 !!]"},
		{"es-ES", "page_size_exceeded: n de continuación de búsqueda supera max_page_size=750"},
	} {
		t.Run(test.locale, func(t *testing.T) {
			locale := language.MustParse(test.locale)
			got, tag, err := manager.Render(WithPreferences(context.Background(), locale), message)
			require.NoError(t, err, "the message must render from the selected catalog")
			require.Equal(t, locale, tag)
			require.Equal(t, test.want, got)
		})
	}
	registered := false
	for _, entry := range CatalogManifest {
		if entry.ID == message.ID {
			registered = true
			require.Equal(t, "SearchPageSizeExceeded", entry.Constructor)
			require.Equal(t, []string{"MaxPageSize"}, entry.Fields)
		}
	}
	require.True(t, registered, "the message must have a typed constructor in the generated manifest")
}
