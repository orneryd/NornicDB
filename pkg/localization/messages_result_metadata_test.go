package localization

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestResultMetadataErrorsRenderEveryBundledLocale(t *testing.T) {
	manager, err := NewManager(nil, nil)
	require.NoError(t, err)
	for _, message := range []Message{
		CypherResultMetadataUnresolved(),
		CypherResultMetadataMismatch(),
		CypherResultMetadataImportVariablesRequired(),
	} {
		for _, locale := range []string{"en-US", "en-XA", "es-ES"} {
			t.Run(string(message.ID)+"/"+locale, func(t *testing.T) {
				tag := language.MustParse(locale)
				text, selected, err := manager.Render(WithPreferences(context.Background(), tag), message)
				require.NoError(t, err)
				require.Equal(t, tag, selected)
				require.NotEmpty(t, text)
				if locale == "en-US" {
					require.Equal(t, message.Fallback, text)
				} else {
					require.NotEqual(t, message.Fallback, text, "selected locale must not silently use the English fallback")
				}
			})
		}
	}
}
