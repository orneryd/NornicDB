package localization

import (
	"context"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"testing"
)

func TestTemporalOffsetMismatchRendersEveryBundledLocale(t *testing.T) {
	manager, err := NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	input := "2026-10-25T01:30:00+04:00[Europe/London]"
	for _, tc := range []struct{ locale, prefix string }{
		{"en-US", "Timezone and offset do not match: "},
		{"es-ES", "La zona horaria y el desfase no coinciden: "},
		{"en-XA", "[!! Timezone and offset do not match: "},
	} {
		t.Run(tc.locale, func(t *testing.T) {
			locale := language.MustParse(tc.locale)
			actual, tag, err := manager.Render(WithPreferences(context.Background(), locale), CypherCoreTemporalOffsetMismatch(input))
			require.NoError(t, err)
			require.Equal(t, locale, tag)
			expected := tc.prefix + input
			if tc.locale == "en-XA" {
				expected += " !!]"
			}
			require.Equal(t, expected, actual)
		})
	}
}
