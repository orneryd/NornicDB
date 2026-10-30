package localization

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestResolveProcessPreferencesPrecedence(t *testing.T) {
	detectCalls := 0
	detect := func() ([]language.Tag, error) {
		detectCalls++
		return []language.Tag{language.German}, nil
	}

	resolved, err := resolveProcessPreferences("fr-FR", func(key string) (string, bool) {
		require.Equal(t, EnvLanguage, key)
		return "es_ES.UTF-8", true
	}, detect)
	require.NoError(t, err)
	require.Equal(t, "env", resolved.Source)
	require.Equal(t, []language.Tag{language.EuropeanSpanish}, resolved.Preferences)
	require.Zero(t, detectCalls)

	resolved, err = resolveProcessPreferences("fr-FR", func(string) (string, bool) { return "", false }, detect)
	require.NoError(t, err)
	require.Equal(t, "config", resolved.Source)
	require.Equal(t, []language.Tag{language.MustParse("fr-FR")}, resolved.Preferences)
	require.Zero(t, detectCalls)

	resolved, err = resolveProcessPreferences("fr-FR", func(string) (string, bool) { return "auto", true }, detect)
	require.NoError(t, err)
	require.Equal(t, "os", resolved.Source)
	require.Equal(t, []language.Tag{language.German}, resolved.Preferences)
	require.Equal(t, 1, detectCalls)
}

func TestResolveProcessPreferencesFallbackAndValidation(t *testing.T) {
	detectionErr := errors.New("not available")
	resolved, err := resolveProcessPreferences("auto", func(string) (string, bool) { return "", false }, func() ([]language.Tag, error) {
		return nil, detectionErr
	})
	require.NoError(t, err)
	require.Equal(t, "fallback", resolved.Source)
	require.ErrorIs(t, resolved.DetectionErr, detectionErr)
	require.Equal(t, []language.Tag{language.AmericanEnglish}, resolved.Preferences)

	_, err = resolveProcessPreferences("auto", func(string) (string, bool) { return "not_a_locale_@", true }, func() ([]language.Tag, error) {
		return nil, nil
	})
	require.ErrorContains(t, err, EnvLanguage)
}

func TestNormalizeLanguage(t *testing.T) {
	tag, err := NormalizeLanguage("en_US.UTF-8")
	require.NoError(t, err)
	require.Equal(t, language.AmericanEnglish, tag)

	for _, value := range []string{"", "auto", "C", "POSIX", "C.UTF-8"} {
		tag, err = NormalizeLanguage(value)
		require.NoError(t, err, value)
		require.Equal(t, language.Und, tag, value)
	}
}
