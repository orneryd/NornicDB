package oslocale

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestParseLocaleForms(t *testing.T) {
	tests := []struct {
		value string
		want  language.Tag
	}{
		{value: "en_US.UTF-8", want: language.AmericanEnglish},
		{value: "pt_BR@custom", want: language.BrazilianPortuguese},
		{value: "zh-Hant", want: language.TraditionalChinese},
		{value: "C.UTF-8", want: language.Und},
		{value: "POSIX", want: language.Und},
	}
	for _, test := range tests {
		t.Run(test.value, func(t *testing.T) {
			tag, err := parse(test.value)
			require.NoError(t, err)
			require.Equal(t, test.want, tag)
		})
	}
}

func TestEnvironmentPreferencePrecedence(t *testing.T) {
	t.Setenv("LANG", "en_US.UTF-8")
	t.Setenv("LANGUAGE", "fr:en")
	t.Setenv("LC_MESSAGES", "de_DE.UTF-8")
	t.Setenv("LC_ALL", "es_ES.UTF-8")

	values, err := environmentPreferences()
	require.NoError(t, err)
	require.Equal(t, []string{"es_ES.UTF-8"}, values)
}
