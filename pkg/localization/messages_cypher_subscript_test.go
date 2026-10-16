package localization

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestListSubscriptMessagesRenderEveryBundledLocale(t *testing.T) {
	manager, err := NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		message Message
		spanish string
	}{
		{CypherCoreListIndexNonInteger("Double(1.500000e+00)"), "No se puede acceder a una lista mediante un índice numérico no entero, se recibió Double(1.500000e+00)"},
		{CypherCoreListIndexNonNumber("List{Long(1)}", "String(\"first\")"), "No se puede acceder a la lista 'List{Long(1)}' mediante un índice no numérico, se recibió String(\"first\")"},
		{CypherCoreListIndexBoolean("List{Long(1)}", "Boolean('true')", true), "No se puede acceder a la lista 'List{Long(1)}' mediante un índice no numérico, se recibió Boolean('true'): se esperaba que Boolean('true') fuera org.neo4j.values.storable.NumberValue, pero era org.neo4j.values.storable.BooleanValue$1"},
		{CypherCoreListIndexBoolean("List{Long(1)}", "Boolean('false')", false), "No se puede acceder a la lista 'List{Long(1)}' mediante un índice no numérico, se recibió Boolean('false'): se esperaba que Boolean('false') fuera org.neo4j.values.storable.NumberValue, pero era org.neo4j.values.storable.BooleanValue$2"},
	} {
		for locale, want := range map[string]string{
			"en-US": tc.message.Fallback,
			"es-ES": tc.spanish,
			"en-XA": "[!! " + tc.message.Fallback + " !!]",
		} {
			t.Run(string(tc.message.ID)+"/"+locale+"/"+tc.message.Fallback, func(t *testing.T) {
				tag := language.MustParse(locale)
				got, actual, err := manager.Render(WithPreferences(context.Background(), tag), tc.message)
				require.NoError(t, err)
				require.Equal(t, tag, actual)
				require.Equal(t, want, got)
			})
		}
	}
}
