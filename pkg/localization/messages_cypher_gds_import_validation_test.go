package localization

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestGDSImportValidationMessages(t *testing.T) {
	manager, err := NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		message Message
		want    string
	}{
		{CypherGraphProceduresUnsafeDoubleConversion("9007199254740993"), "Cannot safely convert 9007199254740993 into an double value"},
		{CypherGraphProceduresProjectDistinct(), "Invalid use of DISTINCT with function 'project'"},
	} {
		for _, locale := range []language.Tag{language.AmericanEnglish, language.MustParse("es-ES"), language.MustParse("en-XA")} {
			rendered, _, err := manager.Render(WithPreferences(context.Background(), locale), tc.message)
			require.NoError(t, err)
			require.NotEmpty(t, rendered)
			if locale == language.AmericanEnglish {
				require.Equal(t, tc.want, rendered)
			}
		}
	}
}
