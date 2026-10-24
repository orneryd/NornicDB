package localization_test

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestTransactionalBatchSizeDiagnostic(t *testing.T) {
	manager, err := localization.NewManager(nil, nil)
	require.NoError(t, err)
	message := localization.CypherSubqueriesBatchSizeInvalid("1.5")
	text, _, err := manager.Render(localization.WithPreferences(context.Background(), language.AmericanEnglish), message)
	require.NoError(t, err)
	require.Equal(t, "CALL IN TRANSACTIONS requires a positive INTEGER batch size, got 1.5", text)
	registered := false
	for _, entry := range localization.CatalogManifest {
		if entry.ID == message.ID {
			registered = true
			require.Equal(t, "CypherSubqueriesBatchSizeInvalid", entry.Constructor)
			require.Equal(t, []string{"Value"}, entry.Fields)
		}
	}
	require.True(t, registered)
}

func TestTransactionalBatchAdmissionDiagnostics(t *testing.T) {
	manager, err := localization.NewManager(nil, nil)
	require.NoError(t, err)
	for _, sample := range []struct {
		name, constructor, english string
		message                    localization.Message
		fields                     []string
	}{
		{"context", "CypherSubqueriesBatchContextInvalid", "invalid transactional CALL context: nested", localization.CypherSubqueriesBatchContextInvalid("nested"), []string{"Reason"}},
		{"aggregate", "CypherSubqueriesBatchAggregateInvalid", "Aggregations should not be used like this.", localization.CypherSubqueriesBatchAggregateInvalid(), []string{}},
	} {
		t.Run(sample.name, func(t *testing.T) {
			for _, locale := range []language.Tag{language.AmericanEnglish, language.MustParse("es-ES"), language.MustParse("en-XA")} {
				text, _, err := manager.Render(localization.WithPreferences(context.Background(), locale), sample.message)
				require.NoError(t, err)
				require.NotEmpty(t, text)
				require.NotContains(t, text, "{{")
				if locale == language.AmericanEnglish {
					require.Equal(t, sample.english, text)
				}
			}
			registered := false
			for _, entry := range localization.CatalogManifest {
				if entry.ID == sample.message.ID {
					registered = true
					require.Equal(t, sample.constructor, entry.Constructor)
					require.ElementsMatch(t, sample.fields, entry.Fields)
				}
			}
			require.True(t, registered)
		})
	}
}
