package nornicdb

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestNornicDBRetentionSweepBudgetError(t *testing.T) {
	message := localization.NornicDBRetentionSweepBudgetExhausted(25)
	err := localizedError(message, nil)
	localizedErr := requireNornicDBLocalizedError(t, err, localization.MessageNornicDBRetentionSweepBudgetExhausted, "sweep budget exhausted (25 records)")
	require.Equal(t, 25, localizedErr.Message.Data["Budget"])
}

func TestNornicDBRetentionSweepBudgetRendersAtBoundary(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	message := localization.NornicDBRetentionSweepBudgetExhausted(25)

	spanish, tag, err := manager.Render(localization.WithPreferences(context.Background(), language.EuropeanSpanish), message)
	require.NoError(t, err)
	require.Equal(t, language.EuropeanSpanish, tag)
	require.Equal(t, "presupuesto del barrido agotado (25 registros)", spanish)

	pseudoTag := language.MustParse("en-XA")
	pseudo, tag, err := manager.Render(localization.WithPreferences(context.Background(), pseudoTag), message)
	require.NoError(t, err)
	require.Equal(t, pseudoTag, tag)
	require.Equal(t, "[!! sweep budget exhausted (25 records) !!]", pseudo)
}
