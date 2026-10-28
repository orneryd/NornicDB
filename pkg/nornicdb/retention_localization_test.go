package nornicdb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

func TestNornicDBRetentionSweepBudgetError(t *testing.T) {
	message := localization.NornicDBRetentionSweepBudgetExhausted(25)
	err := localizedError(message, nil)
	localizedErr := requireNornicDBLocalizedError(t, err, localization.MessageNornicDBRetentionSweepBudgetExhausted, "sweep budget exhausted (25 records)")
	require.Equal(t, 25, localizedErr.Message.Data["Budget"])
}
