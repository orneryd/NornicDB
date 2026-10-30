package localization

import "strconv"

const (
	MessageNornicDBRetentionSweepBudgetExhausted MessageID = "nornicdb.retention_sweep_budget_exhausted"
)

// NornicDBRetentionSweepBudgetExhausted identifies a retention sweep paused at its configured record budget.
func NornicDBRetentionSweepBudgetExhausted(budget int) Message {
	return Message{
		ID:       MessageNornicDBRetentionSweepBudgetExhausted,
		Fallback: "sweep budget exhausted (" + strconv.Itoa(budget) + " records)",
		Data:     map[string]any{"Budget": budget},
	}
}
