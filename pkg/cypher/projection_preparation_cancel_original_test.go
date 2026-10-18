package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The original reader remains the fallback until the canonical checked
// method is available, so this repro can run without a production patch.
func TestProjectionPreparationCanceledOriginal(t *testing.T) {
	executor, _ := newTestExecutor(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	probe := &pipelineCancellation{ctx: ctx}
	clause := "'" + strings.Repeat("x", 1<<20) + "' AS value"
	if checked, ok := interface{}(executor).(interface {
		parseReturnItemsWithCancellation(string, *pipelineCancellation) []returnItem
	}); ok {
		checked.parseReturnItemsWithCancellation(clause, probe)
	} else {
		executor.parseReturnItems(clause)
	}
	require.ErrorIs(t, probe.err, context.Canceled, "projection preparation must stop before parsing after cancellation")
}
