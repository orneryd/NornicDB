// Author: Codex, NOR-180 public outcome message ownership, 2026-11-01.
package errors_test

import (
	"errors"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTerminalOutcomeOwnsNestedMessageValues(t *testing.T) {
	for _, escape := range []string{"input", "diagnostic_getter"} {
		t.Run(escape, func(t *testing.T) {
			values := []string{"before"}
			descriptor := localization.Message{ID: localization.MessageID("nor180.snapshot"), Fallback: "before", Data: map[string]any{"items": values}}
			failure := nornicerrors.NewTerminalFailure(nornicerrors.TerminalCommit, false, nil, errors.New("wal append"), descriptor)
			render := func(m localization.Message) string {
				if m.ID == descriptor.ID {
					return m.Data["items"].([]string)[0]
				}
				return m.Fallback
			}
			code, before := nornicerrors.RenderStatus(failure, render)
			require.Contains(t, before, "before")
			if escape == "input" {
				values[0] = "after"
			} else {
				copy := failure.StatusDiagnostic()
				copy.Data["items"].([]string)[0] = "after"
			}
			afterCode, after := nornicerrors.RenderStatus(failure, render)
			require.Equal(t, code, afterCode)
			require.Equal(t, before, after, "producer-owned terminal diagnostic changed through "+escape)
		})
	}
}
