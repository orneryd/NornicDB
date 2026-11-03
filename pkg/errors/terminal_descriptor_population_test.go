// Author: Codex, NOR-180 descriptor input/output population, 2026-11-01.
package errors_test

import (
	"errors"
	"fmt"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type mutableOutcomeDiagnostic struct{ text string }

func (e *mutableOutcomeDiagnostic) Error() string { return e.text }

type nestedOutcomeDiagnostic struct{ Items []string }

func TestTerminalOutcomeOwnsDescriptorValuePopulation(t *testing.T) {
	for _, kind := range []string{"typed_map", "pointer", "array", "error", "plural", "time"} {
		for _, escape := range []string{"input", "diagnostic_getter", "renderer"} {
			t.Run(kind+"/"+escape, func(t *testing.T) {
				values := []string{"before"}
				pointer := &nestedOutcomeDiagnostic{Items: values}
				cause := &mutableOutcomeDiagnostic{text: "before"}
				count := 1
				timestamp := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
				descriptor := localization.Message{ID: "nor180.population", Fallback: "before", Data: map[string]any{}}
				switch kind {
				case "typed_map":
					descriptor.Data["item"] = map[string][]string{"items": values}
				case "pointer":
					descriptor.Data["item"] = pointer
				case "array":
					descriptor.Data["item"] = [1][]string{values}
				case "error":
					descriptor.Data["item"] = cause
				case "plural":
					descriptor.PluralCount = &count
				case "time":
					descriptor.Data["item"] = &timestamp
				}
				read := func(m localization.Message) string {
					switch kind {
					case "typed_map":
						return m.Data["item"].(map[string][]string)["items"][0]
					case "pointer":
						return m.Data["item"].(*nestedOutcomeDiagnostic).Items[0]
					case "array":
						return m.Data["item"].([1][]string)[0][0]
					case "error":
						return m.Data["item"].(error).Error()
					case "time":
						return m.Data["item"].(*time.Time).Format(time.RFC3339)
					default:
						return fmt.Sprint(*m.PluralCount.(*int))
					}
				}
				mutate := func(m localization.Message) {
					switch kind {
					case "typed_map":
						m.Data["item"].(map[string][]string)["items"][0] = "after"
					case "pointer":
						m.Data["item"].(*nestedOutcomeDiagnostic).Items[0] = "after"
					case "array":
						m.Data["item"].([1][]string)[0][0] = "after"
					case "error":
						if original, ok := m.Data["item"].(*mutableOutcomeDiagnostic); ok {
							original.text = "after"
						}
					case "plural":
						*m.PluralCount.(*int) = 2
					case "time":
						*m.Data["item"].(*time.Time) = timestamp.Add(time.Hour)
					}
				}
				failure := nornicerrors.NewTerminalFailure(nornicerrors.TerminalCommit, false, nil, errors.New("WAL failure"), descriptor)
				render := func(m localization.Message) string {
					if m.ID == descriptor.ID {
						return read(m)
					}
					return m.Fallback
				}
				beforeCode, before := nornicerrors.RenderStatus(failure, render)
				switch escape {
				case "input":
					mutate(descriptor)
				case "diagnostic_getter":
					mutate(failure.StatusDiagnostic())
				case "renderer":
					nornicerrors.RenderStatus(failure, func(m localization.Message) string {
						if m.ID == descriptor.ID {
							mutate(m)
						}
						return m.Fallback
					})
				}
				code, after := nornicerrors.RenderStatus(failure, render)
				require.Equal(t, beforeCode, code)
				require.Equal(t, before, after)
			})
		}
	}
}
