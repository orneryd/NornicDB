package observability

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"os/exec"
	"runtime/debug"
	"strings"
	"testing"
	"time"
)

func TestRedactingHandlerCyclicDiagnostic(t *testing.T) {
	if mode := os.Getenv("NORNIC_REDACTION_CYCLE_CHILD"); mode != "" {
		debug.SetMaxStack(8 << 20)
		var output bytes.Buffer
		var handler slog.Handler = slog.NewJSONHandler(&output, nil)
		if mode == "wrapped" {
			handler = newRedactingHandler(handler, defaultRedactSet())
		}
		value := map[string]any{"safe": "visible"}
		value["self"] = value
		list := make([]any, 1)
		list[0] = list
		indirect := map[string]any{"safe": "visible"}
		indirect["list"] = []any{indirect}
		for _, cyclic := range []any{value, list, indirect} {
			slog.New(handler).Info("cycle", "value", cyclic)
		}
		if !strings.Contains(output.String(), "cycle") || strings.Count(output.String(), "unsupported value") != 3 || strings.Count(output.String(), "encountered a cycle") != 3 {
			t.Fatal("lost unsupported-cycle diagnostic")
		}
		return
	}
	for _, mode := range []string{"plain", "wrapped"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRedactingHandlerCyclicDiagnostic$")
			command.Env = append(os.Environ(), "NORNIC_REDACTION_CYCLE_CHILD="+mode)
			if _, err := command.CombinedOutput(); err != nil {
				t.Fatal("isolated cyclic diagnostic failed")
			}
		})
	}
}

func TestRedactValueSharedDAGIsNotACycle(t *testing.T) {
	shared := map[string]any{"password": "synthetic-shared-password", "visible": "ordinary"}
	input := []any{shared, shared}
	safe := RedactValue(input).([]any)
	for _, branch := range safe {
		projected := branch.(map[string]any)
		if projected["password"] != RedactedPlaceholder() || projected["visible"] != "ordinary" {
			t.Fatal("shared noncycle branch was not fully projected")
		}
	}
	safe[0].(map[string]any)["visible"] = "first projection only"
	if safe[1].(map[string]any)["visible"] != "ordinary" || shared["password"] != "synthetic-shared-password" || shared["visible"] != "ordinary" {
		t.Fatal("projection mutated or conflated shared branches")
	}
}
