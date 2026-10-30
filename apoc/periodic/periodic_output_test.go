package periodic

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// A subprocess captures real public output without replacing the suite's global
// stdout. Virtual time exercises the unchanged hourly Schedule placeholder and
// the bubble waits for every canceled recurring goroutine to finish.
func TestPeriodicDiagnosticOutput(t *testing.T) {
	if api := os.Getenv("NORNIC_PERIODIC_OUTPUT_CHILD"); api != "" {
		for _, input := range []struct{ name, statement string }{
			{"ordinary-task", "RETURN 42 AS ordinary_value"},
			{"quoted-word-task", "RETURN 'PASSWORD guide' AS ordinary_value"},
			{"opaque-password-task", "CREATE COMPOSITE DATABASE c ALIAS r FOR DATABASE remote AT 'http://example.invalid' USER remoteuser PASSWORD 'periodic-opaque-password-817269'"},
			{"https://periodic-name-user:periodic-name-password@example.invalid/task?api_key=periodic-name-key", "RETURN 'https://periodic-query-user:periodic-query-password@example.invalid/data?api_key=periodic-query-key'"},
		} {
			synctest.Test(t, func(t *testing.T) {
				var cancel context.CancelFunc
				delay := time.Second
				switch api {
				case "Repeat":
					cancel = Repeat(input.name, input.statement, 1)
				case "Schedule":
					cancel = Schedule(input.name, "0 0 * * *", input.statement)
					delay = time.Hour
				case "Countdown":
					Countdown(input.name, input.statement, 1)
				case "Submit":
					result := Submit(input.name, input.statement)
					if result["name"] != input.name || result["delay"] != 0 || result["rate"] != 0 || result["done"] != false || result["cancelled"] != false {
						t.Fatal("Submit changed its public task metadata")
					}
				case "Rock":
					cancel = Rock(input.name, input.statement, 1)
				default:
					t.Fatal("unknown output test API")
				}
				if cancel != nil {
					defer cancel()
				}
				synctest.Wait()
				if api != "Submit" {
					time.Sleep(delay)
					synctest.Wait()
				}
				if cancel != nil {
					cancel()
					synctest.Wait()
				}
			})
		}
		return
	}

	for _, entry := range []struct{ api, label string }{
		{"Repeat", "periodic task"},
		{"Schedule", "scheduled task"},
		{"Countdown", "countdown task"},
		{"Submit", "background job"},
		{"Rock", "periodic task"},
	} {
		t.Run(entry.api, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPeriodicDiagnosticOutput$")
			command.Env = append(os.Environ(), "NORNIC_PERIODIC_OUTPUT_CHILD="+entry.api)
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatal("isolated public output check failed")
			}
			text := string(output)
			for _, marker := range []string{"periodic-name-user", "periodic-name-password", "periodic-name-key", "periodic-query-user", "periodic-query-password", "periodic-query-key", "periodic-opaque-password-817269"} {
				if strings.Contains(text, marker) {
					t.Error("synthetic credential reached asynchronous task output")
				}
			}
			ordinary := "Executing " + entry.label + " 'ordinary-task': RETURN 42 AS ordinary_value\n"
			if !strings.Contains(text, ordinary) {
				t.Error("ordinary task display changed")
			}
			if !strings.Contains(text, "Executing "+entry.label+" 'quoted-word-task': RETURN 'PASSWORD guide' AS ordinary_value\n") {
				t.Error("a quoted ordinary word was misclassified as a credential clause")
			}
			if strings.Count(text, "Executing "+entry.label+" '") != 4 {
				t.Error("public task emission count changed")
			}
			if !strings.Contains(text, "example.invalid/task") || !strings.Contains(text, "example.invalid/data") {
				t.Error("redaction removed ordinary endpoint context")
			}
		})
	}
}
