package observability

import (
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type diagnosticStringer string

func (v diagnosticStringer) String() string { return string(v) }

type diagnosticLogValue struct{ value slog.Value }

func (v diagnosticLogValue) LogValue() slog.Value { return v.value }

func TestRedactingHandlerDiagnosticValues(t *testing.T) {
	const diagnostic = "request https://diagnostic-user:diagnostic-secret@example.test/v1 failed"
	values := []struct {
		name  string
		value any
	}{
		{"error", errors.New(diagnostic)},
		{"stringer", diagnosticStringer(diagnostic)},
		{"strings", []string{"safe detail", diagnostic}},
		{"lazy string", diagnosticLogValue{slog.StringValue(diagnostic)}},
		{"lazy error", diagnosticLogValue{slog.AnyValue(errors.New(diagnostic))}},
		{"lazy group", diagnosticLogValue{slog.GroupValue(slog.Any("cause", errors.New(diagnostic)), slog.Int("attempt", 7))}},
	}
	for _, value := range values {
		for _, path := range []string{"record", "with", "group"} {
			t.Run(value.name+"/"+path, func(t *testing.T) {
				outputPath := filepath.Join(t.TempDir(), "diagnostics.json")
				logger, writer, err := NewLogger(LoggerConfig{Output: outputPath}, ServiceInfo{})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, writer.(*os.File).Close()) })
				attr := slog.Any("detail", value.value)
				attrs := []any{attr, slog.Group("safe",
					slog.Any("error", errors.New("public error")),
					slog.Any("stringer", diagnosticStringer("public detail")),
					slog.Any("strings", []string{"a", "b"}),
					slog.Any("nil", nil), slog.Any("bytes", []byte{0xab, 0xcd}))}
				switch path {
				case "with":
					logger = logger.With(attr)
					attrs = attrs[1:]
				case "group":
					logger = logger.WithGroup("provider")
				}
				logger.Info("provider failed", attrs...)
				output, err := os.ReadFile(outputPath)
				require.NoError(t, err)
				require.NotContains(t, string(output), "diagnostic-secret")
				require.Contains(t, string(output), "example.test/v1")
				var record map[string]any
				require.NoError(t, json.Unmarshal(output, &record))
				if path == "group" {
					record = record["provider"].(map[string]any)
				}
				require.Equal(t, map[string]any{"error": "public error", "stringer": "public detail", "strings": []any{"a", "b"}, "nil": nil, "bytes": "abcd"}, record["safe"])
				if value.name == "strings" {
					details := record["detail"].([]any)
					require.Len(t, details, 2)
					require.Equal(t, "safe detail", details[0])
				} else if value.name == "lazy group" {
					require.Equal(t, float64(7), record["detail"].(map[string]any)["attempt"])
				}
			})
		}
	}
	// Logging must not change the source values used by runtime callers.
	require.Equal(t, diagnostic, values[0].value.(error).Error())
	require.Equal(t, []string{"safe detail", diagnostic}, values[2].value)
}
