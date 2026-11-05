package observability

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/stretchr/testify/require"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestSensitiveKeySources owns log setup for built-in, log-only environment,
// and canonical registrations; only canonical registrations extend displays and spans.
func TestSensitiveKeySources(t *testing.T) {
	previous := DefaultRedactKeys
	t.Cleanup(func() { DefaultRedactKeys = previous })
	const credential = "synthetic-canonical-extension-719832"
	for _, scenario := range []struct {
		name string
		keys []string
	}{
		{"defaults", []string{"password", "Password", "PASSWORD", "token", "TOKEN", "Token", "authorization", "Authorization", "secret", "Secret", "api_key", "API_KEY", "Api_Key", "credentials", "Credentials"}},
		{"log_environment", []string{"session_id", "custom_secret"}},
		{"canonical_registration", []string{"custom.sealed_value", "custom.hidden_material"}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			canonical := scenario.name == "canonical_registration"
			if canonical {
				// Warm before append, then replace the registered key at the same list length.
				_ = IsSensitiveFieldName("custom.sealed_value")
				DefaultRedactKeys = append(append([]string(nil), previous...), "custom.sealed_value")
				t.Cleanup(func() { DefaultRedactKeys = previous })
			}
			if scenario.name == "log_environment" {
				t.Setenv("NORNICDB_LOG_REDACT_EXTRA", "session_id,custom_secret")
			}
			for _, key := range scenario.keys {
				t.Run(key, func(t *testing.T) {
					alias := key
					if canonical {
						DefaultRedactKeys[len(DefaultRedactKeys)-1] = key
						alias = "vendor." + strings.ToUpper(strings.ReplaceAll(strings.ReplaceAll(key, ".", "-"), "_", "-"))
					}
					logger, buf := newCapturingLogger(t, func(h slog.Handler) slog.Handler {
						keys := defaultRedactSet()
						if scenario.name == "log_environment" {
							keys = redactSetFromEnv()
						}
						return newRedactingHandler(h, keys)
					})
					logger.Info("auth", alias, credential, "ok", "visible")
					var record map[string]any
					require.NoError(t, json.Unmarshal(buf.Bytes(), &record))
					require.Equal(t, "<REDACTED>", record[alias], "key %s must be redacted", alias)
					require.NotContains(t, buf.String(), credential)
					require.Equal(t, "visible", record["ok"])
					if !canonical {
						return
					}
					flag := strings.ReplaceAll(strings.ReplaceAll(key, ".", "-"), "_", "-")

					if !IsSensitiveFieldName(alias) {
						t.Error("canonical extension alias missed the shared classifier")
					}
					outputs := []string{
						DisplayValue(alias, credential),
						DisplayFlagDefault(flag, credential),
						strings.Join(DisplayArgs([]string{"serve", "--" + flag + "=" + credential}), " "),
						RedactText(`{"` + key + `":"` + credential + `"}`),
						RedactText(alias+"="+credential, credential),
						RedactURL("https://example.invalid/?" + flag + "=" + credential),
						RedactError(errors.New(`{"` + alias + `":"` + credential + `"}`)).Error(),
					}
					for _, output := range outputs {
						if strings.Contains(output, credential) {
							t.Error("canonical extension escaped a display path; capture withheld")
						}
					}
					if DisplayValue(alias, "") != "" || DisplayFlagDefault(flag, "") != RedactedPlaceholder() {
						t.Error("unset config/flag presence contract changed")
					}
					for _, public := range []string{"safe.context", alias + "_path", "token_limit"} {
						if IsSensitiveFieldName(public) || DisplayValue(public, "visible") != "visible" {
							t.Error("public metadata was over-redacted")
						}
					}
					recorder := tracetest.NewSpanRecorder()
					provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(NewRedactingSpanProcessor(recorder)))
					defer func() { _ = provider.Shutdown(context.Background()) }()
					_, span := provider.Tracer("canonical-extension").Start(context.Background(), "registered")
					span.SetAttributes(attribute.String(alias, credential))
					span.End()
					if len(recorder.Ended()) != 1 || len(recorder.Ended()[0].Attributes()) != 1 {
						t.Fatal("registered span attribute missing")
					}
					if strings.Contains(recorder.Ended()[0].Attributes()[0].Value.AsString(), credential) {
						t.Error("new span processor missed the canonical extension; capture withheld")
					}
				})
			}
			if scenario.name == "log_environment" {
				if IsSensitiveFieldName("session_id") || DisplayValue("session_id", "visible") != "visible" {
					t.Error("log-only environment registration changed the global display classifier")
				}
			}
			if canonical {
				if IsSensitiveFieldName("custom.sealed_value") {
					t.Error("display matcher retained a replaced extension")
				}
				for _, name := range []string{"password", "safe.context"} {
					if allocs := testing.AllocsPerRun(100, func() { _ = IsSensitiveFieldName(name) }); allocs != 0 {
						t.Errorf("classifier allocated %.0f objects for %s", allocs, name)
					}
				}
			}
		})
	}
}
