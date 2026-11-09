package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// This population comes from the output policy itself, not a selected list of
// field spellings. It checks each policy key at real display/log/span boundaries;
// the source census separately checks the complete discovered caller population.
func TestCredentialFieldPolicyPopulation(t *testing.T) {
	const credential = "synthetic-policy-population-839217"
	var logs bytes.Buffer
	logger := slog.New(newRedactingHandler(slog.NewJSONHandler(&logs, nil), defaultRedactSet()))
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(NewRedactingSpanProcessor(recorder)))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	require.NotEmpty(t, DefaultRedactKeys)
	for _, key := range DefaultRedactKeys {
		for _, alias := range []string{key, strings.ToUpper(key), "vendor." + strings.ReplaceAll(key, "_", "-")} {
			t.Run(alias, func(t *testing.T) {
				require.True(t, IsSensitiveFieldName(alias))
				serialized, err := json.Marshal(map[string]string{alias: credential})
				require.NoError(t, err)
				outputs := []string{
					DisplayValue(alias, credential),
					strings.Join(DisplayArgs([]string{"serve", "--" + alias + "=" + credential}), " "),
					RedactText(string(serialized)),
					RedactError(errors.New(string(serialized))).Error(),
				}
				logs.Reset()
				logger.Info("policy population", alias, credential, "safe.context", "visible")
				outputs = append(outputs, logs.String())
				_, span := provider.Tracer("policy-population").Start(t.Context(), "probe")
				span.SetAttributes(attribute.String(alias, credential), attribute.String("safe.context", "visible"))
				span.End()
				ended := recorder.Ended()
				found := false
				for _, item := range ended[len(ended)-1].Attributes() {
					outputs = append(outputs, item.Value.Emit())
					if string(item.Key) == "safe.context" {
						found = true
						require.Equal(t, "visible", item.Value.AsString())
					}
				}
				require.True(t, found)
				for _, output := range outputs {
					require.NotContains(t, output, credential, "credential reached a policy output boundary; details withheld")
				}
			})
		}
	}
	for _, key := range []string{"safe.context", "token_limit", "signing_key_path"} {
		require.Equal(t, "visible", DisplayValue(key, "visible"))
	}
}
