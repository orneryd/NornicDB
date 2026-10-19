package observability

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestCanonicalRedactKeysExtensions(t *testing.T) {
	previous := DefaultRedactKeys
	t.Cleanup(func() { DefaultRedactKeys = previous })
	const credential = "synthetic-canonical-extension-719832"
	// Warm the display view before registration, then change the extension at
	// the same list length. Both append and replacement must invalidate it.
	_ = IsSensitiveFieldName("custom.sealed_value")
	DefaultRedactKeys = append(append([]string(nil), previous...), "custom.sealed_value")
	for _, key := range []string{"custom.sealed_value", "custom.hidden_material"} {
		DefaultRedactKeys[len(DefaultRedactKeys)-1] = key
		t.Run(key, func(t *testing.T) {
			flag := strings.ReplaceAll(strings.ReplaceAll(key, ".", "-"), "_", "-")
			alias := "vendor." + strings.ToUpper(flag)
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
			var output bytes.Buffer
			logger := slog.New(newRedactingHandler(slog.NewJSONHandler(&output, nil), defaultRedactSet()))
			logger.Info("registered", alias, credential)
			if strings.Contains(output.String(), credential) {
				t.Error("new log handler missed the canonical extension; capture withheld")
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
	if IsSensitiveFieldName("custom.sealed_value") {
		t.Error("display matcher retained a replaced extension")
	}
	for _, name := range []string{"password", "safe.context"} {
		if allocs := testing.AllocsPerRun(100, func() { _ = IsSensitiveFieldName(name) }); allocs != 0 {
			t.Errorf("classifier allocated %.0f objects for %s", allocs, name)
		}
	}
}
