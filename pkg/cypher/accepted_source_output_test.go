package cypher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/internal/contextslot"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	cantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func acceptedSourceOutputObserver(t *testing.T) func(*testing.T, string, []string) {
	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	previous := otel.GetTracerProvider()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(previous); _ = provider.Shutdown(context.Background()) })
	var logs bytes.Buffer
	exec.SetLogger(slog.New(slog.NewJSONHandler(&logs, nil)))
	exec.SetSlowQueryThreshold(time.Nanosecond)
	return func(t *testing.T, query string, secrets []string) {
		logs.Reset()
		_, err := exec.Execute(t.Context(), query, nil)
		outputs := []string{RedactLiterals(query)}
		appendError := func(err error) {
			if err == nil {
				return
			}
			outputs = append(outputs, err.Error())
			var localized *localization.LocalizedError
			if errors.As(err, &localized) {
				outputs = append(outputs, localized.Message.Fallback, fmt.Sprint(localized.Message.Data))
			}
		}
		appendError(err)
		ctx, source := cyphertext.WithSource(t.Context(), query)
		ctx = contextslot.Ensure[cyphertext.CredentialSet](ctx)
		contextslot.Get[cyphertext.CredentialSet](ctx).Source = cyphertext.SourceFor(ctx, query)
		_, nativeErr := exec.executeCompositeCommand(ctx, source.Text, compositeCreateCommand)
		appendError(nativeErr)
		_, grammarErr := cantlr.ParseSyntax(ctx, source.Text, cantlr.StatementEntry, false, cantlr.AcceptedSource)
		redactAdminCommandError(query, &grammarErr, contextslot.Get[cyphertext.CredentialSet](ctx))
		appendError(grammarErr)
		exec.emitSlowQueryLog(t.Context(), query, nil, time.Second)
		require.NotEmpty(t, logs.String())
		outputs = append(outputs, logs.String())
		_, span := exec.startExecuteSpan(t.Context(), "read", query)
		span.End()
		ended := recorder.Ended()
		found := false
		for _, attr := range ended[len(ended)-1].Attributes() {
			if string(attr.Key) == "cypher.query" {
				found = true
				outputs = append(outputs, attr.Value.AsString())
			}
		}
		require.True(t, found)
		for boundary, output := range outputs {
			for _, secret := range secrets {
				if strings.Contains(output, secret) {
					t.Errorf("credential reached original-source output boundary %d; details withheld", boundary)
				}
			}
		}
	}
}

func TestCredentialAcceptedSourceOutput(t *testing.T) {
	observe := acceptedSourceOutputObserver(t)

	for _, character := range []rune{'\u200b', '\u200c', '\u200d', '\u2060', '\ufeff'} {
		for position := 0; position <= len("PASSWORD"); position++ {
			for _, tail := range []string{"MATCH", "'probe741928' probe741928", "'probe741928"} {
				t.Run(fmt.Sprintf("U%04X/%d/%d", character, position, len(tail)), func(t *testing.T) {
					keyword := "PASSWORD"[:position] + string(character) + "PASSWORD"[position:]
					query := "CREATE COMPOSITE DATABASE `accepted_output` ALIAS `r` FOR DATABASE `nornic` AT 'http://example.invalid' USER 'u' " + keyword + " " + tail
					observe(t, query, []string{"MATCH", "probe741928"})
				})
			}
		}
	}
}

func TestCredentialNormalizationClassesOutput(t *testing.T) {
	data, err := os.ReadFile("../../internal/cyphertext/testdata/normalization.json")
	require.NoError(t, err)
	var classes []struct{ Character, Input, Accepted, Kind string }
	require.NoError(t, json.Unmarshal(data, &classes))
	observe := acceptedSourceOutputObserver(t)
	for index, class := range classes {
		for _, position := range []int{0, 4, 8} {
			for tailIndex, tail := range []string{"'probe741928'", "'probe741928", "'probe741928' unexpected"} {
				t.Run(fmt.Sprintf("class%d/position%d/tail%d", index, position, tailIndex), func(t *testing.T) {
					keyword := "PASSWORD"[:position] + class.Character + "PASSWORD"[position:]
					query := "CREATE COMPOSITE DATABASE accepted_classes ALIAS r FOR DATABASE nornic AT 'http://example.invalid' USER 'u' " + keyword + " " + tail
					observe(t, query, []string{"probe741928"})
				})
			}
		}
		// Conditional arrows require a relationship context to be transformed.
		// Place that whole accepted class before and after the credential head.
		if class.Kind == "arrow" {
			for _, query := range []string{
				"MATCH " + class.Input + " CREATE COMPOSITE DATABASE accepted_classes ALIAS r FOR DATABASE nornic AT 'http://example.invalid' USER 'u' PASSWORD probe741928",
				"CREATE COMPOSITE DATABASE accepted_classes ALIAS r FOR DATABASE nornic AT 'http://example.invalid' USER 'u' PASSWORD probe741928 " + class.Input,
			} {
				observe(t, query, []string{"probe741928"})
			}
		}
	}
}

func TestCredentialAcceptedSourceSpanMapping(t *testing.T) {
	query := "CREATE COMPOSITE DATABASE accepted_spans ALIAS r FOR DATABASE nornic AT 'http://example.invalid' USER 'u' PASS\u200bWORD 'probe741928' unexpected"
	ctx, source := cyphertext.WithSource(t.Context(), query)
	ctx = contextslot.Ensure[cyphertext.CredentialSet](ctx)
	contextslot.Get[cyphertext.CredentialSet](ctx).Source = cyphertext.SourceFor(ctx, query)
	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	_, err = executor.executeCompositeCommand(ctx, source.Text, compositeCreateCommand)
	require.Error(t, err)
	set := contextslot.Get[cyphertext.CredentialSet](ctx)
	found := false
	for _, span := range set.Spans {
		if span.Password {
			found = true
			require.Equal(t, "'probe741928'", query[span.Start:span.End])
			require.Equal(t, query[span.Start:span.End], span.Raw)
			require.Equal(t, "probe741928", span.Value)
		}
	}
	require.True(t, found)
}
