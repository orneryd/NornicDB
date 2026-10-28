package cypher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"golang.org/x/text/language"
)

func TestParameterReferenceFoundationConsumerAgreement(t *testing.T) {
	query := "RETURN $0 AS a, $123abc AS b, $x AS c // $hidden"
	var names, adapted []string
	cyphertext.ParameterReferences(query, func(name string, _, _ int) { names = append(names, name) })
	scanParameterReferences(query, func(_, start, end int) { adapted = append(adapted, query[start:end]) })
	require.Equal(t, []string{"0", "123abc", "x"}, names)
	require.Equal(t, names, adapted)
	require.Equal(t, names, ExtractParameters(query))
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	result, err := executor.Execute(t.Context(), query, map[string]interface{}{"0": int64(7), "123abc": int64(9), "x": int64(11)})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(7), int64(9), int64(11)}}, result.Rows)
}

func TestAcceptedSourceProjectionKeepsOriginalCoordinates(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	var logs bytes.Buffer
	executor.SetLogger(slog.New(slog.NewJSONHandler(&logs, nil)))
	executor.SetSlowQueryThreshold(time.Nanosecond)
	previous := otel.GetTracerProvider()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(previous); _ = provider.Shutdown(context.Background()) })
	for _, query := range []string{
		"\ufeffRET\u200bURN\u00a0（123） AS café",
		"MATCH （a）-→（b） RETURN 123 AS café",
	} {
		expected := strings.Replace(query, "123", RedactedPlaceholder, 1)
		require.Equal(t, expected, RedactLiterals(query))
		logs.Reset()
		executor.emitSlowQueryLog(t.Context(), query, nil, time.Second)
		var event map[string]interface{}
		require.NoError(t, json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &event))
		require.Contains(t, logs.String(), "café")
		// Inspect the decoded event, since JSON's Unicode escapes are transport.
		var hasProjection func(interface{}) bool
		hasProjection = func(value interface{}) bool {
			switch value := value.(type) {
			case string:
				return value == expected
			case map[string]interface{}:
				for _, member := range value {
					if hasProjection(member) {
						return true
					}
				}
			}
			return false
		}
		require.True(t, hasProjection(event), "raw original-source log projection missing")
		_, span := executor.startExecuteSpan(t.Context(), "read", query)
		span.End()
		ended := recorder.Ended()
		found := false
		for _, attribute := range ended[len(ended)-1].Attributes() {
			if string(attribute.Key) == "cypher.query" {
				found = true
				require.Equal(t, expected, attribute.Value.AsString())
			}
		}
		require.True(t, found)
	}
}

func TestSourceFoundationDiagnosticCoordinates(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	for _, query := range []string{
		"\ufeffRET\u200bURN\u00a0（'\\uZZZZ'）",
		"\ufeffCYPHER 5 RETURN 1_000, '\\uZZZZ'",
		"\ufeffRET\u200bURN\u00a01 − 2",
	} {
		_, err := executor.Execute(t.Context(), query, nil)
		require.Error(t, err)
		offset := strings.Index(query, "\\u")
		if offset < 0 {
			offset = strings.Index(query, "−")
		}
		require.Contains(t, err.Error(), "byte offset "+strconv.Itoa(offset), query)
	}
	cause := errors.New("data decoder failed at byte offset 9")
	rewrite := sourceQueryRewrite(cyphertext.NormalizeSource("\ufeffRETURN 1"))
	_, restored := (resultRestoration{source: rewrite}).restore(nil, cause, nil)
	require.Equal(t, cause.Error(), restored.Error())
	require.ErrorIs(t, restored, cause)
}

func TestSourceFoundationLocalizedDiagnosticCoordinates(t *testing.T) {
	query := "\ufeffRETURN\u00a0（1） @"
	source := cyphertext.NormalizeSource(query)
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	cause := executor.validateSyntax(t.Context(), source.Text, false, syntaxANTLRCore)
	require.Error(t, cause)
	_, err := (resultRestoration{source: sourceQueryRewrite(source)}).restore(nil, cause, nil)
	require.ErrorIs(t, err, cause)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", errorStatusCode(err))
	check := func(err error) {
		var typed *localization.LocalizedError
		require.ErrorAs(t, err, &typed)
		manager, managerErr := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
		require.NoError(t, managerErr)
		for locale, diagnostic := range map[string]string{"en-US": "line 1:12", "es-ES": "línea 1:12", "en-XA": "line 1:12"} {
			text, _, renderErr := manager.Render(localization.WithPreferences(t.Context(), language.MustParse(locale)), localization.ErrorRequest(typed))
			require.NoError(t, renderErr)
			require.Contains(t, text, diagnostic)
		}
	}
	check(err)
	if config.IsANTLRParser() {
		_, err = executor.Execute(t.Context(), query, nil)
		check(err)
	}
}

// Contextual pattern and guard structure stays with these advisory consumers;
// names, trivia and opaque extents come from the common foundation.
func TestSourceFoundationAdvisoryConsumers(t *testing.T) {
	builder := NewASTBuilder()
	patterns := builder.parsePatterns(cyphertext.NormalizeSource("(café:`A``B` {`é:key`: [1, 2], name: 'x, (fake)'})-[`r``x`:关系]->(二)"))
	require.Len(t, patterns, 1)
	require.Len(t, patterns[0].Nodes, 2)
	require.Equal(t, "café", patterns[0].Nodes[0].Variable)
	require.Equal(t, []string{"A`B"}, patterns[0].Nodes[0].Labels)
	require.Contains(t, patterns[0].Nodes[0].Properties, "é:key")
	require.Len(t, patterns[0].Nodes[0].Properties["é:key"].List, 2)
	require.Len(t, patterns[0].Relationships, 1)
	require.Equal(t, "r`x", patterns[0].Relationships[0].Variable)
	require.Equal(t, "关系", patterns[0].Relationships[0].Type)
	for _, query := range []string{"café IS NULL", "`a``b`/* gap */IS NOT NULL"} {
		pass, handled := evalWhereNullGuard(query, map[string]interface{}{"a`b": 1})
		require.True(t, handled, query)
		require.True(t, pass, query)
	}
	for _, query := range []string{"café IS NULL + 1", "café = 1", "(café IS NULL) OR (x IS NULL)"} {
		_, handled := evalWhereNullGuard(query, nil)
		require.False(t, handled, query)
	}
}
