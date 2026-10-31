package cypher

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/internal/statementdiagtest"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

type statementDiagnosticPosition = statementdiagtest.Position

func observeStatementDiagnostic(err error) statementdiagtest.Diagnostic {
	got, observeErr := statementdiagtest.Observe(err, nil)
	if observeErr != nil {
		panic(observeErr)
	}
	return got
}

func TestStatementDiagnosticContract(t *testing.T) {
	restoreParserTypeAfter(t)
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	for _, item := range statementdiagtest.Cases() {
		for _, query := range statementdiagtest.RoutedForms(item, "test") {
			t.Run(item.Name+"/"+query, func(t *testing.T) {
				var baseline []byte
				for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
					config.SetParserType(parser)
					exec, _ := newTestExecutor(t)
					for attempt := 0; attempt < 2; attempt++ {
						result, failure := exec.Execute(t.Context(), query, nil)
						if item.ExpectedStatus != "" {
							assert.Error(t, failure, query)
						} else {
							assert.NoError(t, failure, query)
							assert.NotNil(t, result)
						}
						diagnostic, observeErr := statementdiagtest.Observe(failure, manager)
						require.NoError(t, observeErr)
						assert.Equal(t, item.ExpectedStatus, diagnostic.Status, query)
						// Plans contain measured time; rows/columns and diagnostics are
						// the statement result contract under parser selection.
						var columns []string
						var rows [][]interface{}
						if result != nil {
							columns, rows = result.Columns, result.Rows
						}
						// EXPLAIN deliberately has an empty row set under both arms.
						payload, encodeErr := statementdiagtest.Encode(struct {
							Diagnostic statementdiagtest.Diagnostic
							Columns    []string
							Rows       [][]interface{}
						}{diagnostic, columns, rows})
						require.NoError(t, encodeErr)
						if baseline == nil {
							baseline = payload
						} else {
							assert.JSONEq(t, string(baseline), string(payload), parser+" cold/warm")
						}
					}
				}
			})
		}
	}
}

func TestStatementDiagnosticSourceFrames(t *testing.T) {
	for _, query := range []string{
		"EXPLAIN RETURN 1 @", "PROFILE /* comment */ RETURN 1 @",
		"  EXPLAIN\r\n RETURN 'π' @", "EXPLAIN PROFILE RETURN 1 @",
	} {
		ctx, source := cyphertext.WithSource(t.Context(), query)
		ctx = executingStatementContext(ctx)
		ctx, _, body := executionModeSource(ctx, source.Text)
		retained, ok := cyphertext.RetainedSource(ctx, body, AcceptedSource)
		require.True(t, ok)
		// A reader-provided extent maps exact bytes, including comments and
		// repeated spellings, without substring search or parser-cache identity.
		require.Equal(t, query, retained.Original)
		span, mapped := retained.OriginalSpan(cyphertext.Span{End: len(body)}, cyphertext.SourceExtent)
		require.True(t, mapped)
		require.Equal(t, body, query[span.Start:span.End])
	}
	ctx, source := cyphertext.WithSource(t.Context(), `RETURN '\u03c0' @`)
	ctx = executingStatementContext(ctx)
	_, child := statementSource(ctx, source.Text, false)
	require.Equal(t, source.Text, child.Original, "a new raw statement owns its own original bytes")
	require.NotEqual(t, source.Original, child.Original)
	_, continued := statementSource(ctx, source.Text, true)
	require.Same(t, source, continued)
}

func TestStatementDiagnosticUseSourceFrame(t *testing.T) {
	for _, query := range []string{
		"USE test RETURN 1 @",
		"  USE test /* same text: RETURN 1 @ */ RETURN 1 @",
		`\u0055SE test RETURN '\u03c0' @`,
	} {
		ctx, source := cyphertext.WithSource(t.Context(), query)
		ctx = executingStatementContext(ctx)
		ctx, _, body, hasUse, err := readUseStatement(ctx, source.Text, false)
		require.NoError(t, err)
		require.True(t, hasUse)
		retained, found := cyphertext.RetainedSource(ctx, body, AcceptedSource)
		require.True(t, found)
		require.Equal(t, query, retained.Original)
		// The grammar receives precisely this retained window. Its typed
		// diagnostic is projected once into the complete submitted source.
		exec, _ := newTestExecutor(t)
		failure := exec.validateStatementSyntax(ctx, body, false)
		got := observeStatementDiagnostic(failure)
		require.Contains(t, got.Positions, statementDiagnosticPosition{Line: 1, Column: len(query) - 1})
	}
}

func TestStatementDiagnosticProcedureSourceFrame(t *testing.T) {
	restoreParserTypeAfter(t)
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		config.SetParserType(parser)
		// The procedure's input deliberately equals its caller's accepted
		// bytes. Equality cannot make an independent statement inherit the
		// caller's earlier Unicode expansion and diagnostic coordinates.
		ctx, caller := cyphertext.WithSource(t.Context(), `RETURN '\u03c0' @`)
		ctx = executingStatementContext(ctx)
		exec, _ := newTestExecutor(t)
		_, expected := exec.Execute(t.Context(), caller.Text, nil)
		_, actual := exec.executeUserStatement(ctx, caller.Text, nil)
		want, got := observeStatementDiagnostic(expected), observeStatementDiagnostic(actual)
		require.Contains(t, got.Positions, statementDiagnosticPosition{Line: 1, Column: utf8.RuneCountInString(caller.Text[:strings.IndexByte(caller.Text, '@')])})
		wantJSON, err := statementdiagtest.Encode(want)
		require.NoError(t, err)
		gotJSON, err := statementdiagtest.Encode(got)
		require.NoError(t, err)
		require.JSONEq(t, string(wantJSON), string(gotJSON))
	}
}
