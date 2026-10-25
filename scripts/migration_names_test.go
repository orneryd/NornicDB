package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testutil/cyphernames"
	"github.com/orneryd/nornicdb/internal/testutil/emittercheck"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type migrationCaptureTransport func(*http.Request) (*http.Response, error)

func (f migrationCaptureTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestLegacyMigrationEmitterProperty(t *testing.T) {
	previous, stats := http.DefaultClient, allStats
	t.Cleanup(func() { http.DefaultClient = previous; allStats = stats })
	captured := []struct {
		Statement  string
		Parameters map[string]any
	}{}
	http.DefaultClient = &http.Client{Transport: migrationCaptureTransport(func(r *http.Request) (*http.Response, error) {
		var batch struct {
			Statements []struct {
				Statement  string
				Parameters map[string]any
			}
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&batch))
		captured = append(captured, batch.Statements...)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"errors":[]}`)), Header: make(http.Header)}, nil
	})}
	var nodes []Neo4jNode
	for _, name := range cyphernames.Names() {
		nodes = append(nodes, Neo4jNode{Labels: []string{name, "Other"}, Properties: map[string]interface{}{name: int64(9)}})
	}
	_, err := insertNodesIntoNornic(nodes)
	require.NoError(t, err)
	require.Len(t, captured, len(nodes))
	store := testfixture.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	executor := cypher.NewStorageExecutor(storage.NewNamespacedEngine(store, "legacy_migration"))
	for i, name := range cyphernames.Names() {
		t.Run(name, func(t *testing.T) {
			statement := captured[i]
			start := strings.Index(statement.Statement, "(n:") + 3
			token, ok := cyphertext.ScanIdentifier(statement.Statement, start)
			got, end := token.Value, token.End
			require.True(t, ok)
			require.Equal(t, name, got)
			require.Equal(t, byte(':'), statement.Statement[end])
			_, err := executor.Execute(context.Background(), statement.Statement, statement.Parameters)
			require.NoError(t, err, statement.Statement)
			for j, form := range cyphernames.DeclarationForms {
				t.Run(fmt.Sprintf("form-%d", j), func(t *testing.T) {
					executor := emittercheck.Executor(t)
					text := strings.Replace(statement.Statement, "(n:", "(n"+form, 1)
					if strings.Contains(form, "IS") {
						// IS uses conjunction rather than the legacy colon chain.
						token, ok := cyphertext.ScanIdentifier(text, strings.Index(text, form)+len(form))
						end := token.End
						require.True(t, ok)
						require.Equal(t, byte(':'), text[end])
						text = text[:end] + "&" + text[end+1:]
					}
					text = strings.TrimSuffix(text, "RETURN id(n)") + "RETURN n"
					result, err := executor.Execute(context.Background(), text, statement.Parameters)
					require.NoError(t, err, text)
					require.Len(t, result.Rows, 1)
					node := result.Rows[0][0].(*storage.Node)
					require.Equal(t, []string{name, "Other"}, node.Labels)
					require.EqualValues(t, 9, node.Properties[name])
				})
			}
		})
	}
}
