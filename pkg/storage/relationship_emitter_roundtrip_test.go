package storage_test

import (
	"context"
	"encoding/json"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil/cyphernames"
	"github.com/orneryd/nornicdb/internal/testutil/emittercheck"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type emitterTransport func(*http.Request) (*http.Response, error)

func (f emitterTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRemoteLabelEmitterRoundTrip(t *testing.T) {
	for _, name := range cyphernames.Names() {
		t.Run(name, func(t *testing.T) {
			executor := emittercheck.Executor(t)
			_, err := executor.Execute(context.Background(), "CREATE (:Unrelated {id:'unrelated'})", nil)
			require.NoError(t, err)
			calls := 0
			remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{
				URI: "http://emitter.invalid", Database: "test", HTTPClient: &http.Client{Transport: emitterTransport(func(request *http.Request) (*http.Response, error) {
					var batch struct {
						Statements []struct {
							Statement  string
							Parameters map[string]any
						}
					}
					require.NoError(t, json.NewDecoder(request.Body).Decode(&batch))
					var results []any
					for _, statement := range batch.Statements {
						calls++
						actual, err := executor.Execute(context.Background(), statement.Statement, statement.Parameters)
						require.NoError(t, err, statement.Statement)
						var data []any
						for _, row := range actual.Rows {
							node := row[0].(*storage.Node)
							value := map[string]any{"id": node.Properties["id"], "labels": node.Labels, "properties": node.Properties}
							data = append(data, map[string]any{"row": []any{value}})
						}
						results = append(results, map[string]any{"columns": actual.Columns, "data": data})
						result, err := executor.Execute(context.Background(), "MATCH (n) RETURN n", nil)
						require.NoError(t, err)
						require.Len(t, result.Rows, 1+min(calls, 2))
						for _, row := range result.Rows {
							node := row[0].(*storage.Node)
							if node.Properties["id"] == "unrelated" {
								require.Equal(t, []string{"Unrelated"}, node.Labels)
								continue
							}
							require.Equal(t, []string{name}, node.Labels)
							require.EqualValues(t, 1, node.Properties[name])
						}
					}
					body, err := json.Marshal(map[string]any{"results": results, "errors": []any{}})
					require.NoError(t, err)
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body)))}, nil
				})},
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, remote.Close()) })
			_, err = remote.CreateNode(&storage.Node{ID: "first", Labels: []string{name}, Properties: map[string]any{name: 1}})
			require.NoError(t, err)
			require.NoError(t, remote.BulkCreateNodes([]*storage.Node{{ID: "second", Labels: []string{name}, Properties: map[string]any{name: 1}}}))
			nodes, err := remote.GetNodesByLabel(name)
			require.NoError(t, err)
			require.Len(t, nodes, 2)
			for _, node := range nodes {
				require.Equal(t, []string{name}, node.Labels)
				require.NotEqual(t, storage.NodeID("unrelated"), node.ID)
			}
			first, err := remote.GetFirstNodeByLabel(name)
			require.NoError(t, err)
			require.Equal(t, []string{name}, first.Labels)
			require.NotEqual(t, storage.NodeID("unrelated"), first.ID)
			require.Equal(t, 4, calls)
			all, err := remote.GetNodesByLabel("")
			require.NoError(t, err)
			require.Len(t, all, 3, "only exact empty string is the no-filter sentinel")
		})
	}
}

// Capture the public remote engine's actual transport statements, then import
// their brackets into the public local executor. No external cluster is claimed.
func TestRemoteRelationshipEmitterRoundTrip(t *testing.T) {
	for _, name := range cyphernames.Names() {
		t.Run(name, func(t *testing.T) {
			calls := 0
			executor := emittercheck.Executor(t)
			_, err := executor.Execute(context.Background(), "CREATE (:"+cyphertext.QuoteName(name)+" {id:'a'}), (:"+cyphertext.QuoteName(name)+" {id:'b'})", nil)
			require.NoError(t, err)
			remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{
				URI: "http://emitter.invalid", Database: "test", HTTPClient: &http.Client{Transport: emitterTransport(func(r *http.Request) (*http.Response, error) {
					var batch struct {
						Statements []struct {
							Statement  string
							Parameters map[string]any
						}
					}
					require.NoError(t, json.NewDecoder(r.Body).Decode(&batch))
					for _, statement := range batch.Statements {
						calls++
						_, err := executor.Execute(context.Background(), statement.Statement, statement.Parameters)
						require.NoError(t, err, statement.Statement)
						if calls <= 3 {
							rows, err := emittercheck.Observe(executor)("MATCH ()-[r]->() RETURN r", nil)
							require.NoError(t, err)
							require.NotEmpty(t, rows)
							for _, row := range rows {
								require.Equal(t, name, row.Type)
								require.EqualValues(t, 1, row.Properties[name])
							}
						}
						emittercheck.Check(t, statement.Statement, name, statement.Parameters, nil)
					}
					var value any = map[string]any{"id": "edge", "type": name, "properties": map[string]any{name: 1}}
					if strings.Contains(batch.Statements[0].Statement, "count(r)") {
						value = 1
					}
					body, err := json.Marshal(map[string]any{"results": []any{map[string]any{"columns": []string{"value"}, "data": []any{map[string]any{"row": []any{value}}}}}, "errors": []any{}})
					require.NoError(t, err)
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body)))}, nil
				})},
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, remote.Close()) })
			edge := &storage.Edge{StartNode: "a", EndNode: "b", Type: name, Properties: map[string]any{name: int64(1)}}
			require.NoError(t, remote.CreateEdge(edge))
			edge.ID = "edge"
			require.NoError(t, remote.CreateEdge(edge))
			require.NoError(t, remote.BulkCreateEdges([]*storage.Edge{edge}))
			_, err = remote.GetEdgesByType(name)
			require.NoError(t, err)
			remote.GetEdgeBetween("a", "b", name)
			_, err = remote.EdgeCountByType(name)
			require.NoError(t, err)
			_, err = remote.EdgeCountByStartLabel(name, name)
			require.NoError(t, err)
			_, err = remote.EdgeCountByEndLabel(name, name)
			require.NoError(t, err)
			require.Equal(t, 8, calls, "every remote type writer reached the round-trip property")
		})
	}
}
