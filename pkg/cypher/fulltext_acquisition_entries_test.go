// Author: Codex implementation lane, 2026-10-27 (NOR-148 acquisition classes).
package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFulltextAcquisitionWorkEarlyReturns(t *testing.T) {
	const count = 16
	_, store := fulltextWorkFixture(t, count, true)
	probe := &fulltextWorkProbe{NamespacedEngine: store}
	exec := NewStorageExecutor(probe, QueryCachePolicy{MaxEntries: 0})
	for _, relationships := range []bool{false, true} {
		for _, mode := range []string{"empty", "unknown_field", "missing_index", "invalid_option", "wildcard_limit0", "presence_limit0", "ranked_limit0"} {
			t.Run(fmt.Sprintf("relationships=%t/%s", relationships, mode), func(t *testing.T) {
				q, query := "hotel", fulltextWorkQuery(relationships)
				opts := map[string]any{}
				wantError, acquired, formats := false, int64(0), int64(0)
				switch mode {
				case "empty":
					q = ""
				case "unknown_field":
					q = "undeclared:*"
				case "missing_index":
					query = strings.ReplaceAll(query, "'work_nodes'", "'missing'")
					query = strings.ReplaceAll(query, "'work_edges'", "'missing'")
					wantError = true
				case "invalid_option":
					opts["skip"] = int64(-1)
					wantError = true
				case "wildcard_limit0", "presence_limit0":
					q = "*"
					if mode == "presence_limit0" {
						q = "text:*"
					}
					opts["limit"] = int64(0)
					if !relationships {
						acquired = count
					}
				case "ranked_limit0":
					opts["limit"] = int64(0)
					acquired, formats = count, count-1
				}
				probe.reset()
				got, err := exec.Execute(context.Background(), query, map[string]any{"q": q, "opts": opts})
				if wantError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Empty(t, got.Rows)
				}
				require.Equal(t, acquired, probe.nodes.Load()+probe.edges.Load())
				require.Equal(t, formats, probe.formatted.Load())
			})
		}
	}
}

func TestFulltextAcquisitionExplicitTransaction(t *testing.T) {
	_, store := fulltextWorkFixture(t, 16, true)
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	_, err := exec.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = exec.Execute(context.Background(), "ROLLBACK", nil) })
	for _, relationships := range []bool{false, true} {
		for _, q := range []string{"*", "text:*"} {
			for call := 0; call < 3; call++ {
				got, err := exec.Execute(context.Background(), fulltextWorkQuery(relationships), map[string]any{"q": q, "opts": map[string]any{"limit": int64(1)}})
				require.NoError(t, err)
				require.Len(t, got.Rows, 1)
				got, err = exec.Execute(context.Background(), fulltextWorkQuery(relationships), map[string]any{"q": q, "opts": map[string]any{}})
				require.NoError(t, err)
				require.Len(t, got.Rows, 14, "transaction prefix became complete retained population")
			}
		}
	}
}

func TestFulltextAcquisitionRemoteMaterializedWork(t *testing.T) {
	const count = 16
	var requests, issued atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Statements []struct {
				Statement string `json:"statement"`
			} `json:"statements"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if len(body.Statements) != 1 {
			http.Error(w, "one materialized read required", 400)
			return
		}
		statement := body.Statements[0].Statement
		if statement != "MATCH (n) RETURN n" && statement != "MATCH ()-[r]->() RETURN r" {
			http.Error(w, "unexpected read: "+statement, 400)
			return
		}
		requests.Add(1)
		data := make([]any, count)
		for i := range data {
			value := map[string]any{"id": fmt.Sprint(i), "labels": []string{"Source"}, "properties": map[string]any{"id": fmt.Sprint(i), "text": "hotel"}, "type": "LINK", "startNodeElementId": "a", "endNodeElementId": "b"}
			data[i] = map[string]any{"row": []any{value}}
		}
		issued.Add(count)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{map[string]any{"columns": []string{"entity"}, "data": data}}, "errors": []any{}})
	}))
	t.Cleanup(server.Close)
	remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: server.URL, Database: "remote"})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, remote.Close()) })
	exec := NewStorageExecutor(remote, QueryCachePolicy{MaxEntries: 0})
	for _, relationships := range []bool{false, true} {
		query := "CALL db.index.fulltext.queryNodes('',$q,$opts) YIELD node RETURN node.id"
		if relationships {
			query = "CALL db.index.fulltext.queryRelationships('',$q,$opts) YIELD relationship RETURN relationship.id"
		}
		for _, q := range []string{"*", "text:*"} {
			for call := 0; call < 3; call++ {
				requests.Store(0)
				issued.Store(0)
				got, err := exec.Execute(context.Background(), query, map[string]any{"q": q, "opts": map[string]any{"limit": int64(1)}})
				require.NoError(t, err)
				require.Len(t, got.Rows, 1)
				require.Equal(t, int64(1), requests.Load())
				require.Equal(t, int64(count), issued.Load(), "remote fallback must preserve complete acquisition, not claim native prefix")
			}
		}
	}
}

func TestFulltextAcquisitionCompositePublicGuard(t *testing.T) {
	_, store := fulltextWorkFixture(t, 16, false)
	probe := &fulltextWorkProbe{NamespacedEngine: store}
	composite := storage.NewCompositeEngine(map[string]storage.Engine{"part": probe}, map[string]string{"part": "acquisition"}, map[string]string{"part": "read"})
	exec := NewStorageExecutor(composite, QueryCachePolicy{MaxEntries: 0})
	for _, relationships := range []bool{false, true} {
		probe.reset()
		query := strings.ReplaceAll(strings.ReplaceAll(fulltextWorkQuery(relationships), "'work_nodes'", "''"), "'work_edges'", "''")
		_, err := exec.Execute(context.Background(), query, map[string]any{"q": "*", "opts": map[string]any{"limit": int64(1)}})
		require.Error(t, err)
		require.Zero(t, probe.nodes.Load()+probe.edges.Load()+probe.formatted.Load(), "public composite rejection must precede acquisition")
	}
}
