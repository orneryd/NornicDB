package nornicdb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCredentialVoyageEmbeddingSpacePersistence(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The real exported provider must still send the configured credentials.
		require.Equal(t, "synthetic-key-871429", strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		username, password, ok := r.BasicAuth()
		// Bearer auth takes precedence over URL userinfo in Go's HTTP client.
		require.False(t, ok)
		require.Empty(t, username+password)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":[{"index":0,"embedding":[1,0]}],"model":"voyage-fixture","usage":{"total_tokens":2}}`)
	}))
	defer upstream.Close()
	var identities []string
	for generation := 0; generation < 2; generation++ {
		endpoint, err := url.Parse(upstream.URL)
		require.NoError(t, err)
		endpoint.User = url.UserPassword(fmt.Sprintf("synthetic/user-%d", generation), fmt.Sprintf("synthetic/password-%d", generation))
		endpoint.RawQuery = fmt.Sprintf("api_key=synthetic-query-%d&region=public", generation)
		endpoint.Fragment = fmt.Sprintf("token=synthetic-fragment-%d", generation)
		provider, err := embed.NewEmbedder(&embed.Config{Provider: "voyage", APIURL: endpoint.String(), APIKey: "synthetic-key-871429", Model: "voyage-fixture", Dimensions: 2})
		require.NoError(t, err)
		dir := t.TempDir()
		engine, err := storage.NewBadgerEngine(dir)
		require.NoError(t, err)
		t.Cleanup(func() { _ = engine.Close() })
		node := &storage.Node{ID: "docs:credential-free", Properties: map[string]any{"content": "synthetic document"}}
		_, err = engine.CreateNode(node)
		require.NoError(t, err)
		worker := NewEmbedWorker(provider, &sequenceEmbeddingEngine{Engine: engine, nodes: []*storage.Node{node}}, &EmbedWorkerConfig{NumWorkers: 0, MaxRetries: 1, ChunkSize: 512})
		require.True(t, worker.processNextBatch())
		require.NoError(t, engine.Close())
		engine, err = storage.NewBadgerEngine(dir)
		require.NoError(t, err)
		stored, err := engine.GetNode(node.ID)
		require.NoError(t, err)
		require.NotEmpty(t, stored.ChunkEmbeddings)
		identity, ok := stored.EmbedMeta["embedding_space"].(string)
		require.True(t, ok)
		identities = append(identities, identity)
		encoded, err := json.Marshal(stored.EmbedMeta)
		require.NoError(t, err)
		for _, secret := range []string{"synthetic/user", "synthetic%2Fuser", "synthetic/password", "synthetic%2Fpassword", "synthetic-query", "synthetic-fragment"} {
			if strings.Contains(string(encoded), secret) {
				t.Error("persisted embedding metadata contains endpoint credentials; capture withheld")
			}
		}
		require.Contains(t, identity, "region=public")
		// The provider identity is also the query-side compatibility contract.
		require.Equal(t, provider.(embed.EmbeddingSpaceProvider).EmbeddingSpace(), identity)
		_, err = provider.Embed(context.Background(), "query")
		require.NoError(t, err)
	}
	require.Equal(t, identities[0], identities[1], "credential rotation must not change vector-space identity")
}
