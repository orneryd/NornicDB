package server

// Whole-head issues report, rows 29/30 check (untracked; never committed): as
// zz_wha_r30_test.go, but the node's vectors are made by the server's own
// embedding worker (stub provider, three chunks), as in a real server, instead
// of being written into the sidecar by the probe. One ordinary update over
// HTTP; then the stop between journal write and store write.

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type wha30Embedder struct{}

func (wha30Embedder) Embed(_ context.Context, text string) ([]float32, error) {
	return []float32{float32(len(text)), 1, 2, 3}, nil
}

func (e wha30Embedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, text := range texts {
		out[i], _ = e.Embed(ctx, text)
	}
	return out, nil
}

func (wha30Embedder) ChunkText(text string, _, _ int) ([]string, error) {
	third := len(text) / 3
	return []string{text[:third], text[third : 2*third], text[2*third:]}, nil
}

func (wha30Embedder) Dimensions() int { return 4 }
func (wha30Embedder) Model() string   { return "wha30-stub" }
func (wha30Embedder) Backend() string { return "cpu" }

// wha30OpenEmbedding is whaOpen with the embedding worker on (stub provider).
func wha30OpenEmbedding(t *testing.T, dataDir string) *whaNode {
	t.Helper()
	dbConfig := nornicdb.DefaultConfig()
	dbConfig.Memory.DecayEnabled = false
	dbConfig.Memory.AutoLinksEnabled = false
	dbConfig.Database.AsyncWritesEnabled = false
	dbConfig.Database.StrictDurability = true
	dbConfig.Database.WALSyncMode = "immediate"
	dbConfig.Memory.EmbeddingEnabled = true
	dbConfig.Memory.EmbeddingDimensions = 4
	dbConfig.EmbeddingWorker.PropertiesInclude = []string{"text"}
	dbConfig.EmbeddingWorker.IncludeLabels = false

	stdout := os.Stdout
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = writer
	captured := make(chan string)
	go func() {
		var buffer bytes.Buffer
		_, _ = io.Copy(&buffer, reader)
		captured <- buffer.String()
	}()
	database, openErr := nornicdb.Open(dataDir, dbConfig)
	os.Stdout = stdout
	_ = writer.Close()
	printed := <-captured
	_ = reader.Close()
	require.NoError(t, openErr)
	database.SetEmbedder(wha30Embedder{})

	authStorage := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = authStorage.Close() })
	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{SecurityEnabled: true, JWTSecret: []byte("test-secret-key-for-testing-only-32b")}, authStorage)
	require.NoError(t, err)
	require.NoError(t, wha30CreateAdmin(authenticator))
	serverConfig := DefaultConfig()
	serverConfig.Address = "127.0.0.1"
	serverConfig.Port = 0
	serverConfig.EmbeddingEnabled = false
	serverConfig.ProcessConfig = dbConfig
	server, err := New(database, authenticator, serverConfig)
	require.NoError(t, err)
	node := &whaNode{server: server, auth: authenticator, db: database, stdout: printed, dataDir: dataDir}
	node.token = "Bearer " + getAuthToken(t, authenticator, "admin")
	t.Cleanup(func() { node.stop(t) })
	return node
}

func wha30AwaitChunks(t *testing.T, n *whaNode, want int) *storage.Node {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = n.db.WaitForEmbeddings(ctx)
		cancel()
		node, err := wha30Node(t, n)
		require.NoError(t, err)
		if node != nil && len(node.ChunkEmbeddings) == want {
			return node
		}
		require.True(t, time.Now().Before(deadline), "the worker did not embed the node")
		time.Sleep(100 * time.Millisecond)
	}
}

func TestWhaR30WorkerVectorsOneUpdate(t *testing.T) {
	for _, size := range []int{1024, 300 * 1024} {
		t.Run(strings.Repeat("x", 0)+map[int]string{1024: "1 KiB text", 300 * 1024: "300 KiB text"}[size], func(t *testing.T) {
			dirA, dirB := t.TempDir(), t.TempDir()
			a := wha30OpenEmbedding(t, dirA)
			whaRows(t, a, "CREATE (:Doc30 {id: 'd', text: $text, flag: false})", map[string]any{"text": strings.Repeat("y", size)})
			node := wha30AwaitChunks(t, a, 3)
			t.Logf("R30W before the update: chunks=%d flag=%v embedMeta=%v", len(node.ChunkEmbeddings), node.Properties["flag"], node.EmbedMeta)
			a.stop(t)
			whaCopyDir(t, dirA, dirB)

			a = wha30OpenEmbedding(t, dirA)
			node, _ = wha30Node(t, a)
			t.Logf("R30W after a clean restart: chunks=%d", len(node.ChunkEmbeddings))
			whaRows(t, a, "MATCH (n:Doc30 {id: 'd'}) SET n.flag = true", nil)
			node, _ = wha30Node(t, a)
			require.Len(t, node.ChunkEmbeddings, 3)
			require.Equal(t, true, node.Properties["flag"])
			t.Logf("R30W RESULT live right after the update: chunks=%d flag=%v needsEmbedding=%v", len(node.ChunkEmbeddings), node.Properties["flag"], storage.NodeNeedsEmbedding(node))
			time.Sleep(2 * time.Second)
			node, _ = wha30Node(t, a)
			t.Logf("R30W RESULT live 2 s later: chunks=%d", len(node.ChunkEmbeddings))
			a.stop(t)

			require.NoError(t, os.RemoveAll(filepath.Join(dirB, "wal")))
			whaCopyDir(t, filepath.Join(dirA, "wal"), filepath.Join(dirB, "wal"))
			b := wha30OpenEmbedding(t, dirB)
			node, err := wha30Node(t, b)
			require.NoError(t, err)
			require.Len(t, node.ChunkEmbeddings, 3)
			require.Equal(t, true, node.Properties["flag"])
			t.Logf("R30W RESULT restart after a stop before application: recovery=%q chunks=%d flag=%v", whaRecoveredLine(b.stdout), len(node.ChunkEmbeddings), node.Properties["flag"])
			b.stop(t)
		})
	}
}

func wha30CreateAdmin(authenticator *auth.Authenticator) error {
	_, err := authenticator.CreateUser(context.Background(), "admin", "password123", []auth.Role{auth.RoleAdmin})
	return err
}

func wha30Node(t *testing.T, n *whaNode) (*storage.Node, error) {
	t.Helper()
	ctx := t.Context()
	store := n.db.GetStorage()
	nodes, err := store.GetNodesByLabel(ctx, "Doc30")
	if err != nil {
		return nil, err
	}
	require.Len(t, nodes, 1)
	return store.GetNode(ctx, nodes[0].ID)
}
