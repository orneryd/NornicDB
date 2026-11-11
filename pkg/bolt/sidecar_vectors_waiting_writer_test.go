package bolt

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A node whose text is too large to keep its vectors inline holds the vectors
// the server's embedding worker computed for it in the embedding sidecar. Two
// clients update other properties of that node in explicit transactions; the
// second one waits for the first one's write lock and commits after it. Both
// updates leave the embedded text alone, so the node must keep its vectors and
// its embedding metadata, also after a restart.

type waitingWriterEmbedder struct{}

func (waitingWriterEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	return []float32{float32(len(text)), 1, 2, 3}, nil
}

func (e waitingWriterEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, text := range texts {
		out[i], _ = e.Embed(ctx, text)
	}
	return out, nil
}

// ChunkText splits the text into three chunks, so the node holds three vectors.
func (waitingWriterEmbedder) ChunkText(text string, _, _ int) ([]string, error) {
	third := len(text) / 3
	return []string{text[:third], text[third : 2*third], text[2*third:]}, nil
}

func (waitingWriterEmbedder) Dimensions() int { return 4 }
func (waitingWriterEmbedder) Model() string   { return "waiting-writer-test" }
func (waitingWriterEmbedder) Backend() string { return "cpu" }

type waitingWriterServer struct {
	db     *nornicdb.DB
	driver neo4j.DriverWithContext
	server *Server
	done   chan error
}

func openWaitingWriterServer(t *testing.T, dir string, asyncWrites, embedder bool) *waitingWriterServer {
	t.Helper()
	config := nornicdb.DefaultConfig()
	config.Database.AsyncWritesEnabled = asyncWrites
	config.Memory.EmbeddingEnabled = true
	config.Memory.EmbeddingDimensions = 4
	config.EmbeddingWorker.PropertiesInclude = []string{"text"}
	config.EmbeddingWorker.IncludeLabels = false
	db, err := nornicdb.Open(dir, config)
	require.NoError(t, err)
	if embedder {
		db.SetEmbedder(waitingWriterEmbedder{})
	}
	manager, err := multidb.NewDatabaseManager(context.Background(), db.GetBaseStorageForManager(), nil)
	if err != nil {
		_ = db.Close()
		require.NoError(t, err)
	}
	server := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 16}, nil, manager)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", listener.Addr().(*net.TCPAddr).Port), neo4j.NoAuth())
	require.NoError(t, err)
	s := &waitingWriterServer{db: db, driver: driver, server: server, done: done}
	t.Cleanup(func() { s.close(t) })
	return s
}

func (s *waitingWriterServer) close(t *testing.T) {
	if s.db == nil {
		return
	}
	_ = s.driver.Close(context.Background())
	_ = s.server.Close()
	<-s.done
	require.NoError(t, s.db.Close())
	s.db = nil
}

func (s *waitingWriterServer) query(t *testing.T, query string, params map[string]any) []*neo4j.Record {
	t.Helper()
	ctx := context.Background()
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer session.Close(ctx)
	result, err := session.Run(ctx, query, params)
	require.NoError(t, err, query)
	records, err := result.Collect(ctx)
	require.NoError(t, err, query)
	return records
}

// storedDoc reads the document node as a new storage transaction reads it.
func (s *waitingWriterServer) storedDoc(t *testing.T) *storage.Node {
	t.Helper()
	nodes, err := s.db.GetStorage().GetNodesByLabel(context.Background(), "Doc")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	node, err := s.db.GetStorage().GetNode(context.Background(), nodes[0].ID)
	require.NoError(t, err)
	return node
}

func TestBoltWaitingWriterKeepsSidecarVectorsOfLargeNode(t *testing.T) {
	for _, asyncWrites := range []bool{false, true} {
		t.Run(fmt.Sprintf("asyncWrites=%v", asyncWrites), func(t *testing.T) {
			dir := t.TempDir()
			s := openWaitingWriterServer(t, dir, asyncWrites, true)
			ctx := context.Background()
			s.query(t, "CREATE (:Doc {id: 'd', text: $text, flag: false})", map[string]any{"text": strings.Repeat("large text ", 30000)})
			require.Eventually(t, func() bool {
				return len(s.storedDoc(t).ChunkEmbeddings) == 3
			}, 20*time.Second, 20*time.Millisecond, "the embedding worker embeds the document")
			embedded := s.storedDoc(t)
			t.Logf("embedded: chunks=%d meta=%v", len(embedded.ChunkEmbeddings), embedded.EmbedMeta)
			require.Equal(t, true, embedded.EmbedMeta["has_embedding"], "%v", embedded.EmbedMeta)

			waiter := s.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
			defer waiter.Close(ctx)
			waiterTx, err := waiter.BeginTransaction(ctx)
			require.NoError(t, err)
			defer waiterTx.Close(ctx)
			_, err = waiterTx.Run(ctx, "MATCH (n:Doc {id: 'd'}) RETURN n.flag", nil)
			require.NoError(t, err)

			writer := s.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
			defer writer.Close(ctx)
			writerTx, err := writer.BeginTransaction(ctx)
			require.NoError(t, err)
			defer writerTx.Close(ctx)
			result, err := writerTx.Run(ctx, "MATCH (n:Doc {id: 'd'}) SET n.writer = true RETURN n.id", nil)
			require.NoError(t, err)
			_, err = result.Consume(ctx)
			require.NoError(t, err)

			// The waiter's update waits for the writer's lock on the node.
			waited := make(chan error, 1)
			go func() {
				result, err := waiterTx.Run(ctx, "MATCH (n:Doc {id: 'd'}) SET n.waiter = true RETURN n.id", nil)
				if err == nil {
					_, err = result.Consume(ctx)
				}
				waited <- err
			}()
			select {
			case err := <-waited:
				t.Fatalf("the waiter's update did not wait for the writer: %v", err)
			case <-time.After(300 * time.Millisecond):
			}
			require.NoError(t, writerTx.Commit(ctx))
			require.NoError(t, <-waited)
			require.NoError(t, waiterTx.Commit(ctx))

			after := s.storedDoc(t)
			t.Logf("after both commits: chunks=%d meta=%v", len(after.ChunkEmbeddings), after.EmbedMeta)
			assert.Equal(t, embedded.ChunkEmbeddings, after.ChunkEmbeddings, "the stored vectors")
			assert.Equal(t, fmt.Sprint(embedded.EmbedMeta), fmt.Sprint(after.EmbedMeta), "the stored embedding metadata")

			// After a restart (without an embedding model, so nothing is
			// embedded again) a client still reads the node as embedded.
			s.close(t)
			s = openWaitingWriterServer(t, dir, asyncWrites, false)
			records := s.query(t, "MATCH (n:Doc {id: 'd'}) RETURN n.has_embedding, n.writer, n.waiter", nil)
			require.Len(t, records, 1)
			t.Logf("after a restart: has_embedding, writer, waiter = %v", records[0].Values)
			assert.Equal(t, []any{true, true, true}, records[0].Values)
			restarted := s.storedDoc(t)
			t.Logf("after a restart: chunks=%d meta=%v", len(restarted.ChunkEmbeddings), restarted.EmbedMeta)
			assert.Equal(t, embedded.ChunkEmbeddings, restarted.ChunkEmbeddings, "the vectors after a restart")
			assert.Equal(t, fmt.Sprint(embedded.EmbedMeta), fmt.Sprint(restarted.EmbedMeta), "the embedding metadata after a restart")
		})
	}
}
