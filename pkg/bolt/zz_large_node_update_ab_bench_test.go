package bolt

// Paired A/B workload for PR #1797 (NOR-412): an untracked overlay, byte-identical on
// both sides of every A/B. One property update of a node whose chunk vectors are
// stored beside it (written by the embedding worker's sidecar route), through Bolt on
// a single-node server with synchronous, strictly durable writes: the workload of the
// report. Each sub-benchmark opens its own server, stores the node and its vectors,
// runs one untimed warm-up update, then times b.N updates. After the timed updates it
// checks that the node still holds every vector unchanged.
//
// It compiles against the previous build, the PR's base and the PR's head: the calls
// whose signatures gained a context argument between them go through reflection or an
// interface assertion.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type abLab struct {
	db   *nornicdb.DB
	d    neo4j.DriverWithContext
	s    *Server
	done chan error
}

func abNewManager(base storage.Engine) (*multidb.DatabaseManager, error) {
	f := reflect.ValueOf(multidb.NewDatabaseManager)
	ft := f.Type()
	args := make([]reflect.Value, 0, ft.NumIn())
	for i := 0; i < ft.NumIn(); i++ {
		switch in := ft.In(i); {
		case in == reflect.TypeOf((*context.Context)(nil)).Elem():
			args = append(args, reflect.ValueOf(context.Background()))
		case in == reflect.TypeOf((*storage.Engine)(nil)).Elem():
			args = append(args, reflect.ValueOf(&base).Elem())
		default:
			args = append(args, reflect.Zero(in))
		}
	}
	out := f.Call(args)
	if err, _ := out[1].Interface().(error); err != nil {
		return nil, err
	}
	return out[0].Interface().(*multidb.DatabaseManager), nil
}

func abNodesByLabel(e storage.Engine, label string) ([]*storage.Node, error) {
	switch s := any(e).(type) {
	case interface {
		GetNodesByLabel(context.Context, string) ([]*storage.Node, error)
	}:
		return s.GetNodesByLabel(context.Background(), label)
	case interface {
		GetNodesByLabel(string) ([]*storage.Node, error)
	}:
		return s.GetNodesByLabel(label)
	}
	return nil, fmt.Errorf("GetNodesByLabel: unknown signature")
}

func abNode(e storage.Engine, id storage.NodeID) (*storage.Node, error) {
	switch s := any(e).(type) {
	case interface {
		GetNode(context.Context, storage.NodeID) (*storage.Node, error)
	}:
		return s.GetNode(context.Background(), id)
	case interface {
		GetNode(storage.NodeID) (*storage.Node, error)
	}:
		return s.GetNode(id)
	}
	return nil, fmt.Errorf("GetNode: unknown signature")
}

func abSidecar(e storage.Engine, n *storage.Node) error {
	switch s := any(e).(type) {
	case interface {
		UpdateNodeEmbeddingSidecar(context.Context, *storage.Node) error
	}:
		return s.UpdateNodeEmbeddingSidecar(context.Background(), n)
	case interface {
		UpdateNodeEmbeddingSidecar(*storage.Node) error
	}:
		return s.UpdateNodeEmbeddingSidecar(n)
	}
	return fmt.Errorf("UpdateNodeEmbeddingSidecar: unknown signature")
}

func abOpen(b *testing.B, root string) *abLab {
	b.Helper()
	c := nornicdb.DefaultConfig()
	c.Database.AsyncWritesEnabled = false
	c.Database.StrictDurability = true
	c.Database.WALSyncMode = "immediate"
	c.Database.WALAutoCompactionEnabled = false
	c.Database.WALSnapshotInterval = time.Hour
	c.Memory.DecayEnabled = false
	c.Memory.AutoLinksEnabled = false
	c.Memory.EmbeddingEnabled = false
	c.Memory.EmbeddingDimensions = 1024
	c.EmbeddingWorker.PropertiesInclude = []string{"text"}
	c.EmbeddingWorker.IncludeLabels = false
	c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	db, err := nornicdb.Open(root, c)
	if err != nil {
		b.Fatal(err)
	}
	manager, err := abNewManager(db.GetBaseStorageForManager())
	if err != nil {
		_ = db.Close()
		b.Fatal(err)
	}
	server := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 16}, nil, manager)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = db.Close()
		b.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", listener.Addr().(*net.TCPAddr).Port), neo4j.NoAuth())
	if err != nil {
		_ = db.Close()
		b.Fatal(err)
	}
	lab := &abLab{db: db, d: driver, s: server, done: done}
	b.Cleanup(func() {
		_ = lab.d.Close(context.Background())
		_ = lab.s.Close()
		<-lab.done
		_ = lab.db.Close()
	})
	return lab
}

func (l *abLab) run(query string, params map[string]any, explicit bool) error {
	ctx := context.Background()
	session := l.d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer session.Close(ctx)
	if !explicit {
		result, err := session.Run(ctx, query, params)
		if err != nil {
			return err
		}
		_, err = result.Consume(ctx)
		return err
	}
	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Close(ctx)
	result, err := tx.Run(ctx, query, params)
	if err != nil {
		return err
	}
	if _, err = result.Consume(ctx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (l *abLab) doc() (*storage.Node, error) {
	nodes, err := abNodesByLabel(l.db.GetStorage(), "Doc")
	if err != nil || len(nodes) != 1 {
		return nil, fmt.Errorf("Doc nodes=%d err=%v", len(nodes), err)
	}
	return abNode(l.db.GetStorage(), nodes[0].ID)
}

func abVectors(chunks, dims int) [][]float32 {
	out := make([][]float32, chunks)
	for i := range out {
		v := make([]float32, dims)
		for j := range v {
			v[j] = 1 + float32(i*dims+j+1)/3
		}
		out[i] = v
	}
	return out
}

func abSame(a, b [][]float32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}

func BenchmarkZZLargeNodeUpdate(b *testing.B) {
	for _, chunks := range []int{1800, 6600} {
		for _, explicit := range []bool{false, true} {
			form := "auto"
			if explicit {
				form = "explicit"
			}
			b.Run(fmt.Sprintf("chunks=%d/%s", chunks, form), func(b *testing.B) {
				lab := abOpen(b, b.TempDir())
				if err := lab.run("CREATE (:Doc {id: 'd', text: 'alpha', flag: 0})", nil, false); err != nil {
					b.Fatal(err)
				}
				node, err := lab.doc()
				if err != nil {
					b.Fatal(err)
				}
				vectors := abVectors(chunks, 1024)
				embedded := storage.CopyNode(node)
				embedded.ID = storage.NodeID("nornic:" + string(node.ID))
				embedded.ChunkEmbeddings = vectors
				embedded.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": chunks, "embedding_dimensions": 1024, "embedding_model": "synthetic"}
				inner := lab.db.GetBaseStorageForManager().(*storage.WALEngine).GetInnerEngine()
				if err := abSidecar(inner, embedded); err != nil {
					b.Fatal(err)
				}
				const update = "MATCH (n:Doc {id: 'd'}) SET n.flag = $flag"
				if err := lab.run(update, map[string]any{"flag": int64(-1)}, explicit); err != nil {
					b.Fatal(err)
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := lab.run(update, map[string]any{"flag": int64(i)}, explicit); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				node, err = lab.doc()
				if err != nil {
					b.Fatal(err)
				}
				if !abSame(vectors, node.ChunkEmbeddings) || fmt.Sprint(node.Properties["flag"]) != fmt.Sprint(int64(b.N-1)) {
					b.Fatalf("after %d updates: flag=%v vectors=%d same=%v", b.N, node.Properties["flag"], len(node.ChunkEmbeddings), abSame(vectors, node.ChunkEmbeddings))
				}
			})
		}
	}
}
