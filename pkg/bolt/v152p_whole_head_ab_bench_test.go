package bolt

// Paired A/B workloads for the whole-head comparison (Part B of
// whole-head-issues-report-brief.md): an untracked fixture, byte-identical on
// both arms (previous build b6d8e762a and the whole head ab789dd08). Every
// operation runs through Bolt against a single-node server with synchronous,
// strictly durable writes (the large-node A/B's configuration). Each write
// benchmark also reports the journal bytes written per operation (journalB/op):
// the growth of the store's WAL files over the timed operations, divided by
// b.N. Calls whose signatures differ between the builds go through reflection
// or an interface assertion, as in the large-node fixture.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type whLab struct {
	root string
	db   *nornicdb.DB
	d    neo4j.DriverWithContext
	s    *Server
	done chan error
}

func whNewManager(base storage.Engine) (*multidb.DatabaseManager, error) {
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

func whNodesByLabel(e storage.Engine, label string) ([]*storage.Node, error) {
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

func whNode(e storage.Engine, id storage.NodeID) (*storage.Node, error) {
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

func whSidecar(e storage.Engine, n *storage.Node) error {
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

func whConfig(dims int) *nornicdb.Config {
	c := nornicdb.DefaultConfig()
	c.Database.AsyncWritesEnabled = false
	c.Database.StrictDurability = true
	c.Database.WALSyncMode = "immediate"
	c.Database.WALAutoCompactionEnabled = false
	c.Database.WALSnapshotInterval = time.Hour
	c.Memory.DecayEnabled = false
	c.Memory.AutoLinksEnabled = false
	c.Memory.EmbeddingEnabled = false
	c.Memory.EmbeddingDimensions = dims
	c.EmbeddingWorker.PropertiesInclude = []string{"text"}
	c.EmbeddingWorker.IncludeLabels = false
	c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	return c
}

func whOpen(b *testing.B, root string, dims int) *whLab {
	b.Helper()
	db, err := nornicdb.Open(root, whConfig(dims))
	if err != nil {
		b.Fatal(err)
	}
	manager, err := whNewManager(db.GetBaseStorageForManager())
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
	lab := &whLab{root: root, db: db, d: driver, s: server, done: done}
	b.Cleanup(lab.close)
	return lab
}

func (l *whLab) close() {
	if l.db == nil {
		return
	}
	_ = l.d.Close(context.Background())
	_ = l.s.Close()
	<-l.done
	_ = l.db.Close()
	l.db = nil
}

// run executes one statement and returns the number of rows it produced.
func (l *whLab) run(query string, params map[string]any) (int, error) {
	ctx := context.Background()
	session := l.d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer session.Close(ctx)
	result, err := session.Run(ctx, query, params)
	if err != nil {
		return 0, err
	}
	rows := 0
	for result.Next(ctx) {
		rows++
	}
	if err := result.Err(); err != nil {
		return rows, err
	}
	_, err = result.Consume(ctx)
	return rows, err
}

func (l *whLab) must(b *testing.B, query string, params map[string]any) {
	b.Helper()
	if _, err := l.run(query, params); err != nil {
		b.Fatalf("%s: %v", query, err)
	}
}

func (l *whLab) scalar(b *testing.B, query string, params map[string]any) int64 {
	b.Helper()
	ctx := context.Background()
	session := l.d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer session.Close(ctx)
	result, err := session.Run(ctx, query, params)
	if err != nil {
		b.Fatal(err)
	}
	record, err := result.Single(ctx)
	if err != nil {
		b.Fatal(err)
	}
	v, _ := record.Values[0].(int64)
	return v
}

// whJournalBytes is the size of the store's journal files (the WAL segments and
// wal.log under <root>/wal), as the timing probe measures them.
func whJournalBytes(root string) int64 {
	var total int64
	_ = filepath.Walk(filepath.Join(root, "wal"), func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && (strings.HasSuffix(path, ".wal") || filepath.Base(path) == "wal.log") {
			total += info.Size()
		}
		return nil
	})
	return total
}

func whJournal(b *testing.B, l *whLab, before int64) {
	b.ReportMetric(float64(whJournalBytes(l.root)-before)/float64(b.N), "journalB/op")
}

// BenchmarkZZWHWrite: one small write per statement, auto-commit.
func BenchmarkZZWHWrite(b *testing.B) {
	b.Run("create-node", func(b *testing.B) {
		l := whOpen(b, b.TempDir(), 1024)
		l.must(b, "CREATE (:Item {id: 'warm', name: 'n', v: 0})", nil)
		before := whJournalBytes(l.root)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			l.must(b, "CREATE (:Item {id: $id, name: 'n', v: $v})", map[string]any{"id": fmt.Sprintf("i-%d", i), "v": int64(i)})
		}
		b.StopTimer()
		whJournal(b, l, before)
		if got := l.scalar(b, "MATCH (n:Item) RETURN count(n)", nil); got != int64(b.N+1) {
			b.Fatalf("items=%d want %d", got, b.N+1)
		}
	})
	b.Run("update-node", func(b *testing.B) {
		l := whOpen(b, b.TempDir(), 1024)
		l.must(b, "CREATE (:Item {id: 'x', name: 'n', v: 0})", nil)
		l.must(b, "MATCH (n:Item {id: 'x'}) SET n.v = -1", nil)
		before := whJournalBytes(l.root)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			l.must(b, "MATCH (n:Item {id: 'x'}) SET n.v = $v", map[string]any{"v": int64(i)})
		}
		b.StopTimer()
		whJournal(b, l, before)
		if got := l.scalar(b, "MATCH (n:Item {id: 'x'}) RETURN n.v", nil); got != int64(b.N-1) {
			b.Fatalf("v=%d want %d", got, b.N-1)
		}
	})
	b.Run("create-rel", func(b *testing.B) {
		l := whOpen(b, b.TempDir(), 1024)
		l.must(b, "CREATE (:Item {id: 'a'}), (:Item {id: 'b'})", nil)
		l.must(b, "MATCH (a:Item {id: 'a'}), (c:Item {id: 'b'}) CREATE (a)-[:REL {v: -1}]->(c)", nil)
		before := whJournalBytes(l.root)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			l.must(b, "MATCH (a:Item {id: 'a'}), (c:Item {id: 'b'}) CREATE (a)-[:REL {v: $v}]->(c)", map[string]any{"v": int64(i)})
		}
		b.StopTimer()
		whJournal(b, l, before)
		if got := l.scalar(b, "MATCH (:Item {id: 'a'})-[r:REL]->(:Item {id: 'b'}) RETURN count(r)", nil); got != int64(b.N+1) {
			b.Fatalf("rels=%d want %d", got, b.N+1)
		}
	})
	b.Run("update-rel", func(b *testing.B) {
		l := whOpen(b, b.TempDir(), 1024)
		l.must(b, "CREATE (:Item {id: 'a'})-[:REL {v: 0}]->(:Item {id: 'b'})", nil)
		l.must(b, "MATCH (:Item {id: 'a'})-[r:REL]->(:Item {id: 'b'}) SET r.v = -1", nil)
		before := whJournalBytes(l.root)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			l.must(b, "MATCH (:Item {id: 'a'})-[r:REL]->(:Item {id: 'b'}) SET r.v = $v", map[string]any{"v": int64(i)})
		}
		b.StopTimer()
		whJournal(b, l, before)
		if got := l.scalar(b, "MATCH (:Item {id: 'a'})-[r:REL]->(:Item {id: 'b'}) RETURN r.v", nil); got != int64(b.N-1) {
			b.Fatalf("v=%d want %d", got, b.N-1)
		}
	})
}

// BenchmarkZZWHBulk: 1,000 new small nodes per statement (UNWIND ... CREATE).
func BenchmarkZZWHBulk(b *testing.B) {
	const rows = 1000
	l := whOpen(b, b.TempDir(), 1024)
	batch := func(op int) []any {
		out := make([]any, rows)
		for j := range out {
			out[j] = map[string]any{"id": fmt.Sprintf("b-%d-%d", op, j), "v": int64(j)}
		}
		return out
	}
	l.must(b, "UNWIND $rows AS row CREATE (:Bulk {id: row.id, v: row.v})", map[string]any{"rows": batch(-1)})
	before := whJournalBytes(l.root)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		params := map[string]any{"rows": batch(i)}
		b.StartTimer()
		l.must(b, "UNWIND $rows AS row CREATE (:Bulk {id: row.id, v: row.v})", params)
	}
	b.StopTimer()
	whJournal(b, l, before)
	if got := l.scalar(b, "MATCH (n:Bulk) RETURN count(n)", nil); got != int64((b.N+1)*rows) {
		b.Fatalf("bulk=%d want %d", got, (b.N+1)*rows)
	}
}

// whCollection seeds one collection and members documents linked to it by
// MEMBER_OF, each with about 1.7 KB of properties; withVectors gives each
// document one stored 2,048-dimension chunk vector through the embedding
// sidecar (the downstream shape: one kept vector per document).
func whCollection(b *testing.B, l *whLab, members int, withVectors bool) {
	b.Helper()
	l.must(b, "CREATE (:Collection {id: 'c', name: 'archive me'})", nil)
	text := strings.Repeat("lorem ipsum dolor sit amet ", 60) // about 1,600 bytes
	for start := 0; start < members; start += 500 {
		end := start + 500
		if end > members {
			end = members
		}
		rows := make([]any, 0, end-start)
		for j := start; j < end; j++ {
			rows = append(rows, map[string]any{"id": fmt.Sprintf("doc-%d", j), "title": fmt.Sprintf("Document %d of the collection", j), "text": text})
		}
		l.must(b, "MATCH (c:Collection {id: 'c'}) UNWIND $rows AS row CREATE (d:Document {id: row.id, title: row.title, text: row.text, archived: false})-[:MEMBER_OF]->(c)", map[string]any{"rows": rows})
	}
	if !withVectors {
		return
	}
	docs, err := whNodesByLabel(l.db.GetStorage(), "Document")
	if err != nil || len(docs) != members {
		b.Fatalf("documents=%d err=%v", len(docs), err)
	}
	inner := l.db.GetBaseStorageForManager().(*storage.WALEngine).GetInnerEngine()
	vector := make([]float32, 2048)
	for i, d := range docs {
		for k := range vector {
			vector[k] = float32(i+1) + float32(k)/2048
		}
		embedded := storage.CopyNode(d)
		embedded.ID = storage.NodeID("nornic:" + string(d.ID))
		embedded.ChunkEmbeddings = [][]float32{append([]float32(nil), vector...)}
		embedded.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": 1, "embedding_dimensions": 2048, "embedding_model": "synthetic"}
		if err := whSidecar(inner, embedded); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkZZWHArchive: the collection archive: one statement sets one boolean
// on every member of a collection (alternating true/false so that every run
// changes every member).
func BenchmarkZZWHArchive(b *testing.B) {
	for _, members := range []int{1000, 8800} {
		b.Run(fmt.Sprintf("docs=%d", members), func(b *testing.B) {
			l := whOpen(b, b.TempDir(), 2048)
			whCollection(b, l, members, true)
			const archive = "MATCH (:Collection {id: 'c'})<-[:MEMBER_OF]-(d:Document) SET d.archived = $flag"
			l.must(b, archive, map[string]any{"flag": true})
			before := whJournalBytes(l.root)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				l.must(b, archive, map[string]any{"flag": i%2 == 1})
			}
			b.StopTimer()
			whJournal(b, l, before)
			want := i64(b.N%2 == 0) * int64(members) // the last run set flag = (b.N-1)%2 == 1
			if got := l.scalar(b, "MATCH (d:Document {archived: true}) RETURN count(d)", nil); got != want {
				b.Fatalf("archived=%d want %d", got, want)
			}
			docs, err := whNodesByLabel(l.db.GetStorage(), "Document")
			if err != nil || len(docs) == 0 {
				b.Fatalf("documents: %v", err)
			}
			n, err := whNode(l.db.GetStorage(), docs[0].ID)
			if err != nil || len(n.ChunkEmbeddings) != 1 || len(n.ChunkEmbeddings[0]) != 2048 {
				b.Fatalf("vectors after archive: err=%v chunks=%d", err, len(n.ChunkEmbeddings))
			}
		})
	}
}

func i64(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

// BenchmarkZZWHRead: reads.
//   - members: the members of a collection of 1,000 documents, each document
//     also linked to 10 topic nodes (the shape of the known collection-to-
//     documents read);
//   - hub-typed: the incoming links of one type on a hub node that has 4,000
//     links of that type and 4,000 of another (a typed one-step read on a
//     highly linked node).
func BenchmarkZZWHRead(b *testing.B) {
	b.Run("members", func(b *testing.B) {
		l := whOpen(b, b.TempDir(), 1024)
		whCollection(b, l, 1000, false)
		l.must(b, "UNWIND range(0, 9) AS t CREATE (:Topic {id: t})", nil)
		l.must(b, "MATCH (d:Document), (t:Topic) CREATE (d)-[:MENTIONS]->(t)", nil)
		const read = "MATCH (:Collection {id: 'c'})<-[:MEMBER_OF]-(d:Document) RETURN d.id"
		if rows, err := l.run(read, nil); err != nil || rows != 1000 {
			b.Fatalf("rows=%d err=%v", rows, err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := l.run(read, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("hub-typed", func(b *testing.B) {
		l := whOpen(b, b.TempDir(), 1024)
		l.must(b, "CREATE (:Def {id: 'hub'})", nil)
		for start := 0; start < 4000; start += 1000 {
			l.must(b, "MATCH (h:Def {id: 'hub'}) UNWIND range($s, $s + 999) AS k CREATE (:Src {id: k})-[:PD_RELATION {k: k}]->(h), (:Other {id: k})-[:OTHER {k: k}]->(h)", map[string]any{"s": int64(start)})
		}
		const read = "MATCH (h:Def {id: 'hub'}) OPTIONAL MATCH (h)<-[r:PD_RELATION]-(s) RETURN count(r)"
		if got := l.scalar(b, read, nil); got != 4000 {
			b.Fatalf("typed links=%d", got)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := l.run(read, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkZZWHStartup: reopening a closed store of 10,000 small nodes and
// 10,000 relationships (written through Bolt, then closed cleanly): each
// operation is one open and one close of the database. It is not a recovery
// after a crash.
func BenchmarkZZWHStartup(b *testing.B) {
	root := b.TempDir()
	l := whOpen(b, root, 1024)
	for start := 0; start < 10000; start += 1000 {
		l.must(b, "UNWIND range($s, $s + 999) AS k CREATE (a:Start {id: k})-[:NEXT {k: k}]->(:End {id: k})", map[string]any{"s": int64(start)})
	}
	l.close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db, err := nornicdb.Open(root, whConfig(1024))
		if err != nil {
			b.Fatal(err)
		}
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
