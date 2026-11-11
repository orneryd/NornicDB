package bolt

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-412: through a real Bolt server over the database manager (as every
// server deployment runs), with the WAL on, immediate sync and strict
// durability, a statement that changes properties or labels of embedded
// nodes but not their vectors must commit however many vectors the changed
// nodes hold, and must keep them. Before the fix the transaction's journal
// record carried every changed node's vectors and was refused above 16 MiB:
// "wal: entry exceeds maximum WAL frame size of 16777216 bytes".
//
// Vectors are 2,048 float32 dimensions. Only fixture seeding writes vectors
// past the journal (the embedding sidecar, as the embedding worker does);
// the embedding text is limited to "text" so the measured changes keep them.

const kvDims = 2048

type kvLab struct {
	t    *testing.T
	root string
	db   *nornicdb.DB
	d    neo4j.DriverWithContext
	s    *Server
	done chan error
}

func kvConfig() *nornicdb.Config {
	c := nornicdb.DefaultConfig()
	c.Database.AsyncWritesEnabled = false
	c.Database.StrictDurability = true
	c.Database.WALSyncMode = "immediate"
	c.Database.WALAutoCompactionEnabled = false
	c.Memory.DecayEnabled = false
	c.Memory.AutoLinksEnabled = false
	c.Memory.EmbeddingEnabled = false
	c.Memory.EmbeddingDimensions = kvDims
	c.EmbeddingWorker.PropertiesInclude = []string{"text"}
	c.EmbeddingWorker.IncludeLabels = false
	return c
}

func kvOpen(t *testing.T, root string, config *nornicdb.Config) *kvLab {
	t.Helper()
	db, err := nornicdb.Open(root, config)
	require.NoError(t, err)
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
	lab := &kvLab{t: t, root: root, db: db, d: driver, s: server, done: done}
	t.Cleanup(lab.close)
	return lab
}

func (l *kvLab) close() {
	if l.db == nil {
		return
	}
	_ = l.d.Close(context.Background())
	_ = l.s.Close()
	<-l.done
	require.NoError(l.t, l.db.Close())
	l.db = nil
}

// reopen closes cleanly and opens the same database again.
func (l *kvLab) reopen(config *nornicdb.Config) *kvLab {
	l.close()
	return kvOpen(l.t, l.root, config)
}

func (l *kvLab) run(query string, params map[string]any, explicit bool) ([][]any, error) {
	ctx := context.Background()
	session := l.d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer session.Close(ctx)
	var result neo4j.ResultWithContext
	var err error
	var tx neo4j.ExplicitTransaction
	if explicit {
		if tx, err = session.BeginTransaction(ctx); err != nil {
			return nil, fmt.Errorf("BEGIN: %w", err)
		}
		defer tx.Close(ctx)
		result, err = tx.Run(ctx, query, params)
	} else {
		result, err = session.Run(ctx, query, params)
	}
	if err != nil {
		return nil, fmt.Errorf("RUN: %w", err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return nil, fmt.Errorf("PULL: %w", err)
	}
	rows := make([][]any, 0, len(records))
	for _, record := range records {
		rows = append(rows, record.Values)
	}
	if explicit {
		if err = tx.Commit(ctx); err != nil {
			return rows, fmt.Errorf("COMMIT: %w", err)
		}
	}
	return rows, nil
}

func (l *kvLab) mustRun(query string, params map[string]any) [][]any {
	l.t.Helper()
	rows, err := l.run(query, params, false)
	require.NoError(l.t, err, query)
	return rows
}

func kvVectors(chunks int, seed int) [][]float32 {
	out := make([][]float32, chunks)
	for i := range out {
		v := make([]float32, kvDims)
		for j := range v {
			v[j] = float32(seed) + float32(i*kvDims+j+1)/3
		}
		out[i] = v
	}
	return out
}

func kvVectorPrint(vectors [][]float32) string {
	if len(vectors) == 0 {
		return "chunks=0"
	}
	last := vectors[len(vectors)-1]
	return fmt.Sprintf("chunks=%d first=%v last=%v", len(vectors), vectors[0][0], last[len(last)-1])
}

func (l *kvLab) nodesByLabel(label string) []*storage.Node {
	l.t.Helper()
	nodes, err := l.db.GetStorage().GetNodesByLabel(context.Background(), label)
	require.NoError(l.t, err)
	out := make([]*storage.Node, 0, len(nodes))
	for _, node := range nodes {
		full, err := l.db.GetStorage().GetNode(context.Background(), node.ID)
		require.NoError(l.t, err)
		out = append(out, full)
	}
	return out
}

// embed stores chunks vectors for every node with label through the
// embedding sidecar, as the embedding worker does.
func (l *kvLab) embed(label string, chunks int) {
	l.t.Helper()
	inner := l.db.GetBaseStorageForManager().(*storage.WALEngine).GetInnerEngine()
	sidecar := inner.(interface {
		UpdateNodeEmbeddingSidecar(context.Context, *storage.Node) error
	})
	for _, node := range l.nodesByLabel(label) {
		node = storage.CopyNode(node)
		node.ID = storage.NodeID("nornic:" + string(node.ID))
		node.ChunkEmbeddings = kvVectors(chunks, 0)
		node.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": chunks, "embedding_dimensions": kvDims, "embedding_model": "synthetic"}
		require.NoError(l.t, sidecar.UpdateNodeEmbeddingSidecar(context.Background(), node))
	}
}

// requireVectors checks every node with label still holds chunks vectors,
// with their seeded values, and its stored embedding metadata.
//
// The metadata is read through the engine's embedding-metadata reader. A
// node read (GetNode) of a node whose body exceeds the inline size reports
// only {chunk_count: 0} as metadata after any update that keeps its vectors,
// until the database reopens; main does the same below the 16 MiB limit, so
// it is a separate read-path issue, not this journal fix. requireNodeMeta
// checks the node read itself after a reopen.
func (l *kvLab) requireVectors(label string, count, chunks int) {
	l.t.Helper()
	nodes := l.nodesByLabel(label)
	require.Len(l.t, nodes, count, label)
	want := kvVectorPrint(kvVectors(chunks, 0))
	reader := l.db.GetBaseStorageForManager().(*storage.WALEngine).GetInnerEngine().(interface {
		GetNodeEmbeddingMeta(context.Context, storage.NodeID) (map[string]any, error)
	})
	for _, node := range nodes {
		require.Equal(l.t, want, kvVectorPrint(node.ChunkEmbeddings), "vectors of %s %s", label, node.ID)
		meta, err := reader.GetNodeEmbeddingMeta(context.Background(), storage.NodeID("nornic:"+string(node.ID)))
		require.NoError(l.t, err)
		require.Equal(l.t, true, meta["has_embedding"], "embedding metadata of %s %s", label, node.ID)
		require.EqualValues(l.t, chunks, meta["chunk_count"], "embedding metadata of %s %s", label, node.ID)
	}
}

// requireNodeMeta checks the node read's own embedding metadata.
func (l *kvLab) requireNodeMeta(label string, chunks int) {
	l.t.Helper()
	for _, node := range l.nodesByLabel(label) {
		require.Equal(l.t, true, node.EmbedMeta["has_embedding"], "node read metadata of %s %s", label, node.ID)
		require.EqualValues(l.t, chunks, node.EmbedMeta["chunk_count"], "node read metadata of %s %s", label, node.ID)
	}
}

// journalSince returns the WAL records after sequence after, and the largest
// entity_commit record among them (its data length).
func (l *kvLab) journalSince(after uint64) (last uint64, largest int) {
	l.t.Helper()
	entries, err := storage.ReadWALEntriesFromDir(context.Background(), filepath.Join(l.root, "wal"))
	require.NoError(l.t, err, "the journal reads back")
	for _, entry := range entries {
		last = max(last, entry.Sequence)
		if entry.Sequence > after && entry.Operation == storage.OpEntityCommit {
			largest = max(largest, len(entry.Data))
		}
	}
	return last, largest
}

func kvForms() []bool { return []bool{true, false} }

func kvForm(explicit bool) string {
	if explicit {
		return "explicit"
	}
	return "autocommit"
}

// One large node: 1.3 MB of text and 1,800 or 6,600 chunk vectors (about 15
// and 54 MB of vectors). Every statement shape changes the node or a
// relationship on it and keeps the vectors.
func TestKeptVectorsLargeNodeStatements(t *testing.T) {
	scalars := []string{"p01", "p02", "p03", "p04", "p05", "p06", "p07", "p08", "p09", "p10", "p11", "p12", "p13", "p14"}
	setScalars := make([]string, len(scalars))
	for i, name := range scalars {
		setScalars[i] = fmt.Sprintf("s.%s = $%s", name, name)
	}
	type step struct {
		name, query string
		params      map[string]any
		check       string // a query returning one value
		want        any
	}
	steps := []step{
		{"set", "MATCH (s:Large {id:$id}) SET s.hidden = $v", map[string]any{"v": true}, "RETURN s.hidden", true},
		{"label-add", "MATCH (s:Large {id:$id}) SET s:Archived", nil, "RETURN s:Archived", true},
		{"property-remove", "MATCH (s:Large {id:$id}) REMOVE s.remove_me", nil, "RETURN s.remove_me IS NULL", true},
		{"map-merge-three", "MATCH (s:Large {id:$id}) SET s += $map", map[string]any{"map": map[string]any{"a": int64(11), "b": int64(22), "c": int64(33)}}, "RETURN [s.a, s.b, s.c]", []any{int64(11), int64(22), int64(33)}},
		// Fourteen scalars set unconditionally; two of them really change.
		{"set-fourteen", "MATCH (s:Large {id:$id}) SET " + strings.Join(setScalars, ", "), nil, "RETURN [s.p01, s.p02, s.p14]", []any{int64(101), int64(102), int64(14)}},
		{"merge-on-match-in-subquery", "WITH $id AS id CALL { WITH id MERGE (s:Large {id: id}) ON MATCH SET s.merged = $v } RETURN count(*) AS n", map[string]any{"v": true}, "RETURN s.merged", true},
		{"map-merge-in-subquery", "WITH $id AS id CALL { WITH id MATCH (s:Large {id: id}) SET s += $map } RETURN count(*) AS n", map[string]any{"map": map[string]any{"d": int64(44)}}, "RETURN s.d", int64(44)},
		{"relationship-property", "MATCH (:Peer {id:'peer'})-[r:REFERS]->(s:Large {id:$id}) SET r.weight = $v", map[string]any{"v": int64(2)}, "MATCH (:Peer {id:'peer'})-[r:REFERS]->(s) RETURN r.weight", int64(2)},
	}
	for _, chunks := range []int{1800, 6600} {
		for _, explicit := range kvForms() {
			t.Run(fmt.Sprintf("chunks%d/%s", chunks, kvForm(explicit)), func(t *testing.T) {
				lab := kvOpen(t, t.TempDir(), kvConfig())
				seed := map[string]any{"id": "large", "text": strings.Repeat("x", 1300000)}
				for i, name := range scalars {
					seed[name] = int64(i + 1)
				}
				lab.mustRun("CREATE (s:Large) SET s = $seed, s.remove_me = 'present', s.a = 1, s.b = 2, s.c = 3 CREATE (:Peer {id:'peer'})-[:REFERS {weight: 1}]->(s)", map[string]any{"seed": seed})
				lab.embed("Large", chunks)
				lab.requireVectors("Large", 1, chunks)
				// Each shape is its own subtest, so each reports its own result;
				// the persistence checks cover the shapes that committed.
				committed := map[string]bool{}
				for _, s := range steps {
					t.Run(s.name, func(t *testing.T) {
						outer := lab.t
						lab.t = t
						defer func() { lab.t = outer }()
						params := map[string]any{"id": "large"}
						for k, v := range s.params {
							params[k] = v
						}
						if s.name == "set-fourteen" {
							for i, name := range scalars {
								params[name] = int64(i + 1)
							}
							params["p01"], params["p02"] = int64(101), int64(102)
						}
						seq, _ := lab.journalSince(0)
						_, err := lab.run(s.query, params, explicit)
						require.NoError(t, err, "%s must commit", s.name)
						_, largest := lab.journalSince(seq)
						t.Logf("KEPT-VECTORS chunks=%d form=%s step=%s committed largestEntityRecord=%d", chunks, kvForm(explicit), s.name, largest)
						// The record holds the 1.3 MB text, never the vectors.
						require.Less(t, largest, 2_000_000, "%s journal record must not carry kept vectors", s.name)
						lab.requireVectors("Large", 1, chunks)
						committed[s.name] = true
					})
				}
				check := func(stage string) {
					for _, s := range steps {
						if !committed[s.name] {
							continue
						}
						rows := lab.mustRun("MATCH (s:Large {id:'large'}) "+s.check, nil)
						require.Len(t, rows, 1)
						require.Equal(t, s.want, rows[0][0], "%s after %s", s.name, stage)
					}
					lab.requireVectors("Large", 1, chunks)
				}
				check("commit")
				lab = lab.reopen(kvConfig())
				check("clean reopen")
				lab.requireNodeMeta("Large", chunks)
			})
		}
	}
}

// Many nodes changed by one statement: their vectors together exceed 16 MiB
// although no node does.
func TestKeptVectorsTransactionWide(t *testing.T) {
	for _, c := range []struct{ nodes, chunks int }{{20, 100}, {2000, 1}} {
		t.Run(fmt.Sprintf("n%d_c%d", c.nodes, c.chunks), func(t *testing.T) {
			lab := kvOpen(t, t.TempDir(), kvConfig())
			lab.mustRun("UNWIND range(1,$n) AS i CREATE (:Wide {id:i, text:'synthetic', flag:false})", map[string]any{"n": c.nodes})
			lab.embed("Wide", c.chunks)
			for i, explicit := range kvForms() {
				flag := i == 0 // explicit sets true, auto-commit back to false
				seq, _ := lab.journalSince(0)
				_, err := lab.run("MATCH (n:Wide) SET n.flag = $v", map[string]any{"v": flag}, explicit)
				require.NoError(t, err, "%s must commit", kvForm(explicit))
				_, largest := lab.journalSince(seq)
				t.Logf("KEPT-VECTORS wide nodes=%d chunks=%d form=%s largestEntityRecord=%d", c.nodes, c.chunks, kvForm(explicit), largest)
				require.Less(t, largest, 1_000_000)
				rows := lab.mustRun("MATCH (n:Wide) WHERE n.flag = $v RETURN count(n)", map[string]any{"v": flag})
				require.Equal(t, int64(c.nodes), rows[0][0])
				lab.requireVectors("Wide", c.nodes, c.chunks)
			}
			lab = lab.reopen(kvConfig())
			rows := lab.mustRun("MATCH (n:Wide) WHERE n.flag = false RETURN count(n)", nil)
			require.Equal(t, int64(c.nodes), rows[0][0])
			lab.requireVectors("Wide", c.nodes, c.chunks)
			lab.requireNodeMeta("Wide", c.chunks)
		})
	}
}

// The journal-size measurement: one boolean on 10 nodes of 100 chunks.
func TestKeptVectorsJournalSizeTenByHundred(t *testing.T) {
	lab := kvOpen(t, t.TempDir(), kvConfig())
	lab.mustRun("UNWIND range(1,10) AS i CREATE (:Wide {id:i, text:'synthetic', flag:false})", nil)
	lab.embed("Wide", 100)
	seq, _ := lab.journalSince(0)
	_, err := lab.run("MATCH (n:Wide) SET n.flag = true", nil, true)
	require.NoError(t, err)
	_, largest := lab.journalSince(seq)
	t.Logf("KEPT-VECTORS JOURNAL-SIZE nodes=10 chunks=100 entityRecordBytes=%d", largest)
	require.Less(t, largest, 64*1024, "ten small node images, no vectors")
	lab.requireVectors("Wide", 10, 100)
}

// One statement from a collection node through its members to their
// summaries: one boolean on every member and summary (two embedded labels,
// one chunk each) and one property on the collection node.
func TestKeptVectorsCollectionTraversal(t *testing.T) {
	for _, members := range []int{1000, 8800} {
		t.Run(fmt.Sprintf("members%d", members), func(t *testing.T) {
			lab := kvOpen(t, t.TempDir(), kvConfig())
			lab.mustRun(`CREATE (g:Group {id:'g', text:'group'})
				WITH g UNWIND range(1,$n) AS i
				CREATE (g)-[:CONTAINS]->(m:Member {id:i, text:'member', hidden:false})-[:SUMMARIZED_BY]->(:Summary {id:i, text:'summary', hidden:false})`, map[string]any{"n": members})
			lab.embed("Member", 1)
			lab.embed("Summary", 1)
			query := `MATCH (g:Group {id:'g'})-[:CONTAINS]->(m:Member)-[:SUMMARIZED_BY]->(s:Summary)
				SET g.reviewed = $v, m.hidden = $v, s.hidden = $v`
			for i, explicit := range kvForms() {
				flag := i == 0
				seq, _ := lab.journalSince(0)
				_, err := lab.run(query, map[string]any{"v": flag}, explicit)
				require.NoError(t, err, "%s must commit", kvForm(explicit))
				_, largest := lab.journalSince(seq)
				t.Logf("KEPT-VECTORS collection members=%d form=%s largestEntityRecord=%d", members, kvForm(explicit), largest)
				rows := lab.mustRun("MATCH (m:Member) WHERE m.hidden = $v WITH count(m) AS m MATCH (s:Summary) WHERE s.hidden = $v RETURN m, count(s)", map[string]any{"v": flag})
				require.Equal(t, []any{int64(members), int64(members)}, rows[0])
			}
			lab.requireVectors("Member", members, 1)
			lab.requireVectors("Summary", members, 1)
			lab = lab.reopen(kvConfig())
			rows := lab.mustRun("MATCH (g:Group {id:'g'}) RETURN g.reviewed", nil)
			require.Equal(t, false, rows[0][0])
			lab.requireVectors("Member", members, 1)
			lab.requireVectors("Summary", members, 1)
		})
	}
}

// Deleting embedded nodes journals identities, not images.
func TestKeptVectorsDeletes(t *testing.T) {
	for _, explicit := range kvForms() {
		t.Run(kvForm(explicit), func(t *testing.T) {
			lab := kvOpen(t, t.TempDir(), kvConfig())
			lab.mustRun("CREATE (:Large {id:'large', text:$text})<-[:REFERS]-(:Peer {id:'peer'})", map[string]any{"text": strings.Repeat("x", 1300000)})
			lab.embed("Large", 1800)
			_, err := lab.run("MATCH (s:Large {id:'large'}) DETACH DELETE s", nil, explicit)
			require.NoError(t, err, "one large node")
			// 600 nodes x 4 chunks: 19.7 MB of vectors in one transaction.
			lab.mustRun("UNWIND range(1,600) AS i CREATE (:Many {id:i, text:'synthetic'})", nil)
			lab.embed("Many", 4)
			_, err = lab.run("MATCH (n:Many) DETACH DELETE n", nil, explicit)
			require.NoError(t, err, "600 embedded nodes")
			lab = lab.reopen(kvConfig())
			rows := lab.mustRun("OPTIONAL MATCH (s:Large) WITH count(s) AS large OPTIONAL MATCH (n:Many) RETURN large, count(n)", nil)
			require.Equal(t, []any{int64(0), int64(0)}, rows[0])
		})
	}
}

// The embedding worker's own vectors: after a text change on a node already
// over the limit, the worker re-embeds it, and the next one-property change
// must commit and keep the new vectors.
type kvEmbedder struct {
	chunks int
	calls  atomic.Int32
}

func (e *kvEmbedder) Embed(context.Context, string) ([]float32, error) {
	v := make([]float32, kvDims)
	v[0] = 1
	return v, nil
}

func (e *kvEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i], _ = e.Embed(ctx, texts[i])
	}
	return out, nil
}

func (e *kvEmbedder) ChunkText(text string, _, _ int) ([]string, error) { return []string{text}, nil }
func (e *kvEmbedder) Dimensions() int                                   { return kvDims }
func (e *kvEmbedder) Model() string                                     { return "kept-vectors-stub" }
func (e *kvEmbedder) Backend() string                                   { return "cpu" }

func (e *kvEmbedder) EmbedDocumentChunks(context.Context, string, int, int) (*embed.DocumentChunkResult, error) {
	call := int(e.calls.Add(1))
	result := &embed.DocumentChunkResult{Chunks: make([]string, e.chunks), Embeddings: kvVectors(e.chunks, call*7), Model: e.Model()}
	for i := range result.Chunks {
		result.Chunks[i] = "synthetic chunk"
	}
	return result, nil
}

var _ embed.Embedder = (*kvEmbedder)(nil)

func kvWorkerConfig() *nornicdb.Config {
	c := kvConfig()
	c.Memory.EmbeddingEnabled = true
	c.EmbeddingWorker.NumWorkers = 1
	c.EmbeddingWorker.MaxRetries = 1
	c.EmbeddingWorker.BatchDelay = 0
	c.EmbeddingWorker.TriggerDebounceDelay = 0
	return c
}

// awaitEmbedding starts the worker and waits until the node holds a full set
// of provider vectors other than previous; it returns their print.
func (l *kvLab) awaitEmbedding(provider *kvEmbedder, previous string) string {
	l.t.Helper()
	l.db.SetEmbedder(provider)
	_, err := l.db.EnsureSearchIndexes(context.Background(), "nornic", nil, nornicdb.SearchIndexBuildWait)
	require.NoError(l.t, err)
	deadline := time.Now().Add(3 * time.Minute)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_ = l.db.WaitForEmbeddings(ctx)
		cancel()
		nodes := l.nodesByLabel("Large")
		require.Len(l.t, nodes, 1)
		got := kvVectorPrint(nodes[0].ChunkEmbeddings)
		if len(nodes[0].ChunkEmbeddings) == provider.chunks && got != previous {
			return got
		}
		require.True(l.t, time.Now().Before(deadline), "the worker stored no new vectors; node has %s", got)
		time.Sleep(500 * time.Millisecond)
	}
}

func TestKeptVectorsAfterReembedding(t *testing.T) {
	const chunks = 1800
	provider := &kvEmbedder{chunks: chunks}
	lab := kvOpen(t, t.TempDir(), kvWorkerConfig())
	lab.mustRun("CREATE (:Large {id:'large', text:$text})", map[string]any{"text": strings.Repeat("x", 1300000)})
	first := lab.awaitEmbedding(provider, "")
	_, err := lab.run("MATCH (s:Large {id:'large'}) SET s.text = $text", map[string]any{"text": strings.Repeat("y", 1300000)}, true)
	require.NoError(t, err, "the text change itself")
	reembedded := lab.awaitEmbedding(provider, first)
	for _, explicit := range kvForms() {
		_, err = lab.run("MATCH (s:Large {id:'large'}) SET s.hidden = $v", map[string]any{"v": explicit}, explicit)
		require.NoError(t, err, "%s one-property change after re-embedding", kvForm(explicit))
		require.Equal(t, reembedded, kvVectorPrint(lab.nodesByLabel("Large")[0].ChunkEmbeddings))
	}
	lab = lab.reopen(kvConfig())
	rows := lab.mustRun("MATCH (s:Large {id:'large'}) RETURN s.hidden, left(s.text, 1)", nil)
	require.Equal(t, []any{false, "y"}, rows[0])
	require.Equal(t, reembedded, kvVectorPrint(lab.nodesByLabel("Large")[0].ChunkEmbeddings))
}
