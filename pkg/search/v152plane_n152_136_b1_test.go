package search

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-390 corruption-policy reproduction for the public search/fulltext build.
//
// A stored record the storage engine cannot decode must fail BuildIndexes, not
// produce an index over the rows that happened to decode. The fixture builds a
// real on-disk Badger store through the public engine, closes it, replaces one
// stored record with undecodable bytes, and reopens it. Each case has a
// valid-record control over the same fixture: the control builds, becomes
// ready, serves the rows and publishes its index, so the corrupt case's
// failure is the corruption, not the fixture.
//
// The record layout is the one documented at pkg/storage/badger.go
// (prefixNode 0x01, prefixEdge 0x02, prefixEmbedding 0x08). The assumption is
// checked, not trusted: after the offline write the reopened engine must fail
// the public read that reaches that record, so a future layout change fails
// this reproduction visibly instead of passing vacuously.

const (
	nor390NodeBodyPrefix  = byte(0x01) // pkg/storage/badger.go: nodes:nodeID -> Node
	nor390EdgeBodyPrefix  = byte(0x02) // pkg/storage/badger.go: edges:edgeID -> Edge
	nor390EmbeddingPrefix = byte(0x08) // pkg/storage/badger.go: embedding records (chunks and sidecar metadata)
)

var (
	nor390Alpha = storage.NodeID("nornic:alpha")
	nor390Bravo = storage.NodeID("nornic:bravo")
	nor390Link  = storage.EdgeID("nornic:link")
)

// nor390CreateClosedStore writes the fixture (two full-text nodes, one of them
// with a chunk vector in the dedicated embedding key space, and one
// relationship) through the public engine API, closes it, and returns the
// directory so a test can corrupt one record offline before reopening.
func nor390CreateClosedStore(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "graph")
	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	require.NoError(t, err)
	fixtureowner.Own(t, engine)
	nodes := map[storage.NodeID]string{nor390Alpha: "alpha machine learning", nor390Bravo: "bravo database"}
	for id, text := range nodes {
		_, err := engine.CreateNode(t.Context(), &storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": text}})
		require.NoError(t, err)
	}
	// The sidecar write is the public owner of the dedicated embedding key
	// space: a small chunk vector stays inline in a node body, so the fixture
	// asks the engine to store it separately, as an embedding writer does.
	require.NoError(t, engine.UpdateNodeEmbeddingSidecar(t.Context(), &storage.Node{
		ID:              nor390Alpha,
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"text": nodes[nor390Alpha]},
		ChunkEmbeddings: [][]float32{{1, 0, 0}},
		EmbedMeta:       map[string]any{"chunk_count": 1},
	}))
	require.NoError(t, engine.CreateEdge(t.Context(), &storage.Edge{ID: nor390Link, StartNode: nor390Alpha, EndNode: nor390Bravo, Type: "LINK"}))
	require.NoError(t, engine.Close())
	return dir
}

// nor390CorruptStoredRecord replaces every stored record whose key is prefix
// followed by the entity's ID (nodes, relationships) or by the node ID and the
// embedding sidecar's 0x00 separator (chunk vectors and their metadata) with
// undecodable bytes in the closed test-owned store.
func nor390CorruptStoredRecord(t *testing.T, dir string, prefix byte, id string) {
	t.Helper()
	raw, err := fixtureowner.TrackResult[*badger.DB](t, nil)(badger.Open(badger.DefaultOptions(dir).WithLogger(nil)))
	require.NoError(t, err)
	defer func() { require.NoError(t, raw.Close()) }()
	var targets [][]byte
	require.NoError(t, raw.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			if len(key) <= 1 || key[0] != prefix {
				continue
			}
			rest := key[1:]
			if !bytes.HasPrefix(rest, []byte(id)) {
				continue
			}
			// The whole record for a node/relationship ID, or an embedding
			// sidecar record (nodeID + 0x00 + chunk index / metadata slot).
			if string(rest) == id || (len(rest) > len(id) && rest[len(id)] == 0x00) {
				targets = append(targets, it.Item().KeyCopy(nil))
			}
		}
		if len(targets) == 0 {
			return os.ErrNotExist
		}
		for _, target := range targets {
			if err := txn.Set(target, []byte{0xff}); err != nil {
				return err
			}
		}
		return nil
	}))
}

// nor390OpenStore reopens the corrupted or control store through the public
// engine constructor.
func nor390OpenStore(t *testing.T, dir string) *storage.BadgerEngine {
	t.Helper()
	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	require.NoError(t, err)
	fixtureowner.Own(t, engine)
	return engine
}

// nor390BuildService returns the public build service over engine, persisting
// its artifacts under dir the same way the repository's search persistence
// fixtures do.
func nor390BuildService(t *testing.T, engine storage.Engine, dir string) *Service {
	t.Helper()
	return newSearchPersistenceTestService(t, engine, dir, false)
}

func TestNOR390SearchBuildFailsOnCorruptStoredNodeRecord(t *testing.T) {
	t.Run("valid control", func(t *testing.T) {
		dir := nor390CreateClosedStore(t)
		engine := nor390OpenStore(t, dir)
		visited := 0
		require.NoError(t, engine.StreamNodesWithOptions(context.Background(), storage.StreamNodesOptions{WithEmbeddings: true}, func(*storage.Node) error {
			visited++
			return nil
		}))
		require.Equal(t, 2, visited, "the build's node read reaches both stored nodes")
		searchDir := filepath.Join(dir, "search")
		svc := nor390BuildService(t, engine, searchDir)

		require.NoError(t, svc.BuildIndexes(context.Background()))
		require.True(t, svc.IsReady(), "a complete valid build publishes readiness")
		require.Equal(t, 2, svc.FulltextDocCount())
		require.FileExists(t, filepath.Join(searchDir, "bm25"), "a complete valid build publishes its index")
		response, err := svc.Search(context.Background(), "alpha", nil, &SearchOptions{Limit: 10})
		require.NoError(t, err)
		require.Len(t, response.Results, 1)
		require.Equal(t, nor390Alpha, response.Results[0].NodeID)
	})

	t.Run("corrupt row", func(t *testing.T) {
		dir := nor390CreateClosedStore(t)
		nor390CorruptStoredRecord(t, dir, nor390EmbeddingPrefix, string(nor390Alpha))
		engine := nor390OpenStore(t, dir)
		// The corruption is reached by the same read the build uses: the
		// node's stored embedding record cannot be decoded.
		err := engine.StreamNodesWithOptions(context.Background(), storage.StreamNodesOptions{WithEmbeddings: true}, func(*storage.Node) error { return nil })
		require.ErrorContains(t, err, "scanning node \"nornic:alpha\"", "the corrupted record must be the row the build reads")
		searchDir := filepath.Join(dir, "search")
		svc := nor390BuildService(t, engine, searchDir)

		err = svc.BuildIndexes(context.Background())
		require.ErrorContains(t, err, "scanning node \"nornic:alpha\"", "the build must report the node it cannot decode")
		require.False(t, svc.IsReady(), "a failed build must not publish readiness")
		require.False(t, svc.BuildInProgress())
		require.Zero(t, svc.FulltextDocCount(), "no document index is installed from a failed population")
		require.NoFileExists(t, filepath.Join(searchDir, "bm25"), "a failed build must not publish a partial index")
	})
}

func TestNOR390SearchBuildFailsOnCorruptStoredEdgeRecord(t *testing.T) {
	t.Run("valid control", func(t *testing.T) {
		dir := nor390CreateClosedStore(t)
		engine := nor390OpenStore(t, dir)
		visited := 0
		require.NoError(t, engine.StreamEdges(context.Background(), func(*storage.Edge) error {
			visited++
			return nil
		}))
		require.Equal(t, 1, visited, "the build's relationship read reaches the stored relationship")
		searchDir := filepath.Join(dir, "search")
		svc := nor390BuildService(t, engine, searchDir)

		require.NoError(t, svc.BuildIndexes(context.Background()))
		require.True(t, svc.IsReady())
		require.Equal(t, 2, svc.FulltextDocCount())
		require.FileExists(t, filepath.Join(searchDir, "bm25"))
	})

	t.Run("corrupt row", func(t *testing.T) {
		dir := nor390CreateClosedStore(t)
		nor390CorruptStoredRecord(t, dir, nor390EdgeBodyPrefix, string(nor390Link))
		engine := nor390OpenStore(t, dir)
		_, err := engine.GetEdge(context.Background(), nor390Link)
		require.ErrorContains(t, err, "0xff", "the corrupted record must be the row the relationship stream reads")
		searchDir := filepath.Join(dir, "search")
		svc := nor390BuildService(t, engine, searchDir)

		err = svc.BuildIndexes(context.Background())
		require.ErrorContains(t, err, "reading relationship \"nornic:link\"", "the relationship stream must report the row it cannot decode")
		require.False(t, svc.IsReady(), "a failed build must not publish readiness")
		require.False(t, svc.BuildInProgress())
		require.NoFileExists(t, filepath.Join(searchDir, "bm25"), "a failed build must not publish a partial index")
	})
}

// TestNOR390CorruptStoredNodeBodyFailsStoreOpen records where a corrupt node
// body is detected in current source: the engine constructor's label-count
// verification decodes the stored bodies, so a corrupt body fails the public
// constructor with the row and the offending byte, before any build exists.
// The node-body BuildIndexes route is therefore unreachable in this process
// shape; TestNOR390SearchBuildFailsOnCorruptStoredNodeRecord covers the
// corrupt node record a running engine's build actually reaches.
func TestNOR390CorruptStoredNodeBodyFailsStoreOpen(t *testing.T) {
	t.Run("valid control", func(t *testing.T) {
		dir := nor390CreateClosedStore(t)
		engine := nor390OpenStore(t, dir)
		_, err := engine.GetNode(context.Background(), nor390Alpha)
		require.NoError(t, err)
	})

	t.Run("corrupt row", func(t *testing.T) {
		dir := nor390CreateClosedStore(t)
		nor390CorruptStoredRecord(t, dir, nor390NodeBodyPrefix, string(nor390Alpha))
		engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
		if engine != nil {
			fixtureowner.Own(t, engine)
		}
		require.ErrorContains(t, err, "failed to initialize label counts")
		require.ErrorContains(t, err, "nornic:alpha")
		require.ErrorContains(t, err, "0xff")
	})
}
