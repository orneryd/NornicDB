package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// NOR-412: the streaming snapshot (SaveStreamingSnapshot, WriteSnapshot), which
// automatic compaction takes, of stores that hold a node with 18 MiB of text
// and, in two tests, the other entities largeSnapshotStore writes.

// largeSnapshotStore writes a node with 18 MiB of text, a node with a list of
// 2,000,000 int32 values, two nodes with 20,000-character IDs and a
// relationship between them with 16 MiB less 16 KiB of text, and returns them
// as the engine reads them back.
func largeSnapshotStore(t *testing.T, engine Engine) (nodes map[NodeID]*Node, edge *Edge) {
	t.Helper()
	ctx := context.Background()
	list := make([]int32, 2_000_000)
	for i := range list {
		list[i] = int32(7_000_000 + i)
	}
	left, right := NodeID("test:"+strings.Repeat("a", 20_000)), NodeID("test:"+strings.Repeat("z", 20_000))
	for _, node := range []*Node{
		{ID: "test:text", Labels: []string{"Large"}, Properties: map[string]any{"text": strings.Repeat("x", 18<<20)}},
		{ID: "test:list", Labels: []string{"Large"}, Properties: map[string]any{"data": list}},
		{ID: left, Labels: []string{"End"}},
		{ID: right, Labels: []string{"End"}},
	} {
		_, err := engine.CreateNode(ctx, node)
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(ctx, &Edge{ID: "test:long", StartNode: left, EndNode: right, Type: "R", Properties: map[string]any{"text": strings.Repeat("y", (16<<20)-(16<<10))}}))
	nodes = make(map[NodeID]*Node)
	for _, id := range []NodeID{"test:text", "test:list", left, right} {
		node, err := engine.GetNode(ctx, id)
		require.NoError(t, err)
		nodes[id] = node
	}
	edge, err := engine.GetEdge(ctx, "test:long")
	require.NoError(t, err)
	return nodes, edge
}

// requireLargeSnapshotStore compares what engine holds with what
// largeSnapshotStore read back: each node's labels and properties, and the
// relationship's end points, type and properties (strings and int64 lists value
// for value).
func requireLargeSnapshotStore(t *testing.T, engine Engine, nodes map[NodeID]*Node, edge *Edge) {
	t.Helper()
	ctx := context.Background()
	for id, want := range nodes {
		got, err := engine.GetNode(ctx, id)
		require.NoError(t, err)
		require.Equal(t, want.Labels, got.Labels)
		require.True(t, propertiesEqual(want.Properties, got.Properties), "properties of node %.40s", id)
	}
	got, err := engine.GetEdge(ctx, edge.ID)
	require.NoError(t, err)
	require.Equal(t, edge.StartNode, got.StartNode)
	require.Equal(t, edge.EndNode, got.EndNode)
	require.Equal(t, edge.Type, got.Type)
	require.True(t, propertiesEqual(edge.Properties, got.Properties), "properties of the relationship")
}

// propertiesEqual reports whether two property maps have the same number of keys and, for each key of the first, an equal value in
// the second: strings and []int64 lists element by element, other values with ==. It returns a bool and prints no values.
func propertiesEqual(want, got map[string]any) bool {
	if len(want) != len(got) {
		return false
	}
	for name, value := range want {
		switch typed := value.(type) {
		case string:
			if other, ok := got[name].(string); !ok || other != typed {
				return false
			}
		case []int64:
			other, ok := got[name].([]int64)
			if !ok || len(other) != len(typed) {
				return false
			}
			for i := range typed {
				if typed[i] != other[i] {
					return false
				}
			}
		default:
			if got[name] != value {
				return false
			}
		}
	}
	return true
}

func newSnapshotBadger(t *testing.T) *BadgerEngine {
	t.Helper()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

// compactionLab is a store behind a journal that writes large records, with
// automatic compaction enabled on a snapshot directory and a snapshot interval
// of one hour: the tests call the compaction themselves.
func compactionLab(t *testing.T) (engine *WALEngine, walDir, snapshots string) {
	t.Helper()
	root := t.TempDir()
	t.Cleanup(config.WithWALEnabled())
	walDir, snapshots = filepath.Join(root, "wal"), filepath.Join(root, "snapshots")
	wal, err := NewWAL("", &WALConfig{Dir: walDir, SyncMode: "immediate", SnapshotInterval: time.Hour, RecordPolicy: WALRecordsLarge})
	require.NoError(t, err)
	engine = NewWALEngine(newSnapshotBadger(t), wal)
	t.Cleanup(func() { _ = engine.Close() })
	require.NoError(t, engine.EnableAutoCompaction(snapshots))
	return engine, walDir, snapshots
}

// SaveStreamingSnapshot writes the snapshot of largeSnapshotStore. ReadSnapshot
// returns its sequence and counts (4 nodes, 1 relationship), LoadSnapshot loads
// 4 nodes and 1 relationship, and a store rebuilt from it with
// RecoverIntoEngine holds the same labels and properties. WriteSnapshot given
// MaxRecordBytes of 16 MiB refuses the same store with "exceeds limit
// 16777216".
func TestStreamingSnapshotTakesRecordsOver16MiB(t *testing.T) {
	ctx := context.Background()
	source := newSnapshotBadger(t)
	nodes, edge := largeSnapshotStore(t, source)
	path := filepath.Join(t.TempDir(), "large.nds")
	require.NoError(t, SaveStreamingSnapshot(ctx, source, path, SnapshotOptions{Sequence: 9}), "the streaming snapshot records entities over 16 MiB")

	// Three readers: ReadSnapshot, LoadSnapshot and RecoverIntoEngine.
	file, err := os.Open(path)
	require.NoError(t, err)
	metadata, err := ReadSnapshot(ctx, file, &collectingSnapshotVisitor{})
	require.NoError(t, file.Close())
	require.NoError(t, err)
	require.Equal(t, uint64(9), metadata.Sequence)
	require.Equal(t, uint64(4), metadata.NodeCount)
	require.Equal(t, uint64(1), metadata.EdgeCount)
	loaded, err := LoadSnapshot(ctx, path)
	require.NoError(t, err)
	require.Len(t, loaded.Nodes, 4)
	require.Len(t, loaded.Edges, 1)

	rebuilt := newSnapshotBadger(t)
	_, _, err = RecoverIntoEngine(ctx, rebuilt, t.TempDir(), path)
	require.NoError(t, err)
	requireLargeSnapshotStore(t, rebuilt, nodes, edge)

	// A writer given MaxRecordBytes of 16 MiB refuses this store.
	var bounded bytes.Buffer
	err = WriteSnapshot(ctx, source, &bounded, SnapshotOptions{MaxRecordBytes: defaultSnapshotRecordSize})
	require.ErrorContains(t, err, "exceeds limit 16777216")
}

// createSnapshotAndCompact, the function automatic compaction runs, called
// directly on such a store: one snapshot file is written and the journal has
// fewer entries afterwards. A node of 17 MiB is then created, and a store
// rebuilt with RecoverIntoEngine from the snapshot and the journal holds the
// same labels and properties, that node included.
func TestCompactionSnapshotsEntitiesOver16MiB(t *testing.T) {
	ctx := context.Background()
	engine, walDir, snapshots := compactionLab(t)
	nodes, edge := largeSnapshotStore(t, engine)
	before, err := ReadWALEntriesFromDir(ctx, walDir)
	require.NoError(t, err)
	require.NotEmpty(t, before)

	require.NoError(t, engine.createSnapshotAndCompact(engine.work.Context()), "compaction snapshots a store holding entities over 16 MiB")
	files, err := filepath.Glob(filepath.Join(snapshots, "snapshot-*"))
	require.NoError(t, err)
	require.Len(t, files, 1, "one snapshot")
	after, err := ReadWALEntriesFromDir(ctx, walDir)
	require.NoError(t, err)
	require.Less(t, len(after), len(before), "the journal has fewer entries after the compaction")

	// A node written after the snapshot.
	_, err = engine.CreateNode(ctx, &Node{ID: "test:later", Labels: []string{"Large"}, Properties: map[string]any{"text": strings.Repeat("l", 17<<20)}})
	require.NoError(t, err)
	later, err := engine.GetNode(ctx, "test:later")
	require.NoError(t, err)
	nodes["test:later"] = later
	require.NoError(t, engine.wal.Sync())

	rebuilt := newSnapshotBadger(t)
	_, _, err = RecoverIntoEngine(ctx, rebuilt, walDir, files[0])
	require.NoError(t, err)
	requireLargeSnapshotStore(t, rebuilt, nodes, edge)
}

// createSnapshotAndCompact with a cancelled context returns an error and leaves
// one file in the snapshot directory, with the previous snapshot's bytes, and
// the journal with the same number of entries. A store rebuilt from that
// snapshot and the journal has both 18 MiB nodes (the length of their text is
// checked).
func TestCancelledLargeSnapshotKeepsThePreviousSnapshotAndTheJournal(t *testing.T) {
	ctx := context.Background()
	engine, walDir, snapshots := compactionLab(t)
	_, err := engine.CreateNode(ctx, &Node{ID: "test:first", Properties: map[string]any{"text": strings.Repeat("x", 18<<20)}})
	require.NoError(t, err)
	require.NoError(t, engine.createSnapshotAndCompact(engine.work.Context()))
	files, err := filepath.Glob(filepath.Join(snapshots, "snapshot-*"))
	require.NoError(t, err)
	require.Len(t, files, 1)
	previous, err := os.ReadFile(files[0])
	require.NoError(t, err)

	_, err = engine.CreateNode(ctx, &Node{ID: "test:second", Properties: map[string]any{"text": strings.Repeat("y", 18<<20)}})
	require.NoError(t, err)
	journal, err := ReadWALEntriesFromDir(ctx, walDir)
	require.NoError(t, err)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.Error(t, engine.createSnapshotAndCompact(cancelled), "a cancelled snapshot fails")

	files, err = filepath.Glob(filepath.Join(snapshots, "*"))
	require.NoError(t, err)
	require.Len(t, files, 1, "no partial snapshot is left beside the previous one")
	kept, err := os.ReadFile(files[0])
	require.NoError(t, err)
	require.True(t, bytes.Equal(previous, kept), "the previous snapshot is untouched")
	unchanged, err := ReadWALEntriesFromDir(ctx, walDir)
	require.NoError(t, err)
	require.Len(t, unchanged, len(journal), "the journal has the same number of entries")

	require.NoError(t, engine.wal.Sync())
	rebuilt := newSnapshotBadger(t)
	_, _, err = RecoverIntoEngine(ctx, rebuilt, walDir, files[0])
	require.NoError(t, err)
	for _, id := range []NodeID{"test:first", "test:second"} {
		node, err := rebuilt.GetNode(ctx, id)
		require.NoError(t, err)
		require.Len(t, node.Properties["text"], 18<<20)
	}
}

// ReadSnapshot on a snapshot that holds a record over 16 MiB: a file cut in
// half and a file without its last byte return an error, and a flipped byte in
// the middle returns "checksum mismatch". A forged record header that claims 4
// GiB returns an error, and no single read asks the source for more than 64
// KiB.
func TestLargeSnapshotRecordDamageIsRefused(t *testing.T) {
	ctx := context.Background()
	source := newSnapshotBadger(t)
	_, err := source.CreateNode(ctx, &Node{ID: "test:text", Properties: map[string]any{"text": strings.Repeat("x", 18<<20)}})
	require.NoError(t, err)
	var whole bytes.Buffer
	require.NoError(t, WriteSnapshot(ctx, source, &whole, SnapshotOptions{}))
	raw := whole.Bytes()
	_, err = ReadSnapshot(ctx, bytes.NewReader(raw), &collectingSnapshotVisitor{})
	require.NoError(t, err)

	for name, damaged := range map[string][]byte{
		"cut inside the large payload": raw[:len(raw)/2],
		"missing footer":               raw[:len(raw)-1],
	} {
		_, err := ReadSnapshot(ctx, bytes.NewReader(damaged), &collectingSnapshotVisitor{})
		require.Error(t, err, name)
	}
	flipped := bytes.Clone(raw)
	flipped[len(flipped)/2] ^= 0xff
	_, err = ReadSnapshot(ctx, bytes.NewReader(flipped), &collectingSnapshotVisitor{})
	require.ErrorContains(t, err, "checksum mismatch")

	var forged bytes.Buffer
	forged.Write(raw[:len(streamSnapshotMagic)+1+8+8])
	forged.WriteByte(streamSnapshotNode)
	require.NoError(t, binary.Write(&forged, binary.LittleEndian, uint32(materializedSnapshotRecordSize)))
	reader := &recordingSnapshotReader{Reader: bytes.NewReader(forged.Bytes())}
	_, err = ReadSnapshot(ctx, reader, &collectingSnapshotVisitor{})
	require.Error(t, err)
	require.LessOrEqual(t, reader.largestRead, 64<<10, "a claimed 4 GiB length is read in pieces of at most 64 KiB")
}
