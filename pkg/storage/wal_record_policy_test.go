package storage

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// NOR-412: the journal record policy. Every WAL and engine writes standard
// records unless its options ask for large ones; the value is fixed at
// construction; it decides what each write path puts on disk; and a runtime
// that writes standard records still reads, replays and keeps the large
// records a single-node server left in its data directory.

// Every constructor that is not given a policy gets the standard one.
func TestWALRecordPolicyDefaultsToStandard(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for name, cfg := range map[string]*WALConfig{"nil config": nil, "DefaultWALConfig": DefaultWALConfig(), "zero config": {}} {
		w, err := NewWAL(t.TempDir(), cfg)
		require.NoError(t, err, name)
		t.Cleanup(func() { _ = w.Close() })
		require.Equal(t, WALRecordsStandard, w.recordPolicy, name)
	}
	persistent, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	inMemory, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	withOptions, err := fixtureowner.OwnResult[*MemoryEngine](t, ErrStorageClosed)(NewMemoryEngineWithOptions(BadgerOptions{}))
	require.NoError(t, err)
	for name, engine := range map[string]*BadgerEngine{
		"NewBadgerEngine": persistent, "NewBadgerEngineInMemory": inMemory,
		"NewMemoryEngine": newTestMemoryEngine(t).BadgerEngine, "NewMemoryEngineWithOptions": withOptions.BadgerEngine,
	} {
		require.Equal(t, WALRecordsStandard, engine.walRecordPolicy, name)
	}
	require.Equal(t, WALRecordPolicy(0), WALRecordsStandard, "the zero value is the standard policy")
}

// The WAL writes under the policy it was constructed with, whatever happens
// to its configuration afterwards.
func TestWALRecordPolicyIsFixedAtConstruction(t *testing.T) {
	large := walEntityCommitData{Payload: standardRecordsBytes(int(walMaxEntrySize) + 1024)}
	standard := standardRecordsWAL(t, t.TempDir())
	standard.Config().RecordPolicy = WALRecordsLarge
	_, err := standard.AppendWithDatabaseReturningSeq(OpEntityCommit, large, "test")
	require.EqualError(t, err, "wal: failed to serialize entry: "+standardRecordsFrameError)

	spanning := newSpannedTestWAL(t, t.TempDir(), nil)
	t.Cleanup(func() { _ = spanning.Close() })
	spanning.Config().RecordPolicy = WALRecordsStandard
	_, err = spanning.AppendWithDatabaseReturningSeq(OpEntityCommit, large, "test")
	require.NoError(t, err)
}

// A record that fits one frame costs the same under both policies: the large
// policy adds no allocation to it.
func TestFittingRecordAllocatesTheSameUnderBothPolicies(t *testing.T) {
	if policyTestRaceDetector {
		t.Skip("the race detector adds its own allocations")
	}
	t.Cleanup(config.WithWALEnabled())
	data := walEntityCommitData{Payload: standardRecordsBytes(4096)}
	allocations := map[WALRecordPolicy]float64{}
	for _, policy := range []WALRecordPolicy{WALRecordsStandard, WALRecordsLarge} {
		cfg := DefaultWALConfig()
		cfg.SyncMode, cfg.BatchSyncInterval, cfg.RecordPolicy = "none", 0, policy
		w, err := NewWAL(t.TempDir(), cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = w.Close() })
		allocations[policy] = testing.AllocsPerRun(500, func() {
			if _, err := w.AppendWithDatabaseReturningSeq(OpEntityCommit, data, "test"); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Logf("allocations per fitting append: standard %v, large %v", allocations[WALRecordsStandard], allocations[WALRecordsLarge])
	require.Equal(t, allocations[WALRecordsStandard], allocations[WALRecordsLarge])
}

// policyTestWriter returns a function that writes one record through path:
// one that fits a frame, or one that does not.
func policyTestWriter(t *testing.T, w *WAL, path string) func(large bool) error {
	t.Helper()
	data := func(large bool) walEntityCommitData {
		if large {
			return walEntityCommitData{Payload: standardRecordsBytes(int(walMaxEntrySize) + 1024)}
		}
		return walEntityCommitData{Payload: []byte("fits")}
	}
	switch path {
	case "append":
		t.Cleanup(func() { _ = w.Close() })
		return func(large bool) error {
			_, err := w.AppendWithDatabaseReturningSeq(OpEntityCommit, data(large), "test")
			return err
		}
	case "batch":
		t.Cleanup(func() { _ = w.Close() })
		return func(large bool) error {
			batch := w.NewBatch()
			if err := batch.append(OpEntityCommit, data(large)); err != nil {
				return err
			}
			return batch.Commit()
		}
	case "buffered":
		t.Cleanup(func() { _ = w.Close() })
		b, _, commit := bindingCostFixture(t, 1)
		return func(large bool) error {
			if large {
				value := make([]byte, int(walMaxEntrySize)+4096)
				_, err := rand.New(rand.NewSource(42)).Read(value)
				require.NoError(t, err)
				commit.writes[string([]byte{prefixNode})+"incompressible"] = value
			}
			return w.AppendBufferedCommit(commit, b.propKeyDict)
		}
	default: // native
		native := openLargeCommitTestEngine(t, t.TempDir())
		fixtureowner.OwnChecked(t, w)
		wrapped := fixtureowner.OwnChecked(t, NewWALEngine(native, w))
		return func(large bool) error {
			nodes, payload := make([]*Node, 2), "small"
			if large {
				nodes, payload = make([]*Node, 512), strings.Repeat("x", 32<<10)
			}
			for i := range nodes {
				nodes[i] = &Node{ID: NodeID(fmt.Sprintf("test:policy-%t-%d", large, i)), Properties: map[string]any{"payload": payload}}
			}
			return wrapped.BulkCreateNodes(t.Context(), nodes)
		}
	}
}

// What each write path puts on disk under each policy. A record that fits
// one frame is an ordinary frame under both. One that does not is refused
// with the frame error under the standard policy and written as a spanned
// record under the large one.
func TestWALFramesWrittenUnderEachPolicy(t *testing.T) {
	for policy, name := range map[WALRecordPolicy]string{WALRecordsStandard: "standard", WALRecordsLarge: "large"} {
		for _, path := range []string{"append", "batch", "buffered", "native"} {
			t.Run(name+"/"+path, func(t *testing.T) {
				t.Cleanup(config.WithWALEnabled())
				dir := t.TempDir()
				cfg := DefaultWALConfig()
				cfg.SyncMode, cfg.RecordPolicy = "immediate", policy
				w, err := NewWAL(dir, cfg)
				require.NoError(t, err)
				write := policyTestWriter(t, w, path)

				require.NoError(t, write(false))
				require.NoError(t, w.Sync())
				fitting := standardRecordsFrames(t, walActivePath(dir))
				require.NotEmpty(t, fitting)
				for _, frame := range fitting {
					require.Equal(t, walFormatVersionCurrent, frame.Version, "a record that fits is an ordinary frame")
				}

				err = write(true)
				require.NoError(t, w.Sync())
				parts := 0
				for _, frame := range standardRecordsFrames(t, walActivePath(dir)) {
					if frame.Version == walFormatVersionSpanned {
						parts++
						require.Equal(t, string(OpRecordPart), frame.Op)
					}
				}
				if policy == WALRecordsStandard {
					require.ErrorContains(t, err, standardRecordsFrameError)
					require.Zero(t, parts, "a standard WAL never writes a part")
					return
				}
				require.NoError(t, err)
				require.GreaterOrEqual(t, parts, 2, "the large record spans frames")
			})
		}
	}
}

// What an engine journals for a node update that keeps the node's vectors,
// through a transaction's prepared commit and through a storage-accessor
// update published natively. Under the standard policy both records are
// version 3 and carry the whole image, managed and named vectors included.
// Under the large policy the transaction's record is version 5, omits the
// kept vectors and marks them; an accessor update supplies only a new image
// and is journaled whole under either policy.
func TestEngineJournalsKeptVectorsByRecordPolicy(t *testing.T) {
	flag := func(n *Node) { n.Properties["flag"] = true }
	for policy, name := range map[WALRecordPolicy]string{WALRecordsStandard: "standard", WALRecordsLarge: "large"} {
		for _, path := range []string{"prepared", "accessor"} {
			t.Run(name+"/"+path, func(t *testing.T) {
				dir := t.TempDir()
				b, w := keptVectorsEngineWith(t, dir, policy)
				keptVectorsSeed(t, b, "test:managed", "alpha", 8, 1)
				_, err := b.CreateNode(t.Context(), &Node{ID: "test:named", Labels: []string{"Doc"}, Properties: map[string]any{"text": "gamma"}, NamedEmbeddings: map[string][]float32{"title": {1, 2, 3}}})
				require.NoError(t, err)

				var payloads [][]byte
				if path == "prepared" {
					payloads = append(payloads, keptVectorsPrepare(t, b, "policy", map[NodeID]func(*Node){"test:managed": flag, "test:named": flag}))
				} else {
					wrapped := NewWALEngine(b, w)
					after := w.Sequence()
					for _, id := range []NodeID{"test:managed", "test:named"} {
						node, err := b.GetNode(t.Context(), id)
						require.NoError(t, err)
						node = CopyNode(node)
						flag(node)
						require.NoError(t, wrapped.UpdateNode(t.Context(), NodeUpdateRequest{After: node}))
					}
					require.NoError(t, w.Sync())
					entries, err := ReadWALEntriesFromDir(t.Context(), filepath.Join(dir, "wal"))
					require.NoError(t, err)
					for _, entry := range entries {
						if entry.Sequence > after && entry.Operation == OpEntityCommit {
							var data walEntityCommitData
							require.NoError(t, decodeRecoveryPayload(entry.Data, &data))
							payloads = append(payloads, data.Payload)
						}
					}
					require.Len(t, payloads, 2, "one journal record per accessor update")
				}

				images, marks, versions := map[NodeID]*Node{}, map[NodeID]vectorRetention{}, map[NodeID]uint16{}
				for _, payload := range payloads {
					envelope, ops, err := DecodeEntityTransaction(payload)
					require.NoError(t, err)
					for _, op := range ops {
						images[op.NodeID], marks[op.NodeID], versions[op.NodeID] = op.Node, op.retainedVectors, envelope.Version
					}
				}
				require.Len(t, images, 2)
				if policy == WALRecordsStandard || path == "accessor" {
					require.Equal(t, map[NodeID]uint16{"test:managed": 3, "test:named": 3}, versions)
					require.Equal(t, map[NodeID]vectorRetention{"test:managed": 0, "test:named": 0}, marks)
					require.Equal(t, keptVectors(8, 1), images["test:managed"].ChunkEmbeddings, "the record carries the managed vectors")
					require.Equal(t, true, images["test:managed"].EmbedMeta["has_embedding"])
					require.Equal(t, map[string][]float32{"title": {1, 2, 3}}, images["test:named"].NamedEmbeddings, "the record carries the named vectors")
					return
				}
				require.Equal(t, map[NodeID]uint16{"test:managed": 5, "test:named": 5}, versions)
				require.Equal(t, map[NodeID]vectorRetention{"test:managed": retainedManagedEmbeddings, "test:named": retainedNamedEmbeddings}, marks)
				require.Empty(t, images["test:managed"].ChunkEmbeddings)
				require.Empty(t, images["test:named"].NamedEmbeddings)
			})
		}
	}
}

func policyTestParts(t *testing.T, path string, seq uint64) int {
	t.Helper()
	parts := 0
	for _, frame := range standardRecordsFrames(t, path) {
		if frame.Version == walFormatVersionSpanned {
			require.Equal(t, seq, frame.Seq)
			parts++
		}
	}
	return parts
}

// A data directory a single-node server wrote holds a record that omits kept
// vectors and a spanned record. A runtime that writes standard records opens
// it, replays both, writes its own records in the standard form, and its
// maintenance keeps the stored records as they are: a snapshot rewrite
// retains the spanned record spanned, segment retention and sealing keep it,
// and the format upgrade leaves it alone.
func TestStandardRuntimeKeepsStoredLargeRecords(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	b, w := keptVectorsEngineWith(t, dir, WALRecordsLarge)
	keptVectorsSeed(t, b, "test:keep", "alpha", 8, 1)
	marked := keptVectorsPrepare(t, b, "marked", map[NodeID]func(*Node){"test:keep": func(n *Node) { n.Properties["flag"] = true }})
	envelope, _, err := DecodeEntityTransaction(marked)
	require.NoError(t, err)
	require.Equal(t, uint16(5), envelope.Version)
	keptVectorsJournal(t, w, marked)
	spanned, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "spanned", Operations: spannedTestOperations(600, 40<<10), Mode: CommitModeExplicit})
	require.NoError(t, err)
	require.Greater(t, len(spanned), int(walMaxEntrySize))
	keptVectorsJournal(t, w, spanned)
	spannedSeq := w.Sequence()
	crashClose(t, b, w)
	parts := policyTestParts(t, walActivePath(walDir), spannedSeq)
	require.GreaterOrEqual(t, parts, 2)

	// The same directory under the standard policy: both records replay.
	b, w = keptVectorsEngineWith(t, dir, WALRecordsStandard)
	replayed, err := RecoverBufferedCommitsAtOpen(b, w)
	require.NoError(t, err)
	require.Equal(t, 2, replayed)
	got := requireKeptVectors(t, b, "test:keep", 8, 1)
	require.Equal(t, true, got.Properties["flag"], "the marked update applied with its kept vectors")
	last, err := b.GetNode(t.Context(), "test:large-599")
	require.NoError(t, err)
	require.Equal(t, int64(599), last.Properties["i"], "the spanned transaction applied")

	// Its own records are standard: whole images, one frame or refused.
	update := keptVectorsPrepare(t, b, "standard-update", map[NodeID]func(*Node){"test:keep": func(n *Node) { n.Properties["a"] = int64(2) }})
	envelope, ops, err := DecodeEntityTransaction(update)
	require.NoError(t, err)
	require.Equal(t, uint16(3), envelope.Version)
	require.Equal(t, keptVectors(8, 1), ops[0].Node.ChunkEmbeddings)
	_, err = w.AppendWithDatabaseReturningSeq(OpEntityCommit, walEntityCommitData{Payload: standardRecordsBytes(int(walMaxEntrySize) + 1024)}, "test")
	require.EqualError(t, err, "wal: failed to serialize entry: "+standardRecordsFrameError)
	keptVectorsJournal(t, w, []byte("standard-after"))

	requireStored := func() {
		t.Helper()
		entries, err := ReadWALEntriesFromDir(t.Context(), walDir)
		require.NoError(t, err)
		var commits []WALEntry
		for _, entry := range entries {
			if entry.Operation == OpEntityCommit && entry.Sequence >= spannedSeq {
				commits = append(commits, entry)
			}
		}
		require.Equal(t, [][]byte{spanned, []byte("standard-after")}, spannedTestPayloads(t, commits))
	}

	// A snapshot that covers everything before the spanned record: the
	// rewrite keeps it, still spanned.
	require.NoError(t, w.ManageSnapshot(t.Context(), spannedSeq-1, WALSnapshotTruncate))
	require.NoError(t, w.Sync())
	require.Equal(t, parts, policyTestParts(t, walActivePath(walDir), spannedSeq), "the retained record is rewritten in its stored form")
	requireStored()
	crashClose(t, b, w)

	// With retained segments the truncation rotates, retains and seals.
	cfg := DefaultWALConfig()
	cfg.SyncMode, cfg.RetentionMaxSegments = "immediate", 8
	segmented, err := NewWAL(walDir, cfg)
	require.NoError(t, err)
	require.NoError(t, segmented.ManageSnapshot(t.Context(), spannedSeq-1, WALSnapshotTruncate))
	require.NoError(t, segmented.Close())
	requireStored()
	manifest, err := accessWALManifest(walDir, walReadManifest, nil)
	require.NoError(t, err)
	sealed := 0
	for _, segment := range manifest.Segments {
		path, err := resolveWALSegmentPath(walDir, segment.Path)
		require.NoError(t, err)
		sealed += policyTestParts(t, path, spannedSeq)
	}
	require.Equal(t, parts, sealed, "the sealed segment holds the spanned record's parts")

	// The historical-format upgrade treats both forms as current.
	_, err = UpgradeWALV4(walDir, WALV4UpgradeOptions{})
	require.NoError(t, err)
	requireStored()
}
