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

// NOR-412: the journal record policy (WALRecordsStandard, WALRecordsLarge), on
// a WAL opened directly. The tests check which policy a WAL opened without one
// has; that a change of the configuration after construction does not change
// what the WAL writes; the frames four write paths put on disk under each
// policy; that a node update's record has the same version and carries the same
// vectors under both; and that a WAL opened with the standard policy on a
// directory that holds a spanned record replays it and keeps it through a
// snapshot rewrite, a truncation with retained segments and the format upgrade.

// NewWAL with a nil configuration, with DefaultWALConfig and with a zero
// configuration opens a WAL with the standard policy, which is the type's zero
// value.
func TestWALRecordPolicyDefaultsToStandard(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for name, cfg := range map[string]*WALConfig{"nil config": nil, "DefaultWALConfig": DefaultWALConfig(), "zero config": {}} {
		w, err := NewWAL(t.TempDir(), cfg)
		require.NoError(t, err, name)
		t.Cleanup(func() { _ = w.Close() })
		require.Equal(t, WALRecordsStandard, w.recordPolicy, name)
	}
	require.Equal(t, WALRecordPolicy(0), WALRecordsStandard, "the zero value is the standard policy")
}

// A WAL keeps the policy it was constructed with when its configuration's
// policy field is changed afterwards: the standard one still refuses a record
// over one frame, the large one still writes it.
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

// Appending a 4 KiB record allocates the same number of times under both
// policies (500 runs each).
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

// What four write paths (append, batch, buffered, native) put on disk under
// each policy. A record that fits one frame is an ordinary frame under both.
// One that does not is refused with the frame error under the standard policy,
// which writes no part frame, and is written as two or more part frames under
// the large one.
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
					require.Zero(t, parts, "the standard WAL wrote no part frame")
					return
				}
				require.NoError(t, err)
				require.GreaterOrEqual(t, parts, 2, "the large record spans frames")
			})
		}
	}
}

// What an engine journals for an update that keeps a node's vectors, through a
// transaction's prepared commit and through the WAL engine's UpdateNode, under
// each policy: the records are version 3, the record of the node with 8 chunk
// vectors carries them value for value, and the record of the node with a named
// vector carries it.
func TestEngineJournalsWholeImagesUnderEachPolicy(t *testing.T) {
	flag := func(n *Node) { n.Properties["flag"] = true }
	for policy, name := range map[WALRecordPolicy]string{WALRecordsStandard: "standard", WALRecordsLarge: "large"} {
		for _, path := range []string{"prepared", "accessor"} {
			t.Run(name+"/"+path, func(t *testing.T) {
				dir := t.TempDir()
				b, w := largeRecordEngine(t, dir, policy)
				largeRecordSeed(t, b, "test:managed", "alpha", 8, 1)
				_, err := b.CreateNode(t.Context(), &Node{ID: "test:named", Labels: []string{"Doc"}, Properties: map[string]any{"text": "gamma"}, NamedEmbeddings: map[string][]float32{"title": {1, 2, 3}}})
				require.NoError(t, err)

				var payloads [][]byte
				if path == "prepared" {
					payloads = append(payloads, largeRecordPrepare(t, b, "policy", map[NodeID]func(*Node){"test:managed": flag, "test:named": flag}))
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

				images, versions := map[NodeID]*Node{}, map[NodeID]uint16{}
				for _, payload := range payloads {
					envelope, ops, err := DecodeEntityTransaction(payload)
					require.NoError(t, err)
					for _, op := range ops {
						images[op.NodeID], versions[op.NodeID] = op.Node, envelope.Version
					}
				}
				require.Len(t, images, 2)
				require.Equal(t, map[NodeID]uint16{"test:managed": 3, "test:named": 3}, versions)
				require.Equal(t, largeRecordVectors(8, 1), images["test:managed"].ChunkEmbeddings, "the record carries the managed vectors")
				require.Equal(t, true, images["test:managed"].EmbedMeta["has_embedding"])
				require.Equal(t, map[string][]float32{"title": {1, 2, 3}}, images["test:named"].NamedEmbeddings, "the record carries the named vectors")
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

// A directory written under the large policy holds a record that fits one frame
// and a spanned record, both journaled without being applied. A WAL and an
// engine opened on it under the standard policy replay both. An update prepared
// there is version 3 with the node's vectors, and a record over one frame is
// refused with the frame error. A snapshot truncation rewrites the spanned
// record with the same number of parts; a truncation with retained segments
// leaves that number of parts in the segment files; UpgradeWALV4 returns no
// error; after each, the journal reads back the spanned record and the record
// written after it.
func TestStandardRuntimeKeepsStoredLargeRecords(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	b, w := largeRecordEngine(t, dir, WALRecordsLarge)
	largeRecordSeed(t, b, "test:keep", "alpha", 8, 1)
	largeRecordJournal(t, w, largeRecordPrepare(t, b, "fitting", map[NodeID]func(*Node){"test:keep": func(n *Node) { n.Properties["flag"] = true }}))
	spanned, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "spanned", Operations: spannedTestOperations(600, 40<<10), Mode: CommitModeExplicit})
	require.NoError(t, err)
	require.Greater(t, len(spanned), int(walMaxEntrySize))
	largeRecordJournal(t, w, spanned)
	spannedSeq := w.Sequence()
	crashClose(t, b, w)
	parts := policyTestParts(t, walActivePath(walDir), spannedSeq)
	require.GreaterOrEqual(t, parts, 2)

	// The same directory under the standard policy: the recovery at open applies both records.
	b, w = largeRecordEngine(t, dir, WALRecordsStandard)
	replayed, err := RecoverBufferedCommitsAtOpen(b, w)
	require.NoError(t, err)
	require.Equal(t, 2, replayed)
	got := requireLargeRecordVectors(t, b, "test:keep", 8, 1)
	require.Equal(t, true, got.Properties["flag"], "the fitting update applied with its vectors")
	last, err := b.GetNode(t.Context(), "test:large-599")
	require.NoError(t, err)
	require.Equal(t, int64(599), last.Properties["i"], "the spanned transaction applied")

	// Its own records: version 3 with the node's vectors; over one frame, refused.
	update := largeRecordPrepare(t, b, "standard-update", map[NodeID]func(*Node){"test:keep": func(n *Node) { n.Properties["a"] = int64(2) }})
	envelope, ops, err := DecodeEntityTransaction(update)
	require.NoError(t, err)
	require.Equal(t, uint16(3), envelope.Version)
	require.Equal(t, largeRecordVectors(8, 1), ops[0].Node.ChunkEmbeddings)
	_, err = w.AppendWithDatabaseReturningSeq(OpEntityCommit, walEntityCommitData{Payload: standardRecordsBytes(int(walMaxEntrySize) + 1024)}, "test")
	require.EqualError(t, err, "wal: failed to serialize entry: "+standardRecordsFrameError)
	largeRecordJournal(t, w, []byte("standard-after"))

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

	// A snapshot sequence just before the spanned record: the rewrite keeps the
	// record in the same number of parts.
	require.NoError(t, w.ManageSnapshot(t.Context(), spannedSeq-1, WALSnapshotTruncate))
	require.NoError(t, w.Sync())
	require.Equal(t, parts, policyTestParts(t, walActivePath(walDir), spannedSeq), "the rewritten file has the same number of parts")
	requireStored()
	crashClose(t, b, w)

	// A truncation with retained segments.
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
	require.Equal(t, parts, sealed, "the segment files hold the spanned record's parts")

	// UpgradeWALV4 returns no error and the records read back as before.
	_, err = UpgradeWALV4(walDir, WALV4UpgradeOptions{})
	require.NoError(t, err)
	requireStored()
}
