package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// The buffered record uses the typed codec, but still keeps dictionary
// bindings in the epoch rather than duplicating them in each physical record.
func TestWALSteadyBufferedPayloadUsesTypedCodec(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, keys := range []int{1, 160} {
		t.Run(fmt.Sprint(keys), func(t *testing.T) {
			b, wal, commit := bindingCostFixture(t, keys)
			for i := 0; i < 3; i++ {
				require.NoError(t, wal.AppendBufferedCommit(commit, b.propKeyDict))
			}
			entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
			require.NoError(t, err)
			require.Len(t, entries, 3)
			for _, entry := range entries {
				payload, err := decompressWALBufferedCommitPayload(entry.Data)
				require.NoError(t, err)
				serializer, _, headered, err := splitSerializationHeader(payload)
				require.NoError(t, err)
				require.True(t, headered)
				require.Equal(t, serializerIDMsgpack, serializer)
				var data WALBufferedCommitData
				require.NoError(t, decodeRecoveryPayload(payload, &data))
				require.Equal(t, commit.txID, data.TxID)
				require.Equal(t, commit.namespace, data.Namespace)
				require.Equal(t, commit.writes, data.Writes)
				require.Empty(t, data.PropertyBindings)
				require.Empty(t, data.PropertyDictionaryID)
			}
		})
	}
}

func TestWALPropertyEpochMidBufferSnapshotAndCrashBoundaries(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, rotate := range []bool{false, true} {
		t.Run(fmt.Sprintf("rotate=%v", rotate), func(t *testing.T) {
			source := reviewBindingEngine(t, t.TempDir())
			baseline := &Snapshot{Nodes: []*Node{{ID: "epoch:a", Properties: map[string]any{"alpha": "before"}}, {ID: "other:boundary"}, {ID: "epoch:b"}}}
			for _, node := range baseline.Nodes {
				_, err := source.CreateNode(node)
				require.NoError(t, err)
			}
			require.NoError(t, source.FlushWriteBehind())
			baselinePath := filepath.Join(t.TempDir(), "before.snapshot")
			require.NoError(t, SaveSnapshot(baseline, baselinePath))
			cfg := DefaultWALConfig()
			cfg.RetentionMaxAge, cfg.RetentionMaxSegments = 0, 0
			if rotate {
				cfg.MaxEntries, cfg.RetentionMaxSegments = 1, 1
			}
			walDir := t.TempDir()
			wal, err := NewWAL(walDir, cfg)
			require.NoError(t, err)
			t.Cleanup(func() { _ = wal.Close() })
			source.SetBufferedCommitWAL(wal)
			update := func(id NodeID, properties map[string]any) {
				tx, err := source.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetNamespace("epoch"))
				require.NoError(t, tx.SetImplicit(true))
				require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: id, Properties: properties}, Policy: ClientNodeEdits}))
				require.NoError(t, tx.Commit())
				require.True(t, tx.AcknowledgedBuffered())
			}
			update("epoch:a", map[string]any{"alpha": "a1"})
			// A new ID is allocated while the earlier acknowledged commit is
			// still buffered, and before the semantic snapshot boundary.
			update("epoch:b", map[string]any{"beta": "b1"})
			entries, err := ReadWALEntriesFromDir(walDir)
			require.NoError(t, err)
			require.Len(t, entries, 2)
			require.NotNil(t, entries[0].PropertyEpoch)
			require.NotNil(t, entries[1].PropertyEpoch)
			require.NotEqual(t, entries[0].PropertyEpoch.state.ID, entries[1].PropertyEpoch.state.ID)
			if !rotate {
				bytes, err := os.ReadFile(filepath.Join(walDir, "wal.log"))
				require.NoError(t, err)
				applied := 0
				for start := 0; start < len(bytes); {
					size := int(binary.LittleEndian.Uint32(bytes[start+5 : start+9]))
					end := start + int(alignUp(int64(9+size+4+8)))
					physical, err := WALEntry{}, error(nil)
					_, err = processWALEntryPayload(walPayloadDecode, nil, &physical, bytes[start+9:start+9+size], bytes[start+4])
					require.NoError(t, err)
					// A torn header/body cannot lend authority to a later body.
					for _, cut := range []int{start + 1, end} {
						want := applied
						if cut == end && physical.Operation == OpBufferedCommit {
							want++
						}
						crashDir := t.TempDir()
						require.NoError(t, os.WriteFile(filepath.Join(crashDir, "wal.log"), bytes[:cut], 0600))
						dest, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir()}))
						require.NoError(t, err)
						result, _, err := RecoverIntoEngine(dest, crashDir, baselinePath)
						require.NoError(t, err)
						require.Zero(t, result.Failed)
						require.Equal(t, want, result.Applied)
						a, err := dest.GetNode("epoch:a")
						require.NoError(t, err)
						if want > 0 {
							require.Equal(t, "a1", a.Properties["alpha"])
						} else {
							require.Equal(t, "before", a.Properties["alpha"])
						}
						b, err := dest.GetNode("epoch:b")
						require.NoError(t, err)
						if want > 1 {
							require.Equal(t, "b1", b.Properties["beta"])
						} else {
							require.Empty(t, b.Properties)
						}
						assertPropertyDictionaryBijection(t, dest)
						require.NoError(t, dest.Close())
					}
					if physical.Operation == OpBufferedCommit {
						applied++
					}
					start = end
				}
			}
			snapshot, err := wal.CreateSnapshot(source)
			require.NoError(t, err)
			byID := map[NodeID]*Node{}
			for _, node := range snapshot.Nodes {
				byID[node.ID] = node
			}
			snapshot.Nodes = []*Node{byID["epoch:b"], byID["other:boundary"], byID["epoch:a"]}
			snapshotPath := filepath.Join(t.TempDir(), "reversed.snapshot")
			require.NoError(t, SaveSnapshot(snapshot, snapshotPath))
			update("epoch:a", map[string]any{"alpha": "a2", "beta": "b2"})
			check := func(alpha, beta string) {
				dest, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir()}))
				require.NoError(t, err)
				result, _, err := RecoverIntoEngine(dest, walDir, snapshotPath)
				require.NoError(t, err)
				require.Zero(t, result.Failed)
				a, err := dest.GetNode("epoch:a")
				require.NoError(t, err)
				require.Equal(t, map[string]any{"alpha": alpha, "beta": beta}, a.Properties)
				b, err := dest.GetNode("epoch:b")
				require.NoError(t, err)
				require.Equal(t, "b1", b.Properties["beta"])
				assertPropertyDictionaryBijection(t, dest)
				require.NoError(t, dest.Close())
			}
			check("a2", "b2") // snapshot published, original WAL still present
			require.NoError(t, wal.ManageSnapshot(snapshot.Sequence, WALSnapshotTruncate))
			check("a2", "b2") // retained entries keep their original epoch
			update("epoch:a", map[string]any{"alpha": "a3", "beta": "b3"})
			check("a3", "b3") // first steady append after truncation
			require.NoError(t, source.FlushWriteBehind())
			assertPropertyDictionaryBijection(t, source)
		})
	}
}

func TestWALPropertyEpochTransportAndRawLegacyBoundary(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source, wal, commit := bindingCostFixture(t, 160)
	require.NoError(t, wal.AppendBufferedCommit(commit, source.propKeyDict))
	entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	raw, err := json.Marshal(entries[0])
	require.NoError(t, err)
	var transported WALEntry
	require.NoError(t, json.Unmarshal(raw, &transported))
	dest, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir()}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dest.Close()) })
	_, err = dest.CreateNode(&Node{ID: "perf:sentinel", Properties: map[string]any{"other": "keep"}})
	require.NoError(t, err)
	require.NoError(t, ReplayWALEntry(dest, transported))
	node, err := dest.GetNode("perf:a")
	require.NoError(t, err)
	require.Len(t, node.Properties, 160)
	for i := 0; i < 160; i++ {
		require.Equal(t, int64(i), node.Properties[fmt.Sprintf("property_%03d", i)])
	}
	assertPropertyDictionaryBijection(t, dest)
	// Dropping transport authority must fail explicitly in a foreign store.
	transported.PropertyEpoch = nil
	require.ErrorContains(t, ReplayWALEntry(dest, transported), "missing property name binding")
	// A generic physical append starts an unbound interval, even if its tokens
	// happen to match the previous writer. A later constructor cannot infer it.
	payload, err := decompressWALBufferedCommitPayload(entries[0].Data)
	require.NoError(t, err)
	var unbound walBufferedCommitPayload
	require.NoError(t, decodeRecoveryPayload(payload, &unbound))
	require.NoError(t, wal.Close())
	reopened, err := NewWAL(wal.Config().Dir, DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	require.NoError(t, reopened.Append(OpBufferedCommit, &unbound))
	entries, err = ReadWALEntriesFromDir(reopened.Config().Dir)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Nil(t, entries[1].PropertyEpoch)
	require.ErrorContains(t, ReplayWALEntry(dest, entries[1]), "missing property name binding")
	// Batch writes share the same unbound boundary owner.
	require.NoError(t, reopened.AppendBufferedCommit(commit, source.propKeyDict))
	batch := reopened.NewBatch()
	require.NoError(t, batch.append(OpBufferedCommit, &unbound))
	require.NoError(t, batch.Commit())
	entries, err = ReadWALEntriesFromDir(reopened.Config().Dir)
	require.NoError(t, err)
	require.Len(t, entries, 4)
	require.NotNil(t, entries[2].PropertyEpoch)
	require.Nil(t, entries[3].PropertyEpoch)
	require.ErrorContains(t, ReplayWALEntry(dest, entries[3]), "missing property name binding")
}

func TestWALPropertyEpochChangesOnlyAtDictionaryBoundaries(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source, wal, commit := bindingCostFixture(t, 160)
	headers := func() int {
		raw, err := os.ReadFile(filepath.Join(wal.Config().Dir, "wal.log"))
		require.NoError(t, err)
		count := 0
		for offset := 0; offset < len(raw); {
			size := int(binary.LittleEndian.Uint32(raw[offset+5 : offset+9]))
			entry, err := WALEntry{}, error(nil)
			_, err = processWALEntryPayload(walPayloadDecode, nil, &entry, raw[offset+9:offset+9+size], raw[offset+4])
			require.NoError(t, err)
			if isWALPropertyEpoch(entry) {
				count++
			}
			offset += int(alignUp(int64(9 + size + 4 + 8)))
		}
		return count
	}
	for i := 0; i < 32; i++ {
		require.NoError(t, wal.AppendBufferedCommit(commit, source.propKeyDict))
	}
	require.Equal(t, 1, headers(), "steady records do not write binding metadata")
	_, err := source.CreateNode(&Node{ID: "perf:new", Properties: map[string]any{"new_name": int64(1)}})
	require.NoError(t, err)
	require.NoError(t, wal.AppendBufferedCommit(commit, source.propKeyDict))
	require.Equal(t, 2, headers())
	require.NoError(t, source.UpdateNode(NodeUpdateRequest{After: &Node{ID: "perf:new", Properties: map[string]any{"new_name": int64(2)}}}))
	require.NoError(t, wal.AppendBufferedCommit(commit, source.propKeyDict))
	require.Equal(t, 2, headers(), "persisting/changing values does not allocate names")
	require.NoError(t, wal.Checkpoint())
	require.Equal(t, 3, headers(), "snapshot boundary publishes a complete binding")
}

func TestWALPropertyEpochMatchingPrefixCannotAuthorizeLegacyAfterReopen(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source, wal, commit := bindingCostFixture(t, 1)
	require.NoError(t, wal.AppendBufferedCommit(commit, source.propKeyDict))
	entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	first := entries[0]
	_, err = source.CreateNode(&Node{ID: "perf:later", Properties: map[string]any{"beta": int64(2)}})
	require.NoError(t, err)
	var body []byte
	require.NoError(t, source.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get(nodeKey("perf:later"))
		if err == nil {
			body, err = item.ValueCopy(nil)
		}
		return err
	}))
	late := &bufferedCommit{txID: "later", namespace: "perf", writes: map[string][]byte{string(nodeKey("perf:later")): body}}
	require.NoError(t, wal.AppendBufferedCommit(late, source.propKeyDict))
	entries, err = ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	require.Len(t, legacyBindingEntries(t, entries[0].PropertyEpoch.state.Bindings), 1, "later headers cannot mutate earlier authority")
	require.Len(t, legacyBindingEntries(t, entries[1].PropertyEpoch.state.Bindings), 2)
	legacy, err := NewWAL(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	payload, err := decompressWALBufferedCommitPayload(entries[1].Data)
	require.NoError(t, err)
	var unbound walBufferedCommitPayload
	require.NoError(t, decodeRecoveryPayload(payload, &unbound))
	require.NoError(t, legacy.Append(OpBufferedCommit, &unbound))
	require.NoError(t, legacy.Close())
	dir := t.TempDir()
	dest := reviewBindingEngine(t, dir)
	_, err = dest.CreateNode(&Node{ID: "perf:seed", Properties: map[string]any{"property_000": int64(0)}})
	require.NoError(t, err)
	_, err = dest.CreateNode(&Node{ID: "perf:later", Properties: map[string]any{"other": int64(0)}})
	require.NoError(t, err)
	require.NoError(t, ReplayWALEntry(dest, first)) // every entry in this epoch matches
	require.NoError(t, dest.Close())
	dest = reviewBindingEngine(t, dir)
	_, err = RecoverBufferedCommitsAtOpen(dest, legacy)
	require.ErrorContains(t, err, "missing property name binding")
	node, err := dest.GetNode("perf:later")
	require.NoError(t, err)
	require.Equal(t, map[string]any{"other": int64(0)}, node.Properties)
	assertPropertyDictionaryBijection(t, dest)
}

func TestWALPropertyEpochOwnDirectoryAllocationBounds(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source, wal, commit := bindingCostFixture(t, 1)
	require.NoError(t, wal.AppendBufferedCommit(commit, source.propKeyDict))
	entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	data, err := decodeBufferedWALRecord(entries[0])
	require.NoError(t, err)
	imported, err := source.importBufferedWALRecord(&data, walOwnDirectoryAtOpen)
	require.NoError(t, err)
	require.Empty(t, imported.propKeyDrain.pending)
	data.PropKeys = nil
	require.Zero(t, testing.AllocsPerRun(32, func() {
		_, err := source.importBufferedWALRecord(&data, walOwnDirectoryAtOpen)
		if err != nil {
			t.Fatal(err)
		}
	}), "known original-store epochs do no binding work per record")
}

func TestWALRejectsLegacyJSONBeforeAppendWithoutChangingIt(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	_, _, commit := bindingCostFixture(t, 1)
	data, err := json.Marshal(WALBufferedCommitData{TxID: "old", Namespace: commit.namespace, Writes: commit.writes})
	require.NoError(t, err)
	old := WALEntry{Sequence: 7, Timestamp: time.Now(), Operation: OpBufferedCommit, Data: data, Checksum: crc32Checksum(data)}
	raw, err := json.Marshal(old)
	require.NoError(t, err)
	dir := t.TempDir()
	oldBytes := append(raw, '\n')
	path := filepath.Join(dir, "wal.log")
	require.NoError(t, os.WriteFile(path, oldBytes, 0600))
	wal, err := NewWAL(dir, DefaultWALConfig())
	require.Nil(t, wal)
	require.ErrorIs(t, err, ErrWALUnsupportedVersion)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, oldBytes, after)
}
