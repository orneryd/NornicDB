package storage

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func legacyBindingEntries(t testing.TB, data []byte) []WALPropKeyEntry {
	t.Helper()
	var out []WALPropKeyEntry
	require.NoError(t, visitWALPropertyBindings(data, nil, func(ns string, id uint64, name string) error {
		out = append(out, WALPropKeyEntry{Namespace: ns, ID: id, Name: name})
		return nil
	}))
	return out
}

func TestWALBindingContextValidation(t *testing.T) {
	for _, data := range [][]byte{nil, {0}, {1, 0x80}, {1, 2, 'a'}, {1, 0, 0x80}, {1, 0, 1, 2, 'a'}} {
		require.Error(t, visitWALPropertyBindings(data, nil, func(string, uint64, string) error { return nil }))
	}
	data := appendWALPropertyBinding([]byte{1}, "ns", 129, "name")
	require.ErrorContains(t, visitWALPropertyBindings(data, nil, func(string, uint64, string) error { return fmt.Errorf("stop") }), "stop")
	_, err := decodeBufferedWALRecord(WALEntry{Operation: OpBufferedCommit, Data: []byte("invalid")})
	require.ErrorContains(t, err, "failed to unmarshal")
	require.NoError(t, beginRecoveryPropertyRestore(newTestMemoryEngine(t)))
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	require.True(t, b.db.oracle.holdReads(time.Second))
	require.Error(t, b.beginSemanticPropertyRestore())
	_, err = b.importBufferedWALRecord(&WALBufferedCommitData{}, walForeignImport)
	require.Error(t, err)
	b.db.oracle.releaseReads()
	require.NoError(t, markRecoveryPropertyForeign(b))
	require.True(t, b.db.oracle.holdReads(time.Second))
	_, err = b.importBufferedWALRecord(&WALBufferedCommitData{PropertyBindings: data}, walForeignImport)
	require.ErrorIs(t, err, ErrStorageRestoring)
	b.db.oracle.releaseReads()
	require.NoError(t, b.Close())
}

func TestWALBindingContextDoesNotGuessFromReopenedDestination(t *testing.T) {
	dir := t.TempDir()
	b := reviewBindingEngine(t, dir)
	_, err := b.CreateNode(&Node{ID: "foreign:a", Properties: map[string]any{"unrelated": "keep"}})
	require.NoError(t, err)
	require.NoError(t, b.Close())
	b = reviewBindingEngine(t, dir)
	body := []byte{nodeFormatTokenizedV1, 3, 1, 1, 0}
	data := &WALBufferedCommitData{Writes: map[string][]byte{string(nodeKey("foreign:a")): body}}
	_, err = b.importBufferedWALRecord(data, walForeignImport)
	require.ErrorContains(t, err, "missing property name binding")
	require.True(t, b.walPropertySource.foreign)
	require.NoError(t, b.Close())
	b = reviewBindingEngine(t, dir)
	_, err = b.importBufferedWALRecord(data, walOwnDirectoryAtOpen)
	require.ErrorContains(t, err, "missing property name binding", "foreign provenance survives reopening")
	assertPropertyDictionaryBijection(t, b)
}

// Every legacy step carries its own typed named property after-image.
// Prefix records and skipped markers supply no cross-record name authority.
func TestWALBindingContextMixedPrefixAndRestart(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, snapshot := range []bool{false, true} {
		name := "markers"
		if snapshot {
			name = "snapshot"
		}
		t.Run(name, func(t *testing.T) {
			source := reviewBindingEngine(t, t.TempDir())
			capture, err := NewWAL(t.TempDir(), DefaultWALConfig())
			require.NoError(t, err)
			t.Cleanup(func() { _ = capture.Close() })
			source.SetBufferedCommitWAL(capture)
			for i := 0; i < 4; i++ {
				tx, err := source.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetNamespace("mixed"))
				require.NoError(t, tx.SetImplicit(true))
				node := &Node{ID: "mixed:a", Properties: map[string]any{"value": int64(i)}}
				if i == 0 {
					_, err = tx.CreateNode(node)
				} else {
					err = tx.UpdateNode(node)
				}
				require.NoError(t, err)
				require.NoError(t, tx.Commit())
				require.NoError(t, source.FlushWriteBehind())
			}
			entries, err := ReadWALEntriesFromDir(capture.Config().Dir)
			require.NoError(t, err)
			wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
			require.NoError(t, err)
			t.Cleanup(func() { _ = wal.Close() })
			for i := range entries {
				data, err := decodeBufferedWALRecord(entries[i])
				require.NoError(t, err)
				if i == 0 {
					data.PropKeys = legacyBindingEntries(t, data.PropertyBindings)
				}
				// Old create, omitted-binding update, complete new update, old update.
				if i != 2 {
					data.PropertyBindings = nil
					data.PropertyDictionaryID = ""
				}
				entries[i].Data = typedPropertyRecordFixture(t, data, map[string]any{"value": int64(i)})
				require.NoError(t, err)
				require.NoError(t, wal.Append(OpBufferedCommit, json.RawMessage(entries[i].Data)))
			}
			require.NoError(t, wal.Close())
			dir := t.TempDir()
			dest := reviewBindingEngine(t, dir)
			if snapshot {
				require.NoError(t, dest.Close())
				dest, err = fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir}))
				require.NoError(t, err)
				t.Cleanup(func() { _ = dest.Close() })
			}
			_, err = dest.CreateNode(&Node{ID: "mixed:sentinel", Properties: map[string]any{"other": "keep"}})
			require.NoError(t, err)
			if snapshot {
				path := filepath.Join(t.TempDir(), "snapshot.bin")
				require.NoError(t, SaveSnapshot(&Snapshot{Sequence: 1, Timestamp: time.Now(), Nodes: []*Node{{ID: "mixed:a", Properties: map[string]any{"value": int64(0)}}}}, path))
				result, _, err := RecoverIntoEngine(dest, wal.Config().Dir, path)
				require.NoError(t, err)
				require.Zero(t, result.Failed, "%s", result.Summary())
				t.Logf("snapshot replay: %s", result.Summary())
				require.Equal(t, 3, result.Applied)
			} else {
				require.NoError(t, ReplayWALEntry(dest, entries[0]))
				require.NoError(t, dest.Close())
				dest = reviewBindingEngine(t, dir)
				n, err := RecoverBufferedCommitsAtOpen(dest, wal)
				require.NoError(t, err)
				require.Equal(t, 3, n)
			}
			node, err := dest.GetNode("mixed:a")
			require.NoError(t, err)
			require.Equal(t, map[string]any{"value": int64(3)}, node.Properties)
			node, err = dest.GetNode("mixed:sentinel")
			require.NoError(t, err)
			require.Equal(t, map[string]any{"other": "keep"}, node.Properties)
			assertPropertyDictionaryBijection(t, dest)
		})
	}
}

func TestWALBindingContextRejectsUnknownSnapshotTokensAfterReopen(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source := reviewBindingEngine(t, t.TempDir())
	_, err := source.CreateNode(&Node{ID: "unknown:a", Properties: map[string]any{"value": int64(0)}})
	require.NoError(t, err)
	capture, err := NewWAL(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = capture.Close() })
	source.SetBufferedCommitWAL(capture)
	tx, err := source.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("unknown"))
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.UpdateNode(&Node{ID: "unknown:a", Properties: map[string]any{"value": int64(1)}}))
	require.NoError(t, tx.Commit())
	entries, err := ReadWALEntriesFromDir(capture.Config().Dir)
	require.NoError(t, err)
	data, err := decodeBufferedWALRecord(entries[0])
	require.NoError(t, err)
	data.PropKeys, data.PropertyBindings = nil, nil
	data.PropertyDictionaryID = ""
	payload, err := json.Marshal(data)
	require.NoError(t, err)
	legacy, err := NewWAL(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	require.NoError(t, legacy.Append(OpBufferedCommit, json.RawMessage(payload)))
	require.NoError(t, legacy.Close())
	dir := t.TempDir()
	dest := reviewBindingEngine(t, dir)
	path := filepath.Join(t.TempDir(), "snapshot.bin")
	require.NoError(t, SaveSnapshot(&Snapshot{Nodes: []*Node{{ID: "unknown:b", Properties: map[string]any{"other": "keep"}}, {ID: "separator:a"}, {ID: "unknown:a", Properties: map[string]any{"value": int64(0)}}}}, path))
	result, _, err := RecoverIntoEngine(dest, legacy.Config().Dir, path)
	require.NoError(t, err)
	require.Equal(t, 1, result.Failed)
	require.ErrorContains(t, result.Errors[0].Error, "missing property name binding")
	require.NoError(t, dest.Close())
	dest = reviewBindingEngine(t, dir)
	n, err := RecoverBufferedCommitsAtOpen(dest, legacy)
	require.ErrorContains(t, err, "missing property name binding")
	require.Zero(t, n)
	node, err := dest.GetNode("unknown:a")
	require.NoError(t, err)
	require.Equal(t, map[string]any{"value": int64(0)}, node.Properties)
}
