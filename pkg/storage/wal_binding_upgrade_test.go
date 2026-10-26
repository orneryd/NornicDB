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

func reviewBindingEngine(t *testing.T, dir string) *BadgerEngine {
	t.Helper()
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour, EngineOptions: EngineOptions{RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: 100}}}))
	require.NoError(t, err)
	// Keep crash fixtures pending even after an explicit flush adapts the delay.
	b.writeBehind.SetDelayBounds(time.Hour, time.Hour)
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// The old writer exported only fresh keys. These transactions use a key that
// was durably assigned before the WAL began, so their old-format PropKeys is
// exactly empty. The recovery destination is the ORIGINAL store, not a snapshot.
func TestReviewLegacySameStoreUpgrade(t *testing.T) {
	for _, route := range []string{"startup", "snapshot"} {
		t.Run(route, func(t *testing.T) {
			t.Cleanup(config.WithWALEnabled())
			dir := t.TempDir()
			source := reviewBindingEngine(t, dir)
			_, err := source.CreateNode(&Node{ID: "legacy:a", Properties: map[string]any{"value": int64(0)}})
			require.NoError(t, err)
			require.NoError(t, source.FlushWriteBehind())
			// Flush can leave an empty rotation request queued. Start the crash phase
			// with a fresh idle buffer, so that request cannot persist later updates.
			source.writeBehind.Close()
			source.writeBehind = NewWriteBehindBuffer(time.Hour, 1000000, source.replayWriteBehind)
			capture, err := NewWAL(t.TempDir(), DefaultWALConfig())
			require.NoError(t, err)
			source.SetBufferedCommitWAL(capture)
			for i := 1; i <= 20; i++ {
				tx, err := source.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetNamespace("legacy"))
				require.NoError(t, tx.SetImplicit(true))
				require.NoError(t, tx.UpdateGraphNode(&Node{ID: "legacy:a", Properties: map[string]any{"value": int64(i)}}, ClientNodeEdits))
				require.NoError(t, tx.Commit())
				require.True(t, tx.AcknowledgedBuffered())
			}
			entries, err := ReadWALEntriesFromDir(capture.Config().Dir)
			require.NoError(t, err)
			legacy, err := NewWAL(t.TempDir(), DefaultWALConfig())
			require.NoError(t, err)
			t.Cleanup(func() { _ = legacy.Close() })
			count := 0
			for _, entry := range entries {
				if entry.Operation != OpBufferedCommit {
					continue
				}
				data, err := decodeBufferedWALRecord(entry)
				require.NoError(t, err)
				require.NotEmpty(t, data.TxID)
				data.PropKeys, data.PropKeyCounters, data.PropertyBindings = nil, nil, nil
				data.PropertyDictionaryID = ""
				payload, err := json.Marshal(data)
				require.NoError(t, err)
				require.NoError(t, legacy.Append(OpBufferedCommit, json.RawMessage(payload)))
				count++
			}
			require.Equal(t, 20, count)
			require.NoError(t, legacy.Close()) // Flush the generic Append buffer before replay.
			crashClose(t, source, capture)
			if route == "snapshot" {
				dest, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir()}))
				require.NoError(t, err)
				t.Cleanup(func() { _ = dest.Close() })
				path := filepath.Join(t.TempDir(), "snapshot.bin")
				require.NoError(t, SaveSnapshot(&Snapshot{Nodes: []*Node{
					{ID: "legacy:b", Properties: map[string]any{"other": "keep"}},
					{ID: "separator:a"},
					{ID: "legacy:a", Properties: map[string]any{"value": int64(0)}},
				}}, path))
				result, _, err := RecoverIntoEngine(dest, legacy.Config().Dir, path)
				require.NoError(t, err)
				require.Equal(t, 20, result.Failed)
				require.Zero(t, result.Applied)
				for _, failure := range result.Errors {
					require.ErrorContains(t, failure.Error, "missing property name binding")
				}
				node, err := dest.GetNode("legacy:a")
				require.NoError(t, err)
				require.Equal(t, map[string]any{"value": int64(0)}, node.Properties)
				assertPropertyDictionaryBijection(t, dest)
				return
			}
			recovered := reviewBindingEngine(t, dir)
			require.NotNil(t, recovered.writeBehind)
			legacyEntries, err := ReadWALEntriesFromDir(legacy.Config().Dir)
			require.NoError(t, err)
			require.Len(t, legacyEntries, 20)
			for _, entry := range legacyEntries {
				data, err := decodeBufferedWALRecord(entry)
				require.NoError(t, err)
				require.NotEmpty(t, data.TxID)
				marked, err := recovered.bufferedFlushMarkerExists("", data.TxID, "")
				require.NoError(t, err)
				require.False(t, marked, "fixture update unexpectedly reached storage")
			}
			name, ok := recovered.propKeyDict.lookup("legacy", 1)
			require.True(t, ok)
			require.Equal(t, "value", name, "the original durable source dictionary survives the crash")
			n, err := RecoverBufferedCommitsAtOpen(recovered, legacy)
			t.Logf("legacy records=20, records missing per-record bindings=20, replayed=%d, error=%v", n, err)
			require.NoError(t, err, "upgrade must retain recoverability when the original source dictionary is intact")
			require.Equal(t, 20, n)
			node, err := recovered.GetNode("legacy:a")
			require.NoError(t, err)
			require.Equal(t, int64(20), node.Properties["value"])
		})
	}
}

// These synthetic typed records each provide a lossless named property image.
// The plain-JSON foreign suffix rejection is tested separately; an older
// prefix never supplies authority for a later record.
func TestReviewLegacyLosslessNamedRecords(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source := reviewBindingEngine(t, t.TempDir())
	capture, err := NewWAL(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = capture.Close() })
	source.SetBufferedCommitWAL(capture)
	for i := 0; i < 2; i++ {
		tx, err := source.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace("prefix"))
		require.NoError(t, tx.SetImplicit(true))
		node := &Node{ID: "prefix:a", Properties: map[string]any{"value": int64(i)}}
		if i == 0 {
			_, err = tx.CreateNode(node)
		} else {
			err = tx.UpdateGraphNode(node, ClientNodeEdits)
		}
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		require.True(t, tx.AcknowledgedBuffered())
		require.NoError(t, source.FlushWriteBehind())
	}
	entries, err := ReadWALEntriesFromDir(capture.Config().Dir)
	require.NoError(t, err)
	destination := reviewBindingEngine(t, t.TempDir())
	index := 0
	for _, entry := range entries {
		if entry.Operation != OpBufferedCommit {
			continue
		}
		data, err := decodeBufferedWALRecord(entry)
		require.NoError(t, err)
		if index == 0 {
			data.PropKeys = legacyBindingEntries(t, data.PropertyBindings)
			require.Len(t, data.PropKeys, 1)
		} else {
			data.PropKeys = nil
			data.PropKeyCounters = nil
		}
		data.PropertyBindings = nil
		data.PropertyDictionaryID = ""
		entry.Data = typedPropertyRecordFixture(t, data, map[string]any{"value": int64(index)})
		entry.PropertyEpoch = nil // This fixture deliberately models a pre-epoch record.
		require.NoError(t, err)
		err = ReplayWALEntry(destination, entry)
		t.Logf("legacy prefix record=%d bindings=%d error=%v", index, len(data.PropKeys), err)
		require.NoError(t, err, "this record supplies its own lossless named properties")
		index++
	}
	require.Equal(t, 2, index)
	node, err := destination.GetNode("prefix:a")
	require.NoError(t, err)
	require.Equal(t, int64(1), node.Properties["value"])
}

func TestReviewBindingGrowthAndRestart(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprint(restart), func(t *testing.T) {
			source := reviewBindingEngine(t, t.TempDir())
			destDir := t.TempDir()
			dest := reviewBindingEngine(t, destDir)
			for _, ns := range []string{"growth", "other"} {
				for _, b := range []*BadgerEngine{source, dest} {
					// Same node order, reversed property dictionary. Extra keys
					// appear only in the log and cross the one-byte ID boundary.
					for i := 0; i < 132; i++ {
						k := i
						if b == dest {
							k = 131 - i
						}
						_, err := b.CreateNode(&Node{ID: NodeID(fmt.Sprintf("%s:seed%03d", ns, i)), Properties: map[string]any{fmt.Sprintf("k%03d", k): int64(k)}})
						require.NoError(t, err)
					}
					_, err := b.CreateNode(&Node{ID: NodeID(ns + ":a"), Properties: map[string]any{"removed": "old"}})
					require.NoError(t, err)
					_, err = b.CreateNode(&Node{ID: NodeID(ns + ":b")})
					require.NoError(t, err)
					require.NoError(t, b.CreateEdge(&Edge{ID: EdgeID(ns + ":r"), StartNode: NodeID(ns + ":a"), EndNode: NodeID(ns + ":b"), Type: "R", Properties: map[string]any{"removed": "old"}}))
					require.NoError(t, b.FlushWriteBehind())
				}
			}
			wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
			require.NoError(t, err)
			t.Cleanup(func() { _ = wal.Close() })
			source.SetBufferedCommitWAL(wal)
			versions := map[string]MVCCVersion{}
			expected := map[string]map[string]any{}
			history := map[string]map[string]any{}
			for stage := 0; stage < 3; stage++ {
				for _, ns := range []string{"growth", "other"} {
					props := map[string]any{"renamed": fmt.Sprintf("%s-%d", ns, stage), "nested": map[string]any{"a": []any{int64(3), "x", []byte{0, 255}}}}
					for k := stage * 80; k < stage*80+160; k++ {
						props[fmt.Sprintf("k%03d", k)] = int64(stage*1000 + k)
					}
					tx, err := source.BeginTransaction()
					require.NoError(t, err)
					require.NoError(t, tx.SetNamespace(ns))
					require.NoError(t, tx.SetImplicit(true))
					require.NoError(t, tx.UpdateGraphNode(&Node{ID: NodeID(ns + ":a"), Properties: props}, ClientNodeEdits))
					require.NoError(t, tx.UpdateEdge(&Edge{ID: EdgeID(ns + ":r"), StartNode: NodeID(ns + ":a"), EndNode: NodeID(ns + ":b"), Type: "R", Properties: props}))
					require.NoError(t, tx.Commit())
					require.True(t, tx.AcknowledgedBuffered())
					require.NoError(t, source.FlushWriteBehind())
					node, err := source.GetNode(NodeID(ns + ":a"))
					require.NoError(t, err)
					expected[ns] = node.Properties
					if stage == 0 {
						head, err := source.loadNodeMVCCHead(node.ID)
						require.NoError(t, err)
						versions[ns], history[ns] = head.Version, node.Properties
					}
				}
			}
			if restart {
				entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
				require.NoError(t, err)
				applied := 0
				for _, entry := range entries {
					if entry.Operation != OpBufferedCommit {
						continue
					}
					require.NoError(t, ReplayWALEntry(dest, entry))
					applied++
					if applied == 3 {
						break
					}
				}
				require.Equal(t, 3, applied)
				require.NoError(t, dest.Close())
				dest = reviewBindingEngine(t, destDir)
			}
			n, err := RecoverBufferedCommits(dest, wal)
			require.NoError(t, err)
			assertPropertyDictionaryBijection(t, dest)
			if restart {
				require.Equal(t, 3, n)
			} else {
				require.Equal(t, 6, n)
			}
			require.NoError(t, dest.Close())
			dest = reviewBindingEngine(t, destDir)
			assertPropertyDictionaryBijection(t, dest)
			for _, ns := range []string{"growth", "other"} {
				node, err := dest.GetNode(NodeID(ns + ":a"))
				require.NoError(t, err)
				require.Equal(t, expected[ns], node.Properties)
				require.NotContains(t, node.Properties, "removed")
				edge, err := dest.GetEdge(EdgeID(ns + ":r"))
				require.NoError(t, err)
				require.Equal(t, expected[ns], edge.Properties)
				node, err = dest.GetNodeVisibleAt(NodeID(ns+":a"), versions[ns])
				require.NoError(t, err)
				require.Equal(t, history[ns], node.Properties)
				edge, err = dest.GetEdgeVisibleAt(EdgeID(ns+":r"), versions[ns])
				require.NoError(t, err)
				require.Equal(t, history[ns], edge.Properties)
				sentinel, err := dest.GetNode(NodeID(ns + ":seed000"))
				require.NoError(t, err)
				require.Equal(t, map[string]any{"k131": int64(131)}, sentinel.Properties)
			}
			// Export bodies from a store that has already imported/rebound them.
			// A direct synchronous write initializes lazy namespace MVCC state.
			// Develop has a separately recorded cold-first-buffered-write deadlock
			// before reaching this PR's writer; do not conflate it with rebinding.
			_, err = dest.CreateNode(&Node{ID: "growth:warm"})
			require.NoError(t, err)
			secondLog, err := NewWAL(t.TempDir(), DefaultWALConfig())
			require.NoError(t, err)
			t.Cleanup(func() { _ = secondLog.Close() })
			dest.SetBufferedCommitWAL(secondLog)
			tx, err := dest.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetNamespace("growth"))
			require.NoError(t, tx.SetImplicit(true))
			node, err := dest.GetNode("growth:a")
			require.NoError(t, err)
			require.NoError(t, tx.UpdateGraphNode(node, ClientNodeEdits))
			require.NoError(t, tx.Commit())
			require.True(t, tx.AcknowledgedBuffered())
			third := reviewBindingEngine(t, t.TempDir())
			n, err = RecoverBufferedCommits(third, secondLog)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			node, err = third.GetNode("growth:a")
			require.NoError(t, err)
			require.Equal(t, expected["growth"], node.Properties)
		})
	}
}
