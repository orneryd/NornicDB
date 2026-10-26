package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestBufferedWALBindingsAcrossStores(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, startup := range []bool{false, true} {
		t.Run(fmt.Sprintf("startup=%v", startup), func(t *testing.T) {
			open := func(dir string) *BadgerEngine {
				b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour, EngineOptions: EngineOptions{RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: 100}}}))
				require.NoError(t, err)
				return b
			}
			source := open(t.TempDir())
			t.Cleanup(func() { _ = source.Close() })
			dir := t.TempDir()
			destination := open(dir)
			t.Cleanup(func() { _ = destination.Close() })
			before := map[string]any{"alpha": "before-alpha"}
			first := map[string]any{"alpha": int64(71), "beta": []byte{4, 2}}
			last := map[string]any{"alpha": []float64{1.25, 2.5}, "beta": time.Date(2026, 10, 21, 1, 2, 3, 0, time.UTC)}
			for _, ns := range []string{"binding", "second"} {
				// Deliberately seed the destination's dictionary in the other order.
				// Node IDs and property IDs are independent; existing bodies must keep
				// their original interpretation after subsequent imported commits.
				for _, b := range []*BadgerEngine{source, destination} {
					props := []map[string]any{before, {"beta": "sentinel-beta"}}
					if b == destination {
						props[0], props[1] = props[1], props[0]
					}
					for i, p := range props {
						_, err := b.CreateNode(&Node{ID: NodeID(fmt.Sprintf("%s:seed%d", ns, i)), Properties: p})
						require.NoError(t, err)
					}
					_, err := b.CreateNode(&Node{ID: NodeID(ns + ":a"), Properties: before})
					require.NoError(t, err)
					_, err = b.CreateNode(&Node{ID: NodeID(ns + ":b")})
					require.NoError(t, err)
					require.NoError(t, b.CreateEdge(&Edge{ID: EdgeID(ns + ":r"), StartNode: NodeID(ns + ":a"), EndNode: NodeID(ns + ":b"), Type: "R", Properties: before}))
					require.NoError(t, b.FlushWriteBehind())
				}
			}
			wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
			require.NoError(t, err)
			t.Cleanup(func() { _ = wal.Close() })
			source.SetBufferedCommitWAL(wal)
			versions := make(map[string]MVCCVersion)
			for stage, props := range []map[string]any{first, last} {
				for _, ns := range []string{"binding", "second"} {
					tx, err := source.BeginTransaction()
					require.NoError(t, err)
					require.NoError(t, tx.SetNamespace(ns))
					require.NoError(t, tx.SetImplicit(true))
					node, err := tx.GetNode(NodeID(ns + ":a"))
					require.NoError(t, err)
					node.Properties = props
					require.NoError(t, tx.UpdateGraphNode(node, ClientNodeEdits))
					edge, err := tx.GetEdge(EdgeID(ns + ":r"))
					require.NoError(t, err)
					edge.Properties = props
					require.NoError(t, tx.UpdateEdge(edge))
					require.NoError(t, tx.Commit())
					require.True(t, tx.AcknowledgedBuffered())
					require.NoError(t, source.FlushWriteBehind())
					if stage == 0 {
						head, err := source.loadNodeMVCCHead(node.ID)
						require.NoError(t, err)
						versions[ns] = head.Version
					}
				}
			}
			if startup {
				n, err := RecoverBufferedCommits(destination, wal)
				require.NoError(t, err)
				require.Equal(t, 4, n)
			} else {
				result, _, err := RecoverIntoEngine(destination, wal.Config().Dir, "")
				require.NoError(t, err)
				require.Zero(t, result.Failed, "%s", result.Summary())
			}
			check := func() {
				assertPropertyDictionaryBijection(t, destination)
				for _, ns := range []string{"binding", "second"} {
					n, err := destination.GetNode(NodeID(ns + ":a"))
					require.NoError(t, err)
					assertRecoveredTypedProperties(t, last, n.Properties, "current node")
					e, err := destination.GetEdge(EdgeID(ns + ":r"))
					require.NoError(t, err)
					assertRecoveredTypedProperties(t, last, e.Properties, "current edge")
					require.Equal(t, NodeID(ns+":a"), e.StartNode)
					require.Equal(t, NodeID(ns+":b"), e.EndNode)
					n, err = destination.GetNodeVisibleAt(NodeID(ns+":a"), versions[ns])
					require.NoError(t, err)
					assertRecoveredTypedProperties(t, first, n.Properties, "historical node")
					e, err = destination.GetEdgeVisibleAt(EdgeID(ns+":r"), versions[ns])
					require.NoError(t, err)
					assertRecoveredTypedProperties(t, first, e.Properties, "historical edge")
					n, err = destination.GetNode(NodeID(ns + ":seed0"))
					require.NoError(t, err)
					require.Equal(t, map[string]any{"beta": "sentinel-beta"}, n.Properties)
				}
			}
			check()
			require.NoError(t, destination.Close())
			destination = open(dir)
			check()
		})
	}
}

// Import must persist only destination allocations. Passing a source drain to
// markPersisted can leave forward[name] and reverse[id] disagreeing in memory.
func assertPropertyDictionaryBijection(t testing.TB, b *BadgerEngine) {
	t.Helper()
	d := b.propKeyDict
	d.mu.RLock()
	defer d.mu.RUnlock()
	for ns, forward := range d.forward {
		for name, id := range forward {
			require.Equal(t, name, d.reverse[ns][id], "forward/reverse mismatch namespace=%s id=%d", ns, id)
		}
	}
	for ns, reverse := range d.reverse {
		for id, name := range reverse {
			require.Equal(t, id, d.forward[ns][name], "reverse/forward mismatch namespace=%s name=%s", ns, name)
		}
	}
}

func TestBufferedWALLegacyBindings(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, startup := range []bool{false, true} {
		for _, bound := range []bool{false, true} {
			t.Run(fmt.Sprintf("startup=%v/bound=%v", startup, bound), func(t *testing.T) {
				source, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
				require.NoError(t, err)
				t.Cleanup(func() { _ = source.Close() })
				_, err = source.CreateNode(&Node{ID: "binding:a", Properties: map[string]any{"alpha": int64(17)}})
				require.NoError(t, err)
				var body []byte
				require.NoError(t, source.db.View(func(txn *badger.Txn) error {
					item, err := txn.Get(nodeKey("binding:a"))
					if err == nil {
						body, err = item.ValueCopy(nil)
					}
					return err
				}))
				data := WALBufferedCommitData{TxID: "historical", Namespace: "binding", Writes: map[string][]byte{string(nodeKey("binding:a")): body}}
				if bound {
					var encoded []byte
					encoded, err = bufferedWALPropertyBindings(data.Writes, source.propKeyDict)
					data.PropKeys = legacyBindingEntries(t, encoded)
					require.NoError(t, err)
				}
				payload, err := json.Marshal(data) // Historical JSON, no typed sidecar.
				require.NoError(t, err)
				wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
				require.NoError(t, err)
				t.Cleanup(func() { _ = wal.Close() })
				destination, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour}))
				require.NoError(t, err)
				t.Cleanup(func() { _ = destination.Close() })
				_, err = destination.CreateNode(&Node{ID: "binding:sentinel", Properties: map[string]any{"beta": "keep"}})
				require.NoError(t, err)
				if startup {
					require.NoError(t, wal.Append(OpBufferedCommit, json.RawMessage(payload)))
					_, err = RecoverBufferedCommits(destination, wal)
				} else {
					err = ReplayWALEntry(destination, WALEntry{Operation: OpBufferedCommit, Data: payload})
				}
				if bound {
					require.NoError(t, err)
					node, err := destination.GetNode("binding:a")
					require.NoError(t, err)
					require.Equal(t, map[string]any{"alpha": int64(17)}, node.Properties)
				} else {
					require.ErrorContains(t, err, "missing property name binding")
					_, err = destination.GetNode("binding:a")
					require.ErrorIs(t, err, ErrNotFound)
				}
				node, err := destination.GetNode("binding:sentinel")
				require.NoError(t, err)
				require.Equal(t, map[string]any{"beta": "keep"}, node.Properties)
				assertPropertyDictionaryBijection(t, destination)
			})
		}
	}
}

func TestRemapPropertyTokensPreservesValueBytes(t *testing.T) {
	values := []any{int8(-3), int64(9007199254740993), uint64(1 << 63), []byte{0, 1, 255}, []float64{1.25, 2.5}, storedTime{value: time.Now()}, map[string]any{"nested": []string{"x", "y"}}}
	for _, value := range values {
		raw, err := msgpack.Marshal(value)
		require.NoError(t, err)
		payload := append([]byte{1, 1}, raw...)
		got, err := remapPropertyTokens(payload, func(uint64) (uint64, error) { return 1000, nil })
		require.NoError(t, err)
		want := binary.AppendUvarint([]byte{1}, 1000)
		require.True(t, bytes.Equal(append(want, raw...), got))
	}
}

func TestBufferedWALNativeBackupBindings(t *testing.T) {
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir()}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	_, err = b.CreateNode(&Node{ID: "binding:a", Properties: map[string]any{"alpha": int64(1)}})
	require.NoError(t, err)
	_, err = b.CreateNode(&Node{ID: "binding:b", Properties: map[string]any{"beta": int64(2)}})
	require.NoError(t, err)
	backup := filepath.Join(t.TempDir(), "native.backup")
	require.NoError(t, b.Backup(backup))
	fresh, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir()}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = fresh.Close() })
	_, err = fresh.CreateNode(&Node{ID: "binding:old", Properties: map[string]any{"beta": "different dictionary"}})
	require.NoError(t, err)
	require.NoError(t, fresh.Restore(backup))
	for i, name := range []string{"alpha", "beta"} {
		node, err := fresh.GetNode(NodeID(fmt.Sprintf("binding:%c", 'a'+i)))
		require.NoError(t, err)
		require.Equal(t, map[string]any{name: int64(i + 1)}, node.Properties)
	}
}

func TestBufferedWALBindingValidation(t *testing.T) {
	identity := func(id uint64) (uint64, error) { return id, nil }
	for _, payload := range [][]byte{{0x80}, {1}, {1, 0x80}, {1, 1, 0xc1}, {0, 0}, {1, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1}} {
		_, err := remapPropertyTokens(payload, identity)
		require.Error(t, err)
	}
	for _, payload := range [][]byte{nil, {0}, {1, 1, 0}} {
		out, err := remapPropertyTokens(payload, identity)
		require.NoError(t, err)
		require.Equal(t, payload, out)
	}
	for _, body := range [][]byte{nil, {0}, {nodeFormatTokenizedV1, 0x80}, {nodeFormatTokenizedV1, 10}} {
		_, err := rewriteWALPropertyBodies(map[string][]byte{string(nodeKey("test:a")): body}, func(_ string, id uint64) (uint64, error) { return id, nil })
		require.Error(t, err)
	}
	for key, body := range map[string][]byte{string(nodeKey("unqualified")): nil, string(edgeKey("test:r")): {0}} {
		_, err := bufferedWALPropertyBindings(map[string][]byte{key: body}, newPropertyKeyDictionary())
		require.Error(t, err)
	}
	body := []byte{nodeFormatTokenizedV1, 3, 1, 1, 0}
	_, err := bufferedWALPropertyBindings(map[string][]byte{string(nodeKey("test:a")): body}, newPropertyKeyDictionary())
	require.ErrorContains(t, err, "no source name")
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	_, err = b.importBufferedWALRecord(&WALBufferedCommitData{PropKeys: []WALPropKeyEntry{{Namespace: "test", Name: "a", ID: 1}, {Namespace: "test", Name: "b", ID: 1}}}, walForeignImport)
	require.ErrorContains(t, err, "conflicting property bindings")
	c, err := b.importBufferedWALRecord(&WALBufferedCommitData{Deletes: []string{"old"}, Writes: map[string][]byte{"": nil, "metadata": {1}}}, walForeignImport)
	require.NoError(t, err)
	require.True(t, c.deletes["old"])
	require.Equal(t, []byte{1}, c.writes["metadata"])
	_, err = remapPropertyTokens([]byte{1, 1, 0}, func(uint64) (uint64, error) { return 0, fmt.Errorf("unbound") })
	require.ErrorContains(t, err, "unbound")
}
