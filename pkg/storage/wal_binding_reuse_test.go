package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestWALBindingReuseKeepsRecordAuthority(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, count := range []int{1, 160} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			source, data := r2BindingData(t, count)
			data.Ops = []Operation{{Type: OpCreateNode, NodeID: "perf:a", Node: &Node{ID: "perf:a"}}}
			original := bytes.Clone(data.PropertyBindings)
			// Re-export another dependency set without modifying an already
			// acknowledged table or including unrelated dictionary names.
			_, err := source.CreateNode(&Node{ID: "perf:other", Properties: map[string]any{"other": "keep"}})
			require.NoError(t, err)
			other, err := bufferedWALPropertyBindings(nil, source.propKeyDict)
			require.NoError(t, err)
			require.Empty(t, legacyBindingEntries(t, other))
			require.Equal(t, original, data.PropertyBindings)
			exported, err := bufferedWALPropertyBindings(data.Writes, source.propKeyDict)
			require.NoError(t, err)
			require.ElementsMatch(t, legacyBindingEntries(t, original), legacyBindingEntries(t, exported))
			destination := reviewBindingEngine(t, t.TempDir())
			_, err = destination.CreateNode(&Node{ID: "warm:seed"})
			require.NoError(t, err)
			play := func() error {
				raw, err := json.Marshal(data)
				require.NoError(t, err)
				return ReplayWALEntry(destination, WALEntry{Operation: OpBufferedCommit, Data: raw})
			}
			for i := 0; i < 3; i++ {
				require.NoError(t, play())
			}
			// A warm complete-table cache must never lend authority to legacy.
			data.PropertyBindings = nil
			require.ErrorContains(t, play(), "missing property name binding")
			data.PropertyBindings = original
			// A namespace reset reuses numeric IDs for different names. Replaying
			// the exact same complete record must resolve through the new mapping.
			_, _, err = destination.DeleteByPrefix("perf:")
			require.NoError(t, err)
			_, err = destination.CreateNode(&Node{ID: "perf:sentinel", Properties: map[string]any{"other": "keep"}})
			require.NoError(t, err)
			require.NoError(t, play())
			node, err := destination.GetNode("perf:a")
			require.NoError(t, err)
			require.Len(t, node.Properties, count)
			for i := 0; i < count; i++ {
				require.Equal(t, int64(i), node.Properties[fmt.Sprintf("property_%03d", i)])
			}
			assertPropertyDictionaryBijection(t, destination)
			for i := 0; i < 3; i++ {
				require.NoError(t, play())
			}
			// Native restore replaces the dictionary in place. A prepared plan
			// for the previous mapping must not survive that replacement.
			donor := reviewBindingEngine(t, t.TempDir())
			_, err = donor.CreateNode(&Node{ID: "perf:seed", Properties: map[string]any{"unused_a": 1, "unused_b": 2}})
			require.NoError(t, err)
			before := *node
			before.Properties = make(map[string]any, count)
			for name := range node.Properties {
				before.Properties[name] = int64(-1)
			}
			_, err = donor.CreateNode(&before)
			require.NoError(t, err)
			backup := filepath.Join(t.TempDir(), "replacement.backup")
			require.NoError(t, donor.Backup(backup))
			require.NoError(t, destination.Restore(backup))
			require.NoError(t, play())
			restored, err := destination.GetNode("perf:a")
			require.NoError(t, err)
			require.Equal(t, node.Properties, restored.Properties)
			assertPropertyDictionaryBijection(t, destination)
		})
	}
}

func TestWALBindingRepeatedCompleteImportAllocationBounds(t *testing.T) {
	for _, count := range []int{1, 160} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			source, data := r2BindingData(t, count)
			for i := 0; i < 2; i++ {
				_, err := source.importBufferedWALRecord(&data, walForeignImport)
				require.NoError(t, err)
			}
			allocs := testing.AllocsPerRun(32, func() {
				_, err := source.importBufferedWALRecord(&data, walForeignImport)
				if err != nil {
					t.Fatal(err)
				}
			})
			require.Zero(t, allocs, "an unchanged complete table needs no repeated decoding or map")
		})
	}
}

func TestWALBindingCompressedTableValidation(t *testing.T) {
	for _, table := range [][]byte{{2}, {2, 2}, {2, 0x28, 0xB5, 0x2F, 0xFD}, {2, 1, 0x80}} {
		require.Error(t, visitWALPropertyBindings(table, nil, func(string, uint64, string) error { return nil }))
	}
}

func TestWALBindingReuseRetentionBound(t *testing.T) {
	_, data := r2BindingData(t, walBindingCacheKeys+1)
	require.Len(t, legacyBindingEntries(t, data.PropertyBindings), walBindingCacheKeys+1)
	destination := reviewBindingEngine(t, t.TempDir())
	commit, err := destination.importBufferedWALRecord(&data, walForeignImport)
	require.NoError(t, err)
	require.Len(t, commit.propKeyDrain.pending, walBindingCacheKeys+1)
	require.Empty(t, destination.walPropertySource.bindings.table, "large remap plans must not be retained")
	assertPropertyDictionaryBijection(t, destination)
}
