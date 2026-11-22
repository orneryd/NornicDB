package storage

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// TestPR1826S1ProjectedCorpusAndOwnership is a revision-independent oracle:
// every projected image must equal the same fields selected from a full image,
// and every delivered container belongs to its caller. The corpus deliberately
// crosses MessagePack scalar, collection, temporal and large-span encodings.
func TestPR1826S1ProjectedCorpusAndOwnership(t *testing.T) {
	// On disk: an in-memory store refuses a value above 64 KiB.
	b, _ := createTestBadgerEngineOnDisk(t)
	when := time.Date(2026, 11, 22, 13, 14, 15, 123456789, time.FixedZone("S1", 3600))
	properties := map[string]any{
		"empty":       "",
		"long":        strings.Repeat("projected-read-span-", 8192),
		"bytes":       []byte{0, 1, 127, 128, 254, 255},
		"bool":        true,
		"signed":      int64(-9223372036854775807),
		"unsigned":    uint64(9223372036854775808),
		"float32":     float32(1.25),
		"float64":     2.5,
		"time":        when,
		"strings":     []string{"", "alpha", strings.Repeat("z", 4096)},
		"ints":        []int64{-1, 0, 1, 1 << 40},
		"vector":      []float32{0.125, -0.5, 1},
		"mixed":       []any{"x", int64(7), false, nil, when},
		"nested":      map[string]any{"list": []any{int64(1), "two"}, "time": when},
		"explicitNil": nil,
	}
	for i := 0; i < 96; i++ {
		properties["many_"+strings.Repeat("x", i%7)+string(rune('A'+i%26))+string(rune('a'+i/26))] = int64(i)
	}
	id := NodeID("s1:corpus")
	_, err := b.CreateNode(t.Context(), &Node{
		ID: id, Labels: []string{"Corpus", "Projected"}, Properties: properties,
		NamedEmbeddings: map[string][]float32{"default": {0.25, 0.5, 0.75}},
		ChunkEmbeddings: [][]float32{{1, 2, 3}, {4, 5, 6}},
	})
	require.NoError(t, err)
	_, err = b.CreateNode(t.Context(), &Node{ID: "s1:empty", Labels: []string{"Corpus"}, Properties: map[string]any{}})
	require.NoError(t, err)

	full, err := b.GetNode(t.Context(), id)
	require.NoError(t, err)
	for name, projection := range map[string][]string{
		"empty non-nil": {},
		"single scalar": {"signed"},
		"large spans":   {"long", "bytes"},
		"collections":   {"strings", "ints", "vector", "mixed", "nested"},
		"missing key":   {"empty", "does_not_exist"},
		"wide":          s1PropertyNames(full.Properties),
	} {
		t.Run(name, func(t *testing.T) {
			projected, readErr := b.GetNodeProjected(t.Context(), id, projection)
			require.NoError(t, readErr)
			require.Equal(t, id, projected.ID)
			require.Equal(t, full.Labels, projected.Labels)
			require.Equal(t, selectProperties(full.Properties, projection), projected.Properties)
		})
	}

	// Warm a narrow image, widen it, and prove mutations of returned containers
	// cannot alter either the cache or a later caller's result.
	narrow, err := b.GetNodeProjected(t.Context(), id, []string{"bytes", "nested", "vector"})
	require.NoError(t, err)
	narrow.Properties["bytes"].([]byte)[0] = 99
	narrow.Properties["vector"].([]float32)[0] = 99
	narrow.Properties["nested"].(map[string]any)["list"].([]any)[0] = int64(99)
	narrow.Properties["injected"] = "caller only"
	again, err := b.GetNodeProjected(t.Context(), id, []string{"bytes", "nested", "vector"})
	require.NoError(t, err)
	require.Equal(t, selectProperties(full.Properties, []string{"bytes", "nested", "vector"}), again.Properties)

	wide, err := b.GetNodeProjected(t.Context(), id, append(s1PropertyNames(full.Properties), "absent"))
	require.NoError(t, err)
	require.Equal(t, full.Properties, wide.Properties)
	empty, err := b.GetNodeProjected(t.Context(), "s1:empty", []string{"missing"})
	require.NoError(t, err)
	require.Empty(t, empty.Properties)
}

func TestPR1826S1TransactionVisibilityProjectedReads(t *testing.T) {
	b := createTestBadgerEngine(t)
	id := NodeID("s1:tx")
	_, err := b.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Before"}, Properties: map[string]any{"value": "old", "remove": "present", "body": strings.Repeat("body", 4096)}})
	require.NoError(t, err)

	writer, err := b.BeginTransaction(t.Context(), OrdinaryBegin)
	require.NoError(t, err)
	beforeCommit, err := b.BeginTransaction(t.Context(), OrdinaryBegin)
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Rollback(); _ = beforeCommit.Rollback() })

	pending, err := writer.GetNode(t.Context(), id)
	require.NoError(t, err)
	pending.Labels = []string{"After"}
	pending.Properties["value"] = "pending"
	delete(pending.Properties, "remove")
	require.NoError(t, writer.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: pending, Policy: ClientNodeEdits}))

	inside, err := writer.GetNodeProjected(t.Context(), id, []string{"value", "remove"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"value": "pending"}, inside.Properties)
	require.Equal(t, []string{"After"}, inside.Labels)
	outside, err := beforeCommit.GetNodeProjected(t.Context(), id, []string{"value", "remove"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"value": "old", "remove": "present"}, outside.Properties)
	require.Equal(t, []string{"Before"}, outside.Labels)
	require.NoError(t, writer.Rollback())

	rolledBack, err := b.GetNodeProjected(t.Context(), id, []string{"value", "remove"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"value": "old", "remove": "present"}, rolledBack.Properties)

	writer, err = b.BeginTransaction(t.Context(), OrdinaryBegin)
	require.NoError(t, err)
	pending, err = writer.GetNode(t.Context(), id)
	require.NoError(t, err)
	pending.Properties["value"] = "committed"
	require.NoError(t, writer.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: pending, Policy: ClientNodeEdits}))
	require.NoError(t, writer.Commit())

	oldSnapshot, err := beforeCommit.GetNodeProjected(t.Context(), id, []string{"value"})
	require.NoError(t, err)
	require.Equal(t, "old", oldSnapshot.Properties["value"])
	latest, err := b.GetNodeProjected(t.Context(), id, []string{"value"})
	require.NoError(t, err)
	require.Equal(t, "committed", latest.Properties["value"])

	deleter, err := b.BeginTransaction(t.Context(), OrdinaryBegin)
	require.NoError(t, err)
	require.NoError(t, deleter.DeleteGraphNode(t.Context(), id, CascadeEdges))
	_, err = deleter.GetNodeProjected(t.Context(), id, []string{"value"})
	require.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, deleter.Rollback())
	latest, err = b.GetNodeProjected(t.Context(), id, []string{"value"})
	require.NoError(t, err)
	require.Equal(t, "committed", latest.Properties["value"])
}

func TestPR1826S1CompositeProjectedIsolation(t *testing.T) {
	first, second := NewNamespacedEngine(newTestMemoryEngine(t), "one"), NewNamespacedEngine(newTestMemoryEngine(t), "two")
	_, err := first.CreateNode(t.Context(), &Node{ID: "same", Properties: map[string]any{"tenant": "one", "secret": "one-secret"}})
	require.NoError(t, err)
	_, err = second.CreateNode(t.Context(), &Node{ID: "same", Properties: map[string]any{"tenant": "two", "secret": "two-secret"}})
	require.NoError(t, err)
	composite := NewCompositeEngine(
		map[string]Engine{"one": first, "two": second},
		map[string]string{"one": "one", "two": "two"},
		map[string]string{"one": "read_write", "two": "read_write"},
	)
	for _, check := range []struct {
		id   NodeID
		want string
	}{{"4:one:same", "one"}, {"4:two:same", "two"}, {"4:one:same", "one"}} {
		node, readErr := composite.GetNodeProjected(t.Context(), check.id, []string{"tenant"})
		require.NoError(t, readErr)
		require.Equal(t, map[string]any{"tenant": check.want}, node.Properties)
	}
}

func FuzzPR1826S1ProjectedAndFullDecoderAgree(f *testing.F) {
	b, err := NewBadgerEngineInMemory()
	require.NoError(f, err)
	f.Cleanup(func() { _ = b.Close() })
	id := NodeID("s1:fuzz")
	_, err = b.CreateNode(context.Background(), &Node{ID: id, Labels: []string{"Fuzz"}, Properties: map[string]any{
		"keep": "value", "bytes": []byte{0, 1, 2, 255}, "skip": strings.Repeat("x", 1024),
		"list": []any{int64(1), "two", false}, "nested": map[string]any{"n": int64(7)},
	}})
	require.NoError(f, err)
	var valid []byte
	require.NoError(f, b.withView(func(txn *badger.Txn) error {
		item, getErr := txn.Get(nodeKey(id))
		if getErr != nil {
			return getErr
		}
		valid, getErr = item.ValueCopy(nil)
		return getErr
	}))
	f.Add(valid, uint8(0))
	f.Add(valid[:len(valid)-1], uint8(1))
	f.Add([]byte{nodeFormatTokenizedV1, 0xff}, uint8(2))
	f.Add([]byte{0xff, 0}, uint8(3))

	f.Fuzz(func(t *testing.T, body []byte, selector uint8) {
		sets := [][]string{{"keep"}, {"bytes", "nested"}, {}, {"missing", "list"}}
		projection := sets[int(selector)%len(sets)]
		include := propertyProjectionSet(projection)
		full, fullErr := b.decodeNodeProjected(nil, "s1", body, include)
		projected, projectedErr := newProjectedNodeDecoder(b, nil, StreamNodesOptions{Projection: projection}).decode(nodeKey(id), body)
		require.Equal(t, fullErr != nil, projectedErr != nil, "full=%v projected=%v body=%x", fullErr, projectedErr, body)
		if fullErr == nil {
			require.True(t, reflect.DeepEqual(full, projected), "full=%#v projected=%#v", full, projected)
		}
	})
}

func s1PropertyNames(properties map[string]any) []string {
	names := make([]string, 0, len(properties))
	for name := range properties {
		names = append(names, name)
	}
	return names
}

func selectProperties(properties map[string]any, projection []string) map[string]any {
	selected := make(map[string]any, len(projection))
	for _, name := range projection {
		if value, ok := properties[name]; ok {
			selected[name] = value
		}
	}
	return selected
}
