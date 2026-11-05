package storage

import (
	"fmt"
	"strconv"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// projectionFixture stages the numeric IDs the raw key builders need. The
// fixture reuses the package engine fixture and the canonical key encoders.
type projectionFixture struct {
	engine *BadgerEngine
	aNum   uint64
	bNum   uint64
	eNum   uint64
}

func newProjectionFixture(t *testing.T) projectionFixture {
	t.Helper()
	engine := newTestEngine(t)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Person"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "KNOWS"}))
	aNum, ok := engine.idDict.lookupNodeNumID("test:a")
	require.True(t, ok, "test:a must have a numeric ID")
	bNum, ok := engine.idDict.lookupNodeNumID("test:b")
	require.True(t, ok, "test:b must have a numeric ID")
	eNum, ok := engine.idDict.lookupEdgeNumID("test:e")
	require.True(t, ok, "test:e must have a numeric ID")
	return projectionFixture{engine: engine, aNum: aNum, bNum: bNum, eNum: eNum}
}

// TestPhysicalCacheProjectionEmissionValuesAndOrder feeds every raw key family
// the projection classifies, interleaved, and requires the original
// AffectedFact values in the original insertion order. It asserts through the
// AffectedFacts contract only, so the same expectations run against the
// previous physicalCacheProjection representation.
func TestPhysicalCacheProjectionEmissionValuesAndOrder(t *testing.T) {
	fixture := newProjectionFixture(t)
	body := encodeEdgeCompactHeader(edgeFormatCompactV2, &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "KNOWS"}, fixture.aNum, fixture.bNum, 0)
	// A body whose endpoints do not resolve keeps the resolved type and leaves
	// the endpoint fields empty, exactly as the projection always has.
	orphanBody := encodeEdgeCompactHeader(edgeFormatCompactV2, &Edge{Type: "KNOWS"}, 1<<40, 1<<41, 0)
	version := MVCCVersion{CommitSequence: 9}

	entries := []struct {
		name  string
		key   []byte
		value []byte
		want  AffectedFact
	}{
		{"node body", nodeKey("test:a"), nil, AffectedFact{NodeID: "test:a"}},
		{"edge type index", edgeTypeIndexKey("KNOWS", fixture.eNum), nil, AffectedFact{EdgeID: "test:e", Type: "KNOWS"}},
		{"label index", labelIndexKey("Person", fixture.aNum), nil, AffectedFact{NodeID: "test:a", Label: "Person"}},
		{"outgoing index", outgoingIndexKey(fixture.aNum, fixture.eNum), nil, AffectedFact{EdgeID: "test:e", StartNode: "test:a"}},
		{"embedding", embeddingKey("test:b", 0), nil, AffectedFact{NodeID: "test:b"}},
		{"edge body", edgeKey("test:e"), body, AffectedFact{EdgeID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "KNOWS"}},
		{"incoming index", incomingIndexKey(fixture.bNum, fixture.eNum), nil, AffectedFact{EdgeID: "test:e", EndNode: "test:b"}},
		{"mvcc outgoing adjacency", mvccOutgoingAdjacencyKey(fixture.aNum, fixture.eNum, version), nil, AffectedFact{EdgeID: "test:e", StartNode: "test:a"}},
		{"mvcc incoming adjacency", mvccIncomingAdjacencyKey(fixture.bNum, fixture.eNum, version), nil, AffectedFact{EdgeID: "test:e", EndNode: "test:b"}},
		{"edge body without a value", edgeKey("test:no-body"), nil, AffectedFact{EdgeID: "test:no-body"}},
		{"node body after an edge", nodeKey("test:b"), nil, AffectedFact{NodeID: "test:b"}},
		{"edge body with unresolvable endpoints", edgeKey("test:e-orphan"), orphanBody, AffectedFact{EdgeID: "test:e-orphan", Type: "KNOWS"}},
		{"adjacency with unresolvable endpoint", outgoingIndexKey(1<<40, fixture.eNum), nil, AffectedFact{EdgeID: "test:e"}},
	}

	var projection physicalCacheProjection
	for _, entry := range entries {
		projection.add(fixture.engine, entry.key, entry.value)
	}
	require.False(t, projection.unknown, "every staged key belongs to a classified family")

	var got []AffectedFact
	projection.VisitAffectedFacts(func(fact AffectedFact) { got = append(got, fact) })
	require.Len(t, got, len(entries))
	for i, entry := range entries {
		require.Equalf(t, entry.want, got[i], "fact %d (%s)", i, entry.name)
	}
}

// TestPhysicalCacheProjectionUnknownAndIgnoredKeys pins the other half of the
// contract: ignored key families and pending-embedding queue keys emit no fact
// and keep the scope known, while malformed keys emit nothing from that key
// and mark the scope unknown.
func TestPhysicalCacheProjectionUnknownAndIgnoredKeys(t *testing.T) {
	fixture := newProjectionFixture(t)
	withoutSeparator := func(prefix byte, text string) []byte {
		key := []byte{prefix}
		key = append(key, []byte(text)...)
		return append(key, make([]byte, 8)...)
	}
	ignored := [][]byte{
		{prefixSchema, 0x01},
		{prefixMVCCMeta, 0x01},
		{prefixBufferedFlush, 0x01},
		{prefixChangeEvent},
		{prefixIDDictNodeForward},
		{prefixIDDictEdgeReverse},
		{prefixIDDictCounter},
		{prefixIDFreelist},
		{prefixPropKeyForward},
		{prefixPropKeyCounter},
		{prefixMVCCNode},
		{prefixMVCCEdgeHead},
		{prefixMVCCPruneFloor},
		{prefixTemporalIndex},
		{prefixTemporalHead},
		{prefixEdgeBetweenIndex},
		{prefixEdgeBetweenHead},
	}

	cases := []struct {
		name        string
		key         []byte
		value       []byte
		nilEngine   bool
		wantUnknown bool
	}{
		{name: "no key"},
		{name: "nil engine", key: nodeKey("test:a"), nilEngine: true},
		{name: "pending embedding queue", key: []byte{prefixPendingEmbed, 'n'}},
		{name: "embedding without separator", key: []byte{prefixEmbedding, 'n', 'o'}, wantUnknown: true},
		{name: "edge body with a foreign format", key: edgeKey("test:e"), value: []byte{0x99}, wantUnknown: true},
		{name: "edge body truncated", key: edgeKey("test:e"), value: []byte{edgeFormatCompactV2, 0x00}, wantUnknown: true},
		{name: "adjacency key too short", key: []byte{prefixOutgoingIndex, 0x00, 0x00, 0x00}, wantUnknown: true},
		{name: "adjacency with an unknown edge", key: outgoingIndexKey(fixture.aNum, 1<<40), wantUnknown: true},
		{name: "incoming adjacency with an unknown edge", key: incomingIndexKey(fixture.bNum, 1<<40), wantUnknown: true},
		{name: "mvcc adjacency with an unknown edge", key: mvccIncomingAdjacencyKey(fixture.bNum, 1<<40, MVCCVersion{CommitSequence: 3}), wantUnknown: true},
		{name: "edge type index without a separator", key: withoutSeparator(prefixEdgeTypeIndex, "KNOWS"), wantUnknown: true},
		{name: "edge type index too short", key: []byte{prefixEdgeTypeIndex, 'K'}, wantUnknown: true},
		{name: "edge type index with an unknown edge", key: edgeTypeIndexKey("KNOWS", 1<<40), wantUnknown: true},
		{name: "label index without a separator", key: withoutSeparator(prefixLabelIndex, "Person"), wantUnknown: true},
		{name: "label index too short", key: []byte{prefixLabelIndex, 'P'}, wantUnknown: true},
		{name: "label index with an unknown node", key: labelIndexKey("Person", 1<<40), wantUnknown: true},
		{name: "unclassified prefix", key: []byte{0xEE, 0x01}, wantUnknown: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var projection physicalCacheProjection
			if tc.nilEngine {
				projection.add(nil, tc.key, tc.value)
			} else {
				projection.add(fixture.engine, tc.key, tc.value)
			}
			var got []AffectedFact
			projection.VisitAffectedFacts(func(fact AffectedFact) { got = append(got, fact) })
			require.Empty(t, got)
			require.Equal(t, tc.wantUnknown, projection.unknown)
		})
	}

	t.Run("ignored metadata families", func(t *testing.T) {
		for _, key := range ignored {
			var projection physicalCacheProjection
			projection.add(fixture.engine, key, nil)
			var got []AffectedFact
			projection.VisitAffectedFacts(func(fact AffectedFact) { got = append(got, fact) })
			require.Emptyf(t, got, "key family 0x%02x emitted facts", key[0])
			require.Falsef(t, projection.unknown, "key family 0x%02x should stay known", key[0])
		}
	})

	// An unknown key revokes the scope but never drops facts already emitted.
	t.Run("facts before an unknown key are kept", func(t *testing.T) {
		var projection physicalCacheProjection
		projection.add(fixture.engine, nodeKey("test:a"), nil)
		projection.add(fixture.engine, []byte{0xEE}, nil)
		var got []AffectedFact
		projection.VisitAffectedFacts(func(fact AffectedFact) { got = append(got, fact) })
		require.Equal(t, []AffectedFact{{NodeID: "test:a"}}, got)
		require.True(t, projection.unknown)
	})
}

// BenchmarkPhysicalCacheProjectionStore measures storing and emitting
// representative raw-key mixes. Keys and values are built before the timer, and
// the loop drains the projection through VisitAffectedFacts so retained storage
// and emission both stay in scope. It uses only the add/visit contract, so the
// same benchmark source runs against the previous representation.
func BenchmarkPhysicalCacheProjectionStore(b *testing.B) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = engine.Close() })
	for _, id := range []NodeID{"bench:a", "bench:b"} {
		if _, err := engine.CreateNode(b.Context(), &Node{ID: id}); err != nil {
			b.Fatal(err)
		}
	}
	if err := engine.CreateEdge(b.Context(), &Edge{ID: "bench:e", StartNode: "bench:a", EndNode: "bench:b", Type: "R"}); err != nil {
		b.Fatal(err)
	}
	aNum, ok := engine.idDict.lookupNodeNumID("bench:a")
	if !ok {
		b.Fatal("bench:a has no numeric ID")
	}
	bNum, ok := engine.idDict.lookupNodeNumID("bench:b")
	if !ok {
		b.Fatal("bench:b has no numeric ID")
	}
	eNum, ok := engine.idDict.lookupEdgeNumID("bench:e")
	if !ok {
		b.Fatal("bench:e has no numeric ID")
	}
	body := encodeEdgeCompactHeader(edgeFormatCompactV2, &Edge{Type: "R"}, aNum, bNum, 0)
	version := MVCCVersion{CommitSequence: 9}
	nodeID := func(i int) NodeID { return NodeID("bench:n-" + strconv.Itoa(i)) }

	shapes := []struct {
		name  string
		build func(i int) (key, value []byte)
	}{
		{name: "node-heavy", build: func(i int) ([]byte, []byte) {
			if i%2 == 0 {
				return nodeKey(nodeID(i)), nil
			}
			return labelIndexKey("Label"+strconv.Itoa(i%8), aNum), nil
		}},
		{name: "edge-heavy", build: func(i int) ([]byte, []byte) {
			switch i % 4 {
			case 0:
				return outgoingIndexKey(aNum, eNum), nil
			case 1:
				return incomingIndexKey(bNum, eNum), nil
			case 2:
				return edgeTypeIndexKey("R", eNum), nil
			default:
				return edgeKey("bench:e"), body
			}
		}},
		{name: "mixed", build: func(i int) ([]byte, []byte) {
			switch i % 8 {
			case 0:
				return nodeKey(nodeID(i)), nil
			case 1:
				return edgeTypeIndexKey("R", eNum), nil
			case 2:
				return labelIndexKey("Label"+strconv.Itoa(i%8), aNum), nil
			case 3:
				return outgoingIndexKey(aNum, eNum), nil
			case 4:
				return embeddingKey(nodeID(i), 0), nil
			case 5:
				return edgeKey("bench:e"), body
			case 6:
				return incomingIndexKey(bNum, eNum), nil
			default:
				return mvccOutgoingAdjacencyKey(aNum, eNum, version), nil
			}
		}},
	}

	for _, shape := range shapes {
		for _, benchmarkFacts := range []int{1, 16, 100_000} {
			b.Run(fmt.Sprintf("%s/%d", shape.name, benchmarkFacts), func(b *testing.B) {
				keys := make([][]byte, benchmarkFacts)
				values := make([][]byte, benchmarkFacts)
				for i := range keys {
					keys[i], values[i] = shape.build(i)
				}
				b.ReportAllocs()
				b.ResetTimer()
				visited := 0
				for n := 0; n < b.N; n++ {
					var projection physicalCacheProjection
					for i := range keys {
						projection.add(engine, keys[i], values[i])
					}
					projection.VisitAffectedFacts(func(AffectedFact) { visited++ })
				}
				b.StopTimer()
				if want := benchmarkFacts * b.N; visited != want {
					b.Fatalf("visited %d facts, want %d", visited, want)
				}
			})
		}
	}
}
