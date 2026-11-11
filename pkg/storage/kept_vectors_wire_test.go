package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// The record marks of NOR-412: which kept vectors a version-6 node update
// omits, and the reader's refusal of any mark a writer cannot produce.

func TestKeptVectorsRecordMarks(t *testing.T) {
	b, _ := keptVectorsEngine(t, t.TempDir())
	keptVectorsSeed(t, b, "test:keep", "alpha", 8, 1)
	keptVectorsSeed(t, b, "test:replace", "beta", 8, 2)
	_, err := b.CreateNode(t.Context(), &Node{ID: "test:named", Labels: []string{"Doc"}, Properties: map[string]any{"text": "gamma"}, NamedEmbeddings: map[string][]float32{"title": {1, 2, 3}}})
	require.NoError(t, err)
	payload := keptVectorsPrepare(t, b, "marks", map[NodeID]func(*Node){
		"test:keep":    func(n *Node) { n.Properties["flag"] = true },
		"test:replace": func(n *Node) { n.ChunkEmbeddings = keptVectors(2, 9) },
		"test:named":   func(n *Node) { n.Properties["flag"] = true },
	})
	envelope, ops, err := DecodeEntityTransaction(payload)
	require.NoError(t, err)
	require.Equal(t, EntityTransactionVersion, envelope.Version)
	marks := map[NodeID]vectorRetention{}
	for _, op := range ops {
		marks[op.NodeID] = op.retainedVectors
	}
	require.Equal(t, map[NodeID]vectorRetention{"test:keep": retainedManagedEmbeddings, "test:replace": 0, "test:named": retainedNamedEmbeddings}, marks)
}

// The reader accepts a retention mark only where a writer produces one.
func TestKeptVectorsDecoderRefusesMalformedMarks(t *testing.T) {
	b, _ := keptVectorsEngine(t, t.TempDir())
	keptVectorsSeed(t, b, "test:keep", "alpha", 4, 1)
	payload := keptVectorsPrepare(t, b, "marks", map[NodeID]func(*Node){
		"test:keep": func(n *Node) { n.Properties["flag"] = true },
	})
	type raw = map[string]msgpack.RawMessage
	encode := func(v any) msgpack.RawMessage {
		out, err := msgpack.Marshal(v)
		require.NoError(t, err)
		return out
	}
	// Only the edited field is re-encoded; every other byte stays the writer's.
	mutate := func(edit func(wire, change, node raw)) []byte {
		var wire raw
		require.NoError(t, msgpack.Unmarshal(payload, &wire))
		var changes []raw
		require.NoError(t, msgpack.Unmarshal(wire["Changes"], &changes))
		var node raw
		require.NoError(t, msgpack.Unmarshal(changes[0]["Node"], &node))
		edit(wire, changes[0], node)
		changes[0]["Node"] = encode(node)
		wire["Changes"] = encode(changes)
		return encode(wire)
	}
	_, ops, err := DecodeEntityTransaction(mutate(func(raw, raw, raw) {}))
	require.NoError(t, err, "the unmodified record decodes")
	require.Equal(t, retainedManagedEmbeddings, ops[0].retainedVectors)
	identity := make([]byte, len(vectorImageID{}))
	for name, edit := range map[string]func(wire, change, node raw){
		"version 3":                        func(wire, _, _ raw) { wire["Version"] = encode(uint16(3)) },
		"version 4 with an identity":       func(wire, _, _ raw) { wire["Version"] = encode(uint16(4)) },
		"unknown bit":                      func(_, _, node raw) { node["RetainedVectors"] = encode(uint8(8)) },
		"vectors still there":              func(_, _, node raw) { node["ChunkEmbeddings"] = encode([][]float32{{1, 2}}) },
		"metadata still there":             func(_, _, node raw) { node["EmbedMeta"] = encode(map[string]any{"has_embedding": true}) },
		"on a create":                      func(_, change, _ raw) { change["Type"] = encode(EntityCreateNode) },
		"no identity":                      func(_, _, node raw) { delete(node, "RetainedManaged") },
		"a short identity":                 func(_, _, node raw) { node["RetainedManaged"] = encode(identity[:31]) },
		"a long identity":                  func(_, _, node raw) { node["RetainedManaged"] = encode(append(identity, 0)) },
		"an identity of an unmarked group": func(_, _, node raw) { node["RetainedNamed"] = encode(identity) },
		"an identity without a mark": func(_, _, node raw) {
			delete(node, "RetainedVectors")
			node["ChunkEmbeddings"] = encode([][]float32{{1, 2}})
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := DecodeEntityTransaction(mutate(edit))
			require.Error(t, err)
		})
	}
}
