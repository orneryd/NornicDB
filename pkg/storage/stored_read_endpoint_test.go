package storage

import (
	"context"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"testing"
)

// Private branch fixtures supply the same explicit fact input as the real
// validation pass, without adding a test-only capture path to production.
func checkEndpointConflictForTest(tx *BadgerTransaction, edge *Edge) error {
	if edge == nil {
		return tx.checkEdgeEndpointConflicts(nil, nil)
	}
	facts, err := ReadStoredMetadata(context.Background(), tx.engine, StoredReadRequest{Nodes: []NodeID{edge.StartNode, edge.EndNode}, Physical: true})
	if err != nil {
		return err
	}
	current := map[NodeID]StoredNodeFact{}
	for _, fact := range facts.Nodes {
		current[fact.ID] = fact
	}
	for _, physical := range facts.PhysicalNodes {
		fact := current[physical.ID]
		fact.Head = physical.Head
		current[physical.ID] = fact
	}
	return tx.checkEdgeEndpointConflicts(edge, current)
}

func TestStoredReadEndpointValidationExcludesPayload(t *testing.T) {
	b := NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	for _, id := range []NodeID{"d7:a", "d7:b"} {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"L"}, Properties: map[string]any{"key": "excluded"}})
		require.NoError(t, err)
	}
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.CreateEdge(&Edge{ID: "d7:e", StartNode: "d7:a", EndNode: "d7:b", Type: "R"}))
	// A pre-head stored record is supported by the integrity validator. Plant an
	// invalid excluded property so a full-body presence substitute cannot pass.
	require.NoError(t, b.withUpdate(func(raw *badger.Txn) error {
		for _, id := range []NodeID{"d7:a", "d7:b"} {
			item, err := raw.Get(nodeKey(id))
			if err != nil {
				return err
			}
			data, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			envelope, err := SplitNodeEnvelope(data)
			if err != nil {
				return err
			}
			// Preserve valid framing/ID/labels; only the excluded token value is invalid.
			offset := len(data) - len(envelope.metadata) - len(envelope.properties)
			count, n, err := readUvarint(envelope.properties)
			if err != nil {
				return err
			}
			if count != 1 {
				return ErrInvalidData
			}
			_, keyBytes, err := readUvarint(envelope.properties[n:])
			if err != nil {
				return err
			}
			data[offset+n+keyBytes] = 0xc1
			if err = raw.Set(nodeKey(id), data); err != nil {
				return err
			}
			ref, found, err := b.snapshotNodeRef(raw, id)
			if err != nil {
				return err
			}
			if !found {
				return ErrInvalidData
			}
			if err = raw.Delete(mvccNodeHeadKey(ref.num)); err != nil {
				return err
			}
		}
		return nil
	}))
	_, err = b.GetStoredNode("d7:a")
	require.Error(t, err, "negative control full hydration must reject the excluded payload")
	require.NoError(t, tx.Commit(), "stored endpoint presence and captured labels require no property hydration")
	edge, err := b.GetStoredEdge("d7:e")
	require.NoError(t, err)
	require.Equal(t, NodeID("d7:a"), edge.StartNode)
}
