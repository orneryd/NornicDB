package storage

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

var errStoredWireWrite = errors.New("injected stored-node write failure")

type storedWireBudgetWriter struct{ remaining int }

func (w *storedWireBudgetWriter) Write(p []byte) (int, error) {
	if len(p) > w.remaining {
		accepted := w.remaining
		w.remaining = 0
		return accepted, errStoredWireWrite
	}
	w.remaining -= len(p)
	return len(p), nil
}

// Exercise our codec's error boundary through msgpack's public dispatcher.
// Budgets and prefixes derive from a complete body rather than a copy of its
// field-writing implementation. No database or mutable process state is needed.
func TestNodeStoredWireErrorContracts(t *testing.T) {
	node := &Node{
		ID: "wire:rich", Labels: []string{"Document", "Wire"},
		Properties:      map[string]any{"count": int64(7), "text": "stored body"},
		CreatedAt:       time.Date(2024, 3, 5, 10, 20, 30, 123456789, time.UTC),
		UpdatedAt:       time.Date(2024, 3, 6, 11, 21, 31, 987654321, time.UTC),
		NamedEmbeddings: map[string][]float32{"title": {0.25, -0.5}},
		ChunkEmbeddings: [][]float32{{1, 0}, {0, 1}},
		EmbedMeta:       map[string]any{"embedding_model": "wire-model", "chunk_count": int64(2)},
	}
	node.SetRuntimeMetadata(NodeRuntimeMetadata{EmbeddingsStoredSeparately: true, VisibilitySuppressed: true})
	var buffer bytes.Buffer
	encoder := msgpack.NewEncoder(&buffer)
	encoder.SetSortMapKeys(true)
	require.NoError(t, encoder.Encode(node))
	body := buffer.Bytes()
	var decoded Node
	require.NoError(t, msgpack.Unmarshal(body, &decoded), "complete-body control")
	require.Equal(t, node.ID, decoded.ID)
	require.Equal(t, node.Labels, decoded.Labels)
	require.Equal(t, node.NamedEmbeddings, decoded.NamedEmbeddings)
	require.Equal(t, node.ChunkEmbeddings, decoded.ChunkEmbeddings)
	require.Equal(t, node.RuntimeMetadata(), decoded.RuntimeMetadata())

	for limit := 0; limit < len(body); limit++ {
		t.Run(fmt.Sprintf("write-budget/%d", limit), func(t *testing.T) {
			writer := &storedWireBudgetWriter{remaining: limit}
			encoder := msgpack.NewEncoder(writer)
			encoder.SetSortMapKeys(true)
			require.ErrorIs(t, encoder.Encode(node), errStoredWireWrite)
		})
	}
	writer := &storedWireBudgetWriter{remaining: len(body)}
	encoder = msgpack.NewEncoder(writer)
	encoder.SetSortMapKeys(true)
	require.NoError(t, encoder.Encode(node), "complete output fits its exact budget")
	require.Zero(t, writer.remaining)

	attempt := &EmbeddingAttempt{ProviderGeneration: 4}
	for length := 0; length < len(body); length++ {
		t.Run(fmt.Sprintf("truncated/%d", length), func(t *testing.T) {
			existing := Node{ID: "wire:previous"}
			state := existing.runtimeState()
			state.metadata.Database = "caller"
			state.embeddingAttempt = attempt
			state.embeddingRevision = EmbeddingBodyRevision{physical: 61}
			existing.setRuntimeState(state)
			var err error
			require.NotPanics(t, func() { err = msgpack.Unmarshal(body[:length], &existing) })
			require.Error(t, err)
			require.Same(t, attempt, existing.EmbeddingAttempt())
			require.Equal(t, state.embeddingRevision, existing.EmbeddingRevision())
			require.Equal(t, "caller", existing.RuntimeMetadata().Database)
		})
	}
}

func TestNodeStoredWireNilUnknownAndMalformedFlag(t *testing.T) {
	attempt := &EmbeddingAttempt{ProviderGeneration: 3}
	existing := Node{ID: "wire:previous"}
	state := existing.runtimeState()
	state.metadata = NodeRuntimeMetadata{Database: "caller", VisibilitySuppressed: true}
	state.embeddingAttempt = attempt
	existing.setRuntimeState(state)

	// Unknown fields are skipped. Invalid unknown values still propagate errors.
	require.NoError(t, msgpack.Unmarshal([]byte{0x81, 0xa1, 'x', 0x01}, &existing))
	require.Equal(t, NodeID("wire:previous"), existing.ID)
	require.Error(t, msgpack.Unmarshal([]byte{0x81, 0xa1, 'x', 0xc1}, &existing))
	require.Same(t, attempt, existing.EmbeddingAttempt())

	var malformed bytes.Buffer
	encoder := msgpack.NewEncoder(&malformed)
	require.NoError(t, encoder.EncodeMapLen(1))
	require.NoError(t, encoder.EncodeString("VisibilitySuppressed"))
	require.NoError(t, encoder.EncodeString("not a boolean"))
	require.Error(t, msgpack.Unmarshal(malformed.Bytes(), &existing))
	require.Same(t, attempt, existing.EmbeddingAttempt())
	require.Equal(t, "caller", existing.RuntimeMetadata().Database)
	// A decoder may partially apply fields before an error; do not invent a
	// rollback contract for flags. Private embedding identity remains intact.

	require.NoError(t, msgpack.Unmarshal([]byte{0xc0}, &existing))
	require.Equal(t, Node{}, existing, "nil native map clears the destination")
}
