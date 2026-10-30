package storage

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeRuntimeMetadataCopiesAreIndependent(t *testing.T) {
	var node Node
	require.Zero(t, node.RuntimeMetadata())
	require.Nil(t, node.runtime)
	node.SetRuntimeMetadata(NodeRuntimeMetadata{})
	require.Nil(t, node.runtime, "zero metadata needs no record")
	attempt := &EmbeddingAttempt{}
	state := node.runtimeState()
	state.embeddingAttempt = attempt
	state.embeddingRevision = EmbeddingBodyRevision{physical: 19, bufferedSequence: 7}
	state.embeddingOwnerID = "test:body"
	node.setRuntimeState(state)
	metadata := NodeRuntimeMetadata{Database: "source", EmbeddingsStoredSeparately: true, VisibilitySuppressed: true}
	node.SetRuntimeMetadata(metadata)
	original := *node.runtime
	copy := node
	require.Same(t, node.runtime, copy.runtime)
	metadata.Database = "destination"
	copy.SetRuntimeMetadata(metadata)
	require.Equal(t, "source", node.RuntimeMetadata().Database)
	require.Equal(t, "destination", copy.RuntimeMetadata().Database)
	require.Equal(t, original, *node.runtime, "no mutation through a shared record")
	require.Same(t, attempt, copy.EmbeddingAttempt())
	require.Equal(t, node.EmbeddingRevision(), copy.EmbeddingRevision())
	require.True(t, copy.RuntimeMetadata().VisibilitySuppressed)
	require.True(t, copy.RuntimeMetadata().EmbeddingsStoredSeparately)
	require.Equal(t, float64(0), testing.AllocsPerRun(100, func() { copy.SetRuntimeMetadata(metadata) }))
	state = copy.runtimeState()
	require.Equal(t, float64(0), testing.AllocsPerRun(100, func() { copy.setRuntimeState(state) }))
}

func TestNodeRuntimeIdentityCopyPreservesDestinationMetadata(t *testing.T) {
	source, destination := &Node{}, &Node{}
	source.SetRuntimeMetadata(NodeRuntimeMetadata{Database: "source"})
	state := source.runtimeState()
	state.embeddingOwnerID = "source:id"
	state.embeddingRevision = EmbeddingBodyRevision{physical: 41}
	state.embeddingAttempt = &EmbeddingAttempt{}
	state.embeddingPublication = embeddingPublicationProof{id: "source:id", commitTs: 43}
	source.setRuntimeState(state)
	metadata := NodeRuntimeMetadata{Database: "destination", VisibilitySuppressed: true}
	destination.SetRuntimeMetadata(metadata)
	destination.CopyEmbeddingIdentityFrom(source)
	require.Equal(t, metadata, destination.RuntimeMetadata())
	got := destination.runtimeState()
	require.Equal(t, state.embeddingOwnerID, got.embeddingOwnerID)
	require.Equal(t, state.embeddingRevision, got.embeddingRevision)
	require.Same(t, state.embeddingAttempt, got.embeddingAttempt)
	require.Equal(t, state.embeddingPublication, got.embeddingPublication)
	destination.SetEmbeddingAttempt(nil)
	require.Same(t, state.embeddingAttempt, source.EmbeddingAttempt())
	require.Nil(t, destination.EmbeddingAttempt())
}

func TestNodeRuntimeMetadataRecordsPreserveIndependentImages(t *testing.T) {
	metadata := NodeRuntimeMetadata{Database: "shared-projection", VisibilitySuppressed: true}
	var first, second Node
	first.SetRuntimeMetadata(metadata)
	second.SetRuntimeMetadata(metadata)
	require.Same(t, first.runtime, second.runtime)
	second.SetRuntimeMetadata(NodeRuntimeMetadata{Database: "other-projection"})
	require.Equal(t, metadata, first.RuntimeMetadata())
	state := first.runtimeState()
	state.embeddingOwnerID = "snapshot:identity"
	first.setRuntimeState(state)
	var third Node
	third.setRuntimeState(state)
	require.NotSame(t, first.runtime, third.runtime, "identity records must not be interned")
	require.Equal(t, first.runtimeState(), third.runtimeState())
	var group sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for iteration := 0; iteration < 100; iteration++ {
				var node Node
				node.SetRuntimeMetadata(metadata)
				if node.RuntimeMetadata() != metadata {
					t.Error("shared immutable metadata changed")
				}
			}
		}()
	}
	group.Wait()
}

func TestNodeRuntimeMetadataRecordReuseIsBounded(t *testing.T) {
	var retained Node
	retained.SetRuntimeMetadata(NodeRuntimeMetadata{Database: "retained-before-eviction"})
	before := retained.runtime
	for index := 0; index < 2050; index++ {
		var node Node
		node.SetRuntimeMetadata(NodeRuntimeMetadata{Database: fmt.Sprintf("bounded-projection-%d", index)})
	}
	runtimeMetadataRecords.RLock()
	count := len(runtimeMetadataRecords.values)
	runtimeMetadataRecords.RUnlock()
	require.LessOrEqual(t, count, 1024)
	require.Same(t, before, retained.runtime)
	require.Equal(t, "retained-before-eviction", retained.RuntimeMetadata().Database)
	long := NodeRuntimeMetadata{Database: strings.Repeat("x", 257)}
	var first, second Node
	first.SetRuntimeMetadata(long)
	second.SetRuntimeMetadata(long)
	require.NotSame(t, first.runtime, second.runtime, "long values must not enter bounded interner")
	require.Equal(t, long, first.RuntimeMetadata())
}
