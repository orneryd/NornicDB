package storage

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// Node runtime metadata now lives in one immutable record that every detached
// copy of a node image shares (node_runtime.go). These tests pin what the cache,
// body-cache, read, namespace and copy paths must preserve across that move:
//
//   - all three metadata fields survive a copy, a cache store/fill, a body-cache
//     store/load, a public read, a namespace projection and a stored read;
//   - embedding identity (owner, owner ID, revision, attempt, commit
//     publication proof) survives those same copies and never follows a
//     metadata-only change;
//   - Database stays a runtime value: it remains absent from the stored body and
//     present in the replication transport, exactly as before the migration.
//
// Every assertion reads production state through the existing accessors. No
// immutable cached image is mutated, and no assertion is satisfied by a copy
// that the production path could not produce.

// runtimeMetadataFull is the metadata value used below: one query provenance
// string and the two formerly exported flags.
func runtimeMetadataFull() NodeRuntimeMetadata {
	return NodeRuntimeMetadata{Database: "runtime:db", EmbeddingsStoredSeparately: true, VisibilitySuppressed: true}
}

// runtimeIdentityFixture is the expected embedding identity, kept apart from
// metadata because a metadata-only change must never move it.
type runtimeIdentityFixture struct {
	state   nodeRuntime
	attempt *EmbeddingAttempt
}

func requireRuntimeIdentity(t *testing.T, label string, node *Node, want runtimeIdentityFixture) {
	t.Helper()
	got := node.runtimeState()
	require.Equal(t, want.state.embeddingOwner, got.embeddingOwner, "%s: embedding owner", label)
	require.Equal(t, want.state.embeddingOwnerID, got.embeddingOwnerID, "%s: embedding owner ID", label)
	require.Equal(t, want.state.embeddingRevision, got.embeddingRevision, "%s: embedding revision", label)
	require.Same(t, want.attempt, got.embeddingAttempt, "%s: embedding attempt", label)
	require.Equal(t, want.state.embeddingPublication, got.embeddingPublication, "%s: publication proof", label)
}

func requireRuntimeMetadata(t *testing.T, label string, node *Node, want NodeRuntimeMetadata) {
	t.Helper()
	require.Equal(t, want, node.RuntimeMetadata(), "%s: runtime metadata", label)
}

// runtimeNewEngine opens a fresh tracked engine, closing it even when a later
// assertion fails.
func runtimeNewEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	engine.SetEmbeddingsEnabled(false)
	return engine
}

// runtimeSyntheticIdentity is a populated identity record carrying all three
// metadata fields and every embedding-identity field. A real engine owner is not
// needed to observe what a copy preserves.
func runtimeSyntheticIdentity() nodeRuntime {
	return nodeRuntime{
		metadata:             runtimeMetadataFull(),
		embeddingRevision:    EmbeddingBodyRevision{physical: 61, bufferedSequence: 13},
		embeddingAttempt:     &EmbeddingAttempt{ProviderGeneration: 3, ClaimEpoch: 7},
		embeddingPublication: embeddingPublicationProof{id: "runtime:doc", commitTs: 71},
	}
}

// TestNodeRuntimeCopyShapesPreserveMetadataAndIdentity covers the copy shapes the
// caches and transactions use. Every shape keeps the metadata value and the
// shared identity record; only the embedding vectors differ.
func TestNodeRuntimeCopyShapesPreserveMetadataAndIdentity(t *testing.T) {
	state := runtimeSyntheticIdentity()
	state.embeddingOwnerID = "runtime:doc"
	source := &Node{
		runtime:         &state,
		ID:              "runtime:doc",
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"text": "body"},
		NamedEmbeddings: map[string][]float32{"title": {1, 2}},
		ChunkEmbeddings: [][]float32{{3, 4}},
		EmbedMeta:       map[string]any{"chunk_count": 1},
	}
	want := runtimeIdentityFixture{state: state, attempt: state.embeddingAttempt}
	for name, copyFn := range map[string]func(*Node) *Node{
		"copyNode":                  copyNode,
		"copyNodeForCaller":         copyNodeForCaller,
		"copyNodeWithoutEmbeddings": copyNodeWithoutEmbeddings,
		"CopyNode":                  CopyNode,
		"value copy":                func(n *Node) *Node { copied := *n; return &copied },
	} {
		t.Run(name, func(t *testing.T) {
			copied := copyFn(source)
			requireRuntimeMetadata(t, name, copied, state.metadata)
			requireRuntimeIdentity(t, name, copied, want)
			require.Same(t, source.runtime, copied.runtime, "copies share the immutable record")
		})
	}

	// A metadata-only change on one copy replaces its record and leaves the
	// source's metadata, identity and record untouched.
	copied := copyNode(source)
	changed := copied.RuntimeMetadata()
	changed.Database = "other:db"
	changed.VisibilitySuppressed = false
	copied.SetRuntimeMetadata(changed)
	requireRuntimeMetadata(t, "changed copy", copied, changed)
	requireRuntimeIdentity(t, "changed copy", copied, want)
	requireRuntimeMetadata(t, "source", source, want.state.metadata)
	requireRuntimeIdentity(t, "source", source, want)
	require.NotSame(t, source.runtime, copied.runtime, "a metadata change allocates its own record")

	// Setting unchanged metadata allocates nothing and keeps the record.
	before := copied.runtime
	require.Zero(t, testing.AllocsPerRun(100, func() { copied.SetRuntimeMetadata(changed) }),
		"an unchanged metadata write must not allocate")
	require.Same(t, before, copied.runtime, "an unchanged metadata write keeps the record")

	// An identity-only change on one copy leaves every other copy's metadata.
	other := copyNode(source)
	otherState := other.runtimeState()
	otherState.embeddingAttempt = &EmbeddingAttempt{ProviderGeneration: 9}
	other.setRuntimeState(otherState)
	requireRuntimeMetadata(t, "identity change", other, want.state.metadata)
	requireRuntimeMetadata(t, "source", source, want.state.metadata)
	require.Same(t, want.attempt, source.EmbeddingAttempt(), "the source keeps its own attempt")
}

// TestNodeRuntimeCacheStorePreservesMetadataAndPolicy checks cacheStoreNode: it
// keeps every metadata field as written but publishes no worker identity (the
// written revision is zeroed and the attempt cleared), so a cache-hit copy
// answers reads without exposing an in-flight token. The publication proof is
// carried unchanged from the written image.
func TestNodeRuntimeCacheStorePreservesMetadataAndPolicy(t *testing.T) {
	engine := runtimeNewEngine(t)
	state := runtimeSyntheticIdentity()
	state.embeddingOwner = engine
	state.embeddingOwnerID = "runtime:doc"
	written := &Node{runtime: &state, ID: "runtime:doc", Labels: []string{"Doc"}, Properties: map[string]any{"v": int64(1)}}
	engine.cacheStoreNode(written, engine.db.oracle.published.Load())

	engine.nodeCacheMu.RLock()
	cached, ok := engine.nodeCache[written.ID]
	engine.nodeCacheMu.RUnlock()
	require.True(t, ok, "cacheStoreNode publishes the written image")
	require.NotSame(t, written, cached, "the cache keeps its own image")
	requireRuntimeMetadata(t, "cache entry", cached, state.metadata)
	cachedState := cached.runtimeState()
	require.Same(t, engine, cachedState.embeddingOwner, "the cache entry records its engine owner")
	require.Equal(t, written.ID, cachedState.embeddingOwnerID)
	require.Zero(t, cachedState.embeddingRevision, "a written image carries no covered revision")
	require.Nil(t, cachedState.embeddingAttempt, "no worker token is published through the cache")
	require.Equal(t, state.embeddingPublication, cachedState.embeddingPublication,
		"the store changes only the covered revision and attempt")

	// A caller copy of the cache entry keeps the metadata and the nil attempt,
	// and changing its metadata cannot reach the cache.
	caller := copyNodeForCaller(cached)
	requireRuntimeMetadata(t, "caller copy", caller, state.metadata)
	require.Nil(t, caller.EmbeddingAttempt())
	modified := caller.RuntimeMetadata()
	modified.Database = "caller:db"
	modified.EmbeddingsStoredSeparately = false
	caller.SetRuntimeMetadata(modified)
	engine.nodeCacheMu.RLock()
	defer engine.nodeCacheMu.RUnlock()
	requireRuntimeMetadata(t, "cache entry after caller change", engine.nodeCache[written.ID], state.metadata)
	require.Same(t, engine, engine.nodeCache[written.ID].runtimeState().embeddingOwner)
}

// TestNodeRuntimeCacheFillFormsPreserveMetadataAndIdentity covers both fill
// modes. A full fill deep-copies the body (vectors too) and keeps its identity;
// a query fill shares vectors but still owns labels, properties and the record.
func TestNodeRuntimeCacheFillFormsPreserveMetadataAndIdentity(t *testing.T) {
	engine := runtimeNewEngine(t)
	for _, mode := range []nodeReadCacheMode{nodeReadCacheFull, nodeReadCacheQuery} {
		name := map[nodeReadCacheMode]string{nodeReadCacheFull: "full", nodeReadCacheQuery: "query"}[mode]
		t.Run(name, func(t *testing.T) {
			state := runtimeSyntheticIdentity()
			state.embeddingOwner = engine
			state.embeddingOwnerID = "runtime:fill"
			read := &Node{
				runtime:         &state,
				ID:              "runtime:fill",
				Labels:          []string{"Doc"},
				Properties:      map[string]any{"v": int64(1)},
				NamedEmbeddings: map[string][]float32{"title": {1, 2}},
				ChunkEmbeddings: [][]float32{{3, 4}},
			}
			engine.cacheDeleteNode(read.ID, nodeCacheAllForms)
			engine.cacheFillNode(engine.nodeCacheGen.current(), read, mode, engine.db.oracle.published.Load())

			var cached *Node
			if mode == nodeReadCacheQuery {
				cached = engine.queryNodeCache[read.ID]
			} else {
				cached = engine.nodeCache[read.ID]
			}
			require.NotNil(t, cached, "the fill publishes the %s cache entry", name)
			require.NotSame(t, read, cached, "the fill owns its body")
			requireRuntimeMetadata(t, name, cached, state.metadata)
			requireRuntimeIdentity(t, name, cached, runtimeIdentityFixture{state: state, attempt: state.embeddingAttempt})

			// A write to the read's image cannot reach the cached body.
			read.Properties["v"] = int64(99)
			read.Labels[0] = "Mutated"
			read.ChunkEmbeddings[0][0] = 99
			require.Equal(t, int64(1), cached.Properties["v"], "%s: cached properties ignore the read image", name)
			require.Equal(t, []string{"Doc"}, cached.Labels, "%s: cached labels ignore the read image", name)
			if mode == nodeReadCacheFull {
				require.Equal(t, float32(3), cached.ChunkEmbeddings[0][0], "full fill deep-copies vectors")
			} else {
				require.Equal(t, float32(99), cached.ChunkEmbeddings[0][0], "query fill shares in-memory vectors")
			}
			requireRuntimeMetadata(t, name, cached, state.metadata)
		})
	}

	// A fill captured at the pre-write generation is dropped; the write's own
	// image stays published.
	const staleID NodeID = "runtime:stale"
	stale := &Node{runtime: &nodeRuntime{metadata: runtimeMetadataFull()}, ID: staleID, Labels: []string{"Doc"}}
	staleGen := engine.nodeCacheGen.current()
	require.NoError(t, engine.UpdateNode(NodeUpdateRequest{After: &Node{ID: staleID, Labels: []string{"Doc"}}}))
	engine.cacheFillNode(staleGen, stale, nodeReadCacheFull, engine.db.oracle.published.Load())
	engine.nodeCacheMu.RLock()
	cachedStale, ok := engine.nodeCache[staleID]
	engine.nodeCacheMu.RUnlock()
	require.True(t, ok, "the write publishes its image")
	requireRuntimeMetadata(t, "written image", cachedStale, NodeRuntimeMetadata{})
	require.NotSame(t, stale, cachedStale, "the dropped fill did not publish the stale body")
}

// TestNodeRuntimeBodyCacheStoreLoadPreservesMetadataAndEvidence covers the
// versioned body cache used by stored reads. It keeps the metadata and the
// embedding identity of the body it stores while projecting the vectors away.
func TestNodeRuntimeBodyCacheStoreLoadPreservesMetadataAndEvidence(t *testing.T) {
	engine := runtimeNewEngine(t)
	state := runtimeSyntheticIdentity()
	state.embeddingOwnerID = "runtime:body"
	state.embeddingRevision = EmbeddingBodyRevision{physical: 123}
	separate := &Node{
		runtime:         &state,
		ID:              "runtime:body",
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"text": "stored"},
		ChunkEmbeddings: [][]float32{{1, 2, 3}},
		EmbedMeta:       map[string]any{"chunk_count": 1},
	}
	engine.cacheStoreNodeBody(separate.ID, 123, separate, engine.db.oracle.published.Load())

	engine.nodeBodyCacheMu.RLock()
	entry, ok := engine.nodeBodyCache[separate.ID]
	engine.nodeBodyCacheMu.RUnlock()
	require.True(t, ok, "a separately stored body is cached")
	requireRuntimeMetadata(t, "body entry", entry.node, state.metadata)
	requireRuntimeIdentity(t, "body entry", entry.node, runtimeIdentityFixture{state: state, attempt: state.embeddingAttempt})

	hit, ok := engine.cacheLoadNodeBody(separate.ID, 123, nil, nodeBodyDetached)
	require.True(t, ok)
	requireRuntimeMetadata(t, "body hit", hit, state.metadata)
	requireRuntimeIdentity(t, "body hit", hit, runtimeIdentityFixture{state: state, attempt: state.embeddingAttempt})
	require.Empty(t, hit.ChunkEmbeddings, "the body cache keeps no vectors")
	hit.Properties["text"] = "mutated"
	engine.nodeBodyCacheMu.RLock()
	require.Equal(t, "stored", engine.nodeBodyCache[separate.ID].node.Properties["text"], "a hit is the caller's copy")
	engine.nodeBodyCacheMu.RUnlock()

	// A different physical version is a miss and evicts the stale entry.
	_, ok = engine.cacheLoadNodeBody(separate.ID, 124, nil, nodeBodyDetached)
	require.False(t, ok, "the body cache is version checked")
	engine.nodeBodyCacheMu.RLock()
	_, stillCached := engine.nodeBodyCache[separate.ID]
	engine.nodeBodyCacheMu.RUnlock()
	require.False(t, stillCached, "a version mismatch removes the stale entry")

	// Inline vectors cannot be reconstructed from separate keys: the body cache
	// must refuse the lossy projection and leave no entry behind.
	inline := &Node{ID: "runtime:inline", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1}}}
	engine.cacheStoreNodeBody(inline.ID, 1, inline, engine.db.oracle.published.Load())
	engine.nodeBodyCacheMu.RLock()
	_, cachedInline := engine.nodeBodyCache[inline.ID]
	engine.nodeBodyCacheMu.RUnlock()
	require.False(t, cachedInline, "an inline-vector body is not cached as a vector-free projection")
}

// TestNodeRuntimeStoredBodyExcludesProvenanceAndReadsIdentity drives the stored
// codec (V2 tokenized body) directly: the two flags persist and Database never
// does. The engine read that decodes the body itself attaches its own owner,
// owner ID and physical-version revision; a cache hit keeps the identity its
// body carried, and no attempt or publication proof ever crosses the store.
func TestNodeRuntimeStoredBodyExcludesProvenanceAndReadsIdentity(t *testing.T) {
	engine := runtimeNewEngine(t)
	const id NodeID = "runtime:stored"
	written := &Node{
		runtime:    &nodeRuntime{metadata: runtimeMetadataFull()},
		ID:         id,
		Labels:     []string{"Doc"},
		Properties: map[string]any{"text": "stored"},
	}
	_, err := engine.CreateNode(CopyNode(written))
	require.NoError(t, err)

	var body []byte
	var physical uint64
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		item, getErr := txn.Get(nodeKey(id))
		if getErr != nil {
			return getErr
		}
		physical = item.Version()
		return item.Value(func(value []byte) error {
			body = append([]byte(nil), value...)
			return nil
		})
	}))
	require.NotEmpty(t, body)
	require.Equal(t, nodeFormatTokenizedV1, body[0], "the stored body is the unchanged V2 format")
	decoded, err := engine.decodeNode("runtime", body)
	require.NoError(t, err)
	requireRuntimeMetadata(t, "decoded store", decoded, NodeRuntimeMetadata{
		Database:                   "",
		EmbeddingsStoredSeparately: true,
		VisibilitySuppressed:       true,
	})
	require.Nil(t, decoded.EmbeddingAttempt())
	require.Zero(t, decoded.EmbeddingRevision())
	require.Nil(t, decoded.runtimeState().embeddingOwner)
	require.Equal(t, embeddingPublicationProof{}, decoded.runtimeState().embeddingPublication)

	// A decode miss attaches the reading engine and the record's version; the
	// three metadata fields are the stored ones and no worker evidence appears.
	wantRevision := EmbeddingBodyRevision{physical: physical}
	for name, read := range map[string]func() (*Node, error){
		"readNodeMetadata/bodyOnly": func() (*Node, error) {
			return engine.readNodeMetadata(context.Background(), id, storedRead, nodeItemBodyOnly)
		},
		"readNodeMetadata/query": func() (*Node, error) {
			return engine.readNodeMetadata(context.Background(), id, storedRead, nodeItemQueryMetadata)
		},
	} {
		t.Run(name, func(t *testing.T) {
			engine.cacheDeleteNode(id, nodeCacheAllForms)
			node, err := read()
			require.NoError(t, err)
			require.Equal(t, id, node.ID)
			require.Same(t, engine, node.runtimeState().embeddingOwner, "the decode attaches its engine owner")
			require.Equal(t, id, node.runtimeState().embeddingOwnerID)
			require.Equal(t, wantRevision, node.EmbeddingRevision(),
				"the read revision is the record's physical version, not an encoded value")
			require.Nil(t, node.EmbeddingAttempt(), "no worker token comes from storage")
			require.Equal(t, embeddingPublicationProof{}, node.runtimeState().embeddingPublication)
			require.True(t, node.RuntimeMetadata().EmbeddingsStoredSeparately, "the stored flag survives the decode")
			require.True(t, node.RuntimeMetadata().VisibilitySuppressed)
			require.Empty(t, node.RuntimeMetadata().Database, "the stored body carries no Database")
		})
	}

	// A public read of the image the write cached keeps the original body's
	// provenance, owner and flags; the cache never re-derives them.
	stored, err := engine.GetStoredNode(id)
	require.NoError(t, err)
	requireRuntimeMetadata(t, "GetStoredNode", stored, NodeRuntimeMetadata{
		EmbeddingsStoredSeparately: true,
		VisibilitySuppressed:       true,
	})
	require.Same(t, engine, stored.runtimeState().embeddingOwner)
	require.Equal(t, id, stored.runtimeState().embeddingOwnerID)
	require.Nil(t, stored.EmbeddingAttempt())
	require.Equal(t, embeddingPublicationProof{}, stored.runtimeState().embeddingPublication)
}

// TestNodeRuntimeNamespaceProjectionPreservesMetadataAndIdentity checks the
// user-facing projection: the namespace's database provenance replaces the
// stored one, the other metadata is retained, and identity is untouched because
// toUserNode copies the record before changing it.
func TestNodeRuntimeNamespaceProjectionPreservesMetadataAndIdentity(t *testing.T) {
	engine := runtimeNewEngine(t)
	const namespace = "runtime"
	const userID NodeID = "doc"
	state := runtimeSyntheticIdentity()
	state.embeddingPublication = embeddingPublicationProof{id: "runtime:doc", commitTs: 91}
	written := &Node{
		runtime:         &state,
		ID:              "runtime:doc",
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"text": "namespace body"},
		ChunkEmbeddings: [][]float32{{1, 2}},
		EmbedMeta:       map[string]any{"chunk_count": 1},
	}
	// NodeWriteInput copies the record; the namespace only rewrites provenance.
	_, err := engine.CreateNode(copyNode(written))
	require.NoError(t, err)
	store := NewNamespacedEngine(engine, namespace)

	// The identity each public read must report, and the settled legacy flag it
	// observes, are taken from the same public read class rather than asserted
	// from an encoded value: a read settles EmbeddingsStoredSeparately against
	// the embedding state it hydrates (loadNodeEmbeddingState).
	probe, err := store.GetNode(userID)
	require.NoError(t, err)
	probeState := probe.runtimeState()
	require.Same(t, engine, probeState.embeddingOwner)
	require.Equal(t, NodeID("runtime:doc"), probeState.embeddingOwnerID)
	wantIdentity := runtimeIdentityFixture{state: probeState, attempt: probeState.embeddingAttempt}
	// The namespace rewrites Database and leaves the suppression flag exactly as
	// stored.
	require.Equal(t, state.metadata.VisibilitySuppressed, probe.RuntimeMetadata().VisibilitySuppressed,
		"the namespace does not rewrite the suppression flag")
	wantMetadata := NodeRuntimeMetadata{
		Database:                   namespace,
		EmbeddingsStoredSeparately: probe.RuntimeMetadata().EmbeddingsStoredSeparately,
		VisibilitySuppressed:       state.metadata.VisibilitySuppressed,
	}

	named := []struct {
		name string
		read func(NodeID) (*Node, error)
	}{
		{"GetNode", store.GetNode},
		{"GetQueryNode", store.GetQueryNode},
		{"GetNodeWithoutEmbeddings", store.GetNodeWithoutEmbeddings},
		{"GetNodeLatestVisible", store.GetNodeLatestVisible},
	}
	for _, tc := range named {
		t.Run(tc.name, func(t *testing.T) {
			node, err := tc.read(userID)
			require.NoError(t, err)
			require.Equal(t, userID, node.ID, "the namespace prefix is stripped")
			requireRuntimeMetadata(t, tc.name, node, wantMetadata)
			requireRuntimeIdentity(t, tc.name, node, wantIdentity)
			require.Equal(t, EntityIdentity{Database: namespace, LocalID: string(userID)}, node.Identity())
		})
	}

	// The stored image keeps no provenance; the namespace supplies it.
	raw, err := engine.GetNode("runtime:doc")
	require.NoError(t, err)
	require.Empty(t, raw.RuntimeMetadata().Database, "the stored body carries no Database")
	require.True(t, raw.RuntimeMetadata().VisibilitySuppressed)

	// Projected and batch reads reach the same projection owner.
	projected, err := store.GetNodeProjected(userID, []string{"text"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"text": "namespace body"}, projected.Properties)
	requireRuntimeMetadata(t, "GetNodeProjected", projected, wantMetadata)
	requireRuntimeIdentity(t, "GetNodeProjected", projected, wantIdentity)
	batch, err := store.BatchGetNodes([]NodeID{userID})
	require.NoError(t, err)
	require.Len(t, batch, 1)
	requireRuntimeMetadata(t, "BatchGetNodes", batch[userID], wantMetadata)
	requireRuntimeIdentity(t, "BatchGetNodes", batch[userID], wantIdentity)
	light, err := store.BatchGetNodesWithoutEmbeddings([]NodeID{userID})
	require.NoError(t, err)
	requireRuntimeMetadata(t, "BatchGetNodesWithoutEmbeddings", light[userID], wantMetadata)
	requireRuntimeIdentity(t, "BatchGetNodesWithoutEmbeddings", light[userID], wantIdentity)
	require.Empty(t, light[userID].ChunkEmbeddings)
}

// TestNodeRuntimeCacheHitCopiesAreIndependent verifies the published-cache
// contract end to end: consecutive public reads return distinct images whose
// labels, properties and metadata are their own, while the cached image and the
// other reads keep the original values.
func TestNodeRuntimeCacheHitCopiesAreIndependent(t *testing.T) {
	engine := runtimeNewEngine(t)
	store := NewNamespacedEngine(engine, "hits")
	_, err := store.CreateNode(&Node{
		ID:              "doc",
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"text": "cached"},
		NamedEmbeddings: map[string][]float32{"title": {0.5, 1.5}},
	})
	require.NoError(t, err)

	first, err := store.GetNode("doc")
	require.NoError(t, err)
	hitsBefore := atomic.LoadInt64(&engine.cacheHits)
	second, err := store.GetNode("doc")
	require.NoError(t, err)
	require.Greater(t, atomic.LoadInt64(&engine.cacheHits), hitsBefore, "the second read is served from the node cache")
	require.NotSame(t, first, second, "each cache hit is a caller-owned image")

	base := NodeRuntimeMetadata{Database: "hits"}
	requireRuntimeMetadata(t, "first read", first, base)
	requireRuntimeMetadata(t, "second read", second, base)

	first.Properties["text"] = "caller write"
	first.Properties["added"] = true
	first.Labels[0] = "Caller"
	mutated := first.RuntimeMetadata()
	mutated.VisibilitySuppressed = true
	mutated.EmbeddingsStoredSeparately = true
	first.SetRuntimeMetadata(mutated)

	requireRuntimeMetadata(t, "second read after caller write", second, base)
	require.Equal(t, map[string]any{"text": "cached"}, second.Properties, "a caller cannot write the cached body")
	require.Equal(t, []string{"Doc"}, second.Labels)
	third, err := store.GetNode("doc")
	require.NoError(t, err)
	requireRuntimeMetadata(t, "third read", third, base)
	require.Equal(t, map[string]any{"text": "cached"}, third.Properties)
	require.False(t, third.RuntimeMetadata().VisibilitySuppressed)
	require.False(t, third.RuntimeMetadata().EmbeddingsStoredSeparately)

	// The cached image itself is unchanged; the stored body has no provenance.
	engine.nodeCacheMu.RLock()
	cached, ok := engine.nodeCache["hits:doc"]
	engine.nodeCacheMu.RUnlock()
	require.True(t, ok)
	requireRuntimeMetadata(t, "cache entry", cached, NodeRuntimeMetadata{})
	require.Equal(t, map[string]any{"text": "cached"}, cached.Properties)
	require.Equal(t, []string{"Doc"}, cached.Labels)
}

// TestNodeRuntimeMetadataChangeKeepsPublicationAcknowledgement pins the ACK
// barrier: after an embedding sidecar commit the accepted body carries a
// publication proof, and a metadata-only change on a copy of that image (the
// namespace projection does exactly this) must leave the proof, revision and
// attempt intact so the ordered publication check still accepts the same body.
func TestNodeRuntimeMetadataChangeKeepsPublicationAcknowledgement(t *testing.T) {
	engine := runtimeNewEngine(t)
	engine.SetEmbeddingsEnabled(true)
	const id NodeID = "ack:doc"
	_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": "accepted"}})
	require.NoError(t, err)
	payload, err := engine.GetNode(id)
	require.NoError(t, err)
	payload, err = SnapshotNodeForEmbedding(payload)
	require.NoError(t, err)
	payload.ChunkEmbeddings = [][]float32{{1, 0}}
	payload.EmbedMeta = map[string]any{"chunk_count": 1}
	attempt := NewEmbeddingAttempt(1, 1, payload.EmbeddingRevision(), func() (func(), bool) { return func() {}, true }, func(*Node) {})
	payload.SetEmbeddingAttempt(attempt)

	var accepted *Node
	engine.OnNodeUpdated(func(node *Node) {
		if accepted == nil {
			accepted = node
		}
	})
	require.NoError(t, engine.UpdateNodeEmbeddingSidecar(payload))
	require.NotNil(t, accepted, "the acknowledged body reaches the ordered listener")
	proof := accepted.runtimeState().embeddingPublication
	require.NotZero(t, proof.commitTs, "the ACK carries its own commit boundary")
	require.True(t, proof.matches(engine, id, accepted.EmbeddingRevision()))
	revision := accepted.EmbeddingRevision()
	same, err := engine.embeddingRevisionIsCurrent(context.Background(), id, revision, proof)
	require.NoError(t, err)
	require.True(t, same, "the accepted body is current at publication time")
	acceptedMetadata := accepted.RuntimeMetadata()

	// The published body is immutable evidence: a metadata change is made on a
	// copy, and that copy keeps the ACK identity.
	upstream := CopyNode(accepted)
	require.NotSame(t, accepted, upstream)
	metadata := upstream.RuntimeMetadata()
	metadata.Database = "ack"
	metadata.VisibilitySuppressed = true
	upstream.SetRuntimeMetadata(metadata)
	requireRuntimeMetadata(t, "projected ACK copy", upstream, metadata)
	require.Equal(t, revision, upstream.EmbeddingRevision(), "metadata does not move the revision")
	require.Same(t, attempt, upstream.EmbeddingAttempt())
	require.Equal(t, proof, upstream.runtimeState().embeddingPublication, "the projected copy keeps the commit proof")
	require.True(t, upstream.runtimeState().embeddingPublication.matches(engine, id, upstream.EmbeddingRevision()))

	// The ACK image itself is unchanged by the copy's metadata write.
	requireRuntimeMetadata(t, "acknowledged body", accepted, acceptedMetadata)
	require.Equal(t, revision, accepted.EmbeddingRevision())
	require.Same(t, attempt, accepted.EmbeddingAttempt())
	require.Equal(t, proof, accepted.runtimeState().embeddingPublication)
	require.NotSame(t, accepted.runtime, upstream.runtime, "the copy owns the changed record")
}

// TestNodeRuntimeReplicationRetainsProvenanceAndExcludesIdentity is the
// transport half of the wire contract: Database and both flags travel, while
// owner, revision, attempt and publication proof do not.
func TestNodeRuntimeReplicationRetainsProvenanceAndExcludesIdentity(t *testing.T) {
	state := runtimeSyntheticIdentity()
	state.embeddingOwnerID = "runtime:wire"
	node := &Node{
		runtime:    &state,
		ID:         "runtime:wire",
		Labels:     []string{"Doc"},
		Properties: map[string]any{"text": "wire"},
	}
	payload, err := EncodeNodePayload(node)
	require.NoError(t, err)
	decoded, err := DecodeNodePayload(payload)
	require.NoError(t, err)
	requireRuntimeMetadata(t, "transport", decoded, state.metadata)
	require.Nil(t, decoded.EmbeddingAttempt(), "attempt identity is never encoded")
	require.Zero(t, decoded.EmbeddingRevision())
	require.Nil(t, decoded.runtimeState().embeddingOwner)
	require.Equal(t, embeddingPublicationProof{}, decoded.runtimeState().embeddingPublication)

	// A zero node needs no record and decodes to a zero node again.
	zero := &Node{ID: "runtime:zero"}
	require.Nil(t, zero.runtime)
	payload, err = EncodeNodePayload(zero)
	require.NoError(t, err)
	decoded, err = DecodeNodePayload(payload)
	require.NoError(t, err)
	require.Nil(t, decoded.runtime, "a decoded zero node needs no record")
	require.Zero(t, decoded.RuntimeMetadata())

	// Stored and transport maps stay distinct shapes: Database travels only in
	// the transport map.
	stored, err := encodeValue(node)
	require.NoError(t, err)
	require.NotEqual(t, payload, stored)
	require.False(t, bytes.Contains(stored, []byte("Database")), "the stored map has no Database key")
	require.True(t, bytes.Contains(payload, []byte("Database")), "the transport map keeps Database")
}
