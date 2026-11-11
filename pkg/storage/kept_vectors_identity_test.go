package storage

import (
	"context"
	"encoding/json"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// NOR-412: a record that leaves out the vectors an update kept says which
// vectors those were, and replay restores them only from a predecessor that
// holds exactly those. These tests cover the identity itself, the record that
// carries it, and every way a replayed record meets a predecessor: the kept
// vectors, other vectors, none, and a record written before records
// identified what they kept.

func TestVectorImageIdentityIsBitExact(t *testing.T) {
	negativeZero := math.Float32frombits(0x80000000)
	nan := math.Float32frombits

	named := map[string][]float32{"a": {1, 0}, "b": {2}}
	require.Equal(t, namedVectorImageID(named), namedVectorImageID(map[string][]float32{"b": {2}, "a": {1, 0}}))
	for name, other := range map[string]map[string][]float32{
		"the sign of a zero":         {"a": {1, negativeZero}, "b": {2}},
		"an element in another name": {"a": {1}, "b": {0, 2}},
		"another name":               {"a": {1, 0}, "c": {2}},
		"a missing vector":           {"a": {1, 0}},
		"a longer vector":            {"a": {1, 0, 0}, "b": {2}},
		"an empty extra vector":      {"a": {1, 0}, "b": {2}, "c": {}},
	} {
		require.NotEqual(t, namedVectorImageID(named), namedVectorImageID(other), name)
	}
	require.Equal(t, namedVectorImageID(map[string][]float32{"a": {nan(0x7fc00001)}}), namedVectorImageID(map[string][]float32{"a": {nan(0x7fc00001)}}))
	require.NotEqual(t, namedVectorImageID(map[string][]float32{"a": {nan(0x7fc00001)}}), namedVectorImageID(map[string][]float32{"a": {nan(0x7fc00002)}}))

	managed := func(chunks [][]float32, meta map[string]any) vectorImageID {
		id, ok := managedVectorImageID(chunks, meta)
		require.True(t, ok)
		return id
	}
	meta := map[string]any{"has_embedding": true}
	base := managed([][]float32{{1, 2}, {3, 0}}, meta)
	require.Equal(t, base, managed([][]float32{{1, 2}, {3, 0}}, map[string]any{"has_embedding": true}))
	for name, other := range map[string][][]float32{
		"another chunk boundary": {{1}, {2, 3, 0}},
		"one chunk":              {{1, 2, 3, 0}},
		"the sign of a zero":     {{1, 2}, {3, negativeZero}},
		"a missing chunk":        {{1, 2}},
		"an empty extra chunk":   {{1, 2}, {3, 0}, {}},
	} {
		require.NotEqual(t, base, managed(other, meta), name)
	}
	require.NotEqual(t, base, managed([][]float32{{1, 2}, {3, 0}}, map[string]any{"has_embedding": false}), "the metadata kept with the vectors is part of the image")
	require.Equal(t, managed([][]float32{{1}}, nil), managed([][]float32{{1}}, map[string]any{}), "empty metadata reads as none")
	require.NotEqual(t, managed(nil, nil), namedVectorImageID(nil), "the two kinds of image never share an identity")
}

// Embedding metadata is held in different Go types in memory, after a read
// from the store, after a journal record and after a snapshot. Its identity
// follows the stored value, as sameStoredValue compares it.
func TestManagedImageIdentityFollowsTheStoredValue(t *testing.T) {
	chunks := keptVectors(2, 1)
	identity := func(meta map[string]any) vectorImageID {
		id, ok := managedVectorImageID(chunks, meta)
		require.True(t, ok)
		return id
	}
	written := map[string]any{
		"has_embedding": true, "chunk_count": 2, "embedding_dimensions": keptVectorDims, "embedding_model": "synthetic",
		"embedded_at": "2026-11-11T00:00:00Z", "score": 0.5, "chunk_spans": []any{[]any{int64(0), "text", int64(0), int64(10)}, []any{int64(1), "text", int64(10), int64(20)}},
	}
	base := identity(written)

	same := map[string]map[string]any{
		"other integer types": {
			"has_embedding": true, "chunk_count": int64(2), "embedding_dimensions": uint16(keptVectorDims), "embedding_model": "synthetic",
			"embedded_at": "2026-11-11T00:00:00Z", "score": float64(0.5), "chunk_spans": [][]any{{0, "text", 0, 10}, {1, "text", 10, 20}},
		},
		"numbers as a JSON reader returns them": {
			"has_embedding": true, "chunk_count": float64(2), "embedding_dimensions": float64(keptVectorDims), "embedding_model": "synthetic",
			"embedded_at": "2026-11-11T00:00:00Z", "score": float32(0.5), "chunk_spans": []any{[]any{float64(0), "text", float64(0), float64(10)}, []any{int8(1), "text", uint16(10), uint64(20)}},
		},
	}
	var stored map[string]any
	data, err := encodeValue(written)
	require.NoError(t, err)
	require.NoError(t, decodeValue(data, &stored))
	same["read back from the store"] = stored
	var fromJSON map[string]any
	data, err = json.Marshal(written)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &fromJSON))
	same["read back from JSON"] = fromJSON
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "meta", Mode: CommitModeExplicit, Operations: []Operation{
		{Type: OpCreateNode, Node: &Node{ID: "test:meta", ChunkEmbeddings: chunks, EmbedMeta: written}},
	}})
	require.NoError(t, err)
	_, ops, err := DecodeEntityTransaction(payload)
	require.NoError(t, err)
	same["decoded from a journal record"] = ops[0].Node.EmbedMeta
	for name, meta := range same {
		require.Equal(t, base, identity(meta), name)
	}

	otherSpans := []any{[]any{0, "text", 0, 10}, []any{1, "text", 10, 21}}
	for name, edit := range map[string]func(map[string]any){
		"another model":       func(meta map[string]any) { meta["embedding_model"] = "other" },
		"another time":        func(meta map[string]any) { meta["embedded_at"] = "2026-11-11T00:00:01Z" },
		"another score":       func(meta map[string]any) { meta["score"] = 0.25 },
		"another span":        func(meta map[string]any) { meta["chunk_spans"] = otherSpans },
		"an added key":        func(meta map[string]any) { meta["embedding_space"] = "s" },
		"a removed key":       func(meta map[string]any) { delete(meta, "score") },
		"a number as a text":  func(meta map[string]any) { meta["embedding_dimensions"] = "256" },
		"a truth as a number": func(meta map[string]any) { meta["has_embedding"] = 1 },
	} {
		other := map[string]any{}
		for key, value := range written {
			other[key] = value
		}
		edit(other)
		require.NotEqual(t, base, identity(other), name)
	}

	// The keys the store writes and rewrites by itself to lay out and date
	// the image are not part of it: the body codec adds chunk_count when it
	// stores vectors apart, the sidecar reader retypes it, and every kept
	// update renews the sidecar's content stamp.
	for name, edit := range map[string]func(map[string]any){
		"another count":       func(meta map[string]any) { meta["chunk_count"] = 3 },
		"no count":            func(meta map[string]any) { delete(meta, "chunk_count") },
		"a content stamp":     func(meta map[string]any) { meta[embeddingContentUpdatedAtKey] = "2026-11-11T00:00:00.5Z" },
		"a range manifest":    func(meta map[string]any) { meta[embeddingRangeManifestKey] = []byte{1} },
		"a count as a number": func(meta map[string]any) { meta["chunk_count"] = float64(2) },
	} {
		other := map[string]any{}
		for key, value := range written {
			other[key] = value
		}
		edit(other)
		require.Equal(t, base, identity(other), name)
	}
	onlyLayout, ok := managedVectorImageID(chunks, map[string]any{"chunk_count": 0, embeddingContentUpdatedAtKey: "x"})
	require.True(t, ok)
	none, ok := managedVectorImageID(chunks, nil)
	require.True(t, ok)
	require.Equal(t, none, onlyLayout, "metadata holding only layout keys reads as none")

	// A value with no stored form cannot be identified. A stored read never
	// holds one: the codec decodes every value into a form the identity has.
	_, ok = managedVectorImageID(chunks, map[string]any{"odd": struct{ A int }{1}})
	require.False(t, ok)
}

// Identifying an image costs no allocation that grows with the image.
func TestVectorImageIdentityAllocatesNothingPerVector(t *testing.T) {
	if policyTestRaceDetector {
		t.Skip("allocation counts are measured without the race detector")
	}
	meta := map[string]any{"has_embedding": true, "chunk_count": 8}
	allocations := func(chunks int) float64 {
		node := &Node{ChunkEmbeddings: keptVectors(chunks, 1), EmbedMeta: meta}
		return testing.AllocsPerRun(20, func() { _, _ = managedVectorImageID(node.ChunkEmbeddings, node.EmbedMeta) })
	}
	small, large := allocations(8), allocations(800)
	t.Logf("allocations to identify a managed image: %v for 8 chunks, %v for 800", small, large)
	require.Equal(t, small, large)
	require.LessOrEqual(t, large, 1.0, "at most the one block of the hash chain")
}

// keptVectorsWithoutIdentities rewrites a record as the version-4 record that
// builds of this change wrote before records identified what they kept: the
// same marks and images, no identities.
func keptVectorsWithoutIdentities(t *testing.T, payload []byte) []byte {
	t.Helper()
	type raw = map[string]msgpack.RawMessage
	encode := func(v any) msgpack.RawMessage {
		out, err := msgpack.Marshal(v)
		require.NoError(t, err)
		return out
	}
	var wire raw
	require.NoError(t, msgpack.Unmarshal(payload, &wire))
	var changes []raw
	require.NoError(t, msgpack.Unmarshal(wire["Changes"], &changes))
	for _, change := range changes {
		var node raw
		require.NoError(t, msgpack.Unmarshal(change["Node"], &node))
		if node == nil {
			continue
		}
		delete(node, "RetainedManaged")
		delete(node, "RetainedNamed")
		change["Node"] = encode(node)
	}
	wire["Changes"] = encode(changes)
	wire["Version"] = encode(semanticTransactionUnidentifiedVersion)
	return encode(wire)
}

// keptVectorsWithEarlyIdentities rewrites a record as the version-5 record
// that builds of this change wrote while the identity was taken from the
// update's prepared image: the same marks and identity fields.
func keptVectorsWithEarlyIdentities(t *testing.T, payload []byte) []byte {
	t.Helper()
	var wire map[string]msgpack.RawMessage
	require.NoError(t, msgpack.Unmarshal(payload, &wire))
	version, err := msgpack.Marshal(semanticTransactionEarlyIdentityVersion)
	require.NoError(t, err)
	wire["Version"] = version
	out, err := msgpack.Marshal(wire)
	require.NoError(t, err)
	return out
}

func keptVectorsStoredKeys(t *testing.T, b *BadgerEngine, id NodeID) int {
	t.Helper()
	keys := 0
	require.NoError(t, b.db.View(func(txn *badger.Txn) error {
		return visitEmbeddingKeys(txn, id, func([]byte) error { keys++; return nil })
	}))
	return keys
}

// keptVectorsRefresh replaces the node's managed vectors through the embedding
// sidecar, as the embedding worker does. The journal gets no replayable record
// of it.
func keptVectorsRefresh(t *testing.T, b *BadgerEngine, id NodeID, chunks int, seed float32, model string) {
	t.Helper()
	stored, err := b.GetNode(t.Context(), id)
	require.NoError(t, err)
	embedded := CopyNode(stored)
	embedded.ChunkEmbeddings = keptVectors(chunks, seed)
	embedded.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": chunks, "embedding_dimensions": keptVectorDims, "embedding_model": model}
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(t.Context(), embedded))
}

func keptVectorsSnapshot(t *testing.T, b *BadgerEngine, w *WAL, dir string, edit func(*Snapshot)) string {
	t.Helper()
	snapshot, err := w.CreateSnapshot(b)
	require.NoError(t, err)
	if edit != nil {
		edit(snapshot)
	}
	path := filepath.Join(dir, "snapshot.json")
	require.NoError(t, SaveSnapshot(snapshot, path))
	return path
}

// requireKeptVectorsNotStored checks how replay reports a record whose kept
// named vectors the store does not hold: as the accepted record's outcome,
// with the reason as its diagnostic.
func requireKeptVectorsNotStored(t *testing.T, err error) {
	t.Helper()
	var accepted *AcceptedCommitError
	require.ErrorAs(t, err, &accepted)
	require.Equal(t, CommitRecoveryPending, accepted.State)
	require.ErrorIs(t, accepted.Err, ErrKeptVectorsNotStored)
}

func requireNoManagedVectors(t *testing.T, node *Node) {
	t.Helper()
	require.Empty(t, node.ChunkEmbeddings, "no vectors are presented as the ones the update kept")
	require.Nil(t, node.EmbedMeta, "nor their metadata")
	require.True(t, NodeNeedsEmbedding(node), "the node is in the state of one that was never embedded")
}

func TestKeptVectorsRecordIdentifiesWhatItOmits(t *testing.T) {
	b, _ := keptVectorsEngine(t, t.TempDir())
	keptVectorsSeed(t, b, "test:keep", "alpha", 8, 1)
	_, err := b.CreateNode(t.Context(), &Node{ID: "test:named", Labels: []string{"Doc"}, Properties: map[string]any{"text": "gamma"}, NamedEmbeddings: map[string][]float32{"title": {1, 2, 3}}})
	require.NoError(t, err)
	flag := func(n *Node) { n.Properties["flag"] = true }
	payload := keptVectorsPrepare(t, b, "identified", map[NodeID]func(*Node){"test:keep": flag, "test:named": flag})

	envelope, ops, err := DecodeEntityTransaction(payload)
	require.NoError(t, err)
	require.Equal(t, uint16(6), envelope.Version, "a record that omits kept vectors is version 6")
	require.Len(t, envelope.retainedVectorIDs, len(ops))
	for i, op := range ops {
		stored, err := b.GetNode(t.Context(), op.NodeID)
		require.NoError(t, err)
		switch op.retainedVectors {
		case retainedManagedEmbeddings:
			id, ok := managedVectorImageID(stored.ChunkEmbeddings, stored.EmbedMeta)
			require.True(t, ok)
			require.Equal(t, id, envelope.retainedVectorIDs[i].managed, "the record identifies the stored managed image")
		case retainedNamedEmbeddings:
			require.Equal(t, namedVectorImageID(stored.NamedEmbeddings), envelope.retainedVectorIDs[i].named, "the record identifies the stored named image")
		default:
			t.Fatalf("operation on %s carries mark %d", op.NodeID, op.retainedVectors)
		}
	}

	// A decoded operation no longer holds the image it kept, so nothing can
	// claim that image again from it.
	_, err = EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "again", Mode: CommitModeExplicit, Operations: ops})
	require.ErrorIs(t, err, ErrInvalidData)

	// The version-4 form of the same record is read, without identities, and
	// so is the version-5 form, whose identities replay cannot check.
	for version, form := range map[uint16][]byte{4: keptVectorsWithoutIdentities(t, payload), 5: keptVectorsWithEarlyIdentities(t, payload)} {
		envelope, old, err := DecodeEntityTransaction(form)
		require.NoError(t, err)
		require.Equal(t, version, envelope.Version)
		require.Nil(t, envelope.retainedVectorIDs)
		for _, op := range old {
			require.NotZero(t, op.retainedVectors)
		}
	}
}

// A rebuild of an empty store from a snapshot plus the journal meets whatever
// vectors the snapshot holds. The update's record says which vectors it kept:
// replay restores those, and presents no others as them.
func TestKeptVectorsRebuildChecksThePredecessor(t *testing.T) {
	flag := map[NodeID]func(*Node){"test:a": func(n *Node) { n.Properties["flag"] = true }}
	rebuild := func(t *testing.T, afterSnapshot func(b *BadgerEngine), beforeSnapshot func(b *BadgerEngine)) *Node {
		dir := t.TempDir()
		b, w := keptVectorsEngine(t, dir)
		beforeSnapshot(b)
		snapshot := keptVectorsSnapshot(t, b, w, dir, nil)
		afterSnapshot(b)
		keptVectorsJournal(t, w, keptVectorsPrepare(t, b, "after-snapshot", flag))
		crashClose(t, b, w)
		destination := newTestMemoryEngine(t)
		result, _, err := RecoverIntoEngine(t.Context(), destination, filepath.Join(dir, "wal"), snapshot)
		require.NoError(t, err)
		require.Zero(t, result.Failed, "%v", result.Errors)
		got, err := destination.GetNode(t.Context(), "test:a")
		require.NoError(t, err)
		require.Equal(t, true, got.Properties["flag"], "the update is applied")
		return got
	}
	seed := func(b *BadgerEngine) { keptVectorsSeed(t, b, "test:a", "unchanged text", 2, 1) }

	t.Run("the snapshot holds the kept vectors", func(t *testing.T) {
		got := rebuild(t, func(*BadgerEngine) {}, seed)
		require.Equal(t, keptVectors(2, 1), got.ChunkEmbeddings)
		require.Equal(t, true, got.EmbedMeta["has_embedding"])
		require.Equal(t, "synthetic", got.EmbedMeta["embedding_model"])
	})
	t.Run("the vectors were replaced after the snapshot", func(t *testing.T) {
		got := rebuild(t, func(b *BadgerEngine) {
			keptVectorsRefresh(t, b, "test:a", 2, 9, "synthetic")
			requireKeptVectors(t, b, "test:a", 2, 9)
		}, seed)
		requireNoManagedVectors(t, got)
	})
	t.Run("only their metadata was replaced after the snapshot", func(t *testing.T) {
		got := rebuild(t, func(b *BadgerEngine) { keptVectorsRefresh(t, b, "test:a", 2, 1, "another model") }, seed)
		requireNoManagedVectors(t, got)
	})
	t.Run("the node was embedded after the snapshot", func(t *testing.T) {
		got := rebuild(t, func(b *BadgerEngine) { keptVectorsRefresh(t, b, "test:a", 2, 1, "synthetic") }, func(b *BadgerEngine) {
			_, err := b.CreateNode(t.Context(), &Node{ID: "test:a", Labels: []string{"Doc"}, Properties: map[string]any{"text": "unchanged text", "flag": false}})
			require.NoError(t, err)
		})
		requireNoManagedVectors(t, got)
	})
}

// Named vectors come from a client and cannot be produced again. When the
// store does not hold the ones an update kept, replay rejects that record and
// reports it, and leaves the stored node as it is.
func TestKeptVectorsRebuildReportsOtherNamedVectors(t *testing.T) {
	dir := t.TempDir()
	b, w := keptVectorsEngine(t, dir)
	_, err := b.CreateNode(t.Context(), &Node{ID: "test:named", Labels: []string{"Doc"}, Properties: map[string]any{"text": "alpha"}, NamedEmbeddings: map[string][]float32{"title": {1, 2, 3}}})
	require.NoError(t, err)
	other := map[string][]float32{"title": {1, 2, 4}}
	snapshot := keptVectorsSnapshot(t, b, w, dir, func(snapshot *Snapshot) {
		found := false
		for _, node := range snapshot.Nodes {
			if strings.HasSuffix(string(node.ID), "named") {
				node.NamedEmbeddings, found = other, true
			}
		}
		require.True(t, found)
	})
	keptVectorsJournal(t, w, keptVectorsPrepare(t, b, "named", map[NodeID]func(*Node){"test:named": func(n *Node) { n.Properties["flag"] = true }}))
	crashClose(t, b, w)

	destination := newTestMemoryEngine(t)
	result, _, err := RecoverIntoEngine(t.Context(), destination, filepath.Join(dir, "wal"), snapshot)
	require.NoError(t, err)
	require.Equal(t, 1, result.Failed)
	require.Len(t, result.Errors, 1)
	require.Equal(t, OpEntityCommit, result.Errors[0].Operation)
	requireKeptVectorsNotStored(t, result.Errors[0].Error)
	got, err := destination.GetNode(t.Context(), "test:named")
	require.NoError(t, err)
	require.Equal(t, other, got.NamedEmbeddings, "the stored vectors are not replaced")
	require.NotContains(t, got.Properties, "flag", "the rejected update is not applied")
}

// A version-4 record does not say which vectors its update kept. When such a
// record still has to be applied, nothing is restored for it; a record that
// was already applied is skipped as before.
func TestKeptVectorsUnidentifiedRecordRestoresNothing(t *testing.T) {
	flag := map[NodeID]func(*Node){"test:keep": func(n *Node) { n.Properties["flag"] = true }}

	t.Run("managed vectors", func(t *testing.T) {
		dir := t.TempDir()
		b, w := keptVectorsEngine(t, dir)
		keptVectorsSeed(t, b, "test:keep", "alpha", 300, 1)
		require.Positive(t, keptVectorsStoredKeys(t, b, "test:keep"))
		keptVectorsJournal(t, w, keptVectorsWithoutIdentities(t, keptVectorsPrepare(t, b, "unidentified", flag)))

		b, w = keptVectorsReopen(t, b, w, dir)
		got, err := b.GetNode(t.Context(), "test:keep")
		require.NoError(t, err)
		require.Equal(t, true, got.Properties["flag"], "the update is applied")
		requireNoManagedVectors(t, got)
		require.Zero(t, keptVectorsStoredKeys(t, b, "test:keep"), "the unverified vectors are removed from the store")

		// The state survives a clean reopen.
		require.NoError(t, b.Close())
		require.NoError(t, w.Close())
		b, _ = keptVectorsEngine(t, dir)
		got, err = b.GetNode(t.Context(), "test:keep")
		require.NoError(t, err)
		requireNoManagedVectors(t, got)
	})

	t.Run("managed vectors, version 5", func(t *testing.T) {
		dir := t.TempDir()
		b, w := keptVectorsEngine(t, dir)
		keptVectorsSeed(t, b, "test:keep", "alpha", 300, 1)
		keptVectorsJournal(t, w, keptVectorsWithEarlyIdentities(t, keptVectorsPrepare(t, b, "early", flag)))

		b, _ = keptVectorsReopen(t, b, w, dir)
		got, err := b.GetNode(t.Context(), "test:keep")
		require.NoError(t, err)
		require.Equal(t, true, got.Properties["flag"], "the update is applied")
		requireNoManagedVectors(t, got)
	})

	t.Run("a record already applied is skipped", func(t *testing.T) {
		dir := t.TempDir()
		b, w := keptVectorsEngine(t, dir)
		keptVectorsSeed(t, b, "test:keep", "alpha", 300, 1)
		payload := keptVectorsPrepare(t, b, "applied", flag)
		keptVectorsJournal(t, w, payload)
		b, w = keptVectorsReopen(t, b, w, dir)
		requireKeptVectors(t, b, "test:keep", 300, 1)

		// The same transaction in its version-4 form: its receipt is stored.
		keptVectorsJournal(t, w, keptVectorsWithoutIdentities(t, payload))
		b, _ = keptVectorsReopen(t, b, w, dir)
		got := requireKeptVectors(t, b, "test:keep", 300, 1)
		require.Equal(t, true, got.Properties["flag"])
	})

	t.Run("named vectors", func(t *testing.T) {
		dir := t.TempDir()
		b, w := keptVectorsEngine(t, dir)
		named := map[string][]float32{"title": {1, 2, 3}}
		_, err := b.CreateNode(t.Context(), &Node{ID: "test:keep", Labels: []string{"Doc"}, Properties: map[string]any{"text": "alpha"}, NamedEmbeddings: named})
		require.NoError(t, err)
		keptVectorsJournal(t, w, keptVectorsWithoutIdentities(t, keptVectorsPrepare(t, b, "unidentified-named", flag)))
		crashClose(t, b, w)

		b, w = keptVectorsEngine(t, dir)
		_, err = RecoverBufferedCommitsAtOpen(b, w)
		requireKeptVectorsNotStored(t, err)
		got, err := b.GetNode(t.Context(), "test:keep")
		require.NoError(t, err)
		require.Equal(t, named, got.NamedEmbeddings, "the stored vectors are untouched")
		require.NotContains(t, got.Properties, "flag")
	})
}

// Ordinary crash recovery replays into an intact store, which holds the
// vectors each record kept. It must keep them for every order of records.
func TestKeptVectorsCrashRecoveryKeepsTheStoredVectors(t *testing.T) {
	t.Run("vectors replaced before the last update", func(t *testing.T) {
		dir := t.TempDir()
		b, w := keptVectorsEngine(t, dir)
		keptVectorsSeed(t, b, "test:keep", "alpha", 300, 1)
		// An applied update that keeps the first vectors.
		keptVectorsJournal(t, w, keptVectorsPrepare(t, b, "keep-first", map[NodeID]func(*Node){"test:keep": func(n *Node) { n.Properties["flag"] = true }}))
		b, w = keptVectorsReopen(t, b, w, dir)
		requireKeptVectors(t, b, "test:keep", 300, 1)
		// An applied text change drops them; the worker embeds the new text.
		keptVectorsJournal(t, w, keptVectorsPrepare(t, b, "new-text", map[NodeID]func(*Node){"test:keep": func(n *Node) { n.Properties["text"] = "beta" }}))
		b, w = keptVectorsReopen(t, b, w, dir)
		dropped, err := b.GetNode(t.Context(), "test:keep")
		require.NoError(t, err)
		require.Empty(t, dropped.ChunkEmbeddings)
		keptVectorsRefresh(t, b, "test:keep", 300, 9, "synthetic")
		// The last update keeps the second vectors and is not applied before the crash.
		keptVectorsJournal(t, w, keptVectorsPrepare(t, b, "keep-second", map[NodeID]func(*Node){"test:keep": func(n *Node) { n.Properties["second"] = true }}))

		b, _ = keptVectorsReopen(t, b, w, dir)
		got := requireKeptVectors(t, b, "test:keep", 300, 9)
		require.Equal(t, true, got.Properties["second"])
		require.Equal(t, "beta", got.Properties["text"])
	})

	t.Run("two updates not applied before the crash", func(t *testing.T) {
		dir := t.TempDir()
		b, w := keptVectorsEngine(t, dir)
		keptVectorsSeed(t, b, "test:keep", "alpha", 300, 1)
		first := keptVectorsPrepare(t, b, "first", map[NodeID]func(*Node){"test:keep": func(n *Node) { n.Properties["first"] = true }})
		second := keptVectorsPrepare(t, b, "second", map[NodeID]func(*Node){"test:keep": func(n *Node) { n.Properties["second"] = true }})
		keptVectorsJournal(t, w, first)
		keptVectorsJournal(t, w, second)

		b, _ = keptVectorsReopen(t, b, w, dir)
		got := requireKeptVectors(t, b, "test:keep", 300, 1)
		require.Equal(t, true, got.Properties["second"])
	})

	for name, chunks := range map[string]int{"vectors stored in the node body": 2, "vectors stored beside the node body": 80} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			b, w := keptVectorsEngine(t, dir)
			_, err := b.CreateNode(t.Context(), &Node{
				ID: "test:keep", Labels: []string{"Doc"}, Properties: map[string]any{"text": "alpha"},
				ChunkEmbeddings: keptVectors(chunks, 3),
				EmbedMeta:       map[string]any{"has_embedding": true, "chunk_count": chunks, "embedding_dimensions": keptVectorDims, "embedding_model": "client"},
			})
			require.NoError(t, err)
			payload := keptVectorsPrepare(t, b, "body-vectors", map[NodeID]func(*Node){"test:keep": func(n *Node) { n.Properties["flag"] = true }})
			_, ops, err := DecodeEntityTransaction(payload)
			require.NoError(t, err)
			require.Equal(t, retainedManagedEmbeddings, ops[0].retainedVectors)
			keptVectorsJournal(t, w, payload)

			b, _ = keptVectorsReopen(t, b, w, dir)
			got := requireKeptVectors(t, b, "test:keep", chunks, 3)
			require.Equal(t, true, got.Properties["flag"])
			require.Equal(t, "client", got.EmbedMeta["embedding_model"])
		})
	}

	// The first update writes new vectors with metadata as a client holds it
	// in memory; the second keeps them. Those vectors exist only in memory and
	// in this record, where they take another form, so the second update is
	// journaled whole: replay installs both images as written.
	t.Run("new vectors and an update that keeps them in one transaction", func(t *testing.T) {
		dir := t.TempDir()
		b, w := keptVectorsEngine(t, dir)
		_, err := b.CreateNode(t.Context(), &Node{ID: "test:keep", Labels: []string{"Doc"}, Properties: map[string]any{"text": "alpha"}})
		require.NoError(t, err)
		ctx := context.Background()
		tx, err := BeginEntityTransaction(ctx, b, CommitModeExplicit)
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace("test"))
		node, err := tx.GetNode(ctx, "test:keep")
		require.NoError(t, err)
		node = CopyNode(node)
		node.ChunkEmbeddings = keptVectors(3, 9)
		node.EmbedMeta = map[string]any{
			"has_embedding": true, "chunk_count": 3, "embedding_dimensions": keptVectorDims, "embedding_model": "synthetic",
			"score": 0.5, "chunk_spans": []any{[]any{0, "text", 0, 5}, []any{1, "text", 5, 9}, []any{2, "text", 9, 12}},
		}
		require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: node}))
		node, err = tx.GetNode(ctx, "test:keep")
		require.NoError(t, err)
		node = CopyNode(node)
		node.Properties["flag"] = true
		require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: node}))
		prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "write-then-keep"})
		require.NoError(t, err)
		payload, err := EncodeEntityTransaction(EntityTransactionRequest{Prepared: prepared})
		require.NoError(t, err)
		prepared.Abort()
		_, ops, err := DecodeEntityTransaction(payload)
		require.NoError(t, err)
		require.Len(t, ops, 2)
		require.Zero(t, ops[0].retainedVectors)
		require.Equal(t, keptVectors(3, 9), ops[0].Node.ChunkEmbeddings, "the first update's record carries its new vectors")
		require.Zero(t, ops[1].retainedVectors, "the second keeps vectors this transaction wrote and carries them")
		require.Equal(t, keptVectors(3, 9), ops[1].Node.ChunkEmbeddings)
		keptVectorsJournal(t, w, payload)

		b, _ = keptVectorsReopen(t, b, w, dir)
		got := requireKeptVectors(t, b, "test:keep", 3, 9)
		require.Equal(t, true, got.Properties["flag"])
	})
}

// A commit staged again in the same process holds whole images: only a record
// leaves kept vectors out. Such an image installs its own vectors, whatever
// the store holds by then.
func TestKeptVectorsWholeImagesInstallTheirOwnVectors(t *testing.T) {
	b, _ := keptVectorsEngine(t, t.TempDir())
	keptVectorsSeed(t, b, "test:keep", "alpha", 8, 1)
	ctx := context.Background()
	tx, err := BeginEntityTransaction(ctx, b, CommitModeExplicit)
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	node, err := tx.GetNode(ctx, "test:keep")
	require.NoError(t, err)
	node = CopyNode(node)
	node.Properties["flag"] = true
	require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: node}))
	prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "whole"})
	require.NoError(t, err)
	ops := prepared.Operations()
	prepared.Abort()
	require.Len(t, ops, 1)
	require.Equal(t, retainedManagedEmbeddings, ops[0].retainedVectors)
	require.Equal(t, keptVectors(8, 1), ops[0].Node.ChunkEmbeddings, "the operation holds the vectors its update kept")

	keptVectorsRefresh(t, b, "test:keep", 8, 9, "synthetic")
	receipt, err := SubmitAcceptedEntityTransaction(ctx, b, EntityTransactionEnvelope{Namespace: "test", LogicalTxID: "whole", Mode: CommitModeDirect}, ops, nil)
	require.NoError(t, err)
	require.NoError(t, receipt.WaitOutcome(ctx).Err)
	got := requireKeptVectors(t, b, "test:keep", 8, 1)
	require.Equal(t, true, got.Properties["flag"])
}

// Review r4, finding 1: a named-vector element that changes only in the sign
// of a zero is a changed vector. Its record carries it, so the live commit and
// the replay after a crash store the same bits.
func TestKeptVectorsNamedVectorBitChangeIsJournaled(t *testing.T) {
	negativeZero := math.Float32frombits(0x80000000)
	for _, replay := range []bool{false, true} {
		name := "live"
		if replay {
			name = "crash before application"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			b, w := keptVectorsEngine(t, dir)
			_, err := b.CreateNode(t.Context(), &Node{ID: "test:zero", Properties: map[string]any{"text": "alpha"}, NamedEmbeddings: map[string][]float32{"custom": {1, 0}}})
			require.NoError(t, err)
			ctx := context.Background()
			tx, err := BeginEntityTransaction(ctx, b, CommitModeExplicit)
			require.NoError(t, err)
			require.NoError(t, tx.SetNamespace("test"))
			node, err := tx.GetNode(ctx, "test:zero")
			require.NoError(t, err)
			node = CopyNode(node)
			node.NamedEmbeddings["custom"][1] = negativeZero
			node.Properties["changed"] = true
			require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: node}))
			prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "zero"})
			require.NoError(t, err)
			data, err := EncodeEntityTransaction(EntityTransactionRequest{Prepared: prepared})
			require.NoError(t, err)
			envelope, ops, err := DecodeEntityTransaction(data)
			require.NoError(t, err)
			require.Equal(t, uint16(3), envelope.Version, "nothing is left out of the record")
			require.Zero(t, ops[0].retainedVectors)
			require.Equal(t, uint32(0x80000000), math.Float32bits(ops[0].Node.NamedEmbeddings["custom"][1]))
			if replay {
				prepared.Abort()
				keptVectorsJournal(t, w, data)
				b, _ = keptVectorsReopen(t, b, w, dir)
			} else {
				receipt, err := NewWALEngine(b, w).SubmitEntityCommit(ctx, prepared, nil)
				require.NoError(t, err)
				require.NoError(t, receipt.Wait(ctx))
			}
			got, err := b.GetNode(t.Context(), "test:zero")
			require.NoError(t, err)
			require.Equal(t, uint32(0x80000000), math.Float32bits(got.NamedEmbeddings["custom"][1]))
			require.Equal(t, true, got.Properties["changed"])
		})
	}
}
