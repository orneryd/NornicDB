package search

import (
	"encoding/binary"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Vector-entry keys identify every node vector in the shared vector store
// (vector index or file store, HNSW, cluster/IVF indexes, GPU index). This file
// is the only code that builds or reads them.
//
// The encoding is injective, so two different (node, entry) pairs never share
// a key, whatever bytes the node ID, vector name or property key contain:
//
//   - A node's main vector (chunk 0) is keyed by the node ID itself, unless the
//     ID starts with vectorEntryKeyMarker.
//   - Every other entry is vectorEntryKeyMarker + kind + uvarint(len(nodeID)) +
//     nodeID + sub, where sub is the chunk index, vector name or property key.
//
// A raw node ID never starts with the marker, so it can't equal an encoded key.
// Decoding is exact: a key is never guessed into an owner, and a key that does
// not decode has no owner.
//
// Persisted artifacts that contain these keys carry vectorEntryKeyFormat in
// their format versions, so a store written with the legacy suffix keys
// ("<id>-chunk-<i>", "<id>-named-<n>", "<id>-prop-<k>") is rebuilt from storage
// rather than decoded.

// vectorEntryKeyFormat names the key encoding in persisted format versions.
const vectorEntryKeyFormat = "vkey2"

const vectorEntryKeyMarker = '\x00'

type vectorEntryKind byte

const (
	vectorEntryMain     vectorEntryKind = 'm'
	vectorEntryChunk    vectorEntryKind = 'c'
	vectorEntryNamed    vectorEntryKind = 'n'
	vectorEntryProperty vectorEntryKind = 'p'
)

// vectorEntry is a decoded vector-entry key. Chunk is set for main (0) and
// chunk entries; Name is the vector name or property key.
type vectorEntry struct {
	NodeID string
	Kind   vectorEntryKind
	Chunk  int
	Name   string
}

func nodeMainVectorKey(nodeID string) string {
	if nodeID == "" || nodeID[0] != vectorEntryKeyMarker {
		return nodeID
	}
	return encodeVectorEntryKey(nodeID, vectorEntryMain, "")
}

// nodeChunkVectorKey keys chunk index of nodeID. Chunk 0 is the main vector.
func nodeChunkVectorKey(nodeID string, chunk int) string {
	if chunk == 0 {
		return nodeMainVectorKey(nodeID)
	}
	return encodeVectorEntryKey(nodeID, vectorEntryChunk, strconv.Itoa(chunk))
}

func nodeNamedVectorKey(nodeID, name string) string {
	return encodeVectorEntryKey(nodeID, vectorEntryNamed, name)
}

func nodePropertyVectorKey(nodeID, property string) string {
	return encodeVectorEntryKey(nodeID, vectorEntryProperty, property)
}

func encodeVectorEntryKey(nodeID string, kind vectorEntryKind, sub string) string {
	var length [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(length[:], uint64(len(nodeID)))
	var b strings.Builder
	b.Grow(2 + n + len(nodeID) + len(sub))
	b.WriteByte(vectorEntryKeyMarker)
	b.WriteByte(byte(kind))
	b.Write(length[:n])
	b.WriteString(nodeID)
	b.WriteString(sub)
	return b.String()
}

// decodeVectorEntryKey returns the entry a key names. ok is false for a key
// that is not a valid encoding; such a key has no owner.
func decodeVectorEntryKey(key string) (entry vectorEntry, ok bool) {
	if key == "" {
		return vectorEntry{}, false
	}
	if key[0] != vectorEntryKeyMarker {
		return vectorEntry{NodeID: key, Kind: vectorEntryMain}, true
	}
	if len(key) < 3 {
		return vectorEntry{}, false
	}
	kind := vectorEntryKind(key[1])
	length, n := binary.Uvarint([]byte(key[2:min(len(key), 2+binary.MaxVarintLen64)]))
	if n <= 0 || length > uint64(len(key)-2-n) {
		return vectorEntry{}, false
	}
	var canonical [binary.MaxVarintLen64]byte
	if binary.PutUvarint(canonical[:], length) != n {
		return vectorEntry{}, false // over-long length encoding
	}
	start := 2 + n
	nodeID := key[start : start+int(length)]
	sub := key[start+int(length):]
	if nodeID == "" {
		return vectorEntry{}, false
	}
	switch kind {
	case vectorEntryMain:
		// Only a marker-prefixed ID is escaped; any other form is not canonical.
		if sub != "" || nodeID[0] != vectorEntryKeyMarker {
			return vectorEntry{}, false
		}
		return vectorEntry{NodeID: nodeID, Kind: kind}, true
	case vectorEntryChunk:
		chunk, ok := parseVectorEntryChunk(sub)
		if !ok {
			return vectorEntry{}, false
		}
		return vectorEntry{NodeID: nodeID, Kind: kind, Chunk: chunk}, true
	case vectorEntryNamed, vectorEntryProperty:
		return vectorEntry{NodeID: nodeID, Kind: kind, Name: sub}, true
	}
	return vectorEntry{}, false
}

// parseVectorEntryChunk accepts only the form nodeChunkVectorKey writes: a
// decimal index of at least 1, with no sign and no leading zero. It does not
// allocate, so decoding results stays allocation-free for long documents.
func parseVectorEntryChunk(sub string) (int, bool) {
	if sub == "" || sub[0] == '0' {
		return 0, false
	}
	for i := 0; i < len(sub); i++ {
		if sub[i] < '0' || sub[i] > '9' {
			return 0, false
		}
	}
	chunk, err := strconv.Atoi(sub) // fails only on overflow
	return chunk, err == nil
}

// vectorEntryOwner returns the node a vector-entry key belongs to.
func vectorEntryOwner(key string) (string, bool) {
	if key != "" && key[0] != vectorEntryKeyMarker {
		return key, true
	}
	entry, ok := decodeVectorEntryKey(key)
	return entry.NodeID, ok
}

// vectorEntryIsMain identifies a valid main entry without suffix guesses.
func vectorEntryIsMain(key string) bool {
	if key != "" && key[0] != vectorEntryKeyMarker {
		return true
	}
	entry, ok := decodeVectorEntryKey(key)
	return ok && entry.Kind == vectorEntryMain
}

// vectorEntryChunkIndex returns the managed chunk index a key names for
// nodeID: 0 for the main vector, i for chunk i. Named and property entries,
// and other nodes' entries, are not chunks of nodeID.
func vectorEntryChunkIndex(nodeID, key string) (int, bool) {
	entry, ok := decodeVectorEntryKey(key)
	if !ok || entry.NodeID != nodeID {
		return 0, false
	}
	switch entry.Kind {
	case vectorEntryMain, vectorEntryChunk:
		return entry.Chunk, true
	}
	return 0, false
}

// describe names the entry in log lines ("main", "chunk 2", "named[v]", "property[p]").
func (e vectorEntry) describe() string {
	switch e.Kind {
	case vectorEntryChunk:
		return "chunk " + strconv.Itoa(e.Chunk)
	case vectorEntryNamed:
		return "named[" + e.Name + "]"
	case vectorEntryProperty:
		return "property[" + e.Name + "]"
	}
	return "main"
}

// nodeVectorEntry is one vector a node stores in the shared vector store.
type nodeVectorEntry struct {
	key    string
	entry  vectorEntry
	vector []float32
}

// forEachNodeVectorEntry visits every vector entry IndexNode stores for node,
// in order: named vectors, managed chunk embeddings (chunk 0 under the main
// key; only when the node is eligible and its first chunk is present) and
// property values that are vectors of dims dimensions. Empty vectors are
// skipped; any other dimension check is the caller's. It stops when fn
// returns false. IndexNode, its unchanged-state check and the capacity
// estimate all enumerate entries here, so they agree on every key.
func (s *Service) forEachNodeVectorEntry(node *storage.Node, dims int, fn func(nodeVectorEntry) bool) {
	nodeID := string(node.ID)
	for name, vector := range node.NamedEmbeddings {
		if len(vector) == 0 {
			continue
		}
		entry := vectorEntry{NodeID: nodeID, Kind: vectorEntryNamed, Name: name}
		if !fn(nodeVectorEntry{key: nodeNamedVectorKey(nodeID, name), entry: entry, vector: vector}) {
			return
		}
	}
	if s.managedEmbeddingEligible(node) && len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0 {
		for chunk, vector := range node.ChunkEmbeddings {
			if len(vector) == 0 {
				continue
			}
			entry := vectorEntry{NodeID: nodeID, Kind: vectorEntryMain}
			if chunk > 0 {
				entry = vectorEntry{NodeID: nodeID, Kind: vectorEntryChunk, Chunk: chunk}
			}
			if !fn(nodeVectorEntry{key: nodeChunkVectorKey(nodeID, chunk), entry: entry, vector: vector}) {
				return
			}
		}
	}
	for property, value := range node.Properties {
		vector, ok := vectorFromPropertyValue(value, dims)
		if !ok {
			continue
		}
		entry := vectorEntry{NodeID: nodeID, Kind: vectorEntryProperty, Name: property}
		if !fn(nodeVectorEntry{key: nodePropertyVectorKey(nodeID, property), entry: entry, vector: vector}) {
			return
		}
	}
}

// nodeMainVectorKeys maps node IDs (fulltext documents used as ANN seeds or
// entry points) to their main vector-entry keys, in place.
func nodeMainVectorKeys(nodeIDs []string) []string {
	for i, nodeID := range nodeIDs {
		nodeIDs[i] = nodeMainVectorKey(nodeID)
	}
	return nodeIDs
}

// NodeChunkVectorKey is the vector-entry key of chunk index of nodeID (chunk 0
// is the node's main vector), for callers outside this package that build
// HNSW or cluster indexes keyed like the search service's own.
func NodeChunkVectorKey(nodeID string, chunk int) string {
	return nodeChunkVectorKey(nodeID, chunk)
}

// forEachNodeVectorKeyLocked visits a node's main and tracked vector-entry keys.
// The main entry and tracked chunk zero share one key and are visited once.
// Callers hold the node metadata locks. Raw node IDs must not enter vector-key
// consumers: marker-prefixed IDs may themselves encode a different node's entry.
func (s *Service) forEachNodeVectorKeyLocked(nodeID string, visit func(string)) {
	mainKey := nodeMainVectorKey(nodeID)
	visit(mainKey)
	for _, key := range s.nodeChunkVectors[nodeID] {
		if key != mainKey {
			visit(key)
		}
	}
	for _, key := range s.nodeNamedVector[nodeID] {
		visit(key)
	}
	for _, key := range s.nodePropVector[nodeID] {
		visit(key)
	}
}

// Node and vector-entry identities are deliberately different types. Only this
// owner constructs or converts them; hint maps cannot be indexed with the other
// identity even when their byte strings are identical.
type vectorNodeID string
type vectorEntryID string

type nodeLexicalHint struct {
	ID        vectorNodeID
	Rank      uint32
	Signature uint64
}

type nodeLexicalHints map[vectorNodeID]nodeLexicalHint
type entryLexicalHints map[vectorEntryID]nodeLexicalHint

func nodeHintsFromValues(hints []LexicalSeedHint) nodeLexicalHints {
	if len(hints) == 0 {
		return nil
	}
	out := make(nodeLexicalHints, len(hints))
	for _, hint := range hints {
		// BM25 supplies graph identities; preserve their exact bytes.
		id := hint.ID
		if id != "" {
			out[vectorNodeID(id)] = nodeLexicalHint{ID: vectorNodeID(id), Rank: hint.Rank, Signature: hint.Signature}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func nodeHintForID(hints nodeLexicalHints, id string) (nodeLexicalHint, bool) {
	hint, ok := hints[vectorNodeID(id)]
	return hint, ok
}

func lexicalHintForVectorID(key string, hints nodeLexicalHints) (nodeLexicalHint, bool) {
	if len(hints) == 0 {
		return nodeLexicalHint{}, false
	}
	owner, ok := vectorEntryOwner(key)
	if !ok {
		return nodeLexicalHint{}, false
	}
	return nodeHintForID(hints, owner)
}

func entryHintForVectorID(hints entryLexicalHints, key string) (nodeLexicalHint, bool) {
	hint, ok := hints[vectorEntryID(key)]
	return hint, ok
}

// mainEntryLexicalHintsForNodes projects only canonical main anchors. It never
// treats a graph ID as an entry key, even when its bytes spell another entry.
func mainEntryLexicalHintsForNodes(docs nodeLexicalHints) entryLexicalHints {
	if len(docs) == 0 {
		return nil
	}
	out := make(entryLexicalHints, len(docs))
	for _, hint := range docs {
		putEntryLexicalHint(out, mainKeyForLexicalHint(hint), hint)
	}
	return out
}

// entryHintsFromValues is the public HNSW per-entry hint API boundary. Its IDs
// already identify entries; the retained hint still names the owning document.
func entryHintsFromValues(hints []LexicalSeedHint) entryLexicalHints {
	if len(hints) == 0 {
		return nil
	}
	out := make(entryLexicalHints, len(hints))
	for _, hint := range hints {
		if owner, ok := vectorEntryOwner(hint.ID); ok {
			out[vectorEntryID(hint.ID)] = nodeLexicalHint{ID: vectorNodeID(owner), Rank: hint.Rank, Signature: hint.Signature}
		}
	}
	return out
}

func putEntryLexicalHint(hints entryLexicalHints, key string, hint nodeLexicalHint) {
	hints[vectorEntryID(key)] = hint
}

func mainKeyForLexicalHint(hint nodeLexicalHint) string {
	return nodeMainVectorKey(string(hint.ID))
}

// lexicalHintValues projects an explicitly node- or entry-keyed map through the
// legacy public hint API. The key domain is preserved; the maps never mix.
func lexicalHintValues[K ~string, H ~map[K]nodeLexicalHint](hints H) []LexicalSeedHint {
	if len(hints) == 0 {
		return nil
	}
	out := make([]LexicalSeedHint, 0, len(hints))
	for key, hint := range hints {
		out = append(out, LexicalSeedHint{ID: string(key), Rank: hint.Rank, Signature: hint.Signature})
	}
	return out
}

// entryLexicalHintsForNodes binds document hints to canonical inventory entries.
// The input remains document-keyed for construction and owner lookup; only graph
// hint caches use this entry-keyed projection. Caller snapshots before pipelineMu.
func (s *Service) entryLexicalHintsForNodes(docs nodeLexicalHints) entryLexicalHints {
	if len(docs) == 0 {
		return nil
	}
	out := make(entryLexicalHints)
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	s.mu.RLock()
	defer s.mu.RUnlock()
	for nodeID, hint := range docs {
		s.forEachNodeVectorKeyLocked(string(nodeID), func(key string) { putEntryLexicalHint(out, key, hint) })
	}
	return out
}

func (s *Service) hnswLexicalSeedNodeSet(ft bm25Index) map[string]struct{} {
	hints := s.hnswLexicalSeedHints(ft, DefaultHNSWConfig().M)
	if len(hints) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(hints))
	for id := range hints {
		out[string(id)] = struct{}{}
	}
	return out
}

func vectorIDInSeedNodeSet(vectorID string, seedNodeIDs map[string]struct{}) bool {
	owner, ok := vectorEntryOwner(vectorID)
	if !ok {
		return false
	}
	_, ok = seedNodeIDs[owner]
	return ok
}

func vectorNodeIdentity(id string) vectorNodeID    { return vectorNodeID(id) }
func vectorEntryIdentity(key string) vectorEntryID { return vectorEntryID(key) }
func (id vectorNodeID) nodeIDString() string       { return string(id) }
func (id vectorEntryID) entryKeyString() string    { return string(id) }
