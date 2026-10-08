package storage

import "slices"

// embeddingMetadataPropertyKeys are the properties that never feed a node's
// managed embedding text: embedding bookkeeping, timestamps and the id.
var embeddingMetadataPropertyKeys = map[string]bool{
	"embedding":            true,
	"has_embedding":        true,
	"embedding_skipped":    true,
	"embedding_model":      true,
	"embedding_dimensions": true,
	"embedded_at":          true,
	"has_chunks":           true,
	"chunk_count":          true,
	"createdAt":            true,
	"updatedAt":            true,
	"id":                   true,
}

// IsEmbeddingMetadataProperty reports whether key is embedding bookkeeping,
// a timestamp or the id: a property that never feeds embedding text.
func IsEmbeddingMetadataProperty(key string) bool {
	return embeddingMetadataPropertyKeys[key]
}

// EmbeddingTextPolicy says which of a node's properties and labels feed its
// managed embedding text (NORNICDB_EMBEDDING_PROPERTIES_INCLUDE /
// _EXCLUDE and NORNICDB_EMBEDDING_INCLUDE_LABELS). It is the one rule the
// text builder (embeddingutil.BuildText) and storage's "did the embedding
// source change" decisions share (#963): an update that leaves every
// property and label the policy feeds unchanged keeps the node's
// embeddings, and one that changes any of them invalidates them.
type EmbeddingTextPolicy struct {
	// Include, when non-empty, lists the only properties that feed the text.
	Include []string
	// Exclude lists properties that never feed it.
	Exclude []string
	// IncludeLabels prepends the node's labels to the text.
	IncludeLabels bool
}

// defaultEmbeddingTextPolicy feeds every property but the metadata ones,
// and the labels.
var defaultEmbeddingTextPolicy = EmbeddingTextPolicy{IncludeLabels: true}

// FeedsText reports whether property key feeds the embedding text.
func (p EmbeddingTextPolicy) FeedsText(key string) bool {
	if embeddingMetadataPropertyKeys[key] || slices.Contains(p.Exclude, key) {
		return false
	}
	return len(p.Include) == 0 || slices.Contains(p.Include, key)
}

// SetEmbeddingTextPolicy sets the policy of the engine's managed embedding
// text. Until it is set, every property but the metadata ones feeds the
// text, and the labels do too.
func (b *BadgerEngine) SetEmbeddingTextPolicy(policy EmbeddingTextPolicy) {
	policy.Include = slices.Clone(policy.Include)
	policy.Exclude = slices.Clone(policy.Exclude)
	b.embeddingTextPolicy.Store(&policy)
}

func (b *BadgerEngine) embeddingTextPolicyOrDefault() EmbeddingTextPolicy {
	if b != nil {
		if policy := b.embeddingTextPolicy.Load(); policy != nil {
			return *policy
		}
	}
	return defaultEmbeddingTextPolicy
}

// SameSource reports whether two copies of a node have the same embedding
// source under the policy: the same labels (in any order) when it feeds
// labels, and the same values for every property it feeds, comparing values
// as stored values rather than by Go type (a copy taken from a cache may
// hold an int or a []string where a decoded one holds an int64 or a []any).
// Writers that decide themselves whether to keep a node's embeddings use it
// with the engine's policy (#963).
func (p EmbeddingTextPolicy) SameSource(x, y *Node) bool {
	if p.IncludeLabels && !sameLabelSet(x.Labels, y.Labels) {
		return false
	}
	for name, value := range x.Properties {
		if !p.FeedsText(name) {
			continue
		}
		other, ok := y.Properties[name]
		if !ok || !sameStoredValue(value, other) {
			return false
		}
	}
	for name := range y.Properties {
		if _, ok := x.Properties[name]; !ok && p.FeedsText(name) {
			return false
		}
	}
	return true
}

// sameEmbeddingSource is SameSource under the engine's policy.
func (b *BadgerEngine) sameEmbeddingSource(x, y *Node) bool {
	return b.embeddingTextPolicyOrDefault().SameSource(x, y)
}

// embeddingSourceJudge is an engine that decides whether two copies of a
// node share an embedding source under its policy.
type embeddingSourceJudge interface {
	sameEmbeddingSource(x, y *Node) bool
}

// dropStaleCarriedEmbeddings drops the managed embeddings node carries over
// unchanged from prev when the update changes the embedding source (#963),
// so a writer that read the node, changed an embedded property and wrote it
// back doesn't store the old vectors with the new text. Managed embeddings
// are the embed worker's: prev has embedding metadata (EmbedMeta). Vectors
// a caller stored without it are the caller's and are kept as given, as
// are nil embeddings (an explicit clear) and different ones (new vectors).
// The policy is the one of the engine judge wraps (UnwrapEngine), or the
// default policy when it has none.
func dropStaleCarriedEmbeddings(judge Engine, prev, node *Node) {
	if prev == nil || len(prev.EmbedMeta) == 0 || len(node.ChunkEmbeddings) == 0 || !sameChunkEmbeddings(node.ChunkEmbeddings, prev.ChunkEmbeddings) {
		return
	}
	same := defaultEmbeddingTextPolicy.SameSource(prev, node)
	if engine, ok := UnwrapEngine(judge).(embeddingSourceJudge); ok {
		same = engine.sameEmbeddingSource(prev, node)
	}
	if !same {
		node.ChunkEmbeddings = nil
		node.EmbedMeta = nil
	}
}

// sameLabelSet reports whether two label lists hold the same labels in any
// order.
func sameLabelSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, label := range a {
		counts[label]++
	}
	for _, label := range b {
		if counts[label] == 0 {
			return false
		}
		counts[label]--
	}
	return true
}

// sameChunkEmbeddings reports whether two embedding chunk lists hold the
// same vectors.
func sameChunkEmbeddings(a, b [][]float32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !slices.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}
