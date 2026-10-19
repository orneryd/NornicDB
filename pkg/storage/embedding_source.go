package storage

import "github.com/orneryd/nornicdb/pkg/embeddingsource"

// EmbeddingSourceMatches is the single validity decision for a computed result,
// a retained result, or persisted managed vectors. The first node owns the
// consumed-source record; legacy vectors retain their engine text policy.
func EmbeddingSourceMatches(source, current *Node, legacy EmbeddingTextPolicy) bool {
	// Failure metadata belongs to one body attempt, rather than an embedding
	// space. Any later body version clears terminal state and starts MAIN anew.
	if failed, _ := source.EmbedMeta["embedding_failed"].(bool); failed && !source.UpdatedAt.Equal(current.UpdatedAt) {
		return false
	}
	s, present, valid := embeddingsource.ReadView(source.EmbedMeta)
	if !valid {
		return false
	}
	if !present {
		return legacy.SameSource(source, current)
	}
	feedsText := func(key string) bool {
		return !IsEmbeddingMetadataProperty(key) && !s.Exclude.Contains(key) && (s.Include.Empty() || s.Include.Contains(key))
	}
	if s.Text && !sameEmbeddingTextSource(source, current, s.IncludeLabels, feedsText) {
		return false
	}
	if s.AllProperties {
		return sameStoredValue(source.Properties, current.Properties)
	}
	keys := s.Properties.Iterator()
	for key, ok := keys.Next(); ok; key, ok = keys.Next() {
		a, aok := source.Properties[key]
		b, bok := current.Properties[key]
		if aok != bok || !sameStoredValue(a, b) {
			return false
		}
	}
	return true
}
