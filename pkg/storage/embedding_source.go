package storage

import "github.com/orneryd/nornicdb/pkg/embeddingsource"

// EmbeddingSourceMatches is the single validity decision for a computed result,
// a retained result, or persisted managed vectors. The first node owns the
// consumed-source record; legacy vectors retain their engine text policy.
func EmbeddingSourceMatches(source, current *Node, legacy EmbeddingTextPolicy) bool {
	s, present, valid := embeddingsource.Read(source.EmbedMeta)
	if !valid {
		return false
	}
	if !present {
		return legacy.SameSource(source, current)
	}
	if s.Text && !(EmbeddingTextPolicy{Include: s.Include, Exclude: s.Exclude, IncludeLabels: s.IncludeLabels}).SameSource(source, current) {
		return false
	}
	if s.AllProperties {
		return sameStoredValue(source.Properties, current.Properties)
	}
	for _, key := range s.Properties {
		a, aok := source.Properties[key]
		b, bok := current.Properties[key]
		if aok != bok || !sameStoredValue(a, b) {
			return false
		}
	}
	return true
}
