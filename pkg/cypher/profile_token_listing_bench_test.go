package cypher

import "testing"

// BenchmarkTokenListingOrdinaryExecution reuses the existing 451-node /
// 450-relationship fixture and uncached scalar-result execution owner.
func BenchmarkTokenListingOrdinaryExecution(b *testing.B) {
	for _, tc := range []struct {
		name, query string
		want        int64
	}{
		{"labels", "CALL db.labels() YIELD label RETURN count(label)", 4},
		{"types", "CALL db.relationshipTypes() YIELD relationshipType RETURN count(relationshipType)", 1},
		{"properties", "CALL db.propertyKeys() YIELD propertyKey RETURN count(propertyKey)", 4},
		{"statistics", "CALL nornicdb.stats() YIELD labels RETURN toInteger(labels)", 4},
	} {
		b.Run(tc.name, func(b *testing.B) {
			runPerRowBench(b, perRowDocGraph(b), tc.query, nil, tc.want)
		})
	}
}
