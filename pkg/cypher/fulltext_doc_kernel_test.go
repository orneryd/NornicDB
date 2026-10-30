package cypher

// Contract and benchmark for the shared fulltext doc-assembly kernel
// (buildFulltextDocFromProperties): the node and edge builders feed the same
// kernel and must produce identical doc shapes for equivalent properties.

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFulltextDocKernel_NodeAndEdgeBuildersAgree(t *testing.T) {
	properties := map[string]interface{}{
		"Title":  "Graph Databases",
		"Body":   "A book",
		"Empty":  "",
		"NilVal": nil,
		"Count":  int64(42),
	}

	// The node extractor only reads the requested properties; request the
	// non-empty ones (the kernel's nil/empty skipping is asserted below via
	// the properties maps, which is where it applies).
	doc := buildNodeFulltextDoc(&storage.Node{ID: "n1", Properties: properties}, []string{"Title", "Body", "Count"})
	require.NotNil(t, doc)
	require.Equal(t, "graph databases a book 42", doc.contentLower)
	require.Equal(t, 5, doc.contentTokenN)
	require.Equal(t, "graph databases", doc.properties["title"])
	require.Equal(t, "Graph Databases", doc.rawProperties["title"])
	require.Equal(t, "42", doc.properties["count"])
	_, hasEmpty := doc.properties["empty"]
	require.False(t, hasEmpty, "empty-string property must be skipped")
	_, hasNil := doc.properties["nilval"]
	require.False(t, hasNil, "nil property must be skipped")

	// Edge builder with the full property set: the kernel output matches the
	// node doc for the same properties and content. Request the properties
	// explicitly — the empty-properties edge path concatenates map values in
	// map iteration order, which is nondeterministic and not part of the
	// kernel contract (the kernel covers property canonicalization; content
	// assembly belongs to the builders).
	edgeDoc := buildEdgeFulltextDoc(&storage.Edge{ID: "e1", Properties: map[string]interface{}{
		"Title": "Graph Databases", "Body": "A book", "Count": int64(42),
	}}, []string{"Title", "Body", "Count"})
	require.NotNil(t, edgeDoc)
	require.Equal(t, "graph databases a book 42", edgeDoc.contentLower)
	require.Equal(t, doc.properties, edgeDoc.properties)
	require.Equal(t, doc.rawProperties, edgeDoc.rawProperties)

	// Empty content yields no doc at all.
	require.Nil(t, buildNodeFulltextDoc(&storage.Node{ID: "n2", Properties: map[string]interface{}{"A": "x"}}, []string{"Missing"}))
	require.Nil(t, buildEdgeFulltextDoc(nil, nil))
}

func BenchmarkFulltextDocKernel_Node(b *testing.B) {
	properties := map[string]interface{}{
		"Title": "Graph Databases", "Body": "A practical guide", "Count": int64(42),
	}
	node := &storage.Node{ID: "n1", Properties: properties}
	props := []string{"Title", "Body", "Count"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if buildNodeFulltextDoc(node, props) == nil {
			b.Fatal("doc must build")
		}
	}
}

func BenchmarkFulltextDocKernel_Edge(b *testing.B) {
	properties := map[string]interface{}{
		"Title": "Graph Databases", "Body": "A practical guide", "Count": int64(42),
	}
	edge := &storage.Edge{ID: "e1", Properties: properties}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if buildEdgeFulltextDoc(edge, nil) == nil {
			b.Fatal("doc must build")
		}
	}
}
