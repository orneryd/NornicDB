package fabric

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Query declarations and physical pattern reads are different purposes of the
// same native occurrence. Only the physical view retains pattern reuse.
func TestNativePatternBindingReadPurpose(t *testing.T) {
	for _, sample := range []struct {
		query           string
		reads, semantic []string
	}{
		{"MATCH named=(first:Person {literal:1})-[rel:LINK]->(last) RETURN first.name", []string{"named", "first", "rel", "last", "first"}, []string{"first"}},
		{"MATCH (`co,mma`:Person)-[r:LINK]->(other) RETURN `co,mma`.name", []string{"co,mma", "r", "other", "co,mma"}, []string{"co,mma"}},
		{"WITH 1 AS alias UNWIND [1] AS introduced RETURN alias", []string{"alias"}, []string{"alias"}},
		{"RETURN EXISTS { MATCH (outer)-[:LINK]->(inner) }", []string{"outer", "inner"}, nil},
	} {
		t.Run(sample.query, func(t *testing.T) {
			query := ReadQueryStructure(sample.query)
			require.NoError(t, query.Err)
			names := func(references []ExpressionReference) []string {
				var names []string
				prior := -1
				for _, reference := range references {
					require.Greater(t, reference.Start, prior)
					require.Equal(t, reference.Name, unquoteSyntaxName(sample.query[reference.Start:reference.End]))
					prior = reference.Start
					names = append(names, reference.Name)
				}
				return names
			}
			for repeat := 0; repeat < 2; repeat++ {
				require.Equal(t, sample.reads, names(query.Input.ReadReferences()))
				require.Equal(t, sample.semantic, names(query.Input.References()))
			}
		})
	}
	query := ReadQueryStructure("MATCH (p:Person) MATCH (p {a:1}) RETURN p.b")
	require.NoError(t, query.Err)
	read := query.Clauses[1].Input.ReadReferences()
	require.Len(t, read, 1)
	require.Equal(t, "p", read[0].Name)
	require.Equal(t, "p", query.Clauses[1].Input.Text()[read[0].Start:read[0].End])
	require.Empty(t, query.Clauses[1].Input.References())
	concurrent := ReadQueryStructure("MATCH (read:ConcurrentRead) MATCH (read {a:1}) RETURN read.b")
	require.NoError(t, concurrent.Err)
	var workers sync.WaitGroup
	for index := 0; index < 16; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if reads, semantic := concurrent.Input.ReadReferences(), concurrent.Input.References(); len(reads) != 3 || len(semantic) != 1 {
				t.Errorf("concurrent native purposes changed: physical=%+v semantic=%+v", reads, semantic)
			}
		}()
	}
	workers.Wait()
}
