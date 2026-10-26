package cypher

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/querytextmatrix"
)

// Cypher 25 nests a block directly in a query body and braces whole query
// parts. Neo4j 5.26 can't run these, so they are compared with a Cypher 25
// reference (NORNICDB_NEO4J_REFERENCE_CYPHER25=1 with a Neo4j 2025 reference
// URI): every introducer, valid and malformed maps in the nested block or
// part, with and without input rows, in both transaction modes, with plain
// and commented spacing.
func TestCypher25BraceBlocksMatchReference(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if uri == "" || os.Getenv("NORNICDB_NEO4J_REFERENCE_CYPHER25") != "1" {
		t.Skip("set NORNICDB_NEO4J_REFERENCE_URI to a Neo4j 2025 reference and NORNICDB_NEO4J_REFERENCE_CYPHER25=1")
	}
	ctx := context.Background()
	reference, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer reference.Close(ctx)
	// Each template's first space-separated words up to "|" stay as written;
	// the rest is respelled with each gap. ZERO marks where a zero-row form
	// inserts its empty input.
	templates := []string{
		"CYPHER 25 | ZERO CALL () { { RETURN {k: 1} AS m } } RETURN m AS out",
		"CYPHER 25 | ZERO RETURN COUNT { { RETURN {k: 1} AS m } } AS out",
		"CYPHER 25 | ZERO RETURN EXISTS { { RETURN {k: 1} AS m } } AS out",
		"CYPHER 25 | ZERO RETURN COLLECT { { RETURN {k: 1} AS m } } AS out",
		"CYPHER 25 | ZERO WITH {a: 1} AS n RETURN COUNT { { RETURN n {.a, b: 2} AS m } } AS out",
		"CYPHER 25 | { ZERO RETURN {k: 1} AS out } UNION { ZERO RETURN {k: 2} AS out }",
		"CYPHER 25 | { ZERO RETURN {k: 1} AS out }",
		"CYPHER 25 | ZERO WITH 1 AS a CALL (a) { { RETURN {a} AS m } } RETURN m AS out",
		"CYPHER 25 | ZERO WITH 1 AS a RETURN COUNT { { RETURN {a} AS m } } AS out",
		"CYPHER 25 | ZERO WITH 1 AS a RETURN EXISTS { { RETURN {a} AS m } } AS out",
		"CYPHER 25 | ZERO WITH 1 AS a RETURN COLLECT { { RETURN {a} AS m } } AS out",
		"CYPHER 25 | ZERO RETURN COUNT { { MATCH (n {bad}) RETURN n } } AS out",
		"CYPHER 25 | ZERO RETURN COUNT { { RETURN {.a} AS m } } AS out",
		"CYPHER 25 | { ZERO WITH 1 AS a RETURN {a} AS out } UNION { RETURN {k: 2} AS out }",
		"CYPHER 25 | { RETURN {k: 1} AS out } UNION { ZERO WITH 1 AS a RETURN {a} AS out }",
	}
	sequence := 0
	for _, explicit := range []bool{false, true} {
		for _, template := range templates {
			prefix, body, _ := strings.Cut(template, "| ")
			for _, zero := range []bool{false, true} {
				for gapIndex, gap := range []string{" ", " /* { } : ( ) */ ", "\n// { :\n"} {
					text := strings.ReplaceAll(body, "ZERO ", map[bool]string{false: "", true: "UNWIND [] AS zero "}[zero])
					sequence++
					alias := fmt.Sprintf("c25_%d", sequence)
					text = strings.ReplaceAll(text, " AS out", " AS "+alias)
					query := prefix + strings.ReplaceAll(text, " ", gap)
					t.Run(fmt.Sprintf("explicit=%v/%s/zero=%v/gap%d", explicit, body, zero, gapIndex), func(t *testing.T) {
						executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "c25_braces"))
						if explicit {
							if _, err := executor.Execute(ctx, "BEGIN", nil); err != nil {
								t.Fatal(err)
							}
							defer executor.Execute(ctx, "ROLLBACK", nil)
						}
						result, err := executor.Execute(ctx, query, nil)
						assertMapBraceReference(t, ctx, reference, explicit, querytextmatrix.MapBraceCase{Query: query}, result, err)
					})
				}
			}
		}
	}
}
