package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Test fixtures name stored literal types, not declaration syntax.
func relationshipTypesForTest(names ...string) relationshipTypes {
	if len(names) == 0 {
		return relationshipTypes{}
	}
	written := make([]string, len(names))
	for index, name := range names {
		written[index] = "`" + strings.ReplaceAll(name, "`", "``") + "`"
	}
	return relationshipDeclarationOf(":" + strings.Join(written, "|")).types
}

func TestRelationshipOwnerDynamicLookingLiteralPaths(t *testing.T) {
	for _, name := range []string{"$(R)", "$any(R)", "$all(R)", "$(R|S)", "$(中`文)", "$(R)*1..2"} {
		quoted := "`" + strings.ReplaceAll(name, "`", "``") + "`"
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit=%v", name, explicit), func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "dynamic_looking_literal"))
				ctx := context.Background()
				for _, query := range []string{
					"CREATE (:DeltaStart)-[:" + quoted + "]->(:DeltaEnd)",
					"MATCH p = ANY SHORTEST (a:DeltaStart)-[:" + quoted + "]->(b:DeltaEnd) RETURN length(p) AS n",
					"MATCH (a:DeltaStart), (b:DeltaEnd) MATCH p = shortestPath((a)-[:" + quoted + "*]->(b)) RETURN length(p) AS n",
					"MATCH (a:DeltaStart) RETURN COUNT { (a)-[:" + quoted + "]->() } AS n",
					"MATCH (a:DeltaStart) RETURN EXISTS { (a)-[:" + quoted + "]->() } AS n",
				} {
					want := []string{"n", "1"}
					if strings.HasPrefix(query, "CREATE") {
						want = nil
					}
					if strings.Contains(query, "EXISTS") {
						want = []string{"n", "TRUE"}
					}
					require.Equal(t, want, relationshipPatternOracleRun(ctx, exec, query, explicit), query)
				}
			})
		}
	}
}

func TestRelationshipOwnerTypeProvenance(t *testing.T) {
	for _, quoted := range []bool{false, true} {
		text := "$(R)"
		if quoted {
			text = "`" + text + "`"
		}
		declaration := relationshipDeclarationOf(":" + text)
		require.Equal(t, 1, declaration.types.len())
		require.Equal(t, quoted, declaration.types.at(0).isQuoted())
		require.Equal(t, !quoted, declaration.types.at(0).isDynamic())
		require.Equal(t, quoted, declaration.types.matches("$(R)"))
	}
}

func TestRelationshipOwnerVectorTypeIdentity(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "vector_type_identity"))
	for _, name := range []string{"$(R)", " R ", "中`文"} {
		quoted := "`" + strings.ReplaceAll(name, "`", "``") + "`"
		pattern, ok := exec.parseSimpleMatchRelationshipPattern("(a)-[r:" + quoted + "]->(b)")
		require.True(t, ok, name)
		require.True(t, pattern.relType.isQuoted())
		require.False(t, pattern.relType.isDynamic())
		require.True(t, pattern.relType.matchesStorageName(name))
		require.False(t, pattern.relType.matchesStorageName(name+"other"))
		schema := storage.NewSchemaManager()
		require.NoError(t, schema.AddVectorIndexForEntity("literal", name, "emb", 3, "cosine", storage.ConstraintEntityRelationship))
		index, ok := findCosineVectorIndexNameMatching(schema, "emb", storage.ConstraintEntityRelationship, pattern.relType.matchesStorageName)
		require.True(t, ok, name)
		require.Equal(t, "literal", index)
	}
	for _, content := range []string{"r:$(R)", "r:R|S", "r:R*1..2", "r:R WHERE r.ok", "r:R {x: 1}"} {
		_, ok := exec.parseSimpleMatchRelationshipPattern("(a)-[" + content + "]->(b)")
		require.False(t, ok, content)
	}
}
