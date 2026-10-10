package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A backtick-quoted relationship type names the type without its quotes in
// CREATE, MERGE, MATCH and the patterns that reuse MATCH's reader, and may
// hold any character, as in Neo4j 5.26. CREATE rejected every quoted type,
// MERGE stored the quotes as part of the name, and MATCH looked for the
// quoted text.
func TestQuotedRelationshipTypes(t *testing.T) {
	for _, tc := range []struct {
		statements []string
		want       [][]interface{}
	}{
		{[]string{"CREATE ()-[r:`KN`]->() RETURN type(r) AS t"}, [][]interface{}{{"KN"}}},
		{[]string{"CREATE ()-[r:`Ts R`]->() RETURN type(r) AS t"}, [][]interface{}{{"Ts R"}}},
		{[]string{"CREATE ()-[r:`a:b`]->() RETURN type(r) AS t"}, [][]interface{}{{"a:b"}}},
		{[]string{"CREATE ()-[r:`a{b`]->() RETURN type(r) AS t"}, [][]interface{}{{"a{b"}}},
		{[]string{"CREATE ()-[r:`a``b`]->() RETURN type(r) AS t"}, [][]interface{}{{"a`b"}}},
		{[]string{"CREATE ()-[r:`a|b`]->() RETURN type(r) AS t"}, [][]interface{}{{"a|b"}}},
		{[]string{"CREATE ()-[r:`a*b`]->() RETURN type(r) AS t"}, [][]interface{}{{"a*b"}}},
		{[]string{"CREATE ()-[r:`a{b` {k: '}'}]->() RETURN r.k AS k"}, [][]interface{}{{"}"}}},
		{[]string{"CREATE ()-[`my r`:`Ts R` {w: 1}]->() RETURN type(`my r`) AS t, `my r`.w AS w"}, [][]interface{}{{"Ts R", int64(1)}}},
		{[]string{"CREATE (a)-[r:`Ts R`]->(b)-[s:`Ts S`]->(c) RETURN type(r) + type(s) AS t"}, [][]interface{}{{"Ts RTs S"}}},
		{[]string{"CREATE (a), (b) WITH a, b CREATE (a)-[r:`Ts W`]->(b) RETURN type(r) AS t"}, [][]interface{}{{"Ts W"}}},
		{[]string{"MERGE ()-[r:`Ts M`]->() RETURN type(r) AS t"}, [][]interface{}{{"Ts M"}}},
		{[]string{"MERGE (a:Rq {id: 1}) MERGE (a)-[r:`Ts M`]->(b:Rq {id: 2}) RETURN type(r) AS t"}, [][]interface{}{{"Ts M"}}},
		{[]string{"CREATE ()-[:`Ts R`]->()", "MERGE ()-[r:`Ts R`]->() RETURN type(r) AS t"}, [][]interface{}{{"Ts R"}}},
		{[]string{"CREATE ()-[:`Ts R`]->()", "MATCH ()-[r:`Ts R`]->() RETURN type(r) AS t"}, [][]interface{}{{"Ts R"}}},
		{[]string{"CREATE ()-[:KN]->()", "MATCH ()-[r:`KN`]->() RETURN type(r) AS t"}, [][]interface{}{{"KN"}}},
		{[]string{"CREATE ()-[:`Ts R`]->()", "MATCH ()-[r:`Ts R`|Other]->() RETURN type(r) AS t"}, [][]interface{}{{"Ts R"}}},
		{[]string{"CREATE ()-[:`Ts R`]->()", "MATCH ()-[:Other|:`Ts R`]->() RETURN count(*) AS t"}, [][]interface{}{{int64(1)}}},
		{[]string{"CREATE ()-[:`a:b`]->()", "MATCH ()-[r:`a:b`]->() RETURN type(r) AS t"}, [][]interface{}{{"a:b"}}},
		{[]string{"CREATE ()-[:`Ts R`]->()-[:`Ts R`]->()", "MATCH p = ()-[:`Ts R`*2]->() RETURN length(p) AS t"}, [][]interface{}{{int64(2)}}},
		{[]string{"CREATE ()-[:`Ts R`]->()-[:`Ts R`]->()", "MATCH p = ()-[:`Ts R`*1..]->() RETURN max(length(p)) AS t"}, [][]interface{}{{int64(2)}}},
		{[]string{"CREATE ()-[:`Ts R`]->()-[:`Ts R`]->()", "MATCH p = ()-[*..1]->() RETURN max(length(p)) AS t"}, [][]interface{}{{int64(1)}}},
		{[]string{"CREATE ()-[:`Ts R`]->()", "MATCH (a) WHERE (a)-[:`Ts R`]->() RETURN count(a) AS t"}, [][]interface{}{{int64(1)}}},
		{[]string{"CREATE ()-[:`Ts R`]->()", "OPTIONAL MATCH ()-[r:`Ts R`]->() RETURN type(r) AS t"}, [][]interface{}{{"Ts R"}}},
		{[]string{"CREATE ()-[:`Ts R`]->()", "MATCH ()-[r]->() WHERE r:`Ts R` RETURN type(r) AS t"}, [][]interface{}{{"Ts R"}}},
		{[]string{"CREATE ()-[:`Ts R` {w: 1}]->()", "MATCH ()-[r:`Ts R` {w: 1}]->() DELETE r RETURN count(*) AS t"}, [][]interface{}{{int64(1)}}},
		{[]string{"CREATE (a:Sp)-[:`Ts R`]->(b:Sp)", "MATCH (a:Sp), (b:Sp) WHERE a <> b MATCH p = shortestPath((a)-[:`Ts R`*]-(b)) RETURN length(p) AS t LIMIT 1"}, [][]interface{}{{int64(1)}}},
	} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quoted_relationship_types"))
		var result *ExecuteResult
		for _, statement := range tc.statements {
			var err error
			result, err = exec.Execute(context.Background(), statement, nil)
			require.NoError(t, err, statement)
		}
		require.Equal(t, tc.want, result.Rows, tc.statements)
	}

	// Alternatives, a variable length, more than one type and an empty
	// quoted type are SyntaxErrors in CREATE and MERGE, as in Neo4j 5.26,
	// whatever a quoted type holds.
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quoted_relationship_type_errors"))
	for _, statement := range []string{
		"CREATE ()-[:`a|b`|C]->()",
		"MERGE ()-[:`a|b`|C]->()",
		"CREATE ()-[:`a*b`*2]->()",
		"MERGE ()-[:`a*b`*2]->()",
		"CREATE ()-[:`a:b`:C]->()",
		"CREATE ()-[r]->()",
		"CREATE ()-[:``]->()",
		"MERGE ()-[:``]->()",
		"CREATE ()-[:Ts R]->()",
	} {
		_, err := exec.Execute(context.Background(), statement, nil)
		require.Error(t, err, statement)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
}

func TestRelationshipDeclarationOf(t *testing.T) {
	for _, tc := range []struct {
		inner string
		want  relationshipDeclaration
	}{
		{"", relationshipDeclaration{}},
		{"r", relationshipDeclaration{variable: "r"}},
		{"r:A|B", relationshipDeclaration{variable: "r", hasColon: true, typeText: "A|B", types: []string{"A", "B"}}},
		{":A|:`B c`", relationshipDeclaration{hasColon: true, typeText: "A|:`B c`", types: []string{"A", "B c"}}},
		{"r:`a|b`*1..2 {k: '*'}", relationshipDeclaration{variable: "r", hasColon: true, typeText: "`a|b`", types: []string{"a|b"}, hasLength: true, length: "1..2", properties: "{k: '*'}"}},
		{":$(t)", relationshipDeclaration{hasColon: true, typeText: "$(t)", types: []string{"$(t)"}}},
		{"r*2..5:KNOWS", relationshipDeclaration{variable: "r", hasColon: true, typeText: "KNOWS", types: []string{"KNOWS"}, hasLength: true, length: "2..5"}},
		{"r:R $p", relationshipDeclaration{variable: "r", hasColon: true, typeText: "R", types: []string{"R"}, properties: "$p"}},
		{":$(a:b)|C", relationshipDeclaration{hasColon: true, typeText: "$(a:b)|C", types: []string{"$(a:b)", "C"}}},
		{":Ts R", relationshipDeclaration{hasColon: true, typeText: "Ts R", types: []string{"Ts R"}, invalidType: "Ts R"}},
		{":``|`a`", relationshipDeclaration{hasColon: true, typeText: "``|`a`", types: []string{"", "a"}, invalidType: "``"}},
	} {
		require.Equal(t, tc.want, relationshipDeclarationOf(tc.inner), tc.inner)
	}
	require.Equal(t, "A|B", relationshipDeclarationOf(":A|B").singleType())
	require.Equal(t, "$(t)", relationshipDeclarationOf(":$(t)").singleType())
	require.Equal(t, "a b", relationshipDeclarationOf(":`a b`").singleType())
}
