package cypher

// NornicDB #860: label expressions (n:A|B, n:A&B, n:!A, n:%, n IS A, r:!R)
// in patterns and predicates. The statements and their expected answers
// were run on neo4j:5.26.30-community (the conformance image); each group
// runs in order on a fresh database, as there.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type issue860Statement struct {
	query   string
	columns []string
	rows    string // JSON, nodes as "N:<id>", relationships as "R:<type>"
	code    string // a Neo4j error code
	message string // its message; "" when Neo4j's is its parser's own (the statement must fail)
	skip    bool   // depends on another issue
}

var issue860Groups = []struct {
	name       string
	statements []issue860Statement
}{
	{
		name: "q",
		statements: []issue860Statement{
			{query: "MATCH (n:Code|Document) RETURN n.id ORDER BY n.id", columns: []string{"n.id"}, rows: "[[\"b\"], [\"c1\"], [\"d1\"]]"},
			{query: "MATCH (n:Code&Document) RETURN n.id", columns: []string{"n.id"}, rows: "[[\"b\"]]"},
			{query: "MATCH (n:!Other) RETURN n.id ORDER BY n.id", columns: []string{"n.id"}, rows: "[[\"b\"], [\"c1\"], [\"d1\"], [\"u\"]]"},
			{query: "MATCH (n:%) RETURN count(n)", columns: []string{"count(n)"}, rows: "[[4]]"},
			{query: "MATCH (n:!%) RETURN n.id", columns: []string{"n.id"}, rows: "[[\"u\"]]"},
			{query: "MATCH (n:(Code|Other)&!Document) RETURN n.id ORDER BY n.id", columns: []string{"n.id"}, rows: "[[\"c1\"], [\"o1\"]]"},
			{query: "MATCH (n:Code|Document:Other) RETURN n.id", code: "Neo.ClientError.Statement.SyntaxError", message: "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :Code|(Document&Other"},
			{query: "MATCH (n:Code|Document {id: 'b'}) RETURN n.id", columns: []string{"n.id"}, rows: "[[\"b\"]]"},
			{query: "MATCH (:Code|Other)-[r]->(m) RETURN m.id ORDER BY m.id", columns: []string{"m.id"}, rows: "[[\"b\"], [\"d1\"]]"},
			{query: "MATCH (a)-[r]->(:Document&Code) RETURN a.id", columns: []string{"a.id"}, rows: "[[\"o1\"]]"},
			{query: "MATCH (:Code|Other)-[r]->(m) RETURN *", columns: []string{"m", "r"}, rows: "[[\"N:d1\", \"R:R\"], [\"N:b\", \"R:S\"]]"},
			{query: "MATCH ()-[r:R|S]->() RETURN count(r)", columns: []string{"count(r)"}, rows: "[[2]]"},
			{query: "MATCH ()-[r:R&S]->() RETURN count(r)", columns: []string{"count(r)"}, rows: "[[0]]"},
			{query: "MATCH ()-[r:!R]->() RETURN count(r)", columns: []string{"count(r)"}, rows: "[[1]]"},
			{query: "MATCH ()-[r:%]->() RETURN count(r)", columns: []string{"count(r)"}, rows: "[[2]]"},
			{query: "MATCH ()-[r:!%]->() RETURN count(r)", columns: []string{"count(r)"}, rows: "[[0]]"},
			{query: "MATCH ()-[r:(R|S)&!S]->() RETURN count(r)", columns: []string{"count(r)"}, rows: "[[1]]"},
			{query: "MATCH ()-[r:R|S*1..2]->() RETURN count(r)", columns: []string{"count(r)"}, rows: "[[2]]"},
			{query: "MATCH ()-[r:R|:S]->() RETURN count(r)", code: "Neo.ClientError.Statement.SyntaxError", message: "The semantics of using colon in the separation of alternative relationship types in conjunction with"},
			{query: "MATCH (n) WHERE (n)-->(:Document|Other) RETURN n.id ORDER BY n.id", columns: []string{"n.id"}, rows: "[[\"c1\"], [\"o1\"]]"},
			// MATCH (n) WHERE exists((n)-->(:Document&Code)) RETURN n.id: depends on #728 (exists() pattern function).
			{query: "MATCH (n) WHERE exists((n)-->(:Document&Code)) RETURN n.id", skip: true},
			{query: "MATCH (n) WHERE EXISTS { (n)-->(:!Document) } RETURN n.id", columns: []string{"n.id"}, rows: "[]"},
			{query: "MATCH (n:Code) RETURN [(n)-->(m:Document|Other) | m.id] AS ms ORDER BY n.id", columns: []string{"ms"}, rows: "[[[]], [[\"d1\"]]]"},
			{query: "MATCH (n) RETURN COUNT { (n)-[:R|S]->(:%) } AS c ORDER BY c DESC LIMIT 1", columns: []string{"c"}, rows: "[[1]]"},
			// MATCH p = shortestPath((a {id:'c1'})-[*]-(b:Document|Other)) RETURN length(p) ORDER BY length(p) LIMIT 1: depends on #863.
			{query: "MATCH p = shortestPath((a {id:'c1'})-[*]-(b:Document|Other)) RETURN length(p) ORDER BY length(p) LIMIT 1", skip: true},
			{query: "MATCH (a {id:'o1'}) OPTIONAL MATCH (a)-->(b:Code&Other) RETURN a.id, b", columns: []string{"a.id", "b"}, rows: "[[\"o1\", null]]"},
			// MATCH ((a:Code|Other)-[:R|S]->(b))+ RETURN count(*): depends on #864.
			{query: "MATCH ((a:Code|Other)-[:R|S]->(b))+ RETURN count(*)", skip: true},
			{query: "MATCH (n IS Code|Document) RETURN count(n)", columns: []string{"count(n)"}, rows: "[[3]]"},
			{query: "MATCH (n) WHERE n IS Code RETURN count(n)", columns: []string{"count(n)"}, rows: "[[2]]"},
			{query: "CREATE (n:A|B) RETURN n", code: "Neo.ClientError.Statement.SyntaxError", message: "Label expressions in patterns are not allowed in a CREATE clause, but only in a MATCH clause and in expressions"},
			{query: "MERGE (n:A|B) RETURN n", code: "Neo.ClientError.Statement.SyntaxError", message: "Label expressions in patterns are not allowed in a MERGE clause, but only in a MATCH clause and in expressions"},
			{query: "CREATE (n:A&B) RETURN labels(n)", columns: []string{"labels(n)"}, rows: "[[[\"A\", \"B\"]]]"},
			{query: "MATCH (n {id:'u'}) SET n:A|B RETURN n", code: "Neo.ClientError.Statement.SyntaxError", message: ""},
			{query: "MATCH (n:Code|Document) WITH n WHERE n.id <> 'b' RETURN count(n)", columns: []string{"count(n)"}, rows: "[[2]]"},
			{query: "MATCH (n:Code|Document) WHERE n.id = 'd1' OR n.id = 'c1' RETURN count(n)", columns: []string{"count(n)"}, rows: "[[2]]"},
			{query: "CALL { MATCH (n:Code|Other) RETURN count(n) AS c } RETURN c", columns: []string{"c"}, rows: "[[3]]"},
			{query: "UNWIND ['c1','o1'] AS i MATCH (n:Code|Other {id: i}) RETURN n.id", columns: []string{"n.id"}, rows: "[[\"c1\"], [\"o1\"]]"},
			{query: "MATCH (n:Code|Document), (m:Other) RETURN count(*)", columns: []string{"count(*)"}, rows: "[[3]]"},
			{query: "MATCH (n:`Code`|`Document`) RETURN count(n)", columns: []string{"count(n)"}, rows: "[[3]]"},
			{query: "MATCH (n: Code | Document) RETURN count(n)", columns: []string{"count(n)"}, rows: "[[3]]"},
			{query: "MATCH (n:Code|Document) DETACH DELETE n RETURN count(n)", columns: []string{"count(n)"}, rows: "[[3]]"},
		},
	},
	{
		name: "q2",
		statements: []issue860Statement{
			{query: "CREATE (n:!A) RETURN n", code: "Neo.ClientError.Statement.SyntaxError", message: "Label expressions in patterns are not allowed in a CREATE clause, but only in a MATCH clause and in expressions"},
			{query: "CREATE (n:%) RETURN n", code: "Neo.ClientError.Statement.SyntaxError", message: "Label expressions in patterns are not allowed in a CREATE clause, but only in a MATCH clause and in expressions"},
			{query: "MERGE (n:A&B) RETURN labels(n)", columns: []string{"labels(n)"}, rows: "[[[\"A\", \"B\"]]]"},
			{query: "CREATE (n IS X) RETURN labels(n)", columns: []string{"labels(n)"}, rows: "[[[\"X\"]]]"},
			{query: "CREATE (n IS X&Y) RETURN labels(n)", columns: []string{"labels(n)"}, rows: "[[[\"X\", \"Y\"]]]"},
			{query: "MERGE (n IS X) RETURN labels(n)", columns: []string{"labels(n)"}, rows: "[[[\"X\"]], [[\"X\", \"Y\"]]]"},
			{query: "CREATE ()-[r:R|S]->() RETURN r", code: "Neo.ClientError.Statement.SyntaxError", message: "A single relationship type must be specified for CREATE"},
			{query: "CREATE ()-[r:!R]->() RETURN r", code: "Neo.ClientError.Statement.SyntaxError", message: "Relationship type expressions in patterns are not allowed in a CREATE clause, but only in a MATCH clause"},
			{query: "MATCH ()-[:R|:S]->() RETURN count(*)", columns: []string{"count(*)"}, rows: "[[2]]"},
			{query: "MATCH ()-[r:R|:S]->() RETURN count(*)", code: "Neo.ClientError.Statement.SyntaxError", message: "The semantics of using colon in the separation of alternative relationship types in conjunction with"},
			{query: "MATCH ()-[:R|:S {w:1}]->() RETURN count(*)", code: "Neo.ClientError.Statement.SyntaxError", message: "The semantics of using colon in the separation of alternative relationship types in conjunction with"},
			{query: "MATCH (n:A&B:C) RETURN n", code: "Neo.ClientError.Statement.SyntaxError", message: "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B&C."},
			{query: "MATCH (n:Code) RETURN n IS Document AS d ORDER BY d", columns: []string{"d"}, rows: "[[false], [true]]"},
			{query: "MATCH (n) WHERE n IS NOT Code RETURN count(n)", code: "Neo.ClientError.Statement.SyntaxError", message: "Invalid input 'Code': expected '::', 'NFC', 'NFD', 'NFKC', 'NFKD', 'NORMALIZED', 'NULL' or 'TYPED'"},
			{query: "MATCH (n) WHERE NOT n IS Code RETURN count(n)", columns: []string{"count(n)"}, rows: "[[6]]"},
			{query: "MATCH ()-[r IS R|S]->() RETURN count(r)", columns: []string{"count(r)"}, rows: "[[2]]"},
			{query: "MATCH ()-[r]->() WHERE r IS R RETURN count(r)", columns: []string{"count(r)"}, rows: "[[1]]"},
			{query: "MATCH (n) WHERE n IS Code|Other RETURN count(n)", columns: []string{"count(n)"}, rows: "[[3]]"},
			{query: "MATCH (n) WHERE n.id IS NOT NULL AND n IS % RETURN count(n)", columns: []string{"count(n)"}, rows: "[[4]]"},
			{query: "MATCH (n IS %) RETURN count(n)", columns: []string{"count(n)"}, rows: "[[7]]"},
			{query: "MATCH (n:Code|Document) WITH * RETURN count(*)", columns: []string{"count(*)"}, rows: "[[3]]"},
			{query: "MATCH (:Code|Other)-[]->(m) WITH * RETURN count(*)", columns: []string{"count(*)"}, rows: "[[2]]"},
			{query: "MATCH (n) WHERE n:Code:Document RETURN count(n)", columns: []string{"count(n)"}, rows: "[[1]]"},
			{query: "MATCH (n) WHERE n:Code&Document RETURN count(n)", columns: []string{"count(n)"}, rows: "[[1]]"},
			{query: "MATCH (n) WHERE n:Code|Document:Other RETURN count(n)", code: "Neo.ClientError.Statement.SyntaxError", message: "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :Code|(Document&Other"},
			{query: "MATCH (n:Code&(Document|Other)) RETURN count(n)", columns: []string{"count(n)"}, rows: "[[1]]"},
			{query: "MATCH (n:Code) WHERE (n)-[:R|S]->(:Document) RETURN count(n)", columns: []string{"count(n)"}, rows: "[[1]]"},
			{query: "MATCH (a)-[:!R]->(b) RETURN count(*)", columns: []string{"count(*)"}, rows: "[[1]]"},
			{query: "MATCH (a)-[:%*1..3]->(b) RETURN count(*)", code: "Neo.ClientError.Statement.SyntaxError", message: "Variable length relationships must not use relationship type expressions."},
			{query: "MATCH (a)-[r:!R*1..3]->(b) RETURN count(*)", code: "Neo.ClientError.Statement.SyntaxError", message: "Variable length relationships must not use relationship type expressions."},
			{query: "MATCH (n) RETURN n:Code|Document AS x ORDER BY x", columns: []string{"x"}, rows: "[[false], [false], [false], [false], [false], [true], [true], [true]]"},
			{query: "MATCH (n:Code|Document) RETURN labels(n) ORDER BY n.id", columns: []string{"labels(n)"}, rows: "[[[\"Code\", \"Document\"]], [[\"Code\"]], [[\"Document\"]]]"},
			{query: "MATCH (n:Code|Document)-[r]->(m) RETURN count(*)", columns: []string{"count(*)"}, rows: "[[1]]"},
			{query: "MATCH (n:Code) OPTIONAL MATCH (n)-[r:!S]->(m:Document|Other) RETURN n.id, m.id ORDER BY n.id", columns: []string{"n.id", "m.id"}, rows: "[[\"b\", null], [\"c1\", \"d1\"]]"},
			{query: "MATCH (n:Code|Document) SET n.seen = true RETURN count(n)", columns: []string{"count(n)"}, rows: "[[3]]"},
			{query: "MATCH (n:Code|Document) RETURN n.id ORDER BY n.id SKIP 1 LIMIT 1", columns: []string{"n.id"}, rows: "[[\"c1\"]]"},
		},
	},
	{
		name: "err",
		statements: []issue860Statement{
			{query: "MATCH (n:Code|Document:Other) RETURN n", code: "Neo.ClientError.Statement.SyntaxError", message: "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :Code|(Document&Other"},
			{query: "MATCH (n:A&B:C) RETURN n", code: "Neo.ClientError.Statement.SyntaxError", message: "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B&C."},
			{query: "MATCH (n:A:B|C) RETURN n", code: "Neo.ClientError.Statement.SyntaxError", message: "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :(A&B)|C."},
			{query: "MATCH (n:A:!B) RETURN n", code: "Neo.ClientError.Statement.SyntaxError", message: "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&!B."},
			{query: "MATCH (n:Code:%) RETURN n", code: "Neo.ClientError.Statement.SyntaxError", message: "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :Code&%."},
			{query: "MATCH ()-[r:R|:S]->() RETURN r", code: "Neo.ClientError.Statement.SyntaxError", message: "The semantics of using colon in the separation of alternative relationship types in conjunction with"},
			{query: "MATCH ()-[:R|:S {w:1}]->() RETURN 1", code: "Neo.ClientError.Statement.SyntaxError", message: "The semantics of using colon in the separation of alternative relationship types in conjunction with"},
			{query: "MATCH ()-[:R|:S*]->() RETURN 1", code: "Neo.ClientError.Statement.SyntaxError", message: "The semantics of using colon in the separation of alternative relationship types in conjunction with"},
			{query: "MATCH ()-[r:R|:S*]->() RETURN 1", code: "Neo.ClientError.Statement.SyntaxError", message: "The semantics of using colon in the separation of alternative relationship types in conjunction with"},
			{query: "MATCH ()-[:R|:S|T]->() RETURN 1", columns: []string{"1"}, rows: "[[1], [1]]"},
			{query: "MATCH ()-[:R|S|:T]->() RETURN 1", columns: []string{"1"}, rows: "[[1], [1]]"},
			{query: "MATCH ()-[:!R*]->() RETURN 1", code: "Neo.ClientError.Statement.SyntaxError", message: "Variable length relationships must not use relationship type expressions."},
			{query: "MATCH ()-[:R&S*1..2]->() RETURN 1", code: "Neo.ClientError.Statement.SyntaxError", message: "Variable length relationships must not use relationship type expressions."},
			{query: "MATCH ()-[:(R|S)*]->() RETURN 1", columns: []string{"1"}, rows: "[[1], [1]]"},
			{query: "MATCH ()-[:R|S*]->() RETURN 1", columns: []string{"1"}, rows: "[[1], [1]]"},
			{query: "MATCH ()-[:R:S]->() RETURN 1", code: "Neo.ClientError.Statement.SyntaxError", message: "Relationship types in a relationship type expressions may not be combined using ':'"},
			{query: "CREATE (n:A|B)", code: "Neo.ClientError.Statement.SyntaxError", message: "Label expressions in patterns are not allowed in a CREATE clause, but only in a MATCH clause and in expressions"},
			{query: "CREATE (n:!A)", code: "Neo.ClientError.Statement.SyntaxError", message: "Label expressions in patterns are not allowed in a CREATE clause, but only in a MATCH clause and in expressions"},
			{query: "MERGE (n:A|B)", code: "Neo.ClientError.Statement.SyntaxError", message: "Label expressions in patterns are not allowed in a MERGE clause, but only in a MATCH clause and in expressions"},
			{query: "MERGE (n:%)", code: "Neo.ClientError.Statement.SyntaxError", message: "Label expressions in patterns are not allowed in a MERGE clause, but only in a MATCH clause and in expressions"},
			{query: "CREATE ()-[:R|S]->()", code: "Neo.ClientError.Statement.SyntaxError", message: "A single relationship type must be specified for CREATE"},
			{query: "CREATE ()-[:!R]->()", code: "Neo.ClientError.Statement.SyntaxError", message: "Relationship type expressions in patterns are not allowed in a CREATE clause, but only in a MATCH clause"},
			{query: "CREATE ()-[:R&S]->()", code: "Neo.ClientError.Statement.SyntaxError", message: "Relationship type expressions in patterns are not allowed in a CREATE clause, but only in a MATCH clause"},
			{query: "MERGE ()-[:R|S]->()", code: "Neo.ClientError.Statement.SyntaxError", message: "A single relationship type must be specified for MERGE"},
			{query: "MERGE ()-[:%]->()", code: "Neo.ClientError.Statement.SyntaxError", message: "Relationship type expressions in patterns are not allowed in a MERGE clause, but only in a MATCH clause"},
			{query: "CREATE (n:A&B:C)", code: "Neo.ClientError.Statement.SyntaxError", message: "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B&C."},
			{query: "CREATE (n IS A:B)", code: "Neo.ClientError.Statement.SyntaxError", message: "Mixing the IS keyword with colon (':') between labels is not allowed. This expression could be expressed as IS A&B."},
			{query: "MATCH (n IS A:B) RETURN n", code: "Neo.ClientError.Statement.SyntaxError", message: "Mixing the IS keyword with colon (':') between labels is not allowed. This expression could be expressed as IS A&B."},
			{query: "MATCH (n IS A|B) WHERE n IS !C RETURN count(n)", columns: []string{"count(n)"}, rows: "[[0]]"},
		},
	},
	{
		name: "q3",
		statements: []issue860Statement{
			{query: "MATCH (:Code|Other)-[r]->(m) RETURN *", columns: []string{"m", "r"}, rows: "[[\"N:d1\", \"R:R\"], [\"N:b\", \"R:S\"]]"},
			// MATCH (:Code|Other)-[r]->(m) RETURN DISTINCT *: depends on #713 (RETURN DISTINCT *).
			{query: "MATCH (:Code|Other)-[r]->(m) RETURN DISTINCT *", skip: true},
			// MATCH (:Code|Other)-->(m) RETURN DISTINCT *: depends on #713 (RETURN DISTINCT *).
			{query: "MATCH (:Code|Other)-->(m) RETURN DISTINCT *", skip: true},
			{query: "MATCH (:Code|Other)-[]->(m) WITH * RETURN *", columns: []string{"m"}, rows: "[[\"N:d1\"], [\"N:b\"]]"},
			{query: "MATCH (:Code|Other)-[]->(m) WITH DISTINCT * RETURN count(*)", columns: []string{"count(*)"}, rows: "[[2]]"},
			{query: "MATCH (a)-[:!R]->(m) RETURN * ORDER BY m.id", columns: []string{"a", "m"}, rows: "[[\"N:o1\", \"N:b\"]]"},
			{query: "MATCH (n:Code) RETURN [(n)-->(m:Document|Other) | m.id] AS ms ORDER BY n.id", columns: []string{"ms"}, rows: "[[[]], [[\"d1\"]]]"},
			{query: "MATCH (n:Code) RETURN [(n)-->(m) WHERE m:Document|Other | m.id] AS ms ORDER BY n.id", columns: []string{"ms"}, rows: "[[[]], [[\"d1\"]]]"},
			{query: "MATCH (n:Code) RETURN [(n)-->(m:Document|Other) WHERE m.id <> 'x' | m.id] AS ms ORDER BY n.id", columns: []string{"ms"}, rows: "[[[]], [[\"d1\"]]]"},
			{query: "MATCH (n:Code) RETURN [(n)-->(m:Document|Other) | m.id]", columns: []string{"[(n)-->(m:Document|Other) | m.id]"}, rows: "[[[\"d1\"]], [[]]]"},
			{query: "MATCH (n) WHERE n IS NOT Code RETURN count(n)", code: "Neo.ClientError.Statement.SyntaxError", message: "Invalid input 'Code': expected '::', 'NFC', 'NFD', 'NFKC', 'NFKD', 'NORMALIZED', 'NULL' or 'TYPED'"},
			{query: "MATCH (n) WHERE n.id IS NOT NULL RETURN count(n)", columns: []string{"count(n)"}, rows: "[[5]]"},
			{query: "MATCH ()-[:R:S]->() RETURN 1", code: "Neo.ClientError.Statement.SyntaxError", message: "Relationship types in a relationship type expressions may not be combined using ':'"},
			{query: "MATCH (n:Code) RETURN n IS Document", columns: []string{"n IS Document"}, rows: "[[false], [true]]"},
			{query: "MATCH (n:Code) RETURN n IS Document AS d, n:Document|Other AS e ORDER BY d", columns: []string{"d", "e"}, rows: "[[false, false], [true, true]]"},
			{query: "MATCH (n:Code)-[r]->(m) RETURN r, n, m", columns: []string{"r", "n", "m"}, rows: "[[\"R:R\", \"N:c1\", \"N:d1\"]]"},
			{query: "UNWIND [1,2] AS x MATCH (n:Code|Other) RETURN x, count(n) ORDER BY x", columns: []string{"x", "count(n)"}, rows: "[[1, 3], [2, 3]]"},
			{query: "MATCH (n:Code|Document) WHERE n.id = 'b' OR n.id = 'd1' RETURN count(n)", columns: []string{"count(n)"}, rows: "[[2]]"},
			// MATCH (n:Code|Document) WHERE n.id = 'c1' XOR n:Code RETURN count(n): depends on #728 (XOR).
			{query: "MATCH (n:Code|Document) WHERE n.id = 'c1' XOR n:Code RETURN count(n)", skip: true},
			{query: "MATCH (n:Code|Document) WITH n MATCH (n)-[:R|S]->(m:!Code) RETURN n.id, m.id", columns: []string{"n.id", "m.id"}, rows: "[[\"c1\", \"d1\"]]"},
			{query: "OPTIONAL MATCH (n:Missing|Other) RETURN n.id", columns: []string{"n.id"}, rows: "[[\"o1\"]]"},
			{query: "MATCH (n:%) WHERE NOT n:Code RETURN n.id ORDER BY n.id", columns: []string{"n.id"}, rows: "[[\"d1\"], [\"o1\"]]"},
			{query: "CALL { MATCH (x:!%) RETURN x } RETURN x.id", columns: []string{"x.id"}, rows: "[[\"u\"]]"},
			{query: "MATCH (n) WHERE COUNT { (n)-->(:Code|Document) } > 0 RETURN n.id ORDER BY n.id", columns: []string{"n.id"}, rows: "[[\"c1\"], [\"o1\"]]"},
			{query: "MATCH (n) RETURN n.id, EXISTS { MATCH (n)-->(m) WHERE m IS Document } AS e ORDER BY n.id", columns: []string{"n.id", "e"}, rows: "[[\"b\", false], [\"c1\", true], [\"d1\", false], [\"o1\", true], [\"u\", false]]"},
		},
	},
}

// issue860Value is a value as the Neo4j side of the test records it.
func issue860Value(value interface{}) interface{} {
	switch v := value.(type) {
	case *storage.Node:
		if id, ok := v.Properties["id"]; ok {
			return fmt.Sprintf("N:%v", id)
		}
		return "N:None"
	case *storage.Edge:
		return "R:" + v.Type
	case []interface{}:
		out := make([]interface{}, len(v))
		for i, item := range v {
			out[i] = issue860Value(item)
		}
		return out
	case []string:
		out := make([]interface{}, len(v))
		for i, item := range v {
			out[i] = item
		}
		return out
	}
	return value
}

// issue860Rows returns rows as comparable JSON values, sorted when the
// statement has no ORDER BY.
func issue860Rows(t *testing.T, query string, rows interface{}) []string {
	t.Helper()
	encoded, err := json.Marshal(rows)
	require.NoError(t, err)
	var decoded [][]interface{}
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	out := make([]string, len(decoded))
	for i, row := range decoded {
		text, err := json.Marshal(row)
		require.NoError(t, err)
		out[i] = string(text)
	}
	if !strings.Contains(strings.ToUpper(query), "ORDER BY") {
		sort.Strings(out)
	}
	return out
}

func TestIssue860LabelExpressionsMatchNeo4j(t *testing.T) {
	stacks := map[string]func(t *testing.T) *StorageExecutor{
		"memory": func(t *testing.T) *StorageExecutor {
			exec, _ := newTestExecutor(t)
			return exec
		},
		"async stack": newAsyncStackTestExecutor,
	}
	for stack, build := range stacks {
		for _, mode := range []string{"auto-commit", "explicit transaction"} {
			for _, group := range issue860Groups {
				t.Run(stack+"/"+mode+"/"+group.name, func(t *testing.T) {
					exec := build(t)
					ctx := context.Background()
					run := func(q string) (*ExecuteResult, error) {
						if mode == "explicit transaction" {
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
						}
						res, err := exec.Execute(ctx, q, nil)
						if mode == "explicit transaction" {
							if err != nil {
								_, _ = exec.Execute(ctx, "ROLLBACK", nil)
							} else {
								_, commitErr := exec.Execute(ctx, "COMMIT", nil)
								require.NoError(t, commitErr, q)
							}
						}
						return res, err
					}
					_, err := run("CREATE (:Code {id: 'c1'})-[:R {w:1}]->(:Document {id: 'd1'}), (:Other {id: 'o1'})-[:S]->(:Code:Document {id: 'b'}), ({id:'u'})")
					require.NoError(t, err)
					for _, st := range group.statements {
						if st.skip {
							continue
						}
						res, err := run(st.query)
						if st.code != "" && st.message == "" {
							// An existing error that Bolt classifies; the
							// statement must fail.
							require.Error(t, err, st.query)
							continue
						}
						if st.code != "" {
							require.Error(t, err, st.query)
							var classified interface{ BoltErrorCode() string }
							require.True(t, errors.As(err, &classified), "%s: %v", st.query, err)
							require.Equal(t, st.code, classified.BoltErrorCode(), "%s: %v", st.query, err)
							if st.message != "" {
								require.True(t, strings.HasPrefix(err.Error(), st.message), "%s:\n got %q\nwant %q", st.query, err.Error(), st.message)
							}
							continue
						}
						require.NoError(t, err, st.query)
						if st.columns != nil {
							require.Equal(t, st.columns, res.Columns, st.query)
						}
						got := make([][]interface{}, len(res.Rows))
						for i, row := range res.Rows {
							got[i] = make([]interface{}, len(row))
							for j, value := range row {
								got[i][j] = issue860Value(value)
							}
						}
						var want [][]interface{}
						require.NoError(t, json.Unmarshal([]byte(st.rows), &want))
						require.Equal(t, issue860Rows(t, st.query, want), issue860Rows(t, st.query, got), st.query)
					}
				})
			}
		}
	}
}
