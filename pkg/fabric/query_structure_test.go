package fabric

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestFabricGrammarRuleOwnership(t *testing.T) {
	for _, name := range []string{"union", "use", "return", "with", "create", "call", "not", "all", "distinct"} {
		text := "WITH 7 AS " + name + " WITH DISTINCT " + name + " CALL { WITH " + name + " USE comp.one RETURN " + name + " AS k } RETURN k"
		q := ReadQueryStructure(text)
		require.NoError(t, q.Err, text)
		require.Len(t, q.Clauses, 4, text)
		require.Equal(t, "WITH", q.Clauses[1].Kind)
		require.Equal(t, name, q.Clauses[1].Projection.Items[0].Name)
		require.Equal(t, "CALL", q.Clauses[2].Kind)
		require.Len(t, q.Clauses[2].Nested.Clauses, 3)
		require.False(t, q.Writes())
	}
	q := ReadQueryStructure("RETURN v ORDER /* UNION CALL RETURN */ BY v DESC LIMIT 1")
	require.NoError(t, q.Err)
	require.Equal(t, "1", q.Clauses[0].Projection.Limit)
	require.Len(t, q.Clauses[0].Projection.Order, 1)
	require.True(t, q.Clauses[0].Projection.Order[0].Desc)
	q = ReadQueryStructure("CALL { USE comp.one RETURN 1 AS k } IN /* USE */ TRANSACTIONS OF 10 ROWS RETURN k")
	require.NoError(t, q.Err)
	require.True(t, q.Clauses[0].InTransactions)
}

func TestFabricStructureRenderingSourceSpans(t *testing.T) {
	for _, text := range []string{"  WITH 7 AS union RETURN coalesce(union,0) /* CALL */", "RETURN 'USE UNION' AS k ORDER\n/* WITH */ BY k", "NODETACH DELETE n"} {
		q := ReadQueryStructure(text)
		require.NoError(t, q.Err)
		var out strings.Builder
		offset := 0
		for _, edit := range q.LocalEdits {
			require.GreaterOrEqual(t, edit.Start, offset)
			out.WriteString(text[offset:edit.Start])
			out.WriteString(edit.Text)
			offset = edit.End
		}
		out.WriteString(text[offset:])
		require.Equal(t, q.LocalText, out.String(), "structural rendering and its source map agree")
	}
}

func TestFabricStructurePublicProjection(t *testing.T) {
	text := "RETURN 1 AS `UNION`, 2 AS `CALL` UNION ALL RETURN 3 AS `CALL`, 4 AS `UNION`"
	q := ReadQueryStructure(text)
	require.NoError(t, q.Err)
	require.Equal(t, []string{"UNION", "CALL"}, q.ResultColumns())
	clause := q.ResultClause()
	require.Equal(t, "1 AS `UNION`, 2 AS `CALL`", text[clause.BodyStart:clause.End])
}

func TestFabricStructureInvocationArgumentNames(t *testing.T) {
	for _, example := range []struct{ text, invocation string }{
		{"WITH 'comp.one' AS distinct USE graph.byName(distinct) RETURN 7 AS k", "graph.byName(`distinct`)"},
		{"WITH 7 AS distinct RETURN toInteger(distinct) AS k", "toInteger(`distinct`)"},
		{"WITH 7 AS distinct RETURN count(DISTINCT distinct) AS k", "count(DISTINCT `distinct`)"},
	} {
		q := ReadQueryStructure(example.text)
		require.NoError(t, q.Err, example.text)
		require.Contains(t, q.LocalText, example.invocation, example.text)
	}
}

func TestFabricStructureGeneratedIdentifierRoles(t *testing.T) {
	for _, name := range []string{"false", "true", "null", "union", "call", "with", "a b", "a`b"} {
		quoted := "`" + strings.ReplaceAll(name, "`", "``") + "`"
		text := "WITH 7 AS " + quoted + " WITH DISTINCT " + quoted
		generated := ensureRowProducingPrefix(text)
		q := ReadQueryStructure(generated)
		require.NoError(t, q.Err, generated)
		require.Equal(t, name, q.ResultClause().Projection.Items[0].Name, generated)
		imported := rewriteLeadingWithImports("RETURN "+quoted+" AS k", []string{name})
		q = ReadQueryStructure(imported)
		require.NoError(t, q.Err, imported)
		require.Equal(t, name, q.Clauses[0].Projection.Items[0].Alias, imported)
		require.Equal(t, name, q.ResultClause().Projection.Items[0].Name, imported)
	}
}

func TestFabricStructureGraphSelectionNames(t *testing.T) {
	for _, name := range []string{"union", "use", "return", "with", "call", "create", "graph", "true", "false", "null"} {
		for _, written := range []string{name, "`" + name + "`"} {
			text := "USE GRAPH " + written + " RETURN 7 AS k"
			q := ReadQueryStructure(text)
			require.NoError(t, q.Err, text)
			require.Equal(t, name, q.Clauses[0].Use.Name, text)
			clause, remaining, found, err := ParseUseClause(text, false)
			require.NoError(t, err, text)
			require.True(t, found, text)
			require.Equal(t, name, clause.Name, text)
			require.Equal(t, "RETURN 7 AS k", remaining, text)
		}
	}
}

func TestFabricStructureIncompleteGraphInvocation(t *testing.T) {
	text := "USE graph.byName(1 +) MATCH (n:EI) RETURN n.v AS v"
	q := ReadQueryStructure(text)
	require.Error(t, q.Err)
	require.Equal(t, []UseClause{{Function: "graph.byName"}}, q.Uses())
}

func TestEmbeddingOptionUsesGrammarContext(t *testing.T) {
	for _, separator := range []string{" ", "  ", "\n", " /* option */ "} {
		text := "CREATE (n:Doc) WITH" + separator + "EMBEDDING RETURN n"
		stripped, ok := StripEmbeddingOption(text)
		if !ok || stripped != "CREATE (n:Doc) RETURN n" {
			t.Fatalf("option %q: %q %v", text, stripped, ok)
		}
		if strings.Contains(LocalQuery(text), "`EMBEDDING`") {
			t.Fatalf("option lowered as a name: %q", LocalQuery(text))
		}
	}
	for _, text := range []string{
		"WITH 7 AS embedding WITH embedding RETURN embedding",
		"CREATE (embedding:Doc) WITH embedding AS retained RETURN retained",
		"WITH $embedding AS embedding RETURN embedding",
		"RETURN 'WITH EMBEDDING' AS s",
		"RETURN 7 AS `WITH EMBEDDING`",
		"RETURN 7 /* WITH EMBEDDING */ AS k",
	} {
		if _, ok := StripEmbeddingOption(text); ok {
			t.Fatalf("name/data read as option: %q", text)
		}
	}
}

func TestEmbeddingProjectionScope(t *testing.T) {
	for _, text := range []string{
		"CREATE (embedding) WITH embedding RETURN embedding",
		"CREATE (EMBEDDING) WITH EMBEDDING RETURN EMBEDDING",
		"INSERT (embedding) WITH embedding RETURN embedding",
		"CREATE (embedding) NODETACH DELETE embedding WITH embedding RETURN 7",
		"WITH 7 AS embedding CREATE (n) WITH embedding RETURN embedding",
		"MATCH (embedding) SET embedding.k=7 WITH embedding RETURN embedding.k",
		"UNWIND [7] AS embedding CREATE (n) WITH embedding RETURN embedding",
		"CALL { RETURN 7 AS embedding } CREATE (n) WITH embedding RETURN embedding",
		"CREATE (`embedding`) WITH `embedding` CREATE (n) WITH embedding RETURN embedding",
		"CREATE (embedding) WITH embedding AS retained RETURN retained",
		"CREATE (embedding) WITH embedding, 7 AS k RETURN k",
		"CALL () { CREATE (embedding) WITH embedding RETURN embedding } RETURN 7",
	} {
		t.Run(text, func(t *testing.T) {
			q := ReadQueryStructure(text)
			require.NoError(t, q.Err)
			stripped, option := StripEmbeddingOption(text)
			require.False(t, option)
			require.Equal(t, text, stripped)
		})
	}
	for _, text := range []string{
		"CREATE (n:embedding {embedding:7}) WITH EMBEDDING RETURN n",
		"CREATE (embedding) WITH 7 AS k CREATE (n) WITH EMBEDDING RETURN n",
		"CREATE (EMBEDDING) WITH EMBEDDING",
		"CREATE (n) WITH EMBEDDING RETURN n UNION CREATE (embedding) WITH embedding RETURN embedding",
	} {
		t.Run(text, func(t *testing.T) {
			q := ReadQueryStructure(text)
			require.NoError(t, q.Err)
			_, option := StripEmbeddingOption(text)
			require.True(t, option)
		})
	}
}
