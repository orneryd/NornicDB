package fabric

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"strings"
	"testing"
)

// The parser lives in cypher. These fixtures exercise Fabric's value projection
// against already-parsed plans, retaining the existing input/row assertions.
func testHeaderProjection(query string) resultmetadata.Projection {
	// Correlated operator tests pin the owner's projection facts as well as
	// the mock's row results. No query parser is duplicated in this package.
	switch query {
	case "WITH textKey128 MATCH (tt:MongoDocument) WHERE tt.translationId = textKey128 RETURN collect(tt) AS texts",
		"WITH textKey128 USE translations.txr MATCH (tt:MongoDocument) WHERE tt.language = 'es' AND tt.translationId = textKey128 RETURN collect(tt) AS texts",
		"WITH textKey128 MATCH (tt:MongoDocument) WHERE textKey128 = tt.translationId RETURN collect(tt) AS texts",
		"WITH textKey128 MATCH (tt) WHERE tt.id = textKey128 RETURN collect(tt) AS texts":
		return resultmetadata.Projection{Expressions: []string{"collect(tt)"}, Names: resultmetadata.Known([]string{"texts"})}
	case "WITH id MATCH (m:Person) WHERE m.id = id RETURN m.name AS name, m.age AS age":
		return resultmetadata.Projection{Expressions: []string{"m.name", "m.age"}, Names: resultmetadata.Known([]string{"name", "age"})}
	case "WITH id MATCH (n) WHERE n.id = id RETURN n.name AS name, n.age AS age":
		return resultmetadata.Projection{Expressions: []string{"n.name", "n.age"}, Names: resultmetadata.Known([]string{"name", "age"})}
	case "WITH id MATCH (n) WHERE n.id = id RETURN n.name AS name":
		return resultmetadata.Projection{Expressions: []string{"n.name"}, Names: resultmetadata.Known([]string{"name"})}
	case "WITH sourceId MATCH (tt:MongoDocument) WHERE tt.translationId = sourceId AND sourceId IS NOT NULL RETURN tt.language AS language, tt.translatedText AS translatedText",
		"WITH textKey128 AS hash MATCH (tt) WHERE tt.translationId = hash RETURN tt.language AS language, tt.translatedText AS translatedText":
		return resultmetadata.Projection{Expressions: []string{"tt.language", "tt.translatedText"}, Names: resultmetadata.Known([]string{"language", "translatedText"})}
	case "WITH sourceId USE translations.txr MATCH (tt:MongoDocument) WHERE tt.translationId = sourceId AND tt.language IS NOT NULL AND tt.translatedText IS NOT NULL RETURN tt.language AS language, tt.translatedText AS translatedText ORDER BY tt.language DESC LIMIT 1":
		return resultmetadata.Projection{Expressions: []string{"tt.language", "tt.translatedText"}, Names: resultmetadata.Known([]string{"language", "translatedText"}), Modifiers: "ORDER BY tt.language DESC LIMIT 1"}
	case "WITH sourceId MATCH (tt:MongoDocument)-[:IN_LANG]->(l:Lang) WHERE tt.translationId = sourceId RETURN count(*) AS c",
		"WITH sourceId MATCH (tt:MongoDocument) WHERE tt.translationId = sourceId AND sourceId IS NOT NULL RETURN count(*) AS c":
		return resultmetadata.Projection{Expressions: []string{"count(*)"}, Names: resultmetadata.Known([]string{"c"})}
	}
	plans := map[string]resultmetadata.Projection{
		"RETURN id, name":               {Expressions: []string{"id", "name"}, Names: resultmetadata.Known([]string{"id", "name"}), Standalone: true},
		"RETURN id AS key, name AS val": {Expressions: []string{"id", "name"}, Names: resultmetadata.Known([]string{"key", "val"}), Standalone: true},
		"MATCH (m) WHERE m.ref = $id RETURN collect(m.name) AS names": {Expressions: []string{"collect(m.name)"}, Names: resultmetadata.Known([]string{"names"})},
		"RETURN textKey AS key, textKey128 AS key128":                 {Expressions: []string{"textKey", "textKey128"}, Names: resultmetadata.Known([]string{"key", "key128"}), Standalone: true},
		"RETURN coalesce(textKey, textKey128) AS key":                 {Expressions: []string{"coalesce(textKey, textKey128)"}, Names: resultmetadata.Known([]string{"key"}), Standalone: true},
		"MATCH (n) RETURN collect(n.id) AS ids":                       {Expressions: []string{"collect(n.id)"}, Names: resultmetadata.Known([]string{"ids"})},
		"RETURN n.id AS id":                                           {Expressions: []string{"n.id"}, Names: resultmetadata.Known([]string{"id"}), Standalone: true},
		"MATCH (n) RETURN n":                                          {Expressions: []string{"n"}, Names: resultmetadata.Known([]string{"n"})},
		"RETURN missing":                                              {Expressions: []string{"missing"}, Names: resultmetadata.Known([]string{"missing"}), Standalone: true},
		"RETURN a AS " + "`weird name`":                               {Expressions: []string{"a"}, Names: resultmetadata.Known([]string{"weird name"}), Standalone: true},
		"RETURN collect(n.id) AS ids, collect(n.name) AS names":       {Expressions: []string{"collect(n.id)", "collect(n.name)"}, Names: resultmetadata.Known([]string{"ids", "names"}), Standalone: true},
	}
	return plans[query]
}
func projectSimpleReturnFromRow(query string, columns []string, row []interface{}) ([]string, []interface{}, bool) {
	return projectSimpleProjectionFromRow(testHeaderProjection(query), columns, row)
}
func synthesizeEmptyCollectOnlyReturn(query string) ([]string, []interface{}, bool) {
	return synthesizeEmptyCollectProjection(testHeaderProjection(query))
}

// Existing value-operator tests supply pinned projection facts explicitly.
// Parsing assertions are in cypher's migrated owner tests and public Fabric test.
func testInMemoryProjectionNames(query string) resultmetadata.Names {
	switch {
	case strings.Contains(query, "RETURN textKey AS key, textKey128 AS hash"):
		return resultmetadata.Known([]string{"key", "hash"})
	case strings.Contains(query, "RETURN textKey128, texts"):
		return resultmetadata.Known([]string{"textKey128", "texts"})
	case strings.Contains(query, "RETURN r.sourceId AS sourceId"):
		return resultmetadata.Known([]string{"sourceId", "textKey", "originalText", "language", "translatedText"})
	case strings.Contains(query, "RETURN r.textKey AS textKey"):
		return resultmetadata.Known([]string{"textKey", "textKey128", "texts"})
	case strings.Contains(query, "RETURN rows"):
		return resultmetadata.Known([]string{"rows"})
	case strings.Contains(query, "RETURN keys"):
		return resultmetadata.Known([]string{"keys"})
	case strings.Contains(query, "RETURN other"):
		return resultmetadata.Known([]string{"other"})
	case strings.Contains(query, " AS x"):
		return resultmetadata.Known([]string{"x"})
	}
	return resultmetadata.Unknown()
}
func executeApplyInMemoryProjection(input *ResultStream, query string) (*ResultStream, bool) {
	return executeApplyInMemoryProjectionColumns(input, query, testInMemoryProjectionNames(query))
}
func executeCollectedMapJoinFlatProjection(input *ResultStream, query string) (*ResultStream, bool) {
	return executeCollectedMapJoinFlatProjectionColumns(input, query, testInMemoryProjectionNames(query))
}

func TestResultHeaderProjectionModifiersStayOnExecutionPath(t *testing.T) {
	for _, plan := range []resultmetadata.Projection{
		{Expressions: []string{"value"}, Names: resultmetadata.Known([]string{"kept"}), Standalone: true, Distinct: true},
		{Expressions: []string{"value"}, Names: resultmetadata.Known([]string{"kept"}), Standalone: true, Modifiers: "SKIP 1"},
		{Expressions: []string{"collect(value)"}, Names: resultmetadata.Known([]string{"kept"}), Standalone: true, Modifiers: "LIMIT 0"},
	} {
		if _, _, ok := projectSimpleProjectionFromRow(plan, []string{"value"}, []interface{}{1}); ok {
			t.Fatal("simple projection consumed DISTINCT or pagination")
		}
		if _, _, ok := synthesizeEmptyCollectProjection(plan); ok {
			t.Fatal("aggregate synthesis consumed DISTINCT or pagination")
		}
	}
}

// The existing execution double supplies pinned projection facts through the
// same interface as the real Cypher owner. It does not parse query text.
func (m *mockCypherExecutor) ResultProjection(ctx context.Context, query string, _ []string) resultmetadata.Projection {
	return testHeaderProjection(query)
}
func (m *mockCypherExecutor) CachedResultColumns(_ context.Context, query string, _ []string) resultmetadata.Names {
	if result := m.results[query]; result != nil && result.Columns != nil {
		return resultmetadata.Known(result.Columns)
	}
	return testHeaderProjection(query).Names
}

func (s *slowBatchingCypherExecutor) ResultProjection(ctx context.Context, query string, _ []string) resultmetadata.Projection {
	return testHeaderProjection(query)
}
func (s *slowBatchingCypherExecutor) CachedResultColumns(_ context.Context, query string, _ []string) resultmetadata.Names {
	return testHeaderProjection(query).Names
}
func (c *callCountCypherExecutor) ResultProjection(ctx context.Context, query string, incoming []string) resultmetadata.Projection {
	return c.mock.ResultProjection(ctx, query, incoming)
}
func (c *callCountCypherExecutor) CachedResultColumns(ctx context.Context, query string, incoming []string) resultmetadata.Names {
	return c.mock.CachedResultColumns(ctx, query, incoming)
}
