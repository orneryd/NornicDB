package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Exercise the exported invalidation API directly. A versioned storage result
// key may independently mask a missing dependency; it cannot prove this API.
func TestStatementSchemaCacheInvalidationMatrix(t *testing.T) {
	for _, tc := range []struct {
		name, query, changed string
		params               map[string]interface{}
	}{
		{"quoted-pipe-alternative", "MATCH ()-[:A|`B|C`]->() RETURN count(*) AS n", "B|C", nil},
		{"quoted-type", "MATCH ()-[:`A|B`]->() RETURN count(*) AS n", "A|B", nil},
		{"escaped-type", "MATCH ()-[:`A``|B`]->() RETURN count(*) AS n", "A`|B", nil},
		{"unicode-variable-type", "MATCH ()-[关系:关系类型]->() RETURN count(*) AS n", "关系类型", nil},
		{"dynamic-looking-literal", "MATCH ()-[:`$(R)`]->() RETURN count(*) AS n", "$(R)", nil},
		{"keyword-variable", "MATCH ()-[is:`B|C`]->() RETURN count(*) AS n", "B|C", nil},
		{"is-type-chain", "MATCH ()-[r IS A|`B|C`]->() RETURN count(*) AS n", "B|C", nil},
		{"dynamic-type", "MATCH ()-[:$($t)]->() RETURN count(*) AS n", "B|C", map[string]interface{}{"t": "B|C"}},
		{"dynamic-any-type", "MATCH ()-[:$any($ts)]->() RETURN count(*) AS n", "B|C", map[string]interface{}{"ts": []string{"A", "B|C"}}},
		{"legacy-dynamic-range", "MATCH ()-[:$($t)*1..2]->() RETURN count(*) AS n", "Unlisted", map[string]interface{}{"t": ""}},
		{"quoted-label", "MATCH (n:`A|B`) RETURN count(*) AS n", "A|B", nil},
		{"escaped-label", "MATCH (n:`A``|B`) RETURN count(*) AS n", "A`|B", nil},
		{"lowercase-label", "MATCH (n:person) RETURN count(*) AS n", "person", nil},
		{"unicode-label", "MATCH (n:人物) RETURN count(*) AS n", "人物", nil},
		{"is-label", "MATCH (n IS `A|B`) RETURN count(*) AS n", "A|B", nil},
		{"predicate-label-alternative", "MATCH (n) WHERE n:A|`B|C` RETURN count(*) AS n", "B|C", nil},
		{"quoted-predicate-subject", "MATCH (`n n`) WHERE `n n`:A|`B|C` RETURN count(*) AS n", "B|C", nil},
		{"parenthesized-predicate", "MATCH (n) WHERE n:(A|`B|C`) RETURN count(*) AS n", "B|C", nil},
		{"negated-conjunction-dependency", "MATCH (n:A) WHERE n:A&!B RETURN count(*) AS n", "B", nil},
		{"dynamic-label", "MATCH (n:$($l)) RETURN count(*) AS n", "B|C", map[string]interface{}{"l": "B|C"}},
		{"negated-label", "MATCH (n:!A) RETURN count(*) AS n", "Unlisted", nil},
		{"unlabelled", "MATCH (n) RETURN count(*) AS n", "Unlisted", nil},
		{"nested-subquery", "MATCH (n:A) WHERE EXISTS { MATCH (n)-[:`B|C`]->() } RETURN count(*) AS n", "B|C", nil},
		{"case-expression-test", "MATCH (n:A) RETURN CASE WHEN n IS `B|C` THEN 1 ELSE 0 END AS value", "B|C", nil},
		{"case-named-subject", "MATCH (case:A) WHERE case IS `B|C` RETURN case", "B|C", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewSmartQueryCache(16)
			result := &ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{int64(1)}}}
			cache.Put(tc.query, tc.params, result, time.Minute)
			_, hit := cache.Get(tc.query, tc.params)
			require.True(t, hit, "must establish a real hit before invalidation")
			control := "MATCH (n:CacheControl) RETURN count(*) AS n"
			cache.Put(control, nil, result, time.Minute)
			if tc.name != "dynamic-type" && tc.name != "dynamic-any-type" && tc.name != "legacy-dynamic-range" && tc.name != "dynamic-label" && tc.name != "negated-label" && tc.name != "unlabelled" {
				cache.InvalidateLabels([]string{"UnrelatedSchemaToken"})
				_, hit = cache.Get(tc.query, tc.params)
				require.True(t, hit, "known static facts must not become global dependencies")
			}
			cache.InvalidateLabels([]string{tc.changed})
			_, hit = cache.Get(tc.query, tc.params)
			require.False(t, hit, "stale hit after %q changes; extracted facts: %q", tc.changed, extractLabelsFromQuery(tc.query))
			_, hit = cache.Get(control, nil)
			require.True(t, hit, "a distinct static label dependency must remain cached")
		})
	}
}

func TestStatementSchemaFactsArePrivateToCachedPlan(t *testing.T) {
	query := "MATCH ()-[:A|`B|C`]->() RETURN count(*) AS n"
	info := NewQueryAnalyzer(16).Analyze(context.Background(), query)
	require.Equal(t, []string{"A", "B|C"}, info.Labels)
	require.Equal(t, []string{"A", "B|C"}, info.RelationshipTypes)
	info.Labels[0] = "ReplacedPublicMetadata"
	require.Equal(t, []string{"A", "B|C"}, info.schemaFacts.labelMetadata(), "public metadata must not mutate the shared dependency plan")
}

func TestStatementSchemaDeferredPublicationKeepsEntrySnapshots(t *testing.T) {
	query := "MATCH ()-[:DeferredA|`DeferredB|C`]->() RETURN count(*) AS n"
	key := resultCacheEntryKey(context.Background(), query, nil)
	cache := NewSmartQueryCache(4)
	first := &ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{int64(1)}}}
	cache.Put(query, nil, first, time.Minute)
	published := cache.cache[key]
	require.True(t, published.dependencies.deferred(), "publication does not demand schema metadata")
	_, hit := cache.Get(query, nil)
	require.True(t, hit)
	require.True(t, published.dependencies.deferred(), "a result hit does not demand schema metadata")

	cache.InvalidateLabels([]string{"UnrelatedDeferredLabel"})
	resolved := cache.cache[key]
	require.NotSame(t, published, resolved, "resolving metadata publishes a new immutable entry")
	require.True(t, published.dependencies.deferred(), "a previously published entry remains unchanged")
	require.False(t, resolved.dependencies.deferred())
	require.Equal(t, 2, resolved.dependencies.labelCount())
	require.Equal(t, "DeferredB|C", resolved.dependencies.labelAt(1))

	second := &ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{int64(2)}}}
	cache.Put(query, nil, second, time.Minute)
	require.False(t, cache.cache[key].dependencies.deferred(), "replacement retains an already resolved exact-statement plan")
	require.Equal(t, int64(1), resolved.result.Rows[0][0])
	current, hit := cache.Get(query, nil)
	require.True(t, hit)
	require.Equal(t, int64(2), current.Rows[0][0])
	cache.InvalidateLabels([]string{"DeferredB|C"})
	_, hit = cache.Get(query, nil)
	require.False(t, hit, "deferred resolution must preserve the complete quoted alternative")

	evicting := NewSmartQueryCache(1)
	evicting.Put(query, nil, first, time.Minute)
	evicted := evicting.cache[key]
	evicting.Put("MATCH (n:AnotherDeferredLabel) RETURN n", nil, first, time.Minute)
	require.True(t, evicted.dependencies.deferred(), "eviction does not demand metadata")
	require.NotContains(t, evicting.cache, key)
}

func TestStatementSchemaFactsMergeKeepsLabelAndTypeKinds(t *testing.T) {
	for _, tc := range []struct {
		query         string
		labels, types []string
	}{
		{"MATCH ()-[:A|A|B]->(n:C) MATCH (n)-[:D]->() RETURN count(*) AS n", []string{"A", "B", "C", "D"}, []string{"A", "B", "D"}},
		{"MATCH (n:C)-[:A|A|B]->() MATCH ()-[:D]->(m:E) RETURN count(*) AS n", []string{"C", "A", "B", "D", "E"}, []string{"A", "B", "D"}},
	} {
		info := NewQueryAnalyzer(16).Analyze(context.Background(), tc.query)
		require.Equal(t, tc.labels, info.Labels)
		require.Equal(t, tc.types, info.RelationshipTypes)
		info.Labels[0] = "PublicMetadataEdit"
		require.Equal(t, tc.labels, info.schemaFacts.labelMetadata())
		require.Equal(t, tc.types, info.schemaFacts.typeMetadata())
	}
}

func TestStatementSchemaASTAndExplain(t *testing.T) {
	query := "MATCH (n:`A|B` {`k:k`: {xs:[11,17]}.xs[1]})-[:`B|C`]->() RETURN ':Decoy'"
	label, ok := extractFirstNodeLabelFromQuery(query)
	require.True(t, ok)
	require.Equal(t, "A|B", label)
	patterns := NewASTBuilder().parsePatterns(cyphertext.NormalizeSource("(n:`A|B` {`k:k`: {xs:[11,17]}.xs[1]})-[:`B|C`]->()"))
	require.Len(t, patterns, 1)
	require.Len(t, patterns[0].Nodes, 2)
	require.Equal(t, []string{"A|B"}, patterns[0].Nodes[0].Labels)
	require.Contains(t, patterns[0].Nodes[0].Properties, "k:k")
	require.Equal(t, "{xs:[11,17]}.xs[1]", patterns[0].Nodes[0].Properties["k:k"].RawText)
	require.Equal(t, "B|C", patterns[0].Relationships[0].Type)
	require.False(t, statementSchemaFactsOf("MATCH (n:A) RETURN '{fake:1}'").hasNodeProperties())
}

func TestStatementSchemaCanonicalProducerAdapters(t *testing.T) {
	_, conjunction, _, alternative, valid := parseUnwindNodePatternClauseInternal("MERGE (n:A:B {k: row.k})", "MERGE", false)
	require.True(t, valid)
	require.False(t, alternative)
	require.Equal(t, []string{"A", "B"}, conjunction)
	_, labels, assignments, anyLabel, ok := parseUnwindNodePatternClauseInternal("MATCH (n:A|`B|C` {`k:k`: row.k})", "MATCH", true)
	require.True(t, ok)
	require.True(t, anyLabel)
	require.Equal(t, []string{"A", "B|C"}, labels)
	require.Equal(t, []unwindSimpleSetAssignment{{prop: "k:k", expr: "row.k"}}, assignments)
	labels, ok = whereLabelDisjunction("n", "n:A|`B|C`")
	require.True(t, ok)
	require.Equal(t, []string{"A", "B|C"}, labels)
	variable, label, ok := parseCallTailIdentifierAndOptionalType("n:`B|C`")
	require.True(t, ok)
	require.Equal(t, "n", variable)
	require.Equal(t, "B|C", label)
	keys := appendPatternPropertyExpressions(nil, "(n:`B|C` {`k:k`: {xs:[1,2]}.xs[1]})", "B|C")
	require.Equal(t, map[string][]string{"k:k": {"{xs:[1,2]}.xs[1]"}}, keys)
	expressions, err := parsePropertyExpressions("{`a,b`: 1, `k:k`: {xs:[1,2]}.xs[1]}")
	require.NoError(t, err)
	require.Equal(t, []pipelinePropertyExpression{{key: "a,b", expr: "1"}, {key: "k:k", expr: "{xs:[1,2]}.xs[1]"}}, expressions)
}

func TestStatementSchemaCacheIgnoresTextData(t *testing.T) {
	for _, query := range []string{
		"MATCH (n:CacheControl {k: ':Decoy|Other'}) RETURN n",
		"MATCH (n:CacheControl) RETURN ':Decoy|Other' AS value",
		"MATCH (n:CacheControl) /* :Decoy|Other */ RETURN n",
		"MATCH (n:CacheControl) RETURN {Decoy: 'Other'} AS value",
	} {
		cache := NewSmartQueryCache(16)
		cache.Put(query, nil, &ExecuteResult{Columns: []string{"n"}}, time.Minute)
		cache.InvalidateLabels([]string{"Decoy", "Other"})
		_, hit := cache.Get(query, nil)
		require.True(t, hit, "literal/comment/map data must not become schema dependencies: %s", query)
	}
}

func TestStatementSchemaCacheReplacementOwnsDependencies(t *testing.T) {
	query := "MATCH ()-[:A|`B|C`]->() RETURN count(*) AS n"
	control := "MATCH (n:CacheControl) RETURN count(*) AS n"
	third := "MATCH (n:ThirdControl) RETURN count(*) AS n"
	first := &ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{int64(1)}}}
	second := &ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{int64(2)}}}
	cache := NewSmartQueryCache(2)
	cache.Put(query, nil, first, time.Minute)
	cache.Put(control, nil, first, time.Minute)
	previous, hit := cache.Get(query, nil)
	require.True(t, hit)
	cache.Put(query, nil, second, time.Minute)
	current, hit := cache.Get(query, nil)
	require.True(t, hit)
	require.Equal(t, int64(2), current.Rows[0][0])
	require.Equal(t, int64(1), previous.Rows[0][0], "replacing an entry must not change a result already read")
	cache.Put(third, nil, first, time.Minute)
	_, hit = cache.Get(control, nil)
	require.False(t, hit, "replacement must refresh LRU order without evicting another entry")
	cache.InvalidateLabels([]string{"B|C"})
	_, hit = cache.Get(query, nil)
	require.False(t, hit, "replacement must retain every owner-produced dependency")
	_, hit = cache.Get(third, nil)
	require.True(t, hit)

	cache.PutWithLabels(query, nil, first, time.Minute, []string{"OldDataName"})
	cache.PutWithLabels(query, nil, second, time.Minute, []string{"NewDataName"})
	cache.InvalidateLabels([]string{"OldDataName"})
	_, hit = cache.Get(query, nil)
	require.True(t, hit, "a changed explicit data dependency must remove its old index")
	cache.InvalidateLabels([]string{"NewDataName"})
	_, hit = cache.Get(query, nil)
	require.False(t, hit)
}

func TestStatementSchemaQuotedSubjectsUseGrammar(t *testing.T) {
	for _, query := range []string{
		"MATCH (`n n`:A) WHERE `n n`:`B|C` RETURN count(*) AS n",
		"MATCH (`n``n`:A) WHERE `n``n` IS `B|C` RETURN count(*) AS n",
		"MATCH (n:A) WHERE n IS LABELED `B|C` RETURN count(*) AS n",
	} {
		require.Equal(t, []string{"A", "B|C"}, NewQueryAnalyzer(16).Analyze(context.Background(), query).Labels)
	}
	query := "MATCH (n) WHERE n IS NOT LABELED `B|C` RETURN count(*) AS n"
	cache := NewSmartQueryCache(16)
	cache.Put(query, nil, &ExecuteResult{Columns: []string{"n"}}, time.Minute)
	cache.InvalidateLabels([]string{"Unlisted"})
	_, hit := cache.Get(query, nil)
	require.False(t, hit, "a negated label test can depend on unlabelled entities")
	query = "MATCH (n:CacheControl) RETURN {`Decoy`: 'Other'} AS value"
	require.Equal(t, []string{"CacheControl"}, NewQueryAnalyzer(16).Analyze(context.Background(), query).Labels)
}

func TestRelationshipDeclarationTypePlanKeepsDynamicValuesPerExecution(t *testing.T) {
	declaration := relationshipDeclarationHead("r:$($t)")
	for _, name := range []string{"A", "B|C", "$(R)", ""} {
		chain, ok := declaration.typeChain()
		require.True(t, ok)
		resolved, constant, err := chain.expr.resolveDynamic(func(string) (interface{}, bool, error) {
			return name, true, nil
		})
		if name == "" {
			require.Error(t, err, "cached syntax must not cache a previous token's validity")
			continue
		}
		require.NoError(t, err)
		require.True(t, constant)
		names, simple := resolved.names()
		require.True(t, simple)
		require.Equal(t, []string{name}, names)
		_, broad := chain.expr.schemaDependencies()
		require.True(t, broad, "resolving one execution must not mutate shared dynamic syntax")
	}
}

func TestStatementSchemaClauseContextKeepsNestedDependencies(t *testing.T) {
	for _, query := range []string{
		"MATCH (n:A) WHERE EXISTS { MATCH ()-[:`B|C`]->() RETURN 1 } RETURN n AS match",
		"MATCH (n:A) WHERE CASE WHEN n.k = 1 THEN EXISTS { MATCH ()-[:`B|C`]->() RETURN 1 } ELSE false END RETURN n AS set",
		"MATCH (n:A) /* RETURN CASE ) */ WHERE EXISTS { MATCH ()-[:`B|C`]->() RETURN 1 } RETURN n",
	} {
		cache := NewSmartQueryCache(16)
		cache.Put(query, nil, &ExecuteResult{Columns: []string{"n"}}, time.Minute)
		cache.InvalidateLabels([]string{"B", "C"})
		_, hit := cache.Get(query, nil)
		require.True(t, hit, "clause context must preserve the quoted nested type")
		cache.InvalidateLabels([]string{"B|C"})
		_, hit = cache.Get(query, nil)
		require.False(t, hit, "nested statement type must remain a dependency")
	}
}
