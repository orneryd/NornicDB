package cypher

import (
	"context"
	"hash/maphash"
	"sync/atomic"
	"time"
)

// statementSchemaFacts is the immutable dependency plan of the parsed statement.
// Its construction and representation belong here; cache/planner consumers
// cannot replace it with names extracted by another statement scanner.
type statementSchemaFacts struct {
	query          string
	names          []string
	types          []string
	propertyKeys   []string
	firstNodeLabel string
	nodeProperties bool
	broad          bool
}

// Only the read-only grammar walk receives a builder. Cache consumers receive
// the finished facts, which have no mutating methods or borrowed slice views.
type statementSchemaBuilder struct{ statementSchemaFacts }

// A result publication owns either an existing parsed plan or the exact
// statement that will produce it. Publication and eviction do not demand
// schema metadata; selective invalidation does. Keep this reference in the
// result entry rather than allocating another lazy holder for every Put.
type statementSchemaReference struct {
	query string
	facts *statementSchemaFacts
}

func deferredStatementSchemaReference(query string) statementSchemaReference {
	return statementSchemaReference{query: query}
}

func resolvedStatementSchemaReference(facts *statementSchemaFacts) statementSchemaReference {
	return statementSchemaReference{facts: facts}
}

func (reference statementSchemaReference) deferred() bool {
	return reference.facts == nil && reference.query != ""
}

func (reference statementSchemaReference) labelCount() int {
	return reference.facts.labelCount()
}

func (reference statementSchemaReference) labelAt(index int) string {
	return reference.facts.labelAt(index)
}

func (reference statementSchemaReference) reuse(previous statementSchemaReference) statementSchemaReference {
	if reference.deferred() && previous.facts != nil && previous.facts.query == reference.query {
		return previous
	}
	return reference
}

// Resolving is pure parsing through the same statement owner. The cache
// publishes a replacement entry if this plan survives invalidation; neither
// the old entry nor a plan held by another reader is mutated.
func (reference statementSchemaReference) resolve() statementSchemaReference {
	if reference.deferred() {
		reference = resolvedStatementSchemaReference(statementSchemaFactsOf(reference.query))
	}
	return reference
}

// Immutable plans need neither map growth nor shard replacement on a cold
// statement. Each slot owns its last exact-text plan; a hash collision only
// causes recomputation. The facts and their key are published together.
var statementSchemaPlans [1024]atomic.Pointer[statementSchemaFacts]

func statementSchemaFactsOf(query string) *statementSchemaFacts {
	slot := &statementSchemaPlans[maphash.String(cacheKeyHashSeed, query)%uint64(len(statementSchemaPlans))]
	if facts := slot.Load(); facts != nil && facts.query == query {
		return facts
	}
	builder := &statementSchemaBuilder{}
	builder.query = query
	// Metadata reads syntax, without compiling execution binding aliases.
	// The shared grammar walker reads quoted symbolic subjects directly.
	text := classificationText(query)
	reader := &labelExpressionRewriter{query: text, schema: builder, cypher25: true}
	if err := reader.statement(0, len(reader.query)); err != nil {
		// Incomplete/unsupported syntax never supplies a partial dependency set.
		builder.broad = true
	}
	facts := &builder.statementSchemaFacts
	slot.Store(facts)
	return facts
}

func (f *statementSchemaFacts) labelCount() int {
	if f == nil || f.broad {
		return 0
	}
	return len(f.names)
}

func (f *statementSchemaFacts) labelAt(index int) string { return f.names[index] }

func (f *statementSchemaFacts) labelMetadata() []string {
	if f.labelCount() == 0 {
		return nil
	}
	return append([]string(nil), f.names...)
}

// invalidate keeps the internal immutable view within its owner. The public
// invalidation API consumes data names without retaining or changing them.
func (f *statementSchemaFacts) invalidate(cache *SmartQueryCache) {
	if f.labelCount() == 0 {
		cache.Invalidate()
		return
	}
	cache.InvalidateLabels(f.names)
}

func (f *statementSchemaFacts) typeMetadata() []string {
	if f == nil {
		return nil
	}
	return append([]string(nil), f.types...)
}

func (f *statementSchemaFacts) firstLabel() (string, bool) {
	return f.firstNodeLabel, f.firstNodeLabel != ""
}

func (f *statementSchemaBuilder) add(names []string, relationship bool) {
	f.names = mergeStatementSchemaNames(f.names, names)
	if relationship {
		f.types = mergeStatementSchemaNames(f.types, names)
	}
}

// Each vector is immutable once produced. The first chain can serve labels
// and types together; later chains copy only when they add distinct names.
// Incoming names are already deduplicated by the label-expression owner.
func mergeStatementSchemaNames(current, incoming []string) []string {
	if len(current) == 0 {
		return incoming
	}
	additional := 0
	for _, name := range incoming {
		if !containsString(current, name) {
			additional++
		}
	}
	if additional == 0 {
		return current
	}
	merged := make([]string, len(current), len(current)+additional)
	copy(merged, current)
	for _, name := range incoming {
		if !containsString(current, name) {
			merged = append(merged, name)
		}
	}
	return merged
}

func (f *statementSchemaBuilder) chain(chain labelChain, relationship bool) {
	names, broad := chain.expr.schemaDependencies()
	f.add(names, relationship)
	f.broad = f.broad || broad
	if !relationship && f.firstNodeLabel == "" {
		if required := chain.expr.requiredLabels(); len(required) != 0 {
			f.firstNodeLabel = required[0]
		}
	}
}

// properties consumes the canonical map-expression plan; keys remain metadata,
// while values are walked only for nested patterns/label tests, never evaluated.
func (f *statementSchemaBuilder) properties(props string, relationship bool) error {
	if props == "" {
		return nil
	}
	expressions, err := parsePropertyExpressions(props)
	if err != nil {
		return err
	}
	f.nodeProperties = f.nodeProperties || !relationship && len(expressions) != 0
	for _, expression := range expressions {
		if !containsString(f.propertyKeys, expression.key) {
			f.propertyKeys = append(f.propertyKeys, expression.key)
		}
		if err := f.expression(expression.expr); err != nil {
			return err
		}
	}
	return nil
}

func (f *statementSchemaFacts) hasNodeProperties() bool { return f.nodeProperties }
func (f *statementSchemaBuilder) expression(text string) error {
	reader := &labelExpressionRewriter{query: text, schema: f, cypher25: true}
	return reader.expression(0, len(text))
}

// mapExpression visits values, never interpreting a map key as a label test.
func (f *statementSchemaBuilder) mapExpression(text string) error {
	var failure error
	err := forEachMapEntry(text, mapCodeText, func(_ string, value string) {
		if failure == nil {
			failure = f.expression(value)
		}
	})
	if err != nil {
		return err
	}
	return failure
}

// Explicit labels are a public data boundary. Production query-text consumers
// must use statementSchemaFactsOf / QueryInfo's plan, not these compatibility
// adapters (the compiler/source guard enforces that call boundary).
func explicitStatementSchemaFacts(labels []string) *statementSchemaFacts {
	return &statementSchemaFacts{names: append([]string(nil), labels...)}
}

func (sc *SmartQueryCache) PutWithLabels(query string, params map[string]interface{}, result *ExecuteResult, ttl time.Duration, labels []string) {
	sc.putWithDependenciesAndTraceForEmbedding(resultCacheEntryKey(context.Background(), query, params), result, ttl, explicitStatementSchemaFacts(labels), HotPathTrace{}, 0)
}

func (sc *SmartQueryCache) putWithLabels(key string, result *ExecuteResult, ttl time.Duration, labels []string) {
	sc.putWithLabelsAndTrace(key, result, ttl, labels, HotPathTrace{})
}

func (sc *SmartQueryCache) putWithLabelsAndTrace(key string, result *ExecuteResult, ttl time.Duration, labels []string, trace HotPathTrace) {
	sc.putWithLabelsAndTraceForEmbedding(key, result, ttl, labels, trace, 0)
}

func (sc *SmartQueryCache) putWithLabelsAndTraceForEmbedding(key string, result *ExecuteResult, ttl time.Duration, labels []string, trace HotPathTrace, generation uint64) {
	sc.putWithDependenciesAndTraceForEmbedding(key, result, ttl, explicitStatementSchemaFacts(labels), trace, generation)
}

// Legacy helper is a view of the plan, never an independent query scanner.
func extractLabelsFromQuery(query string) []string {
	return statementSchemaFactsOf(query).labelMetadata()
}

// Keep exact quoted names at the metadata boundary; no string splitting here.
func firstNodeLabelFromSection(text string) (string, bool) {
	head, _ := splitNodePatternProperties(text)
	_, labels, err := parseNodeHead(head)
	if err != nil || len(labels) == 0 {
		return "", false
	}
	return labels[0], true
}
