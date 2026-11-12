package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// indexSeekConstant evaluates the value side of a predicate an index seek
// answers. The seek may use literals, parameters and values already bound
// in the incoming scope, including functions and property access over them.
// Unbound candidate variables are not constants of that incoming scope.
// Otherwise ok is false and the predicate is left to the row filter. Reading
// the value with the literal parser looked up a function call as its own
// text and returned no rows (#844).
func (e *StorageExecutor) indexSeekConstant(ctx context.Context, expr string) (value interface{}, ok bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false
	}
	for _, variable := range expressionFreeVariables(ctx, expr) {
		if _, bound := e.boundValue(ctx, variable); !bound {
			return nil, false
		}
	}
	value, ok = e.evaluateRowExpressionWithContext(ctx, expr, e.fabricRecordBindings)
	if !ok {
		return nil, false
	}
	return normalizePropValue(value), true
}

// idEqualityLookup reads a WHERE that is one id(<variable>) = <value> or
// elementId(<variable>) = <value> comparison: the variable, and the storage
// ID the value names (an element ID's payload; elementKind is "4" for nodes,
// "5" for relationships). empty is true when the value is null, not a string
// or blank, which matches nothing; ok is false for any other WHERE.
func (e *StorageExecutor) idEqualityLookup(ctx context.Context, whereClause, elementKind string) (variable, lookupID string, empty, ok bool) {
	clause := strings.TrimSpace(whereClause)
	if clause == "" {
		return "", "", false, false
	}

	upper := upperASCII(clause)
	if strings.Contains(upper, " AND ") || strings.Contains(upper, " OR ") || strings.Contains(upper, " IN ") {
		return "", "", false, false
	}

	eqIdx := strings.Index(clause, "=")
	if eqIdx <= 0 {
		return "", "", false, false
	}
	// Ignore >=, <=, !=, <>
	if prev := clause[eqIdx-1]; prev == '!' || prev == '<' || prev == '>' {
		return "", "", false, false
	}
	if eqIdx+1 < len(clause) && clause[eqIdx+1] == '=' {
		return "", "", false, false
	}

	left := strings.TrimSpace(clause[:eqIdx])
	right := strings.TrimSpace(clause[eqIdx+1:])
	if left == "" || right == "" {
		return "", "", false, false
	}

	kind := ""
	lowerLeft := lowerASCII(left)
	switch {
	case strings.HasPrefix(lowerLeft, "id(") && strings.HasSuffix(left, ")"):
		kind = "id"
		variable = strings.TrimSpace(left[3 : len(left)-1])
	case strings.HasPrefix(lowerLeft, "elementid(") && strings.HasSuffix(left, ")"):
		kind = "elementId"
		variable = strings.TrimSpace(left[10 : len(left)-1])
	default:
		return "", "", false, false
	}
	if variable == "" {
		return "", "", false, false
	}

	rawVal, constant := e.indexSeekConstant(ctx, right)
	if !constant {
		return "", "", false, false
	}
	idValue, isString := rawVal.(string)
	if !isString || strings.TrimSpace(idValue) == "" {
		return variable, "", true, true
	}
	lookupID = strings.TrimSpace(idValue)
	// Equality and IN consume the same qualified reference as graph readers.
	lookupID, found, applicable := e.entitySeekLookup(lookupID, elementKind, kind)
	if !applicable {
		return "", "", false, false
	}
	if !found {
		return variable, "", true, true
	}
	return variable, lookupID, false, true
}

// entitySeekLookup retains the database until the selected engine accepts it.
// Unqualified strings retain the existing raw-ID extension.
func (e *StorageExecutor) entitySeekLookup(value, kind, function string) (string, bool, bool) {
	reference, qualified, valid := graphElementIdentity(value, kind)
	if function != "elementId" && !strings.HasPrefix(value, kind+":") {
		reference, qualified, valid = storage.EntityIdentity{LocalID: value}, false, true
	}
	if !valid {
		return "", false, true
	}
	// A raw local ID can name multiple composite sources; the predicate owns it.
	if !qualified && storage.CompositeDatabaseOf(e.storage) != "" {
		return "", false, false
	}
	id, found := reference.LookupID(e.storage, kind)
	return id, found, true
}

// resolveIDEqualityCandidates resolves the WHERE that is one
// id(<variable>) = <value> or elementId(<variable>) = <value> comparison to the
// node ID the value names. The pattern labels and inline properties the listed
// node must still carry are admitted when the plan is read. The one ID stays in
// the result's own array, so an ordinary id lookup lists it without allocating.
func (e *StorageExecutor) resolveIDEqualityCandidates(ctx context.Context, nodePattern nodePatternInfo, whereClause string) (nodeIDResults, bool, error) {
	varName, lookupID, empty, ok := e.idEqualityLookup(ctx, whereClause, "4")
	if !ok || varName != nodePattern.variable {
		return nodeIDResults{}, false, nil
	}
	if empty {
		return nodeIDResults{}, true, nil
	}
	return singleIDResult(storage.NodeID(lookupID)), true, nil
}

// resolveIDEqualityParamCandidates resolves the WHERE that is one
// id(<variable>) = <param> or elementId(<variable>) = <param> comparison to the
// node ID the parameter names. It complements resolveIDEqualityCandidates,
// which only handles literal values on the RHS. The parameter-aware variant is
// what app workloads use: they always pass IDs as parameters (e.g.
// attach/read-by-id paths).
func (e *StorageExecutor) resolveIDEqualityParamCandidates(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
	params map[string]interface{},
) (nodeIDResults, bool, error) {
	if params == nil || strings.TrimSpace(whereClause) == "" {
		return nodeIDResults{}, false, nil
	}
	clause := strings.TrimSpace(whereClause)

	upper := upperASCII(clause)
	if strings.Contains(upper, " AND ") || strings.Contains(upper, " OR ") || strings.Contains(upper, " IN ") {
		return nodeIDResults{}, false, nil
	}

	eqIdx := strings.Index(clause, "=")
	if eqIdx <= 0 {
		return nodeIDResults{}, false, nil
	}
	if eqIdx > 0 {
		prev := clause[eqIdx-1]
		if prev == '!' || prev == '<' || prev == '>' {
			return nodeIDResults{}, false, nil
		}
	}
	if eqIdx+1 < len(clause) {
		next := clause[eqIdx+1]
		if next == '=' {
			return nodeIDResults{}, false, nil
		}
	}

	left := strings.TrimSpace(clause[:eqIdx])
	right := strings.TrimSpace(clause[eqIdx+1:])
	if left == "" || right == "" {
		return nodeIDResults{}, false, nil
	}

	// RHS must be a parameter reference ($paramName).
	if !strings.HasPrefix(right, "$") {
		return nodeIDResults{}, false, nil
	}
	paramName := strings.TrimSpace(right[1:])
	if paramName == "" {
		return nodeIDResults{}, false, nil
	}

	kind := ""
	varName := ""
	lowerLeft := lowerASCII(left)
	switch {
	case strings.HasPrefix(lowerLeft, "id(") && strings.HasSuffix(left, ")"):
		kind = "id"
		varName = strings.TrimSpace(left[3 : len(left)-1])
	case strings.HasPrefix(lowerLeft, "elementid(") && strings.HasSuffix(left, ")"):
		kind = "elementId"
		varName = strings.TrimSpace(left[10 : len(left)-1])
	default:
		return nodeIDResults{}, false, nil
	}

	if varName == "" || varName != nodePattern.variable {
		return nodeIDResults{}, false, nil
	}

	raw, ok := params[paramName]
	if !ok {
		return nodeIDResults{}, true, nil
	}
	idValue, ok := raw.(string)
	if !ok || strings.TrimSpace(idValue) == "" {
		return nodeIDResults{}, true, nil
	}
	idValue = strings.TrimSpace(idValue)

	lookupID, found, applicable := e.entitySeekLookup(idValue, "4", kind)
	if !applicable {
		return nodeIDResults{}, false, nil
	}
	if !found {
		return nodeIDResults{}, true, nil
	}

	return singleIDResult(storage.NodeID(lookupID)), true, nil
}

// resolveIDEqualityCompoundCandidates resolves the ID/elementId direct seeks
// of a simple or compound predicate over a single MATCH variable to the node
// IDs they name.
//
// Supported safe forms:
//   - id(n) = '...'
//   - elementId(n) = '...'
//   - id(n) = $param
//   - (... AND id(n) = ...)
//   - (id(n) = ... OR n.id = ... ) where every OR branch is an ID-equality form
//
// For OR predicates, it only engages when all disjuncts are recognized
// ID-equality predicates; otherwise it returns (nil, false, nil) to avoid unsafe
// pruning.
func (e *StorageExecutor) resolveIDEqualityCompoundCandidates(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
	params map[string]interface{},
) (nodeIDResults, bool, error) {
	clause := unwrapOuterParens(strings.TrimSpace(whereClause))
	if clause == "" {
		return nodeIDResults{}, false, nil
	}

	// 1) Simple clause (literal or param).
	if ids, used, err := e.resolveIDEqualityCandidates(ctx, nodePattern, clause); used || err != nil {
		return ids, used, err
	}
	if ids, used, err := e.resolveIDEqualityParamCandidates(ctx, nodePattern, clause, params); used || err != nil {
		return ids, used, err
	}

	// 2) Conjunction: any recognized id-conjunct is safe to use as a pruning seek.
	if findTopLevelKeyword(clause, " AND ") > 0 {
		for _, raw := range splitTopLevelAndConjuncts(ctx, clause) {
			term := unwrapOuterParens(strings.TrimSpace(raw))
			if term == "" {
				continue
			}
			if ids, used, err := e.resolveIDEqualityCandidates(ctx, nodePattern, term); used || err != nil {
				return ids, used, err
			}
			if ids, used, err := e.resolveIDEqualityParamCandidates(ctx, nodePattern, term, params); used || err != nil {
				return ids, used, err
			}
		}
	}

	// 3) Disjunction: only safe when every branch is recognized as an ID seek.
	orTerms := splitTopLevelOrTerms(clause)
	if len(orTerms) > 1 {
		merged := newIDResults(len(orTerms))
		seen := make(map[storage.NodeID]struct{}, len(orTerms))
		for _, raw := range orTerms {
			term := unwrapOuterParens(strings.TrimSpace(raw))
			if term == "" {
				return nodeIDResults{}, false, nil
			}

			ids, used, err := e.resolveIDEqualityCandidates(ctx, nodePattern, term)
			if err != nil {
				return nodeIDResults{}, true, err
			}
			if !used {
				ids, used, err = e.resolveIDEqualityParamCandidates(ctx, nodePattern, term, params)
				if err != nil {
					return nodeIDResults{}, true, err
				}
			}
			if !used {
				// At least one OR branch is not a recognized ID-equality predicate.
				// Bail out to preserve semantics.
				return nodeIDResults{}, false, nil
			}

			for _, id := range ids.ids() {
				if _, ok := seen[id]; ok {
					continue
				}
				seen[id] = struct{}{}
				merged.add(id)
			}
		}
		return merged, true, nil
	}

	return nodeIDResults{}, false, nil
}

// indexSeekPostings is a set of arity-1 index postings a WHERE seek reads:
// property under value (or, with many, under each of values) on each of
// labels.
type indexSeekPostings struct {
	labels   []string
	property string
	value    interface{}
	values   []interface{}
	many     bool
}

func (p *indexSeekPostings) valueCount() int {
	if p.many {
		return len(p.values)
	}
	return 1
}

func (p *indexSeekPostings) valueAt(i int) interface{} {
	if p.many {
		return p.values[i]
	}
	return p.value
}

// nodeIndexSeek is one index read that can seed a node pattern: a WHERE
// equality, IN or OR seek, whose candidates are the union of its postings
// (two sets for an OR), or the inline pattern-property seek, whose
// candidates are the intersection of its reads. It is planned without
// reading the index; planNodeIndexSeek lists its candidate IDs and
// readNodeIndexCandidatePlan reads the nodes they name.
type nodeIndexSeek struct {
	schema   *storage.SchemaManager
	postings [2]indexSeekPostings

	// A node a WHERE seek lists is admitted while it carries the pattern
	// labels and one of anyOf (seekCandidateHasLabels), and, for an
	// equality, still holds postings[0].property = postings[0].value.
	patternLabels []string
	anyOf         []string
	equality      bool

	// The pattern-property seek's reads: up to two in place (inlineSmall,
	// inlineN of them), or, for more, inline. inlineReads returns them.
	inlineSmall [2]patternPropertyRead
	inlineN     int
	inline      []patternPropertyRead
	// store is the engine the seek reads and counts through, so an explicit
	// transaction's estimate includes its own pending writes, as its lookup
	// does (propertyIndexValueCount).
	store storage.Engine

	// inList marks a seek that answers a WHERE that is exactly an IN list.
	inList bool
}

// isInline reports whether s is the pattern-property seek.
func (s *nodeIndexSeek) isInline() bool {
	return s.inlineN > 0 || len(s.inline) > 0
}

// inlineReads returns the pattern-property seek's reads.
func (s *nodeIndexSeek) inlineReads() []patternPropertyRead {
	if s.inlineN > 0 {
		return s.inlineSmall[:s.inlineN]
	}
	return s.inline
}

// unknownIndexSeekEstimate ranks a seek whose postings cannot be counted after
// every seek that can.
const unknownIndexSeekEstimate = int64(^uint64(0) >> 1)

// estimate is the number of index entries the seek reads, from the lengths
// of the postings in the index (PropertyIndexValueCount): a WHERE seek's
// postings summed, stopping once past bound, which already beats it; the
// inline seek's shortest read, which it reads first. A posting the index
// cannot count makes the estimate unknown. The inline seek counts its
// single-property reads first and its composite reads only while something
// can still be saved (patternPropertyRead.worthCounting).
func (s *nodeIndexSeek) estimate(ctx context.Context, bound int64) (int64, error) {
	if s.isInline() {
		reads := s.inlineReads()
		best := unknownIndexSeekEstimate
		for pass := 0; pass < 2; pass++ {
			for i := range reads {
				if reads[i].hashed() != (pass == 1) || (pass == 1 && !reads[i].worthCounting(min(best, bound))) {
					continue
				}
				count, err := reads[i].count(ctx, s.store, s.schema)
				if err != nil {
					return 0, err
				}
				if count < best {
					best = count
				}
			}
		}
		return best, nil
	}
	sum := int64(0)
	for g := range s.postings {
		postings := &s.postings[g]
		for _, label := range postings.labels {
			for i := 0; i < postings.valueCount(); i++ {
				count, indexed, err := propertyIndexValueCount(ctx, s.store, s.schema, label, postings.property, postings.valueAt(i))
				if err != nil {
					return 0, err
				}
				if !indexed {
					return unknownIndexSeekEstimate, nil
				}
				sum += int64(count)
				if sum > bound {
					return sum, nil
				}
			}
		}
	}
	return sum, nil
}

// admits reports whether a node a WHERE seek's index listed still matches
// the seek: an index can list a node that has since lost a label or value.
func (s *nodeIndexSeek) admits(node *storage.Node) bool {
	if !seekCandidateHasLabels(node, s.patternLabels, s.anyOf) {
		return false
	}
	if !s.equality {
		return true
	}
	actual, exists := node.Properties[s.postings[0].property]
	if !exists {
		return false
	}
	matched, known := cypherEquality(actual, s.postings[0].value).(bool)
	return known && matched
}

// nodeIndexSeekSet holds the seeks planned for one node pattern: the first
// few in place, so planning the usual one or two seeks allocates nothing.
type nodeIndexSeekSet struct {
	n      int
	inline [4]nodeIndexSeek
	more   []nodeIndexSeek
}

func (s *nodeIndexSeekSet) add(seek nodeIndexSeek) {
	if s.n < len(s.inline) {
		s.inline[s.n] = seek
	} else {
		s.more = append(s.more, seek)
	}
	s.n++
}

func (s *nodeIndexSeekSet) at(i int) *nodeIndexSeek {
	if i < len(s.inline) {
		return &s.inline[i]
	}
	return &s.more[i-len(s.inline)]
}

// bestNodeIndexSeek returns the index of the seek that reads the fewest index
// entries, so a unique index (one entry per value) is read before a
// non-unique one and a selective value before a common one, whichever
// predicate or pattern label names it (NOR-414). Ties keep the first. One
// seek is not estimated.
func bestNodeIndexSeek(ctx context.Context, seeks *nodeIndexSeekSet) (int, error) {
	if seeks.n < 2 {
		return 0, nil
	}
	bestAt, best := 0, unknownIndexSeekEstimate
	for i := 0; i < seeks.n; i++ {
		estimate, err := seeks.at(i).estimate(ctx, best)
		if err != nil {
			return 0, err
		}
		if estimate < best {
			bestAt, best = i, estimate
			if best == 0 {
				break
			}
		}
	}
	return bestAt, nil
}

// smallestLabelPosting narrows postings' labels, the indexed labels of a
// labelled pattern, to the one whose index lists the fewest nodes under the
// postings' values: every match carries every pattern label, so one label's
// index lists every match, and reading the others' postings only adds
// nodes the label check drops (NOR-414). Labels of a pattern without labels
// are alternatives the seek must union, and one label is already narrow:
// both are returned as they are, without counting.
func smallestLabelPosting(ctx context.Context, store storage.Engine, schema *storage.SchemaManager, patternLabels []string, postings *indexSeekPostings) ([]string, error) {
	labels := postings.labels
	if len(patternLabels) == 0 || len(labels) < 2 {
		return labels, nil
	}
	bestAt, best := 0, unknownIndexSeekEstimate
	for at, label := range labels {
		sum := int64(0)
		for i := 0; i < postings.valueCount(); i++ {
			count, indexed, err := propertyIndexValueCount(ctx, store, schema, label, postings.property, postings.valueAt(i))
			if err != nil {
				return nil, err
			}
			if !indexed {
				sum = unknownIndexSeekEstimate
				break
			}
			sum += int64(count)
			if sum >= best {
				break
			}
		}
		if sum < best {
			bestAt, best = at, sum
		}
	}
	return labels[bestAt : bestAt+1], nil
}

// planPropertyIndexEquality plans the seek of a simple equality predicate on
// an indexed property:
//   - <var>.<prop> = <value>
//   - <value> = <var>.<prop>
//
// anyOf names the labels a pattern without labels must carry one of (a WHERE
// label disjunction, #858; nil when there is none): see seekCandidateLabels.
// planned is false when the predicate is not eligible for an index read.
func (e *StorageExecutor) planPropertyIndexEquality(ctx context.Context, nodePattern nodePatternInfo, whereClause string, anyOf []string) (nodeIndexSeek, bool, error) {
	if err := ctx.Err(); err != nil {
		return nodeIndexSeek{}, false, err
	}
	property, value, ok := e.parseSimpleIndexedEquality(ctx, nodePattern.variable, whereClause)
	if !ok {
		return nodeIndexSeek{}, false, nil
	}

	schema := e.storage.GetSchema()
	if schema == nil {
		return nodeIndexSeek{}, false, nil
	}

	labels, anyOf, err := e.seekCandidateLabels(ctx, schema, nodePattern.labels, property, anyOf)
	if err != nil || len(labels) == 0 {
		return nodeIndexSeek{}, false, err
	}
	seek := nodeIndexSeek{
		schema:        schema,
		store:         e.storage,
		postings:      [2]indexSeekPostings{{labels: labels, property: property, value: storage.IndexLookupValue(value)}},
		patternLabels: nodePattern.labels,
		anyOf:         anyOf,
		equality:      true,
	}
	if seek.postings[0].labels, err = smallestLabelPosting(ctx, e.storage, schema, nodePattern.labels, &seek.postings[0]); err != nil {
		return nodeIndexSeek{}, false, err
	}
	return seek, true, nil
}

// planPropertyIndexInParam plans the seek of a simple IN-list predicate:
//
//	<var>.<prop> IN $param
//
// where $param is a list value from params; batched correlated APPLY lookups in
// Fabric read their nodes through that seek. anyOf names the labels a pattern
// without labels must carry one of (see seekCandidateLabels). planned is false
// when the predicate is not eligible.
func (e *StorageExecutor) planPropertyIndexInParam(ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
	params map[string]interface{},
	anyOf []string,
) (nodeIndexSeek, bool, error) {
	if err := ctx.Err(); err != nil {
		return nodeIndexSeek{}, false, err
	}
	property, listValues, ok := e.parseSimpleIndexedInParam(ctx, nodePattern.variable, whereClause, params)
	if !ok {
		return nodeIndexSeek{}, false, nil
	}
	return e.planPropertyIndexInList(ctx, nodePattern, property, listValues, anyOf)
}

// planPropertyIndexInList plans the seek of <var>.<property> IN listValues;
// planned is false when no pattern label indexes property.
func (e *StorageExecutor) planPropertyIndexInList(ctx context.Context, nodePattern nodePatternInfo, property string, listValues []interface{}, anyOf []string) (nodeIndexSeek, bool, error) {
	schema := e.storage.GetSchema()
	if schema == nil {
		return nodeIndexSeek{}, false, nil
	}
	labels, anyOf, err := e.seekCandidateLabels(ctx, schema, nodePattern.labels, property, anyOf)
	if err != nil || len(labels) == 0 {
		return nodeIndexSeek{}, false, err
	}
	seek := nodeIndexSeek{
		schema:        schema,
		store:         e.storage,
		postings:      [2]indexSeekPostings{{labels: labels, property: property, values: listValues, many: true}},
		patternLabels: nodePattern.labels,
		anyOf:         anyOf,
	}
	if seek.postings[0].labels, err = smallestLabelPosting(ctx, e.storage, schema, nodePattern.labels, &seek.postings[0]); err != nil {
		return nodeIndexSeek{}, false, err
	}
	return seek, true, nil
}

// planPropertyIndexInLiteral plans the seek of a simple IN-list predicate with
// a literal list:
//
//	<var>.<prop> IN ['a', 'b', ...]
//
// anyOf names the labels a pattern without labels must carry one of (see
// seekCandidateLabels). planned is false when the predicate is not eligible.
func (e *StorageExecutor) planPropertyIndexInLiteral(ctx context.Context, nodePattern nodePatternInfo, whereClause string, anyOf []string) (nodeIndexSeek, bool, error) {
	if err := ctx.Err(); err != nil {
		return nodeIndexSeek{}, false, err
	}
	property, listValues, ok := e.parseSimpleIndexedInLiteral(ctx, nodePattern.variable, whereClause)
	if !ok {
		return nodeIndexSeek{}, false, nil
	}
	return e.planPropertyIndexInList(ctx, nodePattern, property, listValues, anyOf)
}

// addPropertyIndexInCompoundSeeks adds to seeks the indexed IN-list seeks that
// narrow a start-node predicate: the whole clause's when it is a simple IN
// list, otherwise one per indexed IN-list conjunct, including when the IN-list
// conjunct is combined with other predicates via AND — e.g. a
// relationship-pattern traversal's WHERE clause that also filters a bound
// relationship variable, as in `WHERE s.uid IN $u AND rel.evidence_source =
// $e`. Only a single AND conjunct needs to match an indexed property IN-list
// for pruning to be safe: the caller (executeMatchWithRelationshipsWithPath)
// always re-applies the full WHERE clause via filterPathsByWhere afterward,
// so seeding from one recognized conjunct can only over-fetch, never
// under-fetch, and correctness does not depend on this helper understanding
// the rest of the clause. Each seek answers an IN list (inList).
//
// Mirrors resolveIDEqualityCompoundCandidates' AND-conjunct handling
// (match_index_seek.go) for the IN-list case. Tries the param-bound form
// first, then the literal-list form, at both the top level and inside each
// top-level AND conjunct; the caller then reads the conjunct that reads the
// fewest index entries (bestNodeIndexSeek).
func (e *StorageExecutor) addPropertyIndexInCompoundSeeks(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
	params map[string]interface{},
	seeks *nodeIndexSeekSet,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	clause := unwrapOuterParens(strings.TrimSpace(whereClause))
	if clause == "" {
		return nil
	}

	// 1) Simple clause (param or literal list).
	seek, planned, err := e.planPropertyIndexInParam(ctx, nodePattern, clause, params, nil)
	if err == nil && !planned {
		seek, planned, err = e.planPropertyIndexInLiteral(ctx, nodePattern, clause, nil)
	}
	if err != nil {
		return err
	}
	if planned {
		seek.inList = true
		seeks.add(seek)
		return nil
	}

	// 2) Conjunction: any recognized IN-list conjunct is safe to use as a
	// pruning seek (see over-fetch-only argument in the doc comment above).
	// A label disjunction among the conjuncts names the labels a match must
	// carry, for a pattern without labels (#858).
	if findTopLevelKeyword(clause, " AND ") > 0 {
		conjuncts := splitTopLevelAndConjuncts(ctx, clause)
		anyOf := whereLabelDisjunctionConjunct(nodePattern.variable, conjuncts)
		for _, raw := range conjuncts {
			term := unwrapOuterParens(strings.TrimSpace(raw))
			if term == "" {
				continue
			}
			seek, planned, err := e.planPropertyIndexInParam(ctx, nodePattern, term, params, anyOf)
			if err == nil && !planned {
				seek, planned, err = e.planPropertyIndexInLiteral(ctx, nodePattern, term, anyOf)
			}
			if err != nil {
				return err
			}
			if planned {
				seek.inList = true
				seeks.add(seek)
			}
		}
	}
	return nil
}

// addPropertyIndexEqualityCompoundSeeks adds to seeks the indexed equality
// seeks that narrow a start-node predicate: the whole clause's when it is a
// simple equality, otherwise one per indexed equality conjunct, including when
// an equality on an indexed property is combined with other predicates via AND
// — e.g. `WHERE n.repo_id = $repo_id AND n.evidence_source = 'x' AND
// n.generation_id <> $gen`. Only a single AND conjunct needs to match an
// indexed property equality for pruning to be safe: every caller re-applies
// the full WHERE clause afterward, so seeding from one recognized conjunct can
// only over-fetch, never under-fetch, and correctness does not depend on this
// helper understanding the rest of the clause.
//
// Mirrors addPropertyIndexInCompoundSeeks' AND-conjunct handling
// (match_index_seek.go) for the plain-equality case. Tries the whole clause
// first (the simple form), then each top-level AND conjunct; the caller then
// reads the conjunct that reads the fewest index entries (bestNodeIndexSeek).
func (e *StorageExecutor) addPropertyIndexEqualityCompoundSeeks(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
	seeks *nodeIndexSeekSet,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	clause := unwrapOuterParens(strings.TrimSpace(whereClause))
	if clause == "" {
		return nil
	}

	// 1) Simple clause (a lone equality, no conjunction).
	if seek, planned, err := e.planPropertyIndexEquality(ctx, nodePattern, clause, nil); err != nil || planned {
		if planned {
			seeks.add(seek)
		}
		return err
	}

	// 2) Conjunction: any equality conjunct on an indexed property is safe
	// to use as a pruning seek (see over-fetch-only argument in the doc
	// comment above). A label disjunction among the conjuncts
	// (n.id = $id AND (n:A OR n:B)) names the labels a match must carry, for
	// a pattern without labels (#858).
	if findTopLevelKeyword(clause, " AND ") > 0 {
		conjuncts := splitTopLevelAndConjuncts(ctx, clause)
		anyOf := whereLabelDisjunctionConjunct(nodePattern.variable, conjuncts)
		for _, raw := range conjuncts {
			term := unwrapOuterParens(strings.TrimSpace(raw))
			if term == "" {
				continue
			}
			seek, planned, err := e.planPropertyIndexEquality(ctx, nodePattern, term, anyOf)
			if err != nil {
				return err
			}
			if planned {
				seeks.add(seek)
			}
		}
	}
	return nil
}

// planPropertyIndexInOrParam plans the seek of OR-combined IN predicates whose
// branches are both indexed, e.g.:
//
//	n.textKey IN $keys OR n.textKey128 IN $keys
//
// Cleanup and read paths that pass large key lists read their nodes through
// that seek rather than a full label scan. planned is false when the predicate
// is not eligible.
func (e *StorageExecutor) planPropertyIndexInOrParam(ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
	params map[string]interface{},
) (nodeIndexSeek, bool, error) {
	if err := ctx.Err(); err != nil {
		return nodeIndexSeek{}, false, err
	}
	clause := strings.TrimSpace(whereClause)
	if clause == "" || params == nil {
		return nodeIndexSeek{}, false, nil
	}
	orIdx := findTopLevelKeyword(clause, " OR ")
	if orIdx <= 0 {
		return nodeIndexSeek{}, false, nil
	}
	left := strings.TrimSpace(clause[:orIdx])
	right := strings.TrimSpace(clause[orIdx+4:])
	if left == "" || right == "" {
		return nodeIndexSeek{}, false, nil
	}

	lprop, lvals, lok := e.parseSimpleIndexedInParam(ctx, nodePattern.variable, left, params)
	rprop, rvals, rok := e.parseSimpleIndexedInParam(ctx, nodePattern.variable, right, params)
	if !lok || !rok {
		return nodeIndexSeek{}, false, nil
	}

	// Merge values (keep deterministic and duplicate-free).
	merged := make([]interface{}, 0, util.SafePreallocSum(len(lvals), len(rvals)))
	seenVals := make(map[string]struct{}, util.SafePreallocSum(len(lvals), len(rvals)))
	for _, v := range append(lvals, rvals...) {
		if v == nil {
			continue
		}
		k := fmt.Sprintf("%T:%v", v, v)
		if _, ok := seenVals[k]; ok {
			continue
		}
		seenVals[k] = struct{}{}
		merged = append(merged, v)
	}

	schema := e.storage.GetSchema()
	if schema == nil {
		return nodeIndexSeek{}, false, nil
	}

	leftLabels, err := e.indexCandidateLabels(ctx, schema, nodePattern.labels, lprop)
	if err != nil {
		return nodeIndexSeek{}, false, err
	}
	rightLabels, err := e.indexCandidateLabels(ctx, schema, nodePattern.labels, rprop)
	if err != nil {
		return nodeIndexSeek{}, false, err
	}
	if len(leftLabels) == 0 || len(rightLabels) == 0 {
		return nodeIndexSeek{}, false, nil
	}
	return e.orIndexSeek(ctx, schema, nodePattern, [2]indexSeekPostings{
		{labels: leftLabels, property: lprop, values: merged, many: true},
		{labels: rightLabels, property: rprop, values: merged, many: true},
	})
}

// orIndexSeek is the seek of an OR whose two branches are both covered by
// postings: their union, each branch narrowed to its smallest pattern-label
// posting (smallestLabelPosting).
func (e *StorageExecutor) orIndexSeek(ctx context.Context, schema *storage.SchemaManager, nodePattern nodePatternInfo, postings [2]indexSeekPostings) (nodeIndexSeek, bool, error) {
	seek := nodeIndexSeek{schema: schema, store: e.storage, postings: postings, patternLabels: nodePattern.labels}
	for i := range seek.postings {
		labels, err := smallestLabelPosting(ctx, e.storage, schema, nodePattern.labels, &seek.postings[i])
		if err != nil {
			return nodeIndexSeek{}, false, err
		}
		seek.postings[i].labels = labels
	}
	return seek, true, nil
}

// planPropertyIndexOrEquality plans the seek of OR-combined equality predicates
// whose branches are both indexed, e.g.:
//
//	a.prop1 = $x OR a.prop2 = $x
//	a.prop1 = 'val1' OR a.prop2 = 'val2'
//
// The seek splits the OR into its two branches and lists the union of their
// postings, so a dual-key lookup reads its nodes instead of scanning the label.
// planned is false when the predicate is not eligible.
func (e *StorageExecutor) planPropertyIndexOrEquality(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
) (nodeIndexSeek, bool, error) {
	if err := ctx.Err(); err != nil {
		return nodeIndexSeek{}, false, err
	}
	clause := strings.TrimSpace(whereClause)
	if clause == "" {
		return nodeIndexSeek{}, false, nil
	}
	orIdx := findTopLevelKeyword(clause, " OR ")
	if orIdx <= 0 {
		return nodeIndexSeek{}, false, nil
	}
	left := strings.TrimSpace(clause[:orIdx])
	right := strings.TrimSpace(clause[orIdx+4:])
	if left == "" || right == "" {
		return nodeIndexSeek{}, false, nil
	}

	// Try to parse each branch as a simple indexed equality predicate.
	lprop, lval, lok := e.parseSimpleIndexedEquality(ctx, nodePattern.variable, left)
	rprop, rval, rok := e.parseSimpleIndexedEquality(ctx, nodePattern.variable, right)
	if !lok || !rok {
		return nodeIndexSeek{}, false, nil
	}

	schema := e.storage.GetSchema()
	if schema == nil {
		return nodeIndexSeek{}, false, nil
	}

	leftLabels, err := e.indexCandidateLabels(ctx, schema, nodePattern.labels, lprop)
	if err != nil {
		return nodeIndexSeek{}, false, err
	}
	rightLabels, err := e.indexCandidateLabels(ctx, schema, nodePattern.labels, rprop)
	if err != nil {
		return nodeIndexSeek{}, false, err
	}
	// An OR seek lists a node only when an index covers each branch: with one
	// branch unindexed, the nodes that match only that branch are in no
	// index the seek reads, so it is no candidate source and the pattern
	// takes another seek or its scan.
	if len(leftLabels) == 0 || len(rightLabels) == 0 {
		return nodeIndexSeek{}, false, nil
	}
	return e.orIndexSeek(ctx, schema, nodePattern, [2]indexSeekPostings{
		{labels: leftLabels, property: lprop, value: lval},
		{labels: rightLabels, property: rprop, value: rval},
	})
}

// resolveIDInCandidates resolves simple id()/elementId() IN-list predicates:
//
//	id(<var>) IN $param
//	elementId(<var>) IN $param
//	id(<var>) IN ['id-1', $other, …]
//
// The list is a query parameter, or any constant list: a literal, a list of
// parameters, or a parameter the executor already substituted into the query
// text (#940). A missing or non-list $param selects nothing, as before; a
// constant that isn't a list leaves the predicate to the evaluator. This
// gives batched correlated lookups the node IDs the list names without reading
// a node body.
func (e *StorageExecutor) resolveIDInCandidates(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
	params map[string]interface{},
) (nodeIDResults, bool, error) {
	clause := unwrapOuterParens(strings.TrimSpace(whereClause))
	if clause == "" {
		return nodeIDResults{}, false, nil
	}
	upper := upperASCII(clause)
	inIdx := strings.Index(upper, " IN ")
	if inIdx <= 0 {
		return nodeIDResults{}, false, nil
	}
	// Keep this strict/simple to avoid semantic drift for complex predicates.
	if strings.Contains(upper, " AND ") || strings.Contains(upper, " OR ") {
		return nodeIDResults{}, false, nil
	}

	left := strings.TrimSpace(clause[:inIdx])
	right := strings.TrimSpace(clause[inIdx+4:])
	if left == "" || right == "" {
		return nodeIDResults{}, false, nil
	}

	kind := ""
	varName := ""
	lowerLeft := lowerASCII(left)
	switch {
	case strings.HasPrefix(lowerLeft, "id(") && strings.HasSuffix(left, ")"):
		kind = "id"
		varName = strings.TrimSpace(left[3 : len(left)-1])
	case strings.HasPrefix(lowerLeft, "elementid(") && strings.HasSuffix(left, ")"):
		kind = "elementId"
		varName = strings.TrimSpace(left[10 : len(left)-1])
	default:
		return nodeIDResults{}, false, nil
	}
	if varName == "" || varName != nodePattern.variable {
		return nodeIDResults{}, false, nil
	}

	var list []interface{}
	if paramName := strings.TrimPrefix(right, "$"); paramName != right && cyphertext.IsIdentifier(paramName) {
		if params == nil {
			return nodeIDResults{}, false, nil
		}
		raw, ok := params[paramName]
		if !ok {
			return nodeIDResults{}, true, nil
		}
		list = toAnySlice(raw)
	} else {
		value, constant := e.indexSeekConstant(ctx, right)
		if !constant {
			return nodeIDResults{}, false, nil
		}
		if value != nil {
			if list = toAnySlice(value); list == nil {
				return nodeIDResults{}, false, nil
			}
		}
	}
	if len(list) == 0 {
		return nodeIDResults{}, true, nil
	}

	seen := make(map[string]struct{}, len(list))
	ids := newIDResults(len(list))
	for _, item := range list {
		s, ok := item.(string)
		if !ok {
			continue
		}
		lookupID := strings.TrimSpace(s)
		if lookupID == "" {
			continue
		}
		lookupID, found, applicable := e.entitySeekLookup(lookupID, "4", kind)
		if !applicable {
			return nodeIDResults{}, false, nil
		}
		if !found {
			continue
		}
		if _, exists := seen[lookupID]; exists {
			continue
		}
		seen[lookupID] = struct{}{}
		ids.add(storage.NodeID(lookupID))
	}

	if err := ctx.Err(); err != nil {
		return nodeIDResults{}, false, err
	}
	return ids, true, nil
}

// indexOrderMode selects the lookup policy of tryCollectNodesFromPropertyIndexOrderLimit.
// It is a private mode of the one ordered-index owner; callers name the mode
// instead of reaching for a second, nearly identical entry point.
type indexOrderMode int

const (
	// indexOrderGeneral keeps the general MATCH ORDER BY + LIMIT policy: the
	// WHERE may be any filter, the first sort property is bounded only by an
	// implied keyset range, and a partial window falls back to normal
	// selection when the index cannot prove it is complete.
	indexOrderGeneral indexOrderMode = iota
	// indexOrderExplicitNotNull keeps the "WHERE n.prop IS NOT NULL ORDER BY
	// n.prop LIMIT K" policy: the WHERE itself names the indexed property and
	// the returned window is exhaustive over the index.
	indexOrderExplicitNotNull
)

// tryCollectNodesFromPropertyIndexOrderLimit attempts to satisfy ORDER BY + LIMIT
// queries using a property index for sorted iteration. It filters complete
// index-key groups into a bounded ordered window. This avoids a full label scan
// and sort when the non-null index can fill common pagination requests like:
//
//	MATCH (n:Label) WHERE <filter> ORDER BY n.createdAt DESC LIMIT 30
//	MATCH (n:Label) WHERE n.createdAt IS NOT NULL ORDER BY n.createdAt DESC LIMIT 30
//
// It only applies when:
//   - The first ORDER BY property with a matching index exists
//   - The node pattern has at least one label
//   - limit > 0
//   - for indexOrderExplicitNotNull, the WHERE is one <variable>.<prop> IS NOT NULL
//     whose property equals the first sort property
//
// If the ORDER BY has multiple keys, the indexed property is used to fetch
// complete primary-key groups and all keys select the retained top-K window.
//
// Both modes read the same owners in the same order: the shared limit/label
// gate, the mode's own admission check (explicit-not-null mode also parses the
// ORDER BY there because it compares the two properties), the ORDER BY parse,
// the general-mode bounds and null-direction rejection, the schema read, the
// single label carrying an index on the mode's property, and the ordered
// window collection.
func (e *StorageExecutor) tryCollectNodesFromPropertyIndexOrderLimit(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
	orderExpr string,
	limit int,
	mode indexOrderMode,
) ([]*storage.Node, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if limit <= 0 || len(nodePattern.labels) == 0 {
		return nil, false, nil
	}

	// The explicit IS NOT NULL form reads the WHERE property before the ORDER
	// BY because it accepts the seek only when the two name the same property.
	var whereProp string
	if mode == indexOrderExplicitNotNull {
		parsed, ok := e.parseSimpleIndexedIsNotNull(ctx, nodePattern.variable, whereClause)
		if !ok {
			return nil, false, nil
		}
		whereProp = parsed
	}

	orderSpecs, ok := e.parseNodeOrderSpecs(ctx, orderExpr, nodePattern.variable)
	if !ok {
		return nil, false, nil
	}
	spec := orderSpecs[0]

	var bounds storage.PropertyIndexBounds
	primaryNonNull := false
	indexProperty := spec.propName
	switch mode {
	case indexOrderExplicitNotNull:
		if !strings.EqualFold(strings.TrimSpace(spec.propName), strings.TrimSpace(whereProp)) {
			return nil, false, nil
		}
		// The admission match is case-insensitive, but the index lookup keeps
		// the property spelling the WHERE used.
		indexProperty = whereProp
	default:
		// A WHERE that bounds the first sort property (n.t > $last, the keyset
		// form n.t > $t OR (n.t = $t AND n.id > $id)) starts the index scan at
		// the bound, and rules out rows where it is null (#939).
		bounds = impliedPropertyBounds(ctx, nodePattern.variable, spec.propName, whereClause, getParamsFromContext(ctx))
		primaryNonNull = bounds.HasLower || bounds.HasUpper || e.indexedOrderRequiresNonNull(ctx, nodePattern.variable, spec.propName, whereClause)
		if spec.descending && !primaryNonNull {
			return nil, false, nil
		}
	}

	schema := orderedPropertyIndexSchema(e.storage)
	if schema == nil {
		return nil, false, nil
	}

	// More than one candidate label would merge several indexes in order; that
	// ordering is not implemented for either mode yet.
	labels, err := e.indexCandidateLabels(ctx, schema, nodePattern.labels, indexProperty)
	if err != nil {
		return nil, false, err
	}
	if len(labels) != 1 {
		return nil, false, nil
	}
	label := labels[0]

	nodes, used, err := e.collectIndexedOrderWindow(ctx, nodePattern, whereClause, orderSpecs, label, limit, bounds)
	if mode == indexOrderExplicitNotNull {
		// The index holds every row the explicit IS NOT NULL WHERE admits, so
		// a shorter window is already the complete answer.
		return nodes, used, err
	}
	if err == nil && used && len(nodes) < limit {
		if primaryNonNull {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
			return nodes, true, nil
		}
		// The property index omits null keys. An exhausted partial window is
		// not a complete general MATCH result: let normal selection include
		// qualifying nodes whose first sort property is absent.
		return nil, false, nil
	}
	return nodes, used, err
}

func (e *StorageExecutor) indexCandidateLabels(ctx context.Context, schema *storage.SchemaManager, queryLabels []string, property string) ([]string, error) {
	if len(queryLabels) > 0 {
		out := make([]string, 0, len(queryLabels))
		for _, label := range queryLabels {
			exists, err := schema.HasPropertyIndex(ctx, label, property)
			if err != nil {
				return nil, err
			}
			if exists {
				out = append(out, label)
			}
		}
		return out, nil
	}
	labels := make(map[string]struct{})
	for _, raw := range schema.GetIndexes() {
		idx, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		typ, _ := idx["type"].(string)
		if !strings.EqualFold(strings.TrimSpace(typ), "PROPERTY") {
			continue
		}
		label, _ := idx["label"].(string)
		if strings.TrimSpace(label) == "" {
			continue
		}
		prop := ""
		if p, ok := idx["property"].(string); ok {
			prop = p
		}
		if prop == "" {
			if props, ok := idx["properties"].([]string); ok && len(props) == 1 {
				prop = props[0]
			}
			if prop == "" {
				if vals, ok := idx["properties"].([]interface{}); ok && len(vals) == 1 {
					if ps, ok := vals[0].(string); ok {
						prop = ps
					}
				}
			}
		}
		if !strings.EqualFold(strings.TrimSpace(prop), property) {
			continue
		}
		labels[label] = struct{}{}
	}

	out := make([]string, 0, len(labels))
	for label := range labels {
		out = append(out, label)
	}
	sort.Strings(out)
	if !labellessPropertyIndexUsable(ctx, e.storage, out...) {
		return nil, nil
	}
	return out, nil
}

// seekCandidateLabels returns the labels whose property indexes on property
// list every node a seek may return, and the label disjunction the candidates
// must satisfy (nil when none applies).
//
// A pattern with labels uses its indexed labels (indexCandidateLabels). A
// pattern without labels whose WHERE requires one of anyOf (n:A OR n:B, #858)
// uses those labels when every one of them has an index on property: any node
// the WHERE can match carries one of them, so the union of their index
// entries lists it, and Neo4j plans the same predicate as one index seek per
// label. When one of them has no index, the indexes can miss a match and the
// label-less route decides (labellessPropertyIndexUsable).
func (e *StorageExecutor) seekCandidateLabels(ctx context.Context, schema *storage.SchemaManager, patternLabels []string, property string, anyOf []string) ([]string, []string, error) {
	if len(patternLabels) == 0 && len(anyOf) > 0 {
		indexed := true
		for _, label := range anyOf {
			exists, err := schema.HasPropertyIndex(ctx, label, property)
			if err != nil {
				return nil, nil, err
			}
			if !exists {
				indexed = false
				break
			}
		}
		if indexed {
			return anyOf, anyOf, nil
		}
	}
	labels, err := e.indexCandidateLabels(ctx, schema, patternLabels, property)
	if err != nil {
		return nil, nil, err
	}
	return labels, nil, nil
}

// seekCandidateHasLabels reports whether a node an index listed still carries
// the pattern's labels and one of anyOf (when set): an index can list a node
// that has since lost the label.
func seekCandidateHasLabels(node *storage.Node, patternLabels, anyOf []string) bool {
	if len(patternLabels) > 0 && !mergeNodeHasLabels(node, patternLabels) {
		return false
	}
	return len(anyOf) == 0 || nodeHasAnyLabel(node, anyOf)
}

// whereLabelDisjunctionConjunct returns the labels of the first conjunct that
// is a label disjunction on variable (whereLabelDisjunction), or nil.
func whereLabelDisjunctionConjunct(variable string, conjuncts []string) []string {
	for _, conjunct := range conjuncts {
		if labels, ok := whereLabelDisjunction(variable, conjunct); ok {
			return labels
		}
	}
	return nil
}

// whereLabelDisjunction parses a WHERE term that requires variable to carry
// one of a set of labels: v:A, v:A OR v:B …, or the label expression v:A|B,
// with labels plain or backtick-quoted. A conjunction of labels (v:A:B) or any
// other term is not one.
func whereLabelDisjunction(variable, term string) ([]string, bool) {
	want, ok := isOneSymbolicName(variable)
	if !ok {
		return nil, false
	}
	var labels []string
	for _, alternative := range splitTopLevelOrTerms(unwrapOuterParens(strings.TrimSpace(term))) {
		alternative = unwrapOuterParens(strings.TrimSpace(alternative))
		written, end, ok := scanSymbolicName(alternative, 0)
		if !ok || symbolicNameValue(written) != want {
			return nil, false
		}
		rest := strings.TrimSpace(alternative[end:])
		if !strings.HasPrefix(rest, ":") {
			return nil, false
		}
		names := strings.TrimSpace(rest[1:])
		for {
			written, end, ok := scanSymbolicName(names, 0)
			if !ok {
				return nil, false
			}
			labels = append(labels, symbolicNameValue(written))
			names = strings.TrimSpace(names[end:])
			if names == "" {
				break
			}
			if names[0] != '|' {
				return nil, false
			}
			names = strings.TrimSpace(names[1:])
		}
	}
	return labels, len(labels) > 0
}

func (e *StorageExecutor) parseSimpleIndexedEquality(ctx context.Context, variable, whereClause string) (property string, value interface{}, ok bool) {
	clause := unwrapOuterParens(whereClause)
	if clause == "" {
		return "", nil, false
	}

	// Only simple predicates are index-eligible: a = 1 XOR true is not an
	// equality with (1 XOR true).
	if hasTopLevelLogicalOperator(clause) {
		return "", nil, false
	}
	for _, kw := range []string{"NOT", " IN ", " IS ", "<>", "!=", ">=", "<=", ">", "<"} {
		if topLevelKeywordIndex(ctx, clause, kw) >= 0 {
			return "", nil, false
		}
	}

	scan, comparison := scanComparisonChain(clause)
	if !comparison || scan.count != 1 {
		return "", nil, false
	}
	operator := inlineOverflowAt(scan.inline[:], scan.overflow, 0)
	if clause[operator.offset:operator.offset+operator.length] != "=" {
		return "", nil, false
	}
	left, right := scan.operand(clause, 0), scan.operand(clause, 1)
	if left == "" || right == "" {
		return "", nil, false
	}

	prop, isLeftVarProp := parseVariableProperty(left, variable)
	if isLeftVarProp {
		value, constant := e.indexSeekConstant(ctx, right)
		return prop, value, constant
	}
	prop, isRightVarProp := parseVariableProperty(right, variable)
	if isRightVarProp {
		value, constant := e.indexSeekConstant(ctx, left)
		return prop, value, constant
	}
	return "", nil, false
}

func (e *StorageExecutor) parseSimpleIndexedInParam(ctx context.Context, variable, whereClause string, params map[string]interface{}) (property string, values []interface{}, ok bool) {
	clause := strings.TrimSpace(whereClause)
	for strings.HasPrefix(clause, "(") && strings.HasSuffix(clause, ")") && len(clause) >= 2 {
		inner := strings.TrimSpace(clause[1 : len(clause)-1])
		if inner == clause {
			break
		}
		clause = inner
	}
	if clause == "" {
		return "", nil, false
	}
	// Keep this optimization deterministic and safe: only simple standalone IN predicates.
	if containsFold(clause, " AND ") || containsFold(clause, " OR ") {
		return "", nil, false
	}
	inIdx := keywordIndexFrom(ctx, clause, "IN", 0, defaultKeywordScanOpts())
	if inIdx <= 0 || inIdx >= len(clause)-2 {
		return "", nil, false
	}
	left := strings.TrimSpace(clause[:inIdx])
	right := strings.TrimSpace(clause[inIdx+2:])
	if !strings.HasPrefix(right, "$") {
		return "", nil, false
	}
	paramName := strings.TrimSpace(strings.TrimPrefix(right, "$"))
	if paramName == "" || params == nil {
		return "", nil, false
	}
	parsedProp, ok := parseVariableProperty(left, variable)
	if !ok {
		return "", nil, false
	}
	raw, exists := params[paramName]
	if !exists || raw == nil {
		return "", nil, false
	}
	list := coerceInterfaceList(raw)
	if len(list) == 0 {
		// x IN [] holds for no node: the property's index answers it with
		// none, as the literal form does.
		return parsedProp, []interface{}{}, true
	}
	out := make([]interface{}, 0, len(list))
	seen := make(map[string]struct{}, len(list))
	for _, v := range list {
		if v == nil {
			continue
		}
		k := fmt.Sprintf("%T:%v", v, v)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, v)
	}
	return parsedProp, out, true
}

func (e *StorageExecutor) parseSimpleIndexedInLiteral(ctx context.Context, variable, whereClause string) (property string, values []interface{}, ok bool) {
	clause := strings.TrimSpace(whereClause)
	for strings.HasPrefix(clause, "(") && strings.HasSuffix(clause, ")") && len(clause) >= 2 {
		inner := strings.TrimSpace(clause[1 : len(clause)-1])
		if inner == clause {
			break
		}
		clause = inner
	}
	if clause == "" {
		return "", nil, false
	}
	// Keep this optimization deterministic and safe: only simple standalone IN predicates.
	if containsFold(clause, " AND ") || containsFold(clause, " OR ") {
		return "", nil, false
	}
	inIdx := keywordIndexFrom(ctx, clause, "IN", 0, defaultKeywordScanOpts())
	if inIdx <= 0 || inIdx >= len(clause)-2 {
		return "", nil, false
	}
	left := strings.TrimSpace(clause[:inIdx])
	right := strings.TrimSpace(clause[inIdx+2:])
	if left == "" || right == "" {
		return "", nil, false
	}
	parsedProp, ok := parseVariableProperty(left, variable)
	if !ok {
		return "", nil, false
	}
	if !isListLiteral(right) {
		return "", nil, false
	}
	rawList, constant := e.indexSeekConstant(ctx, right)
	if !constant {
		return "", nil, false
	}
	list := coerceInterfaceList(rawList)
	if len(list) == 0 {
		return parsedProp, []interface{}{}, true
	}
	out := make([]interface{}, 0, len(list))
	seen := make(map[string]struct{}, len(list))
	for _, v := range list {
		if v == nil {
			continue
		}
		k := fmt.Sprintf("%T:%v", v, v)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, v)
	}
	return parsedProp, out, true
}

// coerceInterfaceList returns a parameter list's elements as Cypher values
// (cypherListValue), or nil when the value isn't a list; a non-list IN
// parameter is rejected before execution (validateMembershipParameters).
func coerceInterfaceList(v interface{}) []interface{} {
	items, _ := cypherListValue(v)
	return items
}

func (e *StorageExecutor) parseSimpleIndexedIsNotNull(ctx context.Context, variable, whereClause string) (property string, ok bool) {
	clause := strings.TrimSpace(whereClause)
	clause = unwrapOuterParens(clause)
	if clause == "" {
		return "", false
	}
	parts := splitTopLevelAndConjuncts(ctx, clause)
	targetProp := ""
	for _, raw := range parts {
		part := strings.TrimSpace(raw)
		if part == "" {
			continue
		}
		part = unwrapOuterParens(part)
		if p, ok := parseSimpleSingleIndexedIsNotNull(variable, part); ok {
			if targetProp != "" && !strings.EqualFold(targetProp, p) {
				return "", false
			}
			targetProp = p
			continue
		}
		// Allow only constant boolean conjuncts in addition to var.prop IS NOT NULL.
		// This keeps top-K semantics correct while supporting cache-buster style predicates.
		if _, isConst := e.tryEvaluateConstantBooleanConjunct(part); isConst {
			continue
		}
		return "", false
	}
	if targetProp == "" {
		return "", false
	}
	return targetProp, true
}

func parseSimpleSingleIndexedIsNotNull(variable, clause string) (property string, ok bool) {
	upper := upperASCII(strings.TrimSpace(clause))
	sfx := " IS NOT NULL"
	if !strings.HasSuffix(upper, sfx) {
		return "", false
	}
	left := strings.TrimSpace(clause[:len(clause)-len(sfx)])
	if left == "" {
		return "", false
	}
	prop, ok := parseVariableProperty(left, variable)
	if !ok {
		return "", false
	}
	return prop, true
}

func unwrapOuterParens(clause string) string {
	out := lexicalExpressionText(clause)
	// Only a pair that encloses the whole clause: (a) OR (b) starts and ends
	// with a parenthesis, but they don't match each other.
	for strings.HasPrefix(out, "(") && cyphertext.FindMatchingDelimiter(out, 0, '(', ')', cyphertext.OpeningDelimiter) == len(out)-1 {
		out = strings.TrimSpace(out[1 : len(out)-1])
	}
	return out
}

// splitTopLevelAndConjuncts splits a WHERE clause into its top-level AND
// conjuncts. An AND inside a string, brackets or a CASE … END block (a WHEN
// condition, #699) belongs to its operand. It is the one AND splitter of the
// WHERE planners (index seek, cartesian and CREATE / MERGE filters, CALL
// subquery correlation).
func splitTopLevelAndConjuncts(ctx context.Context, clause string) []string {
	if topLevelKeywordIndex(ctx, clause, "OR") >= 0 || topLevelKeywordIndex(ctx, clause, "XOR") >= 0 {
		return []string{strings.TrimSpace(clause)}
	}
	paren, bracket, brace, caseDepth := 0, 0, 0, 0
	parts := make([]string, 0, 2)
	start := 0
	for i := 0; i < len(clause); i++ {
		if end, skipped := skipCypherLexicalToken(clause, i); skipped {
			i = end - 1
			continue
		}
		ch := clause[i]
		switch ch {
		case '(':
			paren++
			continue
		case ')':
			if paren > 0 {
				paren--
			}
			continue
		case '[':
			bracket++
			continue
		case ']':
			if bracket > 0 {
				bracket--
			}
			continue
		case '{':
			brace++
			continue
		case '}':
			if brace > 0 {
				brace--
			}
			continue
		case 'C', 'c':
			if caseKeywordAt(clause, i, "CASE") {
				caseDepth++
				i += len("CASE") - 1
				continue
			}
		case 'E', 'e':
			if caseDepth > 0 && caseKeywordAt(clause, i, "END") {
				caseDepth--
				i += len("END") - 1
				continue
			}
		}
		if paren == 0 && bracket == 0 && brace == 0 && caseDepth == 0 && i+3 <= len(clause) && strings.EqualFold(clause[i:i+3], "AND") {
			prevOK := i == 0 || cyphertext.IsWhitespaceAt(clause, i-1) || clause[i-1] == '('
			nextIdx := i + 3
			nextOK := nextIdx >= len(clause) || cyphertext.IsWhitespaceAt(clause, nextIdx) || clause[nextIdx] == ')'
			if prevOK && nextOK {
				parts = append(parts, strings.TrimSpace(clause[start:i]))
				start = nextIdx
				i = nextIdx - 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(clause[start:]))
	return parts
}

func (e *StorageExecutor) tryEvaluateConstantBooleanConjunct(clause string) (value bool, ok bool) {
	expr := strings.TrimSpace(clause)
	if expr == "" {
		return false, false
	}
	if strings.EqualFold(expr, "TRUE") {
		return true, true
	}
	if strings.EqualFold(expr, "FALSE") {
		return false, true
	}
	// If expression mentions variables/properties, treat as non-constant.
	if strings.Contains(expr, ".") || strings.Contains(expr, "$") {
		return false, false
	}
	for _, op := range []string{"<>", "!=", ">=", "<=", "=", ">", "<"} {
		if idx := topLevelSymbolIndex(expr, op); idx >= 0 {
			left := strings.TrimSpace(expr[:idx])
			right := strings.TrimSpace(expr[idx+len(op):])
			lv, lok := parseLiteralValue(left)
			rv, rok := parseLiteralValue(right)
			if !lok || !rok {
				return false, false
			}
			return compareLiteralValues(lv, rv, op)
		}
	}
	return false, false
}

func topLevelSymbolIndex(expr, sym string) int {
	return findTopLevelOperator(expr, sym, false, true)
}

func parseLiteralValue(raw string) (interface{}, bool) {
	s := strings.TrimSpace(raw)
	if value, quoted := cyphertext.DecodeString(s, cyphertext.AcceptedSource); quoted {
		return value, true
	}
	if strings.EqualFold(s, "true") {
		return true, true
	}
	if strings.EqualFold(s, "false") {
		return false, true
	}
	if strings.EqualFold(s, "null") {
		return nil, true
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i, true
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f, true
	}
	return nil, false
}

func compareLiteralValues(left, right interface{}, op string) (bool, bool) {
	switch op {
	case "=", "!=", "<>":
		return compareCypherPredicateValues(left, right, op), true
	case ">", "<", ">=", "<=":
		if _, comparable := compareCypherOrderedValues(left, right); !comparable {
			return false, false
		}
		return compareCypherPredicateValues(left, right, op), true
	default:
		return false, false
	}
}

func parseVariableProperty(expr, variable string) (string, bool) {
	dot := strings.IndexByte(expr, '.')
	if dot <= 0 || dot >= len(expr)-1 {
		return "", false
	}
	lhs := strings.TrimSpace(expr[:dot])
	if !strings.EqualFold(lhs, strings.TrimSpace(variable)) {
		return "", false
	}
	prop := normalizePropertyKey(strings.TrimSpace(expr[dot+1:]))
	if prop == "" {
		return "", false
	}
	return prop, true
}

func topLevelEqualsIndex(clause string) int {
	for offset := 0; offset < len(clause); {
		index := findTopLevelOperator(clause[offset:], "=", false, true)
		if index < 0 {
			return -1
		}
		index += offset
		if index > 0 {
			previous := clause[index-1]
			if previous == '!' || previous == '<' || previous == '>' {
				offset = index + 1
				continue
			}
		}
		if index+1 < len(clause) && clause[index+1] == '=' {
			return index + 1
		}
		return index
	}
	return -1
}

func nodeHasAnyLabel(node *storage.Node, labels []string) bool {
	for _, want := range labels {
		for _, have := range node.Labels {
			if strings.EqualFold(strings.TrimSpace(want), strings.TrimSpace(have)) {
				return true
			}
		}
	}
	return false
}

// tryRewriteNullNormalizedPredicate rewrites coalesce-wrapped equality predicates
// into OR-expanded forms that are visible to the index seek cascade and compiled
// WHERE fast-paths.
//
// Rewrites:
//
//	coalesce(var.prop, <default>) = <value>  →  (var.prop = <value> OR var.prop IS NULL)
//	                                            when <value> == <default>
//
//	coalesce(var.prop, <default>) <> <value> →  (var.prop <> <value> AND var.prop IS NOT NULL)
//	                                            when <value> == <default>
//
// This is safe because coalesce(x, d) = d is semantically equivalent to
// (x = d OR x IS NULL).
func tryRewriteNullNormalizedPredicate(whereClause string) string {
	clause := strings.TrimSpace(whereClause)
	if clause == "" {
		return clause
	}

	// Only rewrite simple top-level predicates (no AND/OR at the top level).
	// Nested coalesce inside AND/OR conjuncts would need recursive rewrite
	// which risks semantic drift — keep it strict.
	upper := upperASCII(clause)
	if strings.Contains(upper, " AND ") || strings.Contains(upper, " OR ") {
		return clause
	}

	// Look for coalesce( at the start.
	lowerClause := lowerASCII(clause)
	if !strings.HasPrefix(lowerClause, "coalesce(") {
		return clause
	}

	// Find matching closing paren for coalesce(...).
	depth := 0
	closeIdx := -1
	for i := 9; i < len(clause); i++ { // start after "coalesce("
		switch clause[i] {
		case '(':
			depth++
		case ')':
			if depth == 0 {
				closeIdx = i
				goto found
			}
			depth--
		}
	}
found:
	if closeIdx < 0 {
		return clause
	}

	inner := strings.TrimSpace(clause[9:closeIdx])
	afterCoalesce := strings.TrimSpace(clause[closeIdx+1:])

	// Parse inner: expect "var.prop, <default>"
	commaIdx := strings.Index(inner, ",")
	if commaIdx <= 0 {
		return clause
	}
	varProp := strings.TrimSpace(inner[:commaIdx])
	defaultVal := strings.TrimSpace(inner[commaIdx+1:])
	if varProp == "" || defaultVal == "" {
		return clause
	}
	// varProp must look like variable.property
	if !strings.Contains(varProp, ".") {
		return clause
	}

	// Parse operator and RHS: expect "= <value>" or "<> <value>" or "!= <value>"
	op := ""
	rhs := ""
	if strings.HasPrefix(afterCoalesce, "<>") {
		op = "<>"
		rhs = strings.TrimSpace(afterCoalesce[2:])
	} else if strings.HasPrefix(afterCoalesce, "!=") {
		op = "<>"
		rhs = strings.TrimSpace(afterCoalesce[2:])
	} else if strings.HasPrefix(afterCoalesce, "=") {
		op = "="
		rhs = strings.TrimSpace(afterCoalesce[1:])
	} else {
		return clause
	}

	if rhs == "" {
		return clause
	}

	// Only rewrite when the comparison value matches the default value.
	// coalesce(x, false) = false → rewrite
	// coalesce(x, false) = true  → don't rewrite (different semantics)
	if !strings.EqualFold(rhs, defaultVal) {
		return clause
	}

	if op == "=" {
		return fmt.Sprintf("(%s = %s OR %s IS NULL)", varProp, rhs, varProp)
	}
	// <> case: coalesce(x, d) <> d means x <> d AND x IS NOT NULL
	return fmt.Sprintf("(%s <> %s AND %s IS NOT NULL)", varProp, rhs, varProp)
}

// parseIndexedRangeBounds finds, among the top-level AND conjuncts of a
// WHERE, comparisons of variable.property with a literal or parameter, and
// returns the bounds they place on the first such property.
func parseIndexedRangeBounds(ctx context.Context, variable, whereClause string, params map[string]interface{}) (property string, bounds storage.PropertyIndexBounds, ok bool) {
	clause := unwrapOuterParens(strings.TrimSpace(whereClause))
	if clause == "" {
		return "", bounds, false
	}
	for _, raw := range splitTopLevelAndConjuncts(ctx, clause) {
		operands, operators, isChain := splitComparisonChain(unwrapOuterParens(strings.TrimSpace(raw)))
		if !isChain {
			continue
		}
		for index, operator := range operators {
			left, right := strings.TrimSpace(operands[index]), strings.TrimSpace(operands[index+1])
			if leftProperty, isProperty := parseVariableProperty(left, variable); isProperty {
				if value, resolved := rangeBoundValue(right, params); resolved && (property == "" || property == leftProperty) {
					property = leftProperty
					addIndexedRangeBound(&bounds, operator, value)
				}
				continue
			}
			if rightProperty, isProperty := parseVariableProperty(right, variable); isProperty {
				if value, resolved := rangeBoundValue(left, params); resolved && (property == "" || property == rightProperty) {
					property = rightProperty
					addIndexedRangeBound(&bounds, mirroredComparison(operator), value)
				}
			}
		}
	}
	return property, bounds, property != "" && (bounds.HasLower || bounds.HasUpper)
}

// rangeBoundValue resolves a literal or $parameter operand of a range
// comparison.
func rangeBoundValue(operand string, params map[string]interface{}) (interface{}, bool) {
	if strings.HasPrefix(operand, "$") {
		value, exists := params[strings.TrimPrefix(operand, "$")]
		return value, exists
	}
	value, ok := parseLiteralValue(operand)
	if !ok {
		return nil, false
	}
	return value, true
}

// addIndexedRangeBound records "property operator value". A second bound on
// the same side is not combined: keeping one still selects every match.
func addIndexedRangeBound(bounds *storage.PropertyIndexBounds, operator string, value interface{}) {
	switch operator {
	case ">", ">=":
		if !bounds.HasLower {
			bounds.Lower, bounds.HasLower, bounds.LowerInclusive = value, true, operator == ">="
		}
	case "<", "<=":
		if !bounds.HasUpper {
			bounds.Upper, bounds.HasUpper, bounds.UpperInclusive = value, true, operator == "<="
		}
	}
}

// mirroredComparison is the operator with its operands swapped (5 < p.x is
// p.x > 5); others are returned unchanged and ignored by the caller.
func mirroredComparison(operator string) string {
	switch operator {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	}
	return operator
}

// collectPipelineIndexedNodeCandidates is the index half of
// collectPipelineInitialNodeCandidates: the candidates of the index operator
// that narrows the MATCH best, with used false when none can. An ordered
// window or an id seek is read when one applies; otherwise the equality, IN
// and OR seeks of the WHERE and of the pattern's inline properties, on any
// pattern label, compete by the index entries they read (bestNodeIndexSeek),
// and range and IS NOT NULL seeks are the fallbacks. Row reads and the
// filtered count (tryStreamPipelineFilteredNodeCount) both seed from it, so a
// lookup that uses an index for rows uses it for count(*) too (#820).
//
// The operator is prepared once (planPipelineIndexedNodeCandidates) and then
// read (readNodeIndexCandidatePlan): a caller that needs only the size of the
// candidate set reads the preparation alone, and one that drops the plan reads
// no candidate node at all (NOR-414).
func (e *StorageExecutor) collectPipelineIndexedNodeCandidates(ctx context.Context, nodePattern nodePatternInfo, whereClause string, hint pipelineMatchPhysicalHint) (nodes []*storage.Node, whereApplied bool, used bool, err error) {
	plan, used, err := e.planPipelineIndexedNodeCandidates(ctx, nodePattern, whereClause, hint)
	if err != nil || !used {
		return nil, false, false, err
	}
	return e.readNodeIndexCandidatePlan(ctx, plan)
}
