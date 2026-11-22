package cypher

import (
	"context"
	"strings"
)

// chainPrefixPrune applies a chain's WHERE (TraversalMatch.pruneWhere) to its
// path prefixes: after k segments, the conjuncts whose every variable the
// first k segments bind, which no later segment can change. A prefix that
// fails one is not extended: its complete paths would fail the same conjunct
// in the WHERE the caller still applies to them. Conjuncts that name the path
// variable, a variable bound only later or outside the pattern, or no
// variable, or that hold a subquery expression, or that are not
// deterministic, wait for that WHERE.
type chainPrefixPrune struct {
	levels []chainPrefixLevel // levels[k-1]: after k segments
}

type chainPrefixLevel struct {
	match *TraversalMatch // the chain's first k segments
	where *preparedRowPredicate
}

func (e *StorageExecutor) newChainPrefixPrune(ctx context.Context, match *TraversalMatch) *chainPrefixPrune {
	if match.pruneWhere == "" || len(match.Segments) < 2 || len(match.IntermediateNodes) < len(match.Segments)-1 {
		return nil
	}
	conjuncts := splitTopLevelAndConjuncts(ctx, strings.TrimSpace(match.pruneWhere))
	decided := make([]bool, len(conjuncts))
	bound := map[string]bool{}
	bind := func(name string) {
		if name != "" {
			bound[name] = true
		}
	}
	bind(match.Segments[0].FromNode.variable)
	prune := &chainPrefixPrune{levels: make([]chainPrefixLevel, len(match.Segments)-1)}
	found := false
	for k := 1; k < len(match.Segments); k++ {
		segment := match.Segments[k-1]
		bind(segment.Relationship.Variable)
		bind(segment.ToNode.variable)
		var parts []string
		for i, conjunct := range conjuncts {
			if decided[i] || !chainPrefixDecides(ctx, conjunct, bound, match.PathVariable) {
				continue
			}
			decided[i] = true
			parts = append(parts, "("+conjunct+")")
		}
		if len(parts) == 0 {
			continue
		}
		found = true
		where := prepareRowPredicate(strings.Join(parts, " AND "))
		// Retain only immutable, complete syntax plans. A local lazy selection
		// in keep otherwise repeats for every outer row using this chain.
		if plan := planRowPredicate(where.expression); plan != nil && plan.complete {
			where.plan = plan
		}
		prune.levels[k-1] = chainPrefixLevel{match: chainPrefixMatch(match, k), where: where}
	}
	if !found {
		return nil
	}
	return prune
}

// chainPrefixDecides reports whether conjunct can be decided on a path prefix
// that binds bound.
func chainPrefixDecides(ctx context.Context, conjunct string, bound map[string]bool, pathVariable string) bool {
	if referencesVariable(conjunct, pathVariable) || !chainConjunctIsStable(ctx, conjunct) {
		return false
	}
	// A subquery expression (EXISTS / COUNT / COLLECT { … }) reads outer
	// variables in its body, which expressionFreeVariables does not list: a
	// conjunct that holds one waits for the final WHERE, where every variable
	// its body may read is bound.
	if mayContainSubqueryExpression(conjunct) && len(findSubqueryExpressions(conjunct)) > 0 {
		return false
	}
	names := expressionFreeVariables(ctx, conjunct)
	if len(names) == 0 {
		return false
	}
	for _, name := range names {
		if name == pathVariable || !bound[name] {
			return false
		}
	}
	return true
}

// chainConjunctIsStable reports whether every function call in conjunct returns
// the same value when the statement evaluates it again. The function catalog's
// volatility declarations own that decision, so no spelling of a call decides
// it: a call the catalog does not declare (a plugin's function, for example) or
// declares volatile is not stable, and neither is a function that is volatile
// only without arguments unless the call has one.
func chainConjunctIsStable(ctx context.Context, conjunct string) bool {
	stable := true
	err := forEachFunctionInvocation(ctx, conjunct, func(name, normalized string, open int) error {
		if !stable {
			return nil
		}
		volatility, args, known := stableFunctionInvocation(conjunct, normalized, open)
		if !known {
			stable = false
			return nil
		}
		if PluginFunctionLookup != nil {
			if _, opaque := PluginFunctionLookup(name); opaque {
				stable = false
				return nil
			}
		}
		if volatility == functionTemporalConstructor {
			for _, argument := range args {
				value, known := compileExecutionInput(argument, 1)
				if !known || value.bound {
					stable = false
					return nil
				}
				if _, text := value.literal.(string); !text {
					stable = false
					return nil
				}
			}
		}
		return nil
	})
	return err == nil && stable
}

// chainPrefixMatch is match's first k segments as a pattern of their own, so
// buildPathContext binds a k-segment prefix's variables.
func chainPrefixMatch(match *TraversalMatch, k int) *TraversalMatch {
	segments := match.Segments[:k]
	prefix := &TraversalMatch{
		StartNode:    match.StartNode,
		EndNode:      segments[k-1].ToNode,
		Relationship: segments[k-1].Relationship,
		Segments:     segments,
	}
	if k > 1 {
		prefix.IsChained = true
		prefix.IntermediateNodes = match.IntermediateNodes[:k-1]
	}
	return prefix
}

// keep returns the paths of k segments whose prefix passes the conjuncts
// decided after k segments.
func (p *chainPrefixPrune) keep(ctx context.Context, e *StorageExecutor, paths []PathResult, k int) []PathResult {
	if p == nil || k < 1 || k > len(p.levels) || p.levels[k-1].where == nil {
		return paths
	}
	level := p.levels[k-1]
	// holds lazily selects a plan/program. Do not mutate a predicate retained
	// by a statement plan shared across rows or concurrent evaluations.
	where := *level.where
	kept := paths[:0]
	explicitPlan := where.plan != nil && profileRuntimeFromContext(ctx) == nil
	for _, path := range paths {
		values := e.pathContextValues(ctx, e.buildPathContext(path, level.match))
		bindParameterRow(ctx, values)
		// A prefix is speculative: a missing later segment may leave no row
		// on which the complete WHERE would run. Defer expression failures
		// to that final predicate rather than failing an absent path early.
		if explicitPlan {
			// Unlike the final WHERE, speculative pruning consumes explicit errors
			// without recording them on the statement. The canonical value owner
			// already returns them, so no deferred context slot is needed here.
			value, err := e.rowPredicatePartValue(ctx, &where.plan.root, compiledRowScope{values: values}, nil)
			truth, truthErr := cypherPredicateTruth(value)
			if err != nil || truthErr != nil || truth == truthTrue {
				kept = append(kept, path)
			}
			continue
		}
		value, _, err := deferredExpressionValue(ctx, func(prefixCtx context.Context) interface{} {
			return where.holds(e, prefixCtx, values)
		})
		if err != nil || value == true {
			kept = append(kept, path)
		}
	}
	return kept
}
