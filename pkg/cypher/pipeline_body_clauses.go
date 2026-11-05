package cypher

import (
	"context"
	"sync"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// pipelineClauseKey is the retained branch a split was projected from: its one
// parse product, the branch's span, its first and last clause node, and the
// USE/FINISH state the caller normalized. Node IDs mean nothing outside their
// product, so the product identity is part of the key: a body sliced to another
// window, or with USE or FINISH projected out, never returns another view's
// clause list.
type pipelineClauseKey struct {
	product       cypherantlr.SyntaxIdentity
	start, end    int
	first, last   int
	use, finished bool
	cypher25      bool
}

// pipelineClauseBodySplits caches a retained branch's split, so the clauses of
// one body are projected once for semantic validation and for every row of its
// execution. It uses the same bound as the text cache.
var pipelineClauseBodySplits = struct {
	sync.RWMutex
	splits map[pipelineClauseKey]pipelineClauseSplit
}{splits: make(map[pipelineClauseKey]pipelineClauseSplit)}

// pipelineClauseBranchKey is one retained branch's split identity. A branch
// whose clauses come from more than one product is never cached: its node IDs
// are not comparable across them.
func pipelineClauseBranchKey(branch queryBodyBranch, cypher25 bool) (pipelineClauseKey, bool) {
	if len(branch.clauses) == 0 || !branch.clauses[0].syntax.Valid() {
		return pipelineClauseKey{}, false
	}
	product := branch.clauses[0].syntax.Identity()
	for _, clause := range branch.clauses {
		if clause.syntax.Identity() != product {
			return pipelineClauseKey{}, false
		}
	}
	return pipelineClauseKey{
		product:  product,
		start:    branch.start,
		end:      branch.end,
		first:    branch.clauses[0].syntax.ID(),
		last:     branch.clauses[len(branch.clauses)-1].syntax.ID(),
		use:      branch.useStart >= 0,
		finished: branch.finished,
		cypher25: cypher25,
	}, true
}

// pipelineClausesForBody splits a retained query body, for the pipeline
// executor and for a CALL body whose node the statement already holds.
func pipelineClausesForBody(ctx context.Context, body queryBody) ([]pipelineClause, bool) {
	split := cachedPipelineClausesBody(ctx, body, true)
	return split.clauses, split.ok
}

// cachedPipelineClausesBody splits a retained body once: one branch of one read
// product, as parsePipelineClauses reads a whole source.
func cachedPipelineClausesBody(ctx context.Context, body queryBody, procedureCalls bool) pipelineClauseSplit {
	if body.status != queryBodyRead || len(body.branches) != 1 {
		return pipelineClauseSplit{}
	}
	return cachedPipelineClausesBranch(ctx, body.branches[0], procedureCalls)
}

// cachedPipelineClausesBranch projects one retained branch's clauses once.
// Callers get their own copy of the clause list; cancellation is recorded in
// the statement's existing failure slot so dispatch can distinguish it from an
// unsupported body.
func cachedPipelineClausesBranch(ctx context.Context, branch queryBodyBranch, procedureCalls bool) pipelineClauseSplit {
	if err := ctx.Err(); err != nil {
		recordExpressionFailure(ctx, err)
		return pipelineClauseSplit{}
	}
	key, cacheable := pipelineClauseBranchKey(branch, cypherVersionFromContext(ctx) == "25")
	var split pipelineClauseSplit
	cached := false
	if cacheable {
		pipelineClauseBodySplits.RLock()
		split, cached = pipelineClauseBodySplits.splits[key]
		pipelineClauseBodySplits.RUnlock()
	}
	if !cached {
		split = parsePipelineClauseBranch(branch)
		if err := ctx.Err(); err != nil {
			recordExpressionFailure(ctx, err)
			return pipelineClauseSplit{}
		}
		if cacheable {
			pipelineClauseBodySplits.Lock()
			if err := ctx.Err(); err != nil {
				pipelineClauseBodySplits.Unlock()
				recordExpressionFailure(ctx, err)
				return pipelineClauseSplit{}
			}
			if len(pipelineClauseBodySplits.splits) >= pipelineClauseSplitLimit {
				pipelineClauseBodySplits.splits = make(map[pipelineClauseKey]pipelineClauseSplit)
			}
			pipelineClauseBodySplits.splits[key] = split
			pipelineClauseBodySplits.Unlock()
		}
	}
	if split.topLevelCall && !procedureCalls {
		return pipelineClauseSplit{}
	}
	if split.clauses != nil {
		split.clauses = append([]pipelineClause(nil), split.clauses...)
	}
	return split
}
