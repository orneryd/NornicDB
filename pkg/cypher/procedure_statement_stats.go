package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)

type procedureStatementStatsKey struct{}
type procedureSchemaAccessKey struct{}
type procedureStatementStats struct {
	context.Context
	stats     *QueryStats
	committed *QueryStats
}

// Schema admission shares the existing invocation allocation. A schema-mode
// child cannot acquire authority its enclosing data procedure did not have.
type schemaProcedureStatementStats struct{ procedureStatementStats }

func newProcedureStatementStats(ctx context.Context, mode ProcedureMode) (context.Context, *procedureStatementStats) {
	parentAllows, nested := ctx.Value(procedureSchemaAccessKey{}).(bool)
	if mode == ProcedureModeSchema && (!nested || parentAllows) {
		frame := &schemaProcedureStatementStats{procedureStatementStats{Context: ctx}}
		return frame, &frame.procedureStatementStats
	}
	frame := &procedureStatementStats{Context: ctx}
	return frame, frame
}

func (s *schemaProcedureStatementStats) Value(key interface{}) interface{} {
	if _, own := key.(procedureSchemaAccessKey); own {
		return true
	}
	return s.procedureStatementStats.Value(key)
}

// This private marker records a successful read without allocating counters.
// It is never returned or mutated; record replaces it before adding writes.
var procedureStatementReadStats QueryStats

func (s *procedureStatementStats) record(delta *QueryStats) {
	if s.stats == &procedureStatementReadStats {
		s.stats = nil
	}
	s.stats = mergeQueryStats(s.stats, delta)
	if s.stats == nil {
		s.stats = &procedureStatementReadStats
	}
}

func (s *procedureStatementStats) summary() *QueryStats {
	if s.stats == &procedureStatementReadStats {
		return nil
	}
	return s.stats
}

type procedureStatementTransactionScopeKey struct{}

// A transaction scope stages contributions in the invocation's existing
// collector type. Only the transaction owner may publish them after commit.
// Nested procedure invocations still fold through their returned summary.
type procedureStatementStatsScope struct {
	procedureStatementStats
	parent    *procedureStatementStats
	root      *procedureStatementStats
	enclosing *procedureStatementStatsScope
	covered   *QueryStats
}

func beginProcedureStatementStatsScope(ctx context.Context) (context.Context, *procedureStatementStatsScope) {
	parent, _ := ctx.Value(procedureStatementStatsKey{}).(*procedureStatementStats)
	if parent == nil {
		return ctx, nil
	}
	root := parent
	for {
		previous, _ := root.Context.Value(procedureStatementStatsKey{}).(*procedureStatementStats)
		if previous == nil {
			break
		}
		root = previous
	}
	enclosing, _ := ctx.Value(procedureStatementTransactionScopeKey{}).(*procedureStatementStatsScope)
	scope := &procedureStatementStatsScope{
		procedureStatementStats: procedureStatementStats{Context: ctx},
		parent:                  parent,
		root:                    root,
		enclosing:               enclosing,
	}
	return scope, scope
}

func (s *procedureStatementStatsScope) Value(key interface{}) interface{} {
	if _, own := key.(procedureSchemaAccessKey); own {
		return s.Context.Value(key)
	}
	if _, own := key.(procedureStatementTransactionScopeKey); own {
		return s
	}
	return s.procedureStatementStats.Value(key)
}

func (s *procedureStatementStatsScope) commit(batchStats *QueryStats) {
	if s == nil || s.parent == nil {
		return
	}
	// A nested independently committed unit survives an enclosing rollback.
	// Retain only its own contribution at the invocation root; its descendants
	// were retained at their commits. Track the completed summary in the
	// enclosing scope so a successful enclosing commit removes that credit
	// when the same counters arrive through the ordinary result fold.
	if s.enclosing != nil {
		var own QueryStats
		addQueryStats(&own, batchStats)
		subtractQueryStats(&own, s.covered)
		s.root.committed = mergeQueryStats(s.root.committed, &own)
		s.enclosing.covered = mergeQueryStats(s.enclosing.covered, batchStats)
	} else {
		subtractQueryStats(s.root.committed, s.covered)
	}
	if s.stats != nil {
		s.parent.record(s.summary())
	}
	s.discard()
}

func (s *procedureStatementStatsScope) discard() {
	if s != nil {
		s.parent = nil
	}
}

// Value keeps the invocation collector in the same allocation as its context
// frame. Other statement metadata continues through the caller's context.
func (s *procedureStatementStats) Value(key interface{}) interface{} {
	if _, own := key.(procedureSchemaAccessKey); own {
		return false
	}
	if _, own := key.(procedureStatementStatsKey); own {
		return s
	}
	return s.Context.Value(key)
}

// executeProcedureHandler owns the summary of statement-valued procedures.
// Their output rows (including APOC statistics rows) are independent of the
// statement summary. Replace their hand-built summary with the successful
// nested statements' counters, including iterator/export/custom bodies.
func (e *StorageExecutor) executeProcedureHandler(ctx context.Context, proc registeredProcedure, query string, args []interface{}) (*ExecuteResult, error) {
	var release func()
	e, release = e.bindExecutionReadScope(ctx)
	defer release()
	// Administration procedures can execute outside a clause pipeline. Record
	// their actual invocation there, rather than treating its reviewed reads
	// and mutation requests as an unexecuted write statement. Calls already
	// owned by a clause retain that clause's one record.
	var invocation *profileRuntimeRecord
	if profileRuntimeFromContext(ctx) != nil {
		if _, framed := profileRuntimeFrameFromContext(ctx); !framed {
			ctx, invocation = profileRuntimeInvocation(ctx, "ProcedureCall", proc.Spec.Name, 0)
		}
	}
	handlerCtx, totals := newProcedureStatementStats(ctx, proc.Spec.Mode)
	// A registration that declares opaque reads has database access this
	// statement's readers cannot see, so its actual invocation is the one
	// unobserved read to report. A registration whose reads are canonical
	// reports them itself and reports no opaque callback.
	if proc.reads != cypherfn.CanonicalReads {
		queryprofile.Callback(ctx, "procedure callback", proc.Spec.Name)
	}
	result, err := proc.Handler(handlerCtx, e, query, args)
	if err == nil && invocation != nil {
		rows := 0
		if result != nil {
			rows = len(result.Rows)
		}
		invocation.invocationRows(rows)
	}
	if err == nil && (totals.stats != nil || totals.committed != nil) {
		if result == nil {
			result = &ExecuteResult{}
		}
		if totals.stats != nil {
			result.Stats = totals.summary()
		}
		result.Stats = mergeQueryStats(result.Stats, totals.committed)
	}
	return result, err
}
