package cypher

// The PROFILE runtime row collector observes the rows a statement's pipeline
// clauses actually handed on while it ran. PROFILE's owner (executeProfile)
// installs one collector in the execution context; every program the statement
// runs inherits it. This file owns what that collector records and how the
// pipeline reports to it, and nothing else: a statement without a collector
// observes nothing, wraps no source and replaces no context, so an ordinary
// query pays one branch per hook.
//
// A record is one executing clause: the clause text the pipeline actually ran
// (the composed text for a clause the executor built itself), the type it ran
// the clause as, and the record of the clause whose own execution started the
// program the clause ran in (0 for the statement's own program). Keying by what
// executed - not by the statement's text, the plan's operators or the final
// cardinality - is what lets a clause run once per input row, a row-ordered
// write stretch, an UNWIND continuation and a CALL body run per outer row all
// add to the one record that names them.
//
// Rows are what the clause actually handed on: the length of its output rows
// for an eager clause, and, for a clause that hands on a lazy row source, the
// rows that source actually yielded. A lazy source is wrapped where the clause
// installs its output, so a helper that reads or narrows the previous source is
// never counted as the clause it belongs to. Because the wrapper keeps the
// context the source captured, a reader that runs while the source is consumed
// later still reports to the clause that installed it.
//
// A clause the executor fuses with the clauses after it (a CREATE stretch, a
// SET stretch, the clauses an UNWIND consumes) runs as one operator: its rows
// are the operator's one output, reported on the first clause of the stretch.
// The fused clauses have no record of their own, never a zero one.
//
// RowsObserved / DBHitsObserved say whether the collector saw a value at all.
// A record with RowsObserved false is not a zero count, and the statement's
// Unobserved list says what the collector could not observe.

import (
	"context"
	"github.com/orneryd/nornicdb/internal/queryprofile"
	"sync"
	"sync/atomic"
)

// profileRuntimeFrameKey is the context slot holding the record of the clause
// currently executing. Programs the clause starts read it to name their parent,
// and a reader reached while the clause's lazy output is consumed reports hits
// to it.
type profileRuntimeFrameKey struct{}

// profileRuntimeCollector observes one PROFILE statement. It is safe for
// concurrent use: records are looked up under mu, and the counts a clause adds
// while it runs are atomic, so a clause that reads or writes concurrently does
// not contend on a lock per row.
type profileRuntimeCollector struct {
	mu      sync.Mutex
	records map[profileRuntimeRecordKey]*profileRuntimeRecord
	// order lists the records in the order their clauses first ran.
	order       []*profileRuntimeRecord
	accesses    map[profileAccessKey]*profileAccessRecord
	accessOrder []*profileAccessRecord
	// unobserved lists what this collector could not observe, in the order it
	// learned it. Each reason is recorded once.
	unobserved []string
	// finalRows is the row count of the statement's actual returned result.
	// finalRowsObserved says whether the collector saw that count: the writer
	// is profileRuntimeFinishStatement alone.
	finalRows         int64
	finalRowsObserved bool
	// Both source-program caches belong to this observation and disappear with
	// it. The shared typed cache owns cancellation and publication; value and
	// predicate programs retain their own canonical compiler contracts.
	programs          profileProgramCache[*rowValueProgram]
	predicatePrograms profileProgramCache[*rowPredicateProgram]
}

// profileRuntimeRecordKey identifies one executing clause. Two clauses of the
// same program remain distinct by retained source span; re-entrant executions
// of one retained clause aggregate into its own record. occurrence is the
// source occurrence of the subquery body a record runs inside, when the
// expression that wrote it reported one: two written occurrences of one body
// are two operators, and one occurrence stays one however many rows it runs
// for.
type profileRuntimeRecordKey struct {
	parent     int64
	kind       pipelineClauseKind
	text       string
	start      int
	end        int
	operator   string
	ordinal    int
	occurrence int
}

// profileRuntimeRecord is what one executing clause has handed on so far.
type profileRuntimeRecord struct {
	id     int64
	parent int64
	kind   pipelineClauseKind
	// source is the clause text that executed.
	source   string
	operator string
	ordinal  int

	rows               atomic.Int64
	rowsObserved       atomic.Bool
	dbHits             atomic.Int64
	dbHitsObserved     atomic.Bool
	executions         atomic.Int64
	nodeFilterObserved atomic.Bool
}

// profileRuntimeClauseObservation is one clause record as the statement left
// it.
type profileRuntimeClauseObservation struct {
	// ID names the record; Parent is the ID of the clause whose execution
	// started the program this clause ran in, 0 for the statement's own
	// program.
	ID     int64
	Parent int64
	Kind   pipelineClauseKind
	// Source is the clause text that executed, exactly as the pipeline ran it.
	Source string
	// Rows is the rows the clause handed on and Executions the number of times
	// it ran; RowsObserved is false when the collector did not see a count
	// (never a zero count).
	Rows         int64
	RowsObserved bool
	Executions   int64
	// DBHits is what canonical readers reported for this clause while it ran;
	// DBHitsObserved is false when no reader reported any.
	DBHits             int64
	DBHitsObserved     bool
	NodeFilterObserved bool
	Operator           string
	Ordinal            int
}

// profileRuntimeObservation is what one PROFILE statement's pipeline reported.
type profileRuntimeObservation struct {
	// FinalRows is the row count of the statement's actual returned result and
	// FinalRowsObserved whether the collector saw it.
	FinalRows         int64
	FinalRowsObserved bool
	// Clauses holds one record per executing clause, in execution order.
	Clauses []profileRuntimeClauseObservation
	// Unobserved lists what the collector could not observe. It is empty only
	// when every part of the statement the pipeline can report was observed.
	Unobserved []string
}

// newProfileRuntimeCollector returns the collector PROFILE's owner installs for
// one statement execution.
func newProfileRuntimeCollector() *profileRuntimeCollector {
	return &profileRuntimeCollector{records: make(map[profileRuntimeRecordKey]*profileRuntimeRecord)}
}

// withProfileRuntimeCollector returns ctx carrying collector for the statement
// and every program it runs.
func withProfileRuntimeCollector(ctx context.Context, collector *profileRuntimeCollector) context.Context {
	if collector == nil {
		return ctx
	}
	return queryprofile.WithObserver(ctx, collector)
}

// profileRuntimeFromContext returns the statement's collector, or nil when the
// statement is not a PROFILE statement.
func profileRuntimeFromContext(ctx context.Context) *profileRuntimeCollector {
	collector, _ := queryprofile.FromContext(ctx).(*profileRuntimeCollector)
	return collector
}

// profileRuntimeFrameFromContext returns the record of the clause executing in
// ctx, when a clause is.
func profileRuntimeFrameFromContext(ctx context.Context) (*profileRuntimeRecord, bool) {
	record, ok := ctx.Value(profileRuntimeFrameKey{}).(*profileRuntimeRecord)
	return record, ok && record != nil
}

// profileRuntimeProgramParent returns the parent a program entered with ctx
// reports: the record of the clause that started it, or 0 for a statement's own
// program.
func profileRuntimeProgramParent(ctx context.Context) int64 {
	record, ok := profileRuntimeFrameFromContext(ctx)
	if !ok {
		return 0
	}
	return record.id
}

// profileRuntimeClauseContext returns ctx carrying the record of clause, which
// is about to execute in the program ctx belongs to. Without a collector it
// returns ctx unchanged: a statement nobody profiles gets no frame and no
// allocation.
func profileRuntimeClauseContext(ctx context.Context, collector *profileRuntimeCollector, parent int64, clause pipelineClause) context.Context {
	if collector == nil {
		return ctx
	}
	record := collector.record(parent, clause, profileRuntimeSubqueryOccurrence(ctx))
	record.executions.Add(1)
	return context.WithValue(ctx, profileRuntimeFrameKey{}, record)
}

// profileRuntimeClauseOutputSource wraps the lazy row source a clause installs
// as its output, so each row it actually yields counts once against that
// clause. Without a collector, or outside a clause, source is returned
// unwrapped.
func profileRuntimeClauseOutputSource(ctx context.Context, collector *profileRuntimeCollector, source pipelineRowSource) pipelineRowSource {
	if collector == nil || source == nil {
		return source
	}
	record, ok := profileRuntimeFrameFromContext(ctx)
	if !ok {
		return source
	}
	return func(yield func(pipelineRow) bool) bool {
		// Even an empty source is an observed zero once consumed. Merely
		// installing a lazy source does not establish that it executed.
		record.rowsObserved.Store(true)
		return source(func(row pipelineRow) bool {
			record.rows.Add(1)
			return yield(row)
		})
	}
}

// profileRuntimeClauseOutputRows records the rows an eager clause handed on.
func profileRuntimeClauseOutputRows(ctx context.Context, collector *profileRuntimeCollector, rows int) {
	if collector == nil {
		return
	}
	record, ok := profileRuntimeFrameFromContext(ctx)
	if !ok {
		return
	}
	record.rows.Add(int64(rows))
	record.rowsObserved.Store(true)
}

// profileRuntimeRecordHits adds the hits a canonical reader performed for the
// clause executing in ctx. A reader reached from a clause's lazy output passes
// the context that source captured, so the hits land on the clause that
// installed the source. Without a collector this is a branch.
func profileRuntimeRecordHits(ctx context.Context, hits int64) {
	record, ok := profileRuntimeFrameFromContext(ctx)
	if !ok {
		return
	}
	record.dbHits.Add(hits)
	record.dbHitsObserved.Store(true)
}

// profileRuntimeRecordPredicate marks a MATCH filter when its canonical
// predicate evaluator actually runs, including compiled binding predicates.
// Index admission that needs no residual predicate never calls this owner.
func profileRuntimeRecordPredicate(ctx context.Context) {
	if frame, ok := profileRuntimeFrameFromContext(ctx); ok && frame.operator == "" && (frame.kind == pipelineClauseMatch || frame.kind == pipelineClauseOptionalMatch) {
		frame.nodeFilterObserved.Store(true)
	}
}

// profileRuntimeClauseRunsRows reports whether the clause row pipeline executes
// a clause of this kind itself. A catalog command and a LOAD CSV clause are
// never executed here: the splitter declines a statement that carries one.
func profileRuntimeClauseRunsRows(kind pipelineClauseKind) bool {
	switch kind {
	case pipelineClauseLoadCSV, pipelineClauseCommand:
		return false
	}
	return true
}

// record returns the record of clause executing under parent, creating it the
// first time that clause executes.
func (c *profileRuntimeCollector) record(parent int64, clause pipelineClause, occurrence int) *profileRuntimeRecord {
	key := profileRuntimeRecordKey{parent: parent, kind: clause.kind, text: clause.text, occurrence: occurrence}
	if clause.syntax.Valid() {
		key.text = clause.syntax.Source()
		key.start, key.end = clause.syntax.Span()
	}
	return c.executionRecord(key, clause.text)
}

// executionRecord is the shared identity/allocation owner for admitted clause
// and invoked-program operators. Invocation ordinals keep parallel identical
// UNION branches distinct without inventing parser clause kinds.
func (c *profileRuntimeCollector) executionRecord(key profileRuntimeRecordKey, source string) *profileRuntimeRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	if record, ok := c.records[key]; ok {
		return record
	}
	record := &profileRuntimeRecord{id: int64(len(c.order)) + 1, parent: key.parent, kind: key.kind, source: source, operator: key.operator, ordinal: key.ordinal}
	c.records[key] = record
	c.order = append(c.order, record)
	return record
}

func profileRuntimeInvocation(ctx context.Context, operator, source string, ordinal int) (context.Context, *profileRuntimeRecord) {
	c := profileRuntimeFromContext(ctx)
	if c == nil {
		return ctx, nil
	}
	key := profileRuntimeRecordKey{parent: profileRuntimeProgramParent(ctx), operator: operator, text: source, ordinal: ordinal, occurrence: profileRuntimeSubqueryOccurrence(ctx)}
	record := c.executionRecord(key, source)
	record.executions.Add(1)
	return context.WithValue(ctx, profileRuntimeFrameKey{}, record), record
}

func (record *profileRuntimeRecord) invocationRows(rows int) {
	if record != nil {
		record.rows.Add(int64(rows))
		record.rowsObserved.Store(true)
	}
}

// noteUnobserved records one thing this collector could not observe.
func (c *profileRuntimeCollector) noteUnobserved(reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, existing := range c.unobserved {
		if existing == reason {
			return
		}
	}
	c.unobserved = append(c.unobserved, reason)
}

// observeFinalRows records the row count of the statement's returned result.
func (c *profileRuntimeCollector) observeFinalRows(rows int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.finalRows = int64(rows)
	c.finalRowsObserved = true
}

// observation returns what the statement has reported so far.
func (c *profileRuntimeCollector) observation() profileRuntimeObservation {
	c.mu.Lock()
	defer c.mu.Unlock()
	observation := profileRuntimeObservation{
		FinalRows:         c.finalRows,
		FinalRowsObserved: c.finalRowsObserved,
		Clauses:           make([]profileRuntimeClauseObservation, 0, len(c.order)),
		Unobserved:        append([]string(nil), c.unobserved...),
	}
	for _, record := range c.order {
		observation.Clauses = append(observation.Clauses, profileRuntimeClauseObservation{
			ID:                 record.id,
			Parent:             record.parent,
			Kind:               record.kind,
			Source:             record.source,
			Rows:               record.rows.Load(),
			RowsObserved:       record.rowsObserved.Load(),
			Executions:         record.executions.Load(),
			DBHits:             record.dbHits.Load(),
			DBHitsObserved:     record.dbHitsObserved.Load(),
			NodeFilterObserved: record.nodeFilterObserved.Load(),
			Operator:           record.operator, Ordinal: record.ordinal,
		})
	}
	return observation
}

// profileRuntimeFinishStatement records, once per statement, what the pipeline
// outcome means for the rows the client receives. It is the only writer of the
// statement's final row count, and it reports the routes whose per-clause rows
// the row pipeline could not observe instead of leaving them looking like zero.
func profileRuntimeFinishStatement(ctx context.Context, collector *profileRuntimeCollector, clauseRowsRan bool, outcome pipelineDispatchOutcome) {
	if collector == nil {
		return
	}
	if _, nested := profileRuntimeFrameFromContext(ctx); nested {
		// A statement a clause evaluated is that clause's row value: the clause
		// observed the rows it produced.
		return
	}
	if !outcome.terminal() {
		collector.noteUnobserved("the statement did not run as a clause pipeline, so its clauses and rows were not observed")
		return
	}
	if outcome.err != nil {
		collector.noteUnobserved("the statement failed, so it returned no rows")
		return
	}
	if !clauseRowsRan && outcome.state != pipelineDispatchHandledObserved {
		collector.noteUnobserved("the statement ran a physical or placeholder route outside the clause row pipeline, so its per-clause rows were not observed")
		collector.UnobservedRead(ctx, "physical route database reads", "")
	}
	if outcome.result == nil {
		collector.noteUnobserved("the pipeline returned no result")
		return
	}
	if stream, _ := ctx.Value(armedResultStreamKey{}).(*ResultStream); stream != nil && stream.streamed() && outcome.result.Rows == nil {
		for _, clause := range collector.observation().Clauses {
			if clause.Parent == 0 && clause.Kind == pipelineClauseReturn && clause.RowsObserved {
				collector.observeFinalRows(int(clause.Rows))
				return
			}
		}
		collector.noteUnobserved("the streamed statement has no observed terminal RETURN")
		return
	}
	collector.observeFinalRows(len(outcome.result.Rows))
}
