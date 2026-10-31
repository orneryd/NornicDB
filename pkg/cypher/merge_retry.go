// Author: Claude, NOR-180 one MERGE retry classification owner, 2026-10-30.
// The policy and its rules (#657, #700, #961) are unchanged; they moved here
// from the Bolt session so every route's transaction applies the same one.
package cypher

import (
	"context"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// mergeRetryStatements is what a transaction keeps of what its statements did, to
// decide whether a UNIQUE violation, at COMMIT or at the statement that broke the
// constraint, is a retry-safe MERGE race. Only a statement with a MERGE clause is
// kept, as the terminal that ran it received it, with its effective parameters; the
// rule reads them if a failure needs it.
//
// Any other statement is harmless only if the storage transaction shows it did
// nothing: its effect serial didn't move while it ran, nested bodies included. So no
// later registration, declaration or reading of its text can change what it did.
// Its text can only make it harder: one the grammar doesn't see as a read makes the
// transaction mixed even if it wrote nothing. So does a client statement that ended
// without executing through a terminal, an effect outside any statement, or a
// participant whose effects can't be observed. A mixed transaction is never
// retry-safe. A transaction holds a pointer that stays nil until its first statement
// runs; mixed transactions share mixedMergeRetry.
type mergeRetryStatements struct {
	statements []CommitStatement
	// executed counts the statements that reached a terminal.
	executed int
	// accounted is the effect serial up to which every effect belongs to a
	// statement already judged.
	accounted uint64
	// companionRunning is set while the statement last recorded, without MERGE, has
	// not been settled.
	companionRunning bool
	// cypher25 holds the statements accepted as Cypher 25; the statement text
	// no longer carries its language version.
	cypher25 map[int]struct{}
	// firstStatements backs statements, so that a short transaction allocates once.
	firstStatements [2]CommitStatement
}

// mixedMergeRetry is the shared, never modified record of every mixed
// transaction.
var mixedMergeRetry = &mergeRetryStatements{}

// mergeRetryStructure reads the structure of a statement as executed. A cancelled
// request reads the same structure.
func mergeRetryStructure(ctx context.Context, accepted string) (cypherantlr.StatementSyntax, error) {
	if ctx.Err() != nil {
		ctx = context.WithoutCancel(ctx)
	}
	_, body := parseExecutionMode(accepted)
	// The same parse as readStatementExecutionContract on this text, so the same
	// cache entry.
	cypher25 := cypherVersionFromContext(ctx) == "25"
	return cypherantlr.ParseSyntax(ctx, body, cypherantlr.FragmentEntry, cypher25, cypherantlr.AcceptedSource)
}

// recorded counts the statements that reached a terminal so far.
func (s *mergeRetryStatements) recorded() int {
	if s == nil {
		return 0
	}
	return s.executed
}

// mergeRetryEffects reads the transaction's effect serial, summed over the local
// participants of a composite transaction. A participant it can't observe, such as a
// remote one, reports false.
func (tx *TransactionContext) mergeRetryEffects() (uint64, bool) {
	switch owned := tx.tx.(type) {
	case *storage.BadgerTransaction:
		return owned.EffectSerial(), true
	case *fabric.FabricTransaction:
		var total uint64
		for _, name := range owned.Participants() {
			participant := tx.fabricLocalTxExec[name]
			if participant == nil || participant.txContext == nil {
				return 0, false
			}
			effects, ok := participant.txContext.mergeRetryEffects()
			if !ok {
				return 0, false
			}
			total += effects
		}
		return total, true
	}
	return 0, false
}

// settleMergeRetry judges the statement last recorded by what it did: a statement
// without MERGE that moved the effect serial makes the transaction mixed.
func (tx *TransactionContext) settleMergeRetry() {
	record := tx.mergeRetry
	if record == nil || record == mixedMergeRetry {
		return
	}
	effects, observed := tx.mergeRetryEffects()
	if !observed || (record.companionRunning && effects != record.accounted) {
		tx.mergeRetry = mixedMergeRetry
		return
	}
	record.accounted = effects
	record.companionRunning = false
}

// recordMergeRetryExecution records the statement an execution terminal is about
// to run, with the parameters execution reads from ctx. tx is nil for a statement
// that isn't the transaction's own.
func recordMergeRetryExecution(ctx context.Context, tx *TransactionContext, cypher string) {
	if tx == nil {
		return
	}
	tx.settleMergeRetry()
	statement, _ := stripWithEmbeddingSuffix(cypher)
	kept := tx.mergeRetry.kept()
	first := tx.mergeRetry == nil
	recordMergeRetry(ctx, &tx.mergeRetry, statement, getParamsFromContext(ctx))
	record := tx.mergeRetry
	if record == mixedMergeRetry {
		return
	}
	effects, observed := tx.mergeRetryEffects()
	if first {
		// What opening the transaction set, such as a client's metadata at BEGIN,
		// is no statement's; a client statement before this one made it mixed.
		record.accounted = effects
	}
	// Anything done since the last statement was settled belongs to no statement.
	if !observed || effects != record.accounted {
		tx.mergeRetry = mixedMergeRetry
		return
	}
	record.companionRunning = len(record.statements) == kept
}

// kept counts the MERGE statements kept so far.
func (s *mergeRetryStatements) kept() int {
	if s == nil {
		return 0
	}
	return len(s.statements)
}

// recordMergeRetry adds one executed statement to the record *retry.
func recordMergeRetry(ctx context.Context, retry **mergeRetryStatements, accepted string, params map[string]interface{}) {
	if *retry == mixedMergeRetry {
		return
	}
	if *retry == nil {
		*retry = &mergeRetryStatements{}
		(*retry).statements = (*retry).firstStatements[:0]
	}
	record := *retry
	structure, err := mergeRetryStructure(ctx, accepted)
	if err != nil {
		*retry = mixedMergeRetry
		return
	}
	effects := structure.Effects()
	record.executed++
	if effects.Merges() {
		if cypherVersionFromContext(ctx) == "25" {
			if record.cypher25 == nil {
				record.cypher25 = make(map[int]struct{})
			}
			record.cypher25[len(record.statements)] = struct{}{}
		}
		record.statements = append(record.statements, CommitStatement{Query: accepted, Params: params})
		return
	}
	if !effects.ReadOnly(func(cypherantlr.StatementCall) bool { return true }) {
		*retry = mixedMergeRetry
	}
}

// classifyMergeRetry is the one MERGE retry classification, for implicit and
// explicit transactions on every protocol. err is marked as a retry-safe MERGE
// commit race (nornicerrors.MarkMergeCommitTimeUniqueConflict) when the record isn't
// mixed, so that every statement without MERGE did nothing, and every statement with
// MERGE is MERGE-shaped (IsRetrySafeMergeCommitQuery), calls no procedure, and no SET
// in it wrote a violated property (MergeUniqueConflictIsRetrySafe). Any other err,
// and an already-frozen terminal outcome, is returned unchanged.
func classifyMergeRetry(ctx context.Context, analyzer *QueryAnalyzer, s *mergeRetryStatements, err error) error {
	if err == nil || s == nil || s == mixedMergeRetry || len(s.statements) == 0 {
		return err
	}
	// The decision reads statement text only. Cancellation must neither change
	// it nor be recorded by it; the outer flow reports a cancelled request.
	ctx = context.WithoutCancel(ctx)
	for index, statement := range s.statements {
		version := "5"
		if _, ok := s.cypher25[index]; ok {
			version = "25"
		}
		accepted := context.WithValue(ctx, cypherVersionKey{}, version)
		structure, structureErr := mergeRetryStructure(accepted, statement.Query)
		if structureErr != nil || structure.Effects().CallCount() > 0 ||
			!isRetrySafeMergeCommitQuery(accepted, analyzer.Analyze(accepted, statement.Query)) {
			return err
		}
	}
	if !mergeUniqueConflictIsRetrySafe(ctx, s.statements, err) {
		return err
	}
	return nornicerrors.MarkMergeCommitTimeUniqueConflict(err)
}
