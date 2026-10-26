// Author: Claude, NOR-180 one MERGE retry classification owner, 2026-10-30.
// The policy and its rules (#657, #700, #961) are unchanged; they moved here
// from the Bolt session so every route's transaction applies the same one.
package cypher

import (
	"context"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
)

// mergeRetryStatements is what a transaction keeps of the statements it executed,
// as the terminal that ran each one received it, with its effective parameters, to
// decide whether a UNIQUE violation, at COMMIT
// or at the statement that broke the constraint, is a retry-safe MERGE race.
// Whether a statement writes is decided only if a failure needs it, so a
// transaction that succeeds pays for no classification. A statement whose
// structure has a MERGE clause or a procedure call keeps its parameters, which
// the rule reads. Any other keeps only its text, within mergeRetryOtherLimit and
// mergeRetryOtherBytes. Past either, or when a client statement ended without
// executing through a terminal or its structure can't be read, the transaction
// counts as mixed, which is never retry-safe, and its record is dropped.
// A transaction holds a pointer that stays nil until its first statement runs;
// mixed transactions share mixedMergeRetry.
type mergeRetryStatements struct {
	statements []CommitStatement
	others     int
	otherBytes int
	// cypher25 holds the statements accepted as Cypher 25; the statement text
	// no longer carries its language version.
	cypher25 map[int]struct{}
	// firstStatements backs statements, so that a short transaction allocates once.
	firstStatements [2]CommitStatement
}

const (
	mergeRetryOtherLimit = 1024
	mergeRetryOtherBytes = 1 << 20
)

// mixedMergeRetry is the shared, never modified record of every mixed
// transaction.
var mixedMergeRetry = &mergeRetryStatements{}

// mergeRetryShaped reports whether the accepted statement's structure has a
// MERGE clause or a procedure call, and whether that structure could be read at
// all. A cancelled request reads the same structure.
func mergeRetryShaped(ctx context.Context, accepted string) (shaped, known bool) {
	if ctx.Err() != nil {
		ctx = context.WithoutCancel(ctx)
	}
	_, body := parseExecutionMode(accepted)
	// The same parse as readStatementExecutionContract on this text, so the same
	// cache entry.
	cypher25 := cypherVersionFromContext(ctx) == "25"
	structure, err := cypherantlr.ParseSyntax(ctx, body, cypherantlr.FragmentEntry, cypher25, cypherantlr.AcceptedSource)
	if err != nil {
		return false, false
	}
	effects := structure.Effects()
	return effects.Merges() || effects.CallCount() > 0, true
}

// recorded counts the statements kept so far.
func (s *mergeRetryStatements) recorded() int {
	if s == nil {
		return 0
	}
	return len(s.statements)
}

// recordMergeRetryExecution records the statement an execution terminal is about
// to run, with the parameters execution reads from ctx. tx is nil for a statement
// that isn't the transaction's own.
func recordMergeRetryExecution(ctx context.Context, tx *TransactionContext, cypher string) {
	if tx == nil {
		return
	}
	statement, _ := stripWithEmbeddingSuffix(cypher)
	recordMergeRetry(ctx, &tx.mergeRetry, statement, getParamsFromContext(ctx))
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
	shaped, known := mergeRetryShaped(ctx, accepted)
	if !known {
		*retry = mixedMergeRetry
		return
	}
	if cypherVersionFromContext(ctx) == "25" {
		if record.cypher25 == nil {
			record.cypher25 = make(map[int]struct{})
		}
		record.cypher25[len(record.statements)] = struct{}{}
	}
	if shaped {
		record.statements = append(record.statements, CommitStatement{Query: accepted, Params: params})
		return
	}
	if record.others == mergeRetryOtherLimit || record.otherBytes+len(accepted) > mergeRetryOtherBytes {
		*retry = mixedMergeRetry
		return
	}
	record.others++
	record.otherBytes += len(accepted)
	record.statements = append(record.statements, CommitStatement{Query: accepted})
}

// classifyMergeRetry is the one MERGE retry classification, for implicit and
// explicit transactions on every protocol. err is marked as a retry-safe MERGE
// commit race (nornicerrors.MarkMergeCommitTimeUniqueConflict) when every
// statement that writes is MERGE-shaped (IsRetrySafeMergeCommitQuery), none
// needs schema or admin permission, and no SET in them wrote a violated
// property (MergeUniqueConflictIsRetrySafe); any other err, and an
// already-frozen terminal outcome, is returned unchanged. The execution contract
// owns whether a statement writes; one whose contract can't be read is not a
// harmless companion.
func classifyMergeRetry(ctx context.Context, analyzer *QueryAnalyzer, s *mergeRetryStatements, err error) error {
	if err == nil || s == nil || s == mixedMergeRetry {
		return err
	}
	// The decision reads statement text only. Cancellation must neither change
	// it nor be recorded by it; the outer flow reports a cancelled request.
	ctx = context.WithoutCancel(ctx)
	var merges []CommitStatement
	for index, statement := range s.statements {
		version := "5"
		if _, ok := s.cypher25[index]; ok {
			version = "25"
		}
		accepted := context.WithValue(ctx, cypherVersionKey{}, version)
		shaped, known := mergeRetryShaped(accepted, statement.Query)
		if !known {
			return err
		}
		_, body := parseExecutionMode(statement.Query)
		if _, contractErr := readStatementExecutionContract(accepted, body); contractErr != nil {
			return err
		}
		required := QueryPermissionRequirements(accepted, statement.Query, AcceptedSource)
		if !required.Write && !required.Schema && !required.Admin {
			continue
		}
		if required.Schema || required.Admin || !shaped ||
			!isRetrySafeMergeCommitQuery(accepted, analyzer.Analyze(accepted, statement.Query)) {
			return err
		}
		merges = append(merges, statement)
	}
	if len(merges) == 0 || !mergeUniqueConflictIsRetrySafe(ctx, merges, err) {
		return err
	}
	return nornicerrors.MarkMergeCommitTimeUniqueConflict(err)
}
