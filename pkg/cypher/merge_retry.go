// Author: Claude, NOR-180 one MERGE retry classification owner, 2026-10-30.
// The policy and its rules (#657, #700, #961) are unchanged; they moved here
// from the Bolt session so every route's transaction applies the same one.
package cypher

import (
	"context"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
)

// mergeRetryStatements is what a transaction keeps of the statements it ran
// (or attempted) to decide whether a UNIQUE violation, at COMMIT or at the
// statement that broke the constraint, is a retry-safe MERGE race. Whether a
// statement writes is decided only if a failure needs it, so a transaction that
// succeeds pays for no classification. A statement whose structure has a MERGE
// clause or a procedure call keeps its parameters, which the rule reads. Any
// other keeps only its text, within mergeRetryOtherLimit and
// mergeRetryOtherBytes; past either the transaction counts as mixed, which is
// never retry-safe, and its record is dropped.
// A transaction holds a pointer that stays nil until its first statement runs;
// mixed transactions share mixedMergeRetry.
type mergeRetryStatements struct {
	mixed      bool
	statements []CommitStatement
	others     int
	otherBytes int
	// firstStatements backs statements, so that a short transaction allocates once.
	firstStatements [2]CommitStatement
}

const (
	mergeRetryOtherLimit = 1024
	mergeRetryOtherBytes = 1 << 20
)

// mixedMergeRetry is the shared, never modified record of every mixed
// transaction.
var mixedMergeRetry = &mergeRetryStatements{mixed: true}

// mergeRetryShaped reports whether the statement's structure has a MERGE clause
// or a procedure call. A statement that doesn't parse has neither.
func mergeRetryShaped(ctx context.Context, query string) bool {
	_, body := parseExecutionMode(query)
	structure, err := readStatementStructure(ctx, body)
	if err != nil {
		return false
	}
	effects := structure.Effects()
	return effects.Merges() || effects.CallCount() > 0
}

// recordMergeRetry adds one statement to the record *retry.
func recordMergeRetry(ctx context.Context, retry **mergeRetryStatements, query string, params map[string]interface{}) {
	if *retry == mixedMergeRetry {
		return
	}
	if *retry == nil {
		*retry = &mergeRetryStatements{}
		(*retry).statements = (*retry).firstStatements[:0]
	}
	record := *retry
	if mergeRetryShaped(ctx, query) {
		record.statements = append(record.statements, CommitStatement{Query: query, Params: params})
		return
	}
	if record.others == mergeRetryOtherLimit || record.otherBytes+len(query) > mergeRetryOtherBytes {
		*retry = mixedMergeRetry
		return
	}
	record.others++
	record.otherBytes += len(query)
	record.statements = append(record.statements, CommitStatement{Query: query})
}

// classifyMergeRetry is the one MERGE retry classification, for implicit and
// explicit transactions on every protocol. err is marked as a retry-safe MERGE
// commit race (nornicerrors.MarkMergeCommitTimeUniqueConflict) when every
// statement that writes is MERGE-shaped (IsRetrySafeMergeCommitQuery), none
// needs schema or admin permission, and no SET in them wrote a violated
// property (MergeUniqueConflictIsRetrySafe); any other err, and an
// already-frozen terminal outcome, is returned unchanged. The execution contract
// owns whether a statement writes.
func classifyMergeRetry(ctx context.Context, analyzer *QueryAnalyzer, s *mergeRetryStatements, err error) error {
	if err == nil || s == nil || s.mixed {
		return err
	}
	var merges []CommitStatement
	for _, statement := range s.statements {
		required := QueryPermissionRequirements(ctx, statement.Query, RawSource)
		if !required.Write && !required.Schema && !required.Admin {
			continue
		}
		if required.Schema || required.Admin || !mergeRetryShaped(ctx, statement.Query) ||
			!isRetrySafeMergeCommitQuery(ctx, analyzer.Analyze(ctx, statement.Query)) {
			return err
		}
		merges = append(merges, statement)
	}
	if len(merges) == 0 || !mergeUniqueConflictIsRetrySafe(ctx, merges, err) {
		return err
	}
	return nornicerrors.MarkMergeCommitTimeUniqueConflict(err)
}
