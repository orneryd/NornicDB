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
// statement that broke the constraint, is a retry-safe MERGE race. A
// statement that can't write is not kept; a write that isn't MERGE makes the
// transaction mixed, which is never retry-safe, so its text is not kept either.
// A transaction holds a pointer that stays nil until a statement that can
// need the rule runs; mixed transactions share mixedMergeRetry.
type mergeRetryStatements struct {
	mixed      bool
	statements []CommitStatement
}

// mixedMergeRetry is the shared, never modified record of every mixed
// transaction.
var mixedMergeRetry = &mergeRetryStatements{mixed: true}

// recordMergeRetry adds one statement to the record *retry, allocating it only
// when the statement can need the rule.
func recordMergeRetry(retry **mergeRetryStatements, analyzer *QueryAnalyzer, query string, params map[string]interface{}) {
	if *retry == mixedMergeRetry {
		return
	}
	info := analyzer.Analyze(context.Background(), query)
	switch {
	case info != nil && !info.HasMerge && !info.HasCall && !info.IsWriteQuery:
		// Writes nothing: a read or a transaction command.
	case info != nil && !info.HasMerge && !info.HasCall:
		*retry = mixedMergeRetry
	default:
		if *retry == nil {
			*retry = &mergeRetryStatements{}
		}
		(*retry).statements = append((*retry).statements, CommitStatement{Query: query, Params: params})
	}
}

// classifyMergeRetry is the one MERGE retry classification, for implicit and
// explicit transactions on every protocol. err is marked as a retry-safe MERGE
// commit race (nornicerrors.MarkMergeCommitTimeUniqueConflict) when every
// statement that writes is MERGE-shaped (IsRetrySafeMergeCommitQuery) and no
// SET in them wrote a violated property (MergeUniqueConflictIsRetrySafe); any
// other err, and an already-frozen terminal outcome, is returned unchanged.
func classifyMergeRetry(ctx context.Context, analyzer *QueryAnalyzer, s *mergeRetryStatements, err error) error {
	if err == nil || s == nil || s.mixed {
		return err
	}
	var merges []CommitStatement
	for _, statement := range s.statements {
		if !QueryPermissionRequirements(ctx, statement.Query, RawSource).Write {
			continue
		}
		if !IsRetrySafeMergeCommitQuery(analyzer.Analyze(context.Background(), statement.Query)) {
			return err
		}
		merges = append(merges, statement)
	}
	if len(merges) == 0 || !MergeUniqueConflictIsRetrySafe(merges, err) {
		return err
	}
	return nornicerrors.MarkMergeCommitTimeUniqueConflict(err)
}
