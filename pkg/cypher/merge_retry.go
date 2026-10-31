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
// succeeds pays for no classification. A statement that isn't MERGE- or
// CALL-shaped keeps only its text, within mergeRetryOtherLimit and
// mergeRetryOtherBytes; past either the transaction counts as mixed, which is
// never retry-safe, and its record is dropped. MERGE- and CALL-shaped statements
// keep their parameters, which the rule reads.
// A transaction holds a pointer that stays nil until its first statement runs;
// mixed transactions share mixedMergeRetry.
type mergeRetryStatements struct {
	mixed      bool
	statements []CommitStatement
	others     []string
	otherBytes int
	// firstOthers backs others, so that a short transaction allocates once.
	firstOthers [2]string
}

const (
	mergeRetryOtherLimit = 1024
	mergeRetryOtherBytes = 1 << 20
)

// mixedMergeRetry is the shared, never modified record of every mixed
// transaction.
var mixedMergeRetry = &mergeRetryStatements{mixed: true}

// recordMergeRetry adds one statement to the record *retry.
func recordMergeRetry(retry **mergeRetryStatements, analyzer *QueryAnalyzer, query string, params map[string]interface{}) {
	if *retry == mixedMergeRetry {
		return
	}
	if *retry == nil {
		*retry = &mergeRetryStatements{}
		(*retry).others = (*retry).firstOthers[:0]
	}
	record := *retry
	if len(record.others) == mergeRetryOtherLimit || record.otherBytes+len(query) > mergeRetryOtherBytes {
		*retry = mixedMergeRetry
		return
	}
	record.others = append(record.others, query)
	record.otherBytes += len(query)
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
	// The execution contract owns whether a statement writes. A schema or admin
	// statement is not MERGE-shaped, so it counts as mixed too.
	for _, other := range s.others {
		if required := QueryPermissionRequirements(ctx, other, RawSource); required.Write || required.Schema || required.Admin {
			return err
		}
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
