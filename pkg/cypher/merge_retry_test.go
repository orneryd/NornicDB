// Author: Claude, NOR-180, 2026-10-30. These cases moved unchanged from the
// Bolt session tests (#657, #961) when the MERGE retry classification moved
// to the transaction (classifyMergeRetry).
package cypher

import (
	"context"
	stderrors "errors"
	"fmt"
	"strings"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// uidUniqueViolation is a commit-time UNIQUE violation on TerraformResource.uid.
var uidUniqueViolation = fmt.Errorf("commit failed: constraint violation: %w", &storage.ConstraintViolationError{
	Type:       storage.ConstraintUnique,
	Label:      "TerraformResource",
	Properties: []string{"uid"},
	Message:    "Node with uid=X already exists (nodeID: nornic:abc)",
})

func mergeRetrySafe(statements ...CommitStatement) bool {
	analyzer := NewQueryAnalyzer(16)
	var recorded *mergeRetryStatements
	for _, statement := range statements {
		recordMergeRetry(context.Background(), &recorded, statement.Query, statement.Params)
	}
	err := classifyMergeRetry(context.Background(), analyzer, recorded, uidUniqueViolation)
	return stderrors.Is(err, nornicerrors.ErrMergeCommitTimeUniqueConflict)
}

// TestMergeRetryRequiresMergeKeyClash covers #657: only a clash on the MERGE
// key is a retry-safe race; a SET writing another value to the constrained
// property fails on every retry, and what can't be decided is not retried.
func TestMergeRetryRequiresMergeKeyClash(t *testing.T) {
	props := map[string]any{"uid": "X", "props": map[string]any{"name": "n"}, "withUID": map[string]any{"uid": "X"}, "otherUID": map[string]any{"uid": "Y"}}
	for query, retrySafe := range map[string]bool{
		"MERGE (n:TerraformResource {uid: $uid}) SET n.name = $uid":          true,
		"MERGE (n:TerraformResource {uid: $uid}) SET n += $props":            true,
		"MERGE (n:TerraformResource {uid: $uid}) SET n += $withUID":          true,
		"MERGE (n:TerraformResource {uid: $uid}) SET n.uid = 'X'":            true,
		"MERGE (n:TerraformResource {uid: $uid}) SET n += $otherUID":         false,
		"MERGE (n:TerraformResource {uid: $uid}) SET n += $missing":          false,
		"MERGE (n:TerraformResource {uid: $uid}) SET n = m":                  false,
		"MERGE (n:TerraformResource {name: $uid}) SET n.uid = $uid":          false,
		"MERGE (n:TerraformResource {name: 'a'}) SET n += {uid: 'x'}":        false,
		"MERGE (n:TerraformResource {name: 'a'}) ON CREATE SET n.uid = 'x'":  false,
		"MERGE (n:TerraformResource {name: 'a'}) ON MATCH SET n.`uid` = 'x'": false,
		"MERGE (n:TerraformResource {uid: $uid}) SET n.uid = toUpper($uid)":  false,
	} {
		if got := mergeRetrySafe(CommitStatement{Query: query, Params: props}); got != retrySafe {
			t.Fatalf("%s: retry-safe = %v, want %v", query, got, retrySafe)
		}
	}
}

func TestMergeRetryRequiresMergeOnlyTransaction(t *testing.T) {
	merge := CommitStatement{Query: "MERGE (n:TerraformResource {uid: $uid})"}
	if !mergeRetrySafe(merge) {
		t.Fatal("MERGE-only transaction should allow commit-time UNIQUE conflict retry classification")
	}
	if mergeRetrySafe(merge, CommitStatement{Query: "CREATE (n:TerraformResource {uid: $uid})"}) {
		t.Fatal("mixed MERGE and non-MERGE write transaction should keep commit-time UNIQUE conflict as a hard commit failure")
	}
	// A statement that writes nothing (a read, a transaction command) does
	// not make the transaction mixed.
	if !mergeRetrySafe(merge, CommitStatement{Query: "MATCH (n:TerraformResource) RETURN count(n)"}) {
		t.Fatal("a read in a MERGE-only transaction should not prevent retry classification")
	}
}

func TestMergeRetryRejectsMixedSingleStatementWrite(t *testing.T) {
	if mergeRetrySafe(CommitStatement{Query: "CREATE (a:Audit {id: $id}) WITH a MERGE (n:TerraformResource {uid: $uid}) SET n.auditId = a.id"}) {
		t.Fatal("single statement mixing CREATE and MERGE should keep commit-time UNIQUE conflict as a hard commit failure")
	}
}

func TestMergeRetryRejectsCallWithMerge(t *testing.T) {
	if mergeRetrySafe(CommitStatement{Query: "CALL custom.writeProc() YIELD value MERGE (n:TerraformResource {uid: value.uid}) RETURN n"}) {
		t.Fatal("statement mixing CALL and MERGE should keep commit-time UNIQUE conflict as a hard commit failure")
	}
}

// Whether a companion statement writes comes from the execution contract: reads
// and transaction commands leave the MERGE retry-safe; anything that writes, or
// changes schema, does not.
func TestMergeRetryClassifiesCompanionStatements(t *testing.T) {
	merge := CommitStatement{Query: "MERGE (n:TerraformResource {uid: $uid})", Params: map[string]any{"uid": "X"}}
	for query, retrySafe := range map[string]bool{
		"RETURN 1":                        true,
		"MATCH (n) RETURN n":              true,
		"CREATE (:Other)":                 false,
		"MATCH (n) SET n.seen = true":     false,
		"MATCH (n) DELETE n":              false,
		"CREATE INDEX FOR (n:A) ON (n.b)": false,
		"DROP INDEX missing IF EXISTS":    false,
	} {
		t.Run(query, func(t *testing.T) {
			if got := mergeRetrySafe(merge, CommitStatement{Query: query}); got != retrySafe {
				t.Fatalf("retry-safe = %v, want %v", got, retrySafe)
			}
		})
	}
}

// Retention follows the statement's structure, so no query content can reach the
// parameter-keeping population or get past the limits. Past them the transaction
// is mixed, so a retry is never decided without every statement it ran.
func TestMergeRetryBoundsRetainedStatements(t *testing.T) {
	merge := "MERGE (n:TerraformResource {uid: $uid})"
	mergeParams := map[string]any{"uid": "X"}
	retrySafeAfter := func(reads ...string) (bool, *mergeRetryStatements) {
		analyzer := NewQueryAnalyzer(16)
		var recorded *mergeRetryStatements
		recordMergeRetry(context.Background(), &recorded, merge, mergeParams)
		for _, read := range reads {
			recordMergeRetry(context.Background(), &recorded, read, map[string]any{"payload": map[string]any{"large": true}})
		}
		err := classifyMergeRetry(context.Background(), analyzer, recorded, uidUniqueViolation)
		return stderrors.Is(err, nornicerrors.ErrMergeCommitTimeUniqueConflict), recorded
	}
	for _, read := range []string{
		"RETURN 1",
		"RETURN $payload AS payload, 'MERGE' AS operation",
		"RETURN $payload AS payload, 'CALL' AS operation",
		"RETURN $payload AS `MERGE`",
		"RETURN $payload AS `CALL`",
		"MATCH (n:`MERGE`) RETURN n",
		"MATCH (n) WHERE n.name = 'CALL db.labels()' RETURN n",
	} {
		t.Run(read, func(t *testing.T) {
			reads := make([]string, mergeRetryOtherLimit)
			for i := range reads {
				reads[i] = read
			}
			safe, recorded := retrySafeAfter(reads...)
			if !safe || recorded.others != mergeRetryOtherLimit || len(recorded.statements) != mergeRetryOtherLimit+1 {
				t.Fatalf("at the limit: retry-safe %v, others %d, statements %d", safe, recorded.others, len(recorded.statements))
			}
			for _, statement := range recorded.statements[1:] {
				if statement.Params != nil {
					t.Fatal("a statement without MERGE or CALL structure kept its parameters")
				}
			}
			if safe, recorded = retrySafeAfter(append(reads, read)...); safe || recorded != mixedMergeRetry {
				t.Fatalf("past the statement limit: retry-safe %v, mixed %v", safe, recorded == mixedMergeRetry)
			}
		})
	}
	long := "RETURN '" + strings.Repeat("x", mergeRetryOtherBytes) + "'"
	if safe, recorded := retrySafeAfter(long); safe || recorded != mixedMergeRetry {
		t.Fatalf("past the byte limit: retry-safe %v, mixed %v", safe, recorded == mixedMergeRetry)
	}
}

// Every companion answers to the same rule, whichever way it is retained.
func TestMergeRetryJudgesEveryCompanionAlike(t *testing.T) {
	merge := CommitStatement{Query: "MERGE (n:TerraformResource {uid: $uid})", Params: map[string]any{"uid": "X"}}
	for query, retrySafe := range map[string]bool{
		"CALL db.clearQueryCaches()":                 false,
		"CALL db.labels() YIELD label RETURN label":  true,
		"MATCH (n) SET n.note = 'MERGE'":             false,
		"MATCH (n) SET n.`MERGE` = 1":                false,
		"CREATE (:Other {note: 'MERGE'})":            false,
		"RETURN 'CALL db.clearQueryCaches()' AS txt": true,
	} {
		t.Run(query, func(t *testing.T) {
			if got := mergeRetrySafe(merge, CommitStatement{Query: query}); got != retrySafe {
				t.Fatalf("retry-safe = %v, want %v", got, retrySafe)
			}
		})
	}
}

// The decision reads under the failing request: once that request is cancelled
// the clauses can't be read, and what can't be decided is not retried.
func TestMergeRetryFailsClosedOnCancelledRequest(t *testing.T) {
	analyzer := NewQueryAnalyzer(16)
	var recorded *mergeRetryStatements
	recordMergeRetry(context.Background(), &recorded, "MERGE (n:TerraformResource {uid: $uid})", map[string]any{"uid": "X"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := classifyMergeRetry(ctx, analyzer, recorded, uidUniqueViolation); stderrors.Is(err, nornicerrors.ErrMergeCommitTimeUniqueConflict) {
		t.Fatal("a cancelled request was classified as a retry-safe race")
	}
}

func TestMergeRetryKeepsOtherFailures(t *testing.T) {
	other := stderrors.New("disk full")
	analyzer := NewQueryAnalyzer(16)
	var recorded *mergeRetryStatements
	recordMergeRetry(context.Background(), &recorded, "MERGE (n:TerraformResource {uid: $uid})", nil)
	if got := classifyMergeRetry(context.Background(), analyzer, recorded, other); got != other {
		t.Fatalf("a failure that is not a UNIQUE violation must be returned unchanged, got %v", got)
	}
}
