package cypher

import (
	"context"
	"fmt"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

// NOR-81's three original queries, expected verdicts, and 36 parser/transaction/
// execution cells are recovered from the ticket harness retained in OpenSpec.
// Sibling ticket populations are deliberately not introduced by this fix.
type norStaticClaim struct {
	name  string
	query string
	// params are the parameter values supplied with the probe (nil when the
	// reference supplied none).
	params map[string]interface{}
	// accepts is the supplied reference verdict.
	accepts bool
	// messageClass is the leading phrase of Neo4j's rejection message. The exact
	// wording is not asserted: NornicDB may list a union's alternatives
	// differently, as the canonical verdict owner records.
	messageClass string
	// reconstructed marks query text composed from supplied parts because the
	// original was not supplied inline.
	reconstructed bool
}

var nor81StaticClaims = []norStaticClaim{
	{
		name:    "any_over_timestamp",
		query:   "UNWIND [] AS s RETURN any(x IN timestamp() WHERE true) AS a",
		accepts: true,
	},
	{
		// Negative control, supplied as rejected; also the canonical owner's entry.
		name:         "control_list_comprehension_over_date",
		query:        "UNWIND [] AS s RETURN [x IN date() | x] AS a",
		messageClass: "Type mismatch",
	},
	{
		// Negative control, supplied as rejected; also the canonical owner's entry.
		name:         "control_reduce_over_toInteger",
		query:        "UNWIND [] AS s RETURN reduce(a = 0, x IN toInteger('1') | a) AS a",
		messageClass: "Type mismatch",
	},
}

func TestNOR81TimestampSourceOriginalMatrix(t *testing.T) {
	for _, issue := range []struct {
		number string
		claims []norStaticClaim
	}{
		{"NOR-81", nor81StaticClaims},
	} {
		t.Run(issue.number, func(t *testing.T) {
			forEachParser(t, func(t *testing.T) {
				for _, execution := range norStaticExecutions() {
					for _, mode := range norStaticTransactionModes() {
						for _, claim := range issue.claims {
							t.Run(execution.kind+"/"+mode+"/"+claim.name, func(t *testing.T) {
								if claim.reconstructed {
									t.Logf("composed from supplied parts, not supplied inline: %s", claim.query)
								}
								ctx := context.Background()
								exec := norStaticFreshExecutor(t)
								if mode == "tx" {
									_, err := exec.Execute(ctx, "BEGIN", nil)
									require.NoError(t, err, "BEGIN for the explicit-transaction cell")
								}
								result, err := exec.Execute(ctx, execution.prefix+claim.query, claim.params)
								norStaticRollback(t, ctx, exec, mode)
								if err != nil {
									code, message := nornicerrors.Neo4jStatus(err)
									t.Logf("%s: rejected %s: %s", execution.kind, code, message)
								} else {
									t.Logf("%s: accepted", execution.kind)
								}
								if mismatch, detail := norClassifyVerdict(execution.kind, result, err, claim.accepts, claim.messageClass); mismatch != norVerdictMatches {
									t.Errorf("%s: %s: %s", execution.kind, claim.query, detail)
								}
							})
						}
					}
				}
			})
		})
	}
}

func norStaticExecutions() []struct{ kind, prefix string } {
	return []struct{ kind, prefix string }{
		{"RUN", ""},
		{"EXPLAIN", "EXPLAIN "},
		{"PROFILE", "PROFILE "},
	}
}

func norStaticFreshExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor79_to_82_static_claims"))
}

func norStaticRollback(t *testing.T, ctx context.Context, exec *StorageExecutor, mode string) {
	t.Helper()
	if mode != "tx" {
		return
	}
	if _, err := exec.Execute(ctx, "ROLLBACK", nil); err != nil {
		t.Logf("ROLLBACK after the cell failed: %v", err)
	}
}

func norStaticTransactionModes() []string { return []string{"auto", "tx"} }

type norVerdictMismatch int

const (
	// norVerdictMatches is the reference verdict: a supported acceptance, or
	// Neo4j's SyntaxError in the expected message class.
	norVerdictMatches norVerdictMismatch = iota
	// norVerdictUnderRejection is an acceptance where Neo4j rejects.
	norVerdictUnderRejection
	// norVerdictFalseRejection is a rejection where Neo4j accepts.
	norVerdictFalseRejection
	// norVerdictStatusMismatch is a rejection that is not Neo4j's SyntaxError in
	// the expected message class.
	norVerdictStatusMismatch
)

// norClassifyVerdict reads a cell's verdict from the returned error and result -
// the supported acceptance or Neo4j's status, never from logs - and returns how
// it departs from the reference verdict, with a description of the departure.
func norClassifyVerdict(kind string, result *ExecuteResult, err error, accepts bool, messageClass string) (norVerdictMismatch, string) {
	if accepts {
		switch {
		case err != nil:
			return norVerdictFalseRejection, fmt.Sprintf("accepted by Neo4j, rejected here: %v", err)
		case result == nil:
			return norVerdictStatusMismatch, "accepted but returned no result"
		case kind == "RUN" && len(result.Rows) != 0:
			// Every accepted probe starts from UNWIND [], so it can produce no
			// row: acceptance means an empty row set, not a silently filled one.
			return norVerdictStatusMismatch, fmt.Sprintf("accepted with %d rows from an empty input", len(result.Rows))
		}
		return norVerdictMatches, ""
	}
	if err == nil {
		return norVerdictUnderRejection, fmt.Sprintf("rejected by Neo4j (%s), accepted here", messageClass)
	}
	code, message := nornicerrors.Neo4jStatus(err)
	if code != "Neo.ClientError.Statement.SyntaxError" {
		return norVerdictStatusMismatch, fmt.Sprintf("rejected as %s instead of Neo4j's SyntaxError: %s", code, message)
	}
	if messageClass != "" && !strings.HasPrefix(message, messageClass) {
		return norVerdictStatusMismatch, fmt.Sprintf("rejected as %q, not as %s", message, messageClass)
	}
	return norVerdictMatches, ""
}
