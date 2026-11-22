package cypher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// This file is a reproduction matrix for the NOR-79, NOR-80, NOR-81 and NOR-82
// static (compile-time) query claims. It adds no production behavior and changes
// none: every expectation below is the reference verdict supplied with the
// ticket or the canonical owner's recorded Neo4j verdict, so a failing cell is
// the reproduction.
//
// Provenance and source-input gaps (do not widen a claim past these):
//   - Parent reference refresh: the 12 explicit probes are being verified against
//     Neo4j 5.26.30 on Buttocks. The reconstructed [] and n.j NOR-82 variants
//     ACCEPT there; their historical inferred rejection was incorrect. The
//     canonical 258-case population still uses its recorded oracle.
//   - The constructor-family reference verdicts are historical. They were not freshly verified
//     against Neo4j for this file; the rejection class below ("Type mismatch")
//     is Neo4j's message class for the same constructors in the canonical
//     staticTypeNeo4jVerdicts owner, not a newly recorded message.
//   - NOR-79 (linear.app/nornicdb/issue/NOR-79): the acceptance criterion names
//     "the 258 constructor cases and 120 generated parameter cases". The 258 are
//     recovered from the canonical constructor owner (the
//     staticConstructorTypesPopulation population plus the positions its oracle
//     marks as needing parameter-aware admission) and all of them are held to
//     Neo4j's recorded verdict by TestNor79ParameterAwareConstructorClaims. The
//     120 generated parameter cases were not supplied inline, no canonical
//     population of that size exists in the repository, and the ticket keeps its
//     triage findings (D12) in an attachment rather than in the description, so
//     they are not covered and no full-family claim is made.
//   - NOR-80: the ticket's reproduction is the three statements below, including
//     the instruction to replay the CALL-bearing COUNT { CALL { ... } RETURN ... }
//     variant exactly before assuming the shorter form fails too; all three are
//     cells. The comment history (the db.labels() statement accepted, the DELETE
//     statement rejected on the old head) is an observation, not a verdict; the
//     reference expectation for all three statements stays "accepted".
//   - NOR-81: the reproduction and both negative controls are exactly the
//     ticket's. The controls are the canonical matrix's own entries, reused rather
//     than restated (see TestNor79To82NegativeControlsReuseCanonicalVerdicts).
//   - NOR-82 (linear.app/nornicdb/issue/NOR-82): the reproduction is exactly the
//     ticket's; the family additions duration('P1D'), [] and n.j are the ticket's
//     own list, but it gives them as additions only, so each variant below
//     substitutes one supplied addition into the exactly supplied probe and is
//     marked reconstructed, not recovered. The 64 inherited H2 false accepts the
//     ticket's acceptance criterion names were not supplied inline and no
//     canonical population of them exists in the repository, so they are not
//     covered and no string-consumer generalization is drawn from one message.
//
// Every cell runs on a fresh storage engine and executor, in both parsers the
// package's forEachParser owner selects, in auto-commit and in an explicit
// transaction, for RUN, EXPLAIN and PROFILE: Neo4j's static verdict is a
// compile-time verdict, so it applies to the statement before it runs and to the
// plan of it. A rejected statement aborts an explicit transaction, so a shared
// fixture would carry that state into the next cell.
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

var nor79StaticClaims = []norStaticClaim{
	{
		// The parameter's value makes "$i * 2" an Integer, so toUpper has no
		// String argument: Neo4j rejects at compile time.
		name:         "parameter_arithmetic_into_toUpper",
		query:        "UNWIND [] AS r MATCH (n) WITH n, $i * 2 AS w RETURN toUpper(w) AS a",
		params:       map[string]interface{}{"i": int64(5)},
		messageClass: "Type mismatch",
	},
	{
		// The reverse probe: Neo4j accepts the list comprehension and the
		// subscript. This is also index 9402 of the canonical constructor
		// population, the one parameter-aware case Neo4j accepts.
		name:    "parameter_list_comprehension_subscript",
		query:   "UNWIND [] AS r MATCH (n) WITH n, [x IN $f | x] AS v RETURN v[0] AS a",
		params:  map[string]interface{}{"f": 1.5},
		accepts: true,
	},
}

var nor80StaticClaims = []norStaticClaim{
	{
		name:    "procedure_yield_where_count_subquery",
		query:   "UNWIND [] AS s CALL db.labels() YIELD label WHERE COUNT { MATCH (m) } > 0 RETURN label",
		accepts: true,
	},
	{
		// The source's CALL-bearing COUNT { CALL { ... } RETURN ... } variant,
		// kept exactly as supplied.
		name:    "procedure_yield_where_count_call_subquery",
		query:   "UNWIND [] AS s CALL db.labels() YIELD label WHERE COUNT { CALL { MATCH (m) RETURN m } RETURN m } > 0 RETURN label",
		accepts: true,
	},
	{
		// The ticket's second root.
		name:    "delete_case_count_subquery",
		query:   "UNWIND [] AS s MATCH (n) DELETE CASE WHEN COUNT { MATCH (m) } > 0 THEN n END",
		accepts: true,
	},
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

var nor82StaticClaims = []norStaticClaim{
	{
		name:         "toUpper_head_dotk_plus_date",
		query:        "UNWIND [] AS s MATCH (n) RETURN toUpper(head(n.k + date())) AS a",
		messageClass: "Type mismatch",
	},
	{
		name:          "toUpper_head_dotk_plus_duration",
		query:         "UNWIND [] AS s MATCH (n) RETURN toUpper(head(n.k + duration('P1D'))) AS a",
		messageClass:  "Type mismatch",
		reconstructed: true,
	},
	{
		name:          "toUpper_head_dotk_plus_empty_list",
		query:         "UNWIND [] AS s MATCH (n) RETURN toUpper(head(n.k + [])) AS a",
		accepts:       true, // Fresh Neo4j 5.26.30 RUN/EXPLAIN/PROFILE accepts this reconstructed variant.
		reconstructed: true,
	},
	{
		name:          "toUpper_head_dotk_plus_dotj",
		query:         "UNWIND [] AS s MATCH (n) RETURN toUpper(head(n.k + n.j)) AS a",
		accepts:       true, // Fresh Neo4j 5.26.30 RUN/EXPLAIN/PROFILE accepts this reconstructed variant.
		reconstructed: true,
	},
}

// TestNor79To82StaticClaims holds each issue's supplied probes to the supplied
// reference verdict in every cell, so each issue is classified on its own:
// NOR-79, NOR-80, NOR-81 and NOR-82 have separate subtrees whose cells name the
// parser, statement kind, transaction mode and probe.
func TestNor79To82StaticClaims(t *testing.T) {
	for _, issue := range []struct {
		number string
		claims []norStaticClaim
	}{
		{"NOR-79", nor79StaticClaims},
		{"NOR-80", nor80StaticClaims},
		{"NOR-81", nor81StaticClaims},
		{"NOR-82", nor82StaticClaims},
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

// TestNor79ParameterAwareConstructorClaims is the recovered form of NOR-79's
// "258 constructor cases". The case list is not restated here: it is the
// canonical constructor owner's generated population, indexed by the positions
// its oracle records as needing parameter-aware admission, and every case is
// held to Neo4j 5.26.30's verdict recorded there (257 rejected, and the list
// comprehension over the Float parameter accepted).
//
// The cases run in every cell like the supplied probes; each cell reports how
// the 258 depart from the oracle, by kind of departure and with a bounded
// sample, so one issue's classification stays readable.
func TestNor79ParameterAwareConstructorClaims(t *testing.T) {
	var oracle struct {
		Population       int    `json:"population"`
		PopulationSha256 string `json:"populationSha256"`
		Neo4jRejected    []int  `json:"neo4jRejected"`
		Inherited        []int  `json:"inherited"`
	}
	require.NoError(t, json.Unmarshal(staticConstructorTypesOracle, &oracle))
	population := staticConstructorTypesPopulation()
	// The positions below are the oracle's, so the population must be the one the
	// oracle was taken from; the canonical owner pins the same pair.
	require.Equal(t, oracle.Population, len(population))
	digest := sha256.Sum256([]byte(strings.Join(population, "\n")))
	require.Equal(t, oracle.PopulationSha256, hex.EncodeToString(digest[:]))
	require.NotEmpty(t, oracle.Inherited, "the canonical oracle records no parameter-aware cases")

	rejected := make(map[int]bool, len(oracle.Neo4jRejected))
	for _, index := range oracle.Neo4jRejected {
		rejected[index] = true
	}
	params := staticConstructorTypesParams()

	forEachParser(t, func(t *testing.T) {
		for _, execution := range norStaticExecutions() {
			for _, mode := range norStaticTransactionModes() {
				t.Run(execution.kind+"/"+mode, func(t *testing.T) {
					ctx := context.Background()
					// counts[0] under-rejections, [1] false rejections, [2] status
					// mismatches: the enum values above norVerdictMatches, in order.
					var counts [3]int
					var samples []string
					for _, index := range oracle.Inherited {
						query := population[index]
						exec := norStaticFreshExecutor(t)
						if mode == "tx" {
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err, "BEGIN for the explicit-transaction cell")
						}
						result, err := exec.Execute(ctx, execution.prefix+query, params)
						norStaticRollback(t, ctx, exec, mode)
						mismatch, detail := norClassifyVerdict(execution.kind, result, err, !rejected[index], "Type mismatch")
						if mismatch == norVerdictMatches {
							continue
						}
						counts[mismatch-1]++
						if len(samples) < 3 {
							samples = append(samples, fmt.Sprintf("index %d: %s: %s", index, query, detail))
						}
					}
					if diverged := counts[0] + counts[1] + counts[2]; diverged > 0 {
						t.Errorf("%d of %d parameter-aware constructor cases diverge from the Neo4j oracle: %d accepted where Neo4j rejects, %d rejected where Neo4j accepts, %d rejected with the wrong status (showing %d):\n%s",
							diverged, len(oracle.Inherited), counts[0], counts[1], counts[2], len(samples), strings.Join(samples, "\n"))
					}
				})
			}
		}
	})
}

// TestNor79To82NegativeControlsReuseCanonicalVerdicts keeps the NOR-81 negative
// controls on the canonical staticTypeNeo4jVerdicts owner instead of restating
// them: each control must still be in that matrix as a rejection in the expected
// message class, so this file cannot drift away from the canonical verdict data.
func TestNor79To82NegativeControlsReuseCanonicalVerdicts(t *testing.T) {
	controls := 0
	for _, claim := range nor81StaticClaims {
		if claim.accepts {
			continue
		}
		controls++
		var canonical string
		found := false
		for _, tc := range staticTypeNeo4jVerdicts {
			if tc.query == claim.query {
				canonical, found = tc.rejection, true
				break
			}
		}
		require.Truef(t, found, "%q is no longer in the canonical staticTypeNeo4jVerdicts matrix", claim.query)
		require.NotEmptyf(t, canonical, "%q is accepted by the canonical matrix, not rejected", claim.query)
		require.Truef(t, strings.HasPrefix(canonical, claim.messageClass), "%q: canonical message %q is not in the %q class", claim.query, canonical, claim.messageClass)
	}
	require.Equal(t, 2, controls, "the supplied reference has two NOR-81 negative controls")
}

// norStaticExecutions are the statement kinds each cell runs: the statement
// itself, its plan, and its profiled execution.
func norStaticExecutions() []struct{ kind, prefix string } {
	return []struct{ kind, prefix string }{
		{"RUN", ""},
		{"EXPLAIN", "EXPLAIN "},
		{"PROFILE", "PROFILE "},
	}
}

// norStaticTransactionModes are the transaction contexts each cell runs in.
func norStaticTransactionModes() []string { return []string{"auto", "tx"} }

// norStaticFreshExecutor builds the cell's fixture: its own storage engine and
// executor, so no cell inherits another's transaction state or data.
func norStaticFreshExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor79_to_82_static_claims"))
}

// norStaticRollback closes an explicit transaction cell. The fixture is
// discarded either way, but a ROLLBACK that fails would mean the cell was not
// what its own executor saw.
func norStaticRollback(t *testing.T, ctx context.Context, exec *StorageExecutor, mode string) {
	t.Helper()
	if mode != "tx" {
		return
	}
	if _, err := exec.Execute(ctx, "ROLLBACK", nil); err != nil {
		t.Logf("ROLLBACK after the cell failed: %v", err)
	}
}

// norVerdictMismatch classifies how a cell departs from its reference verdict.
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
