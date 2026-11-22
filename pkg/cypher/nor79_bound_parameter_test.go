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
	"github.com/stretchr/testify/require"
)

// These are the recovered NOR79 queries and historical inherited-constructor
// population. Reuse the original matrix helpers now owned by the release's
// NOR81 reproduction. The original120 generated cases remain unavailable.
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

func TestNOR79BoundParameterOriginalMatrix(t *testing.T) {
	for _, issue := range []struct {
		number string
		claims []norStaticClaim
	}{
		{"NOR-79", nor79StaticClaims},
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

func TestNOR79BoundParameterConstructorClaims(t *testing.T) {
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
