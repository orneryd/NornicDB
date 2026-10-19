package cypher

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Generated inputs are evaluated on Neo4j/APOC 5.26.30, not by NornicDB.
// Equality, lookup, equivalence and each public result have independent
// reference fields: equal named-zone overlap values are not equivalent.
// See testdata/temporal_contracts.md for the repeatable reference procedure.
//
//go:embed testdata/temporal_contract_oracle.json
var temporalContractOracleJSON []byte

//go:embed testdata/temporal_contract_review_oracle.json
var temporalContractReviewOracleJSON []byte

type temporalContractCase struct {
	Name         string        `json:"name"`
	A            string        `json:"a"`
	B            string        `json:"b"`
	Equality     interface{}   `json:"equality"`
	Equivalence  bool          `json:"equivalence"`
	Lookup       []interface{} `json:"lookup"`
	Sets         []interface{} `json:"sets"`
	DistinctRows int           `json:"distinctRows"`
	Groups       int           `json:"groups"`
	UnionRows    int           `json:"unionRows"`
}

func temporalContractCases(t *testing.T) []temporalContractCase {
	t.Helper()
	var cases []temporalContractCase
	for index, data := range [][]byte{temporalContractOracleJSON, temporalContractReviewOracleJSON} {
		var fixture struct {
			Reference string                 `json:"reference"`
			Cases     []temporalContractCase `json:"cases"`
		}
		require.NoError(t, json.Unmarshal(data, &fixture))
		require.Equal(t, "Neo4j/APOC 5.26.30", fixture.Reference)
		require.Len(t, fixture.Cases, []int{396, 168}[index])
		cases = append(cases, fixture.Cases...)
	}
	return cases
}

func temporalOracleValues(values []interface{}) []interface{} {
	result := make([]interface{}, len(values))
	for i, value := range values {
		if number, ok := value.(float64); ok {
			result[i] = int64(number)
		} else {
			result[i] = value
		}
	}
	return result
}

// No expectations are inferred from "=". APOC containsAny is NornicDB's
// extension; with one searched item it follows the reference containsAll.
func checkTemporalContract(t *testing.T, exec *StorageExecutor, tc temporalContractCase, a, b string, params map[string]interface{}, with bool) {
	t.Helper()
	left, right, prefix := a, b, ""
	if with {
		prefix = "WITH " + a + " AS a, " + b + " AS b "
		left, right = "a", "b"
	}
	expr := func(format string) string { return fmt.Sprintf(format, left, right) }
	query := prefix + "RETURN " + expr("(%s)=(%s)") + " AS eq, " +
		expr("apoc.coll.contains([%s],%s)") + " AS contained, " +
		expr("apoc.coll.containsAll([%s],[%s])") + " AS allContained, " +
		expr("apoc.coll.containsAny([%s],[%s])") + " AS anyContained, " +
		expr("apoc.coll.indexOf([%s],%s)") + " AS position, " +
		expr("apoc.coll.occurrences([%s],%s)") + " AS occurrences, " +
		expr("size(apoc.coll.split(['__left',%s,'__right'],%s))") + " AS partitions, " +
		expr("size(apoc.coll.toSet([%s,%s]))") + " AS distinctCount, " +
		expr("size(apoc.coll.union([%s],[%s]))") + " AS unionCount, " +
		expr("size(apoc.coll.intersection([%s],[%s]))") + " AS intersectionCount, " +
		expr("size(apoc.coll.subtract([%s],[%s]))") + " AS subtractionCount, " +
		expr("size(apoc.coll.frequencies([%s,%s]))") + " AS frequencyCount"
	lookup, sets := temporalOracleValues(tc.Lookup), temporalOracleValues(tc.Sets)
	want := []interface{}{tc.Equality, lookup[0], lookup[1], lookup[1], lookup[2], lookup[3], lookup[4]}
	want = append(want, sets...)
	result, err := exec.Execute(context.Background(), query, params)
	require.NoError(t, err, query)
	require.Equal(t, [][]interface{}{want}, result.Rows, query)
	distinct, err := exec.Execute(context.Background(), prefix+"UNWIND ["+left+","+right+"] AS x RETURN DISTINCT x", params)
	require.NoError(t, err)
	require.Len(t, distinct.Rows, tc.DistinctRows, "RETURN DISTINCT: %s", tc.Name)
	groups, err := exec.Execute(context.Background(), prefix+"UNWIND ["+left+","+right+"] AS x RETURN x,count(*) AS n", params)
	require.NoError(t, err)
	require.Len(t, groups.Rows, tc.Groups, "grouping: %s", tc.Name)
	unionQuery := "RETURN " + a + " AS x UNION RETURN " + b + " AS x"
	if with {
		unionQuery = "WITH " + a + " AS a RETURN a AS x UNION WITH " + b + " AS b RETURN b AS x"
	}
	union, err := exec.Execute(context.Background(), unionQuery, params)
	require.NoError(t, err)
	require.Len(t, union.Rows, tc.UnionRows, "UNION: %s", tc.Name)
}

func temporalPointerCarrier(value interface{}) interface{} {
	switch typed := value.(type) {
	case CypherDate:
		return &typed
	case CypherLocalTime:
		return &typed
	case CypherTime:
		return &typed
	case CypherLocalDateTime:
		return &typed
	case CypherDateTime:
		return &typed
	case CypherDuration:
		return &typed
	case time.Time:
		return &typed
	case []interface{}:
		values := make([]interface{}, len(typed))
		for i, item := range typed {
			values[i] = temporalPointerCarrier(item)
		}
		return values
	case map[string]interface{}:
		fields := make(map[string]interface{}, len(typed))
		for name, item := range typed {
			fields[name] = temporalPointerCarrier(item)
		}
		return fields
	default:
		return value
	}
}

func TestApocCollectionsTemporalSemantics(t *testing.T) {
	for _, tc := range temporalContractCases(t) {
		t.Run(tc.Name, func(t *testing.T) {
			exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal-reference"), 0, 0)
			raw, err := exec.Execute(context.Background(), "RETURN "+tc.A+" AS a,"+tc.B+" AS b", nil)
			require.NoError(t, err)
			require.Len(t, raw.Rows, 1)
			a, b := raw.Rows[0][0], raw.Rows[0][1]
			for _, programs := range []bool{true, false} {
				t.Run(fmt.Sprintf("programs=%v", programs), func(t *testing.T) {
					rowValueProgramsDisabled.Store(!programs)
					defer rowValueProgramsDisabled.Store(false)
					for _, form := range []string{"literal", "with", "parameter", "pointer"} {
						t.Run(form, func(t *testing.T) {
							switch form {
							case "literal":
								checkTemporalContract(t, exec, tc, tc.A, tc.B, nil, false)
							case "with":
								checkTemporalContract(t, exec, tc, tc.A, tc.B, nil, true)
							case "parameter":
								checkTemporalContract(t, exec, tc, "$a", "$b", map[string]interface{}{"a": a, "b": b}, false)
							case "pointer":
								checkTemporalContract(t, exec, tc, "$a", "$b", map[string]interface{}{"a": temporalPointerCarrier(a), "b": temporalPointerCarrier(b)}, true)
							}
						})
					}
				})
			}
			require.Equal(t, tc.Equivalence, cypherValueKey(a, cypherEquivalenceContract) == cypherValueKey(b, cypherEquivalenceContract), "encoded equivalence")
			require.Equal(t, tc.Lookup[0], cypherValueEquals(a, b), "value lookup")
		})
	}
}

// Preserve raw API representations which constructors normalize before Execute
// sees them. Their expectations are the matching reference-generated cases.
func TestTemporalContractRawParameters(t *testing.T) {
	cases := temporalContractCases(t)
	london, err := time.LoadLocation("Europe/London")
	require.NoError(t, err)
	named := time.Date(2024, 7, 1, 12, 34, 56, 7, london)
	fixed := time.Date(1999, 1, 2, 12, 34, 56, 7, time.FixedZone("", 3600))
	for _, probe := range []struct {
		name string
		a, b interface{}
	}{
		{"time-projected", CypherTime{Time: named}, &CypherTime{Time: fixed}},
		{"duration-negative-arithmetic", CypherDuration{Nanos: -500000000}, &CypherDuration{Seconds: -1, Nanos: 500000000}},
		{"duration-nanos", &CypherDuration{Nanos: 1000000000}, CypherDuration{Seconds: 1}},
		{"negative-borrow", &CypherDuration{Seconds: 1, Nanos: -1500000000}, CypherDuration{Nanos: -500000000}},
		{"positive-carry", CypherDuration{Seconds: -1, Nanos: 1500000000}, &CypherDuration{Nanos: 500000000}},
	} {
		for _, shape := range []string{"scalar", "list", "map"} {
			t.Run(probe.name+"/"+shape, func(t *testing.T) {
				var oracle *temporalContractCase
				for i := range cases {
					if cases[i].Name == probe.name+"/"+shape {
						oracle = &cases[i]
						break
					}
				}
				require.NotNil(t, oracle)
				wrap := func(value interface{}) interface{} {
					if shape == "list" {
						return []interface{}{value}
					}
					if shape == "map" {
						return map[string]interface{}{"v": []interface{}{value}}
					}
					return value
				}
				for _, programs := range []bool{true, false} {
					rowValueProgramsDisabled.Store(!programs)
					exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal-raw"), 0, 0)
					checkTemporalContract(t, exec, *oracle, "$a", "$b", map[string]interface{}{"a": wrap(probe.a), "b": wrap(probe.b)}, false)
					rowValueProgramsDisabled.Store(false)
				}
			})
		}
	}
}
