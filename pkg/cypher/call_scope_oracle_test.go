package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type callScopeOracleOutcome struct {
	Columns []string `json:"columns,omitempty"`
	Rows    [][]any  `json:"rows,omitempty"`
	Code    string   `json:"code,omitempty"`
	Message string   `json:"message,omitempty"`
}

func callScopeOracleValue(value any) any {
	switch value := value.(type) {
	case neo4j.Node:
		labels := append([]string(nil), value.Labels...)
		sort.Strings(labels)
		return map[string]any{"labels": labels, "properties": callScopeOracleValue(value.Props)}
	case *storage.Node:
		if value == nil {
			return nil
		}
		labels := append([]string(nil), value.Labels...)
		sort.Strings(labels)
		return map[string]any{"labels": labels, "properties": callScopeOracleValue(value.Properties)}
	case []any:
		out := make([]any, len(value))
		for index, item := range value {
			out[index] = callScopeOracleValue(item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(value))
		for key, item := range value {
			out[key] = callScopeOracleValue(item)
		}
		return out
	default:
		return value
	}
}

func (outcome callScopeOracleOutcome) equal(other callScopeOracleOutcome) bool {
	if outcome.Code != "" || other.Code != "" {
		return outcome.Code == other.Code
	}
	if !reflect.DeepEqual(outcome.Columns, other.Columns) {
		return false
	}
	rows := func(input [][]any) []string {
		out := make([]string, 0, len(input))
		for _, row := range input {
			encoded, _ := json.Marshal(callScopeOracleValue([]any(row)))
			out = append(out, string(encoded))
		}
		sort.Strings(out)
		return out
	}
	return reflect.DeepEqual(rows(outcome.Rows), rows(other.Rows))
}

// A compatibility query names its outer value scopeInput, then binds a as a
// normal local inside CALL. Project that transport name back to the original
// query's a. If CALL returned a unchanged, the extra transport column is removed.
// The actual NornicDB column order is still compared exactly.
func (outcome callScopeOracleOutcome) compatibilityProjection() callScopeOracleOutcome {
	if outcome.Code != "" {
		return outcome
	}
	indices := make(map[string]int, len(outcome.Columns))
	for index, column := range outcome.Columns {
		if column != "scopeInput" {
			indices[column] = index
		}
	}
	for index, column := range outcome.Columns {
		if column == "scopeInput" {
			if _, returned := indices["a"]; !returned {
				indices["a"] = index
			}
		}
	}
	columns := make([]string, 0, len(indices))
	for column := range indices {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	rows := make([][]any, 0, len(outcome.Rows))
	for _, row := range outcome.Rows {
		projected := make([]any, len(columns))
		for index, column := range columns {
			projected[index] = row[indices[column]]
		}
		rows = append(rows, projected)
	}
	return callScopeOracleOutcome{Columns: columns, Rows: rows}
}

func TestCallScopeNeo4jOracle(t *testing.T) {
	uri := os.Getenv("NORNICDB_CALL_SCOPE_ORACLE_URI")
	if uri == "" {
		t.Skip("set NORNICDB_CALL_SCOPE_ORACLE_URI for the live Neo4j 5.26 scope differential")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	cache := make(map[string]callScopeOracleOutcome)
	oracleRun := fmt.Sprintf("%s-%d", os.Getenv("NORNICDB_PARSER"), time.Now().UnixNano())
	parameters := map[string]any{"outer": int64(1), "oracleRun": oracleRun}
	reference := func(query string) callScopeOracleOutcome {
		if cached, found := cache[query]; found {
			return cached
		}
		outcome := callScopeOracleOutcome{}
		result, runErr := session.Run(ctx, query, parameters)
		if runErr == nil {
			outcome.Columns, runErr = result.Keys()
			if runErr == nil {
				var records []*neo4j.Record
				records, runErr = result.Collect(ctx)
				for _, record := range records {
					outcome.Rows = append(outcome.Rows, record.Values)
				}
			}
		}
		if runErr != nil {
			var neoError *neo4j.Neo4jError
			require.ErrorAs(t, runErr, &neoError, "reference transport must not be classified as a query rejection")
			outcome.Code, outcome.Message = neoError.Code, neoError.Msg
			outcome.Columns, outcome.Rows = nil, nil
		}
		cache[query] = outcome
		return outcome
	}
	version := reference("CALL dbms.components() YIELD versions RETURN versions")
	require.Empty(t, version.Code)
	require.Contains(t, fmt.Sprint(version.Rows), "5.26.30")
	for _, query := range []string{
		"CREATE (:CallScopeOracleTarget {n:7, oracleRun:$oracleRun})",
	} {
		require.Empty(t, reference(query).Code, query)
	}
	t.Cleanup(func() {
		cleanup, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		result, cleanupErr := session.Run(cleanup, "MATCH (n:CallScopeOracleTarget {oracleRun:$oracleRun}) DETACH DELETE n", parameters)
		if cleanupErr == nil {
			_, cleanupErr = result.Consume(cleanup)
		}
		require.NoError(t, cleanupErr)
	})
	store := newTestMemoryEngine(t)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(store, "scope-oracle"))
	_, err = exec.Execute(ctx, "CREATE (:CallScopeOracleTarget {n:7, oracleRun:$oracleRun})", parameters)
	require.NoError(t, err)
	cases := generatedCallScopeOracleCases()
	require.Len(t, cases, 2442, "2430 Cartesian cases plus 12 extension and rejection controls")
	seen := make(map[string]struct{}, len(cases))
	comparisons, mismatches := 0, 0
	extensions := map[string]int{}
	for caseIndex, item := range cases {
		_, duplicate := seen[item.name]
		require.False(t, duplicate, item.name)
		seen[item.name] = struct{}{}
		original := reference(item.reference)
		want := original
		allowed := ""
		if item.extension == "907-unscoped-implicit-import" {
			want = reference(item.compatibility).compatibilityProjection()
			allowed = item.extension
		} else if item.extension == "907-return-outer-unchanged" && original.Code != "" && strings.Contains(original.Message, "already declared") && strings.Contains(original.Message, "outer scope") {
			want = reference(item.compatibility).compatibilityProjection()
			allowed = item.extension
		} else if item.extension == "907-scoped-redeclaration" && original.Code != "" && strings.Contains(original.Message, "shadowing an imported variable") {
			want = reference(item.compatibility).compatibilityProjection()
			allowed = item.extension
		}
		for sourceIndex, source := range []string{"public", "fabric", "context", "both"} {
			runCtx := withParams(ctx, map[string]any{"outer": int64(1), "oracleRun": oracleRun, "nonce": caseIndex*4 + sourceIndex})
			exec.fabricRecordBindings = nil
			if source == "fabric" || source == "both" {
				exec.fabricRecordBindings = map[string]any{"a": int64(1)}
				if source == "both" {
					exec.fabricRecordBindings["a"] = int64(99)
				}
			}
			if source == "context" || source == "both" {
				runCtx = withValueBindings(runCtx, map[string]any{"a": int64(1)})
			}
			var actual *ExecuteResult
			var actualErr error
			if source == "public" {
				actual, actualErr = exec.Execute(runCtx, item.reference, getParamsFromContext(runCtx))
			} else {
				outcome := exec.executePipeline(runCtx, item.query)
				require.True(t, outcome.terminal(), "%s %s", item.name, source)
				actual, actualErr = outcome.result, outcome.err
			}
			got := callScopeOracleOutcome{}
			if actualErr != nil {
				got.Code, got.Message = nornicerrors.Neo4jStatus(actualErr)
			} else if actual != nil {
				got.Columns, got.Rows = actual.Columns, actual.Rows
			}
			comparisons++
			if !got.equal(want) {
				mismatches++
				record, _ := json.Marshal(map[string]any{"case": item.name, "source": source, "query": item.query, "reference": item.reference, "compatibility_query": item.compatibility, "declared_extension": item.extension, "applied_extension": allowed, "oracle": original, "expected": want, "actual": got})
				fmt.Println("CALL_SCOPE_MISMATCH " + string(record))
			} else if allowed != "" && !got.equal(original) {
				extensions[allowed]++
			}
		}
	}
	exec.fabricRecordBindings = nil
	report, _ := json.Marshal(map[string]any{"cases": len(cases), "comparisons": comparisons, "reference_queries": len(cache), "mismatches": mismatches, "allowed_differences": extensions, "reference_version": "5.26.30", "parser": os.Getenv("NORNICDB_PARSER")})
	fmt.Println("CALL_SCOPE_ORACLE " + string(report))
	require.Equal(t, 9768, comparisons)
	require.Zero(t, mismatches)
}
