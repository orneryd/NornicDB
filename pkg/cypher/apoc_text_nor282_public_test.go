// Author: Codex unit-test lane for NOR-282 reproduction, 2026-11-18.
// Oracle correction: 2026-11-19, after the parent's fresh Neo4j 5.26.30 /
// APOC 5.26.30 reference run recorded in fresh-parent-reference.json.
//
// Public Cypher execution over the real registered APOC plugin handlers on
// both supported parser paths. The handlers come from apoc.Initialize/GetInfo
// (the same compiled handlers the bundled plugin exports) and the corpus
// asserts that no case owner is an engine built-in, so a built-in shortcut
// cannot answer these calls. Reproduction evidence only; no production code
// changes are authorized here.
package cypher

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	apocpkg "github.com/orneryd/nornicdb/apoc"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const nor282PublicCorpusPath = "../../openspec/changes/nor282-v151-reproduction/query-cases.json"

type nor282CorpusFile struct {
	Cases []nor282PublicCase `json:"cases"`
}

type nor282PublicCase struct {
	ID         string                  `json:"id"`
	Owner      string                  `json:"owner"`
	Query      string                  `json:"query"`
	Reference  nor282PublicExpectation `json:"reference"`
	Confidence string                  `json:"confidence"`
	Flags      []string                `json:"flags"`
	Observed   nor282PublicExpectation `json:"nornicdb_observed"`
}

type nor282PublicExpectation struct {
	Kind  string          `json:"kind"`
	Value json.RawMessage `json:"value"`
}

func loadNOR282PublicCorpus(t *testing.T) nor282CorpusFile {
	t.Helper()
	data, err := os.ReadFile(nor282PublicCorpusPath)
	if err != nil {
		t.Fatalf("read NOR-282 case corpus: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var corpus nor282CorpusFile
	if err := decoder.Decode(&corpus); err != nil {
		t.Fatalf("decode NOR-282 case corpus: %v", err)
	}
	if len(corpus.Cases) == 0 {
		t.Fatal("NOR-282 case corpus is empty")
	}
	return corpus
}

// requireNOR282RegisteredOwners installs every real bundled handler and proves
// each case owner is a plugin-exported function, never an engine built-in.
func requireNOR282RegisteredOwners(t *testing.T, corpus nor282CorpusFile) {
	t.Helper()
	handlers := bundledAPOCPluginFunctions(t)
	withPluginFunctions(t, handlers)
	exported := apocpkg.PluginFunctionNames()
	seen := map[string]bool{}
	for _, tc := range corpus.Cases {
		if seen[tc.Owner] {
			continue
		}
		seen[tc.Owner] = true
		info, ok := apocpkg.GetInfo(tc.Owner)
		require.True(t, ok, "%s must be registered by the bundled APOC plugin", tc.Owner)
		require.NotNil(t, info.Handler, "%s must have a compiled registered handler", tc.Owner)
		require.False(t, apocpkg.IsBuiltInFunction(tc.Owner), "%s is an engine built-in; this fixture would test the built-in shortcut", tc.Owner)
		require.Contains(t, exported, tc.Owner, "%s must be exported by the bundled APOC plugin", tc.Owner)
		require.Contains(t, handlers, tc.Owner, "%s must be installed from the registered handler table", tc.Owner)
	}
}

func nor282ExecuteOnce(t *testing.T, query string) (interface{}, error) {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor282_public"))
	result, err := exec.Execute(context.Background(), query, nil)
	if err != nil {
		return nil, err
	}
	require.Len(t, result.Rows, 1, "query %q", query)
	require.Len(t, result.Rows[0], 1, "query %q", query)
	return result.Rows[0][0], nil
}

// TestNOR282PublicReferenceParity reproduces the NOR-282 defect through public
// execution on both parser paths against the fresh APOC 5.26.30 oracle.
func TestN15268NonRegexPublicReferenceParity(t *testing.T) {
	corpus := loadNOR282PublicCorpus(t)
	requireNOR282RegisteredOwners(t, corpus)
	previous := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previous) })
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			config.SetParserType(parser)
			require.Equal(t, parser, config.GetParserType())
			for _, tc := range corpus.Cases {
				if tc.Owner == "apoc.text.split" || tc.Owner == "apoc.text.replace" {
					continue
				}
				t.Run(tc.ID, func(t *testing.T) {
					if tc.Confidence == "pending" {
						t.Skipf("expectation is explicitly undecided: %v", tc.Flags)
					}
					got, err := nor282ExecuteOnce(t, tc.Query)
					nor282RequirePublicExpectation(t, parser, tc, got, err)
				})
			}
		})
	}
}

func nor282RequirePublicExpectation(t *testing.T, parser string, tc nor282PublicCase, got interface{}, err error) {
	t.Helper()
	want := nor282PublicExpectationValue(t, tc.Reference)
	switch tc.Reference.Kind {
	case "value":
		require.NoError(t, err, "%s/%s must return the APOC reference value %#v", parser, tc.ID, want)
		require.Equal(t, want, nor282PublicNormalize(got), "%s/%s must match APOC 5.26.30 (confidence %s, flags %v)", parser, tc.ID, tc.Confidence, tc.Flags)
	case "function_error":
		require.Error(t, err, "%s/%s must fail as APOC 5.26.30 does for the same call (confidence %s, flags %v)", parser, tc.ID, tc.Confidence, tc.Flags)
		t.Logf("%s/%s function error: %v", parser, tc.ID, err)
	default:
		t.Fatalf("%s/%s has unsupported reference kind %q", parser, tc.ID, tc.Reference.Kind)
	}
}

func nor282RequirePublicObserved(t *testing.T, parser string, tc nor282PublicCase, got interface{}, err error) {
	t.Helper()
	switch tc.Observed.Kind {
	case "value":
		want := nor282PublicExpectationValue(t, tc.Observed)
		require.NoError(t, err, "%s/%s recorded a NornicDB value", parser, tc.ID)
		require.Equal(t, want, nor282PublicNormalize(got), "%s/%s must reproduce the recorded NornicDB value", parser, tc.ID)
	case "error":
		require.Error(t, err, "%s/%s recorded a NornicDB failure", parser, tc.ID)
		t.Logf("%s/%s observed error: %v", parser, tc.ID, err)
	default:
		t.Fatalf("%s/%s has unsupported observed kind %q", parser, tc.ID, tc.Observed.Kind)
	}
}

func nor282PublicExpectationValue(t *testing.T, expectation nor282PublicExpectation) interface{} {
	t.Helper()
	if len(expectation.Value) == 0 || string(expectation.Value) == "null" {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(expectation.Value))
	decoder.UseNumber()
	var value interface{}
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("decode expectation value %s: %v", expectation.Value, err)
	}
	return nor282PublicNormalize(value)
}

// nor282PublicNormalize converts query row values and JSON values into the
// same comparison domain.
func nor282PublicNormalize(value interface{}) interface{} {
	switch v := value.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i
		}
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case []string:
		out := make([]interface{}, len(v))
		for i := range v {
			out[i] = v[i]
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(v))
		for i := range v {
			out[i] = nor282PublicNormalize(v[i])
		}
		return out
	}
	rv := reflect.ValueOf(value)
	if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
		out := make([]interface{}, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out[i] = nor282PublicNormalize(rv.Index(i).Interface())
		}
		return out
	}
	return value
}

// TestN15268RegisteredErrors verifies that checked owner failures reach both
// registered query paths without converting null or malformed input to values.
func TestN15268RegisteredErrors(t *testing.T) {
	corpus := loadNOR282PublicCorpus(t)
	requireNOR282RegisteredOwners(t, corpus)
	previous := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previous) })
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			config.SetParserType(parser)
			for _, tc := range []struct {
				query   string
				want    interface{}
				failure bool
			}{
				{"RETURN apoc.text.decapitalize(null) AS value", nil, false},
				{"RETURN apoc.text.decapitalize(123) AS value", nil, true},
				{"RETURN apoc.text.capitalize('') AS value", nil, true},
				{"RETURN apoc.text.base64Decode('Zg=') AS value", nil, true},
				{"RETURN apoc.text.base64Decode('Zg') AS value", "f", false},
				{"RETURN apoc.util.decodeBase64('%%%') AS value", nil, true},
				{"RETURN apoc.text.urldecode('%ED%A0') AS value", "�", false},
				{"RETURN apoc.util.decodeURL('%zz') AS value", nil, true},
				{"RETURN apoc.text.compareCleaned('É a λ-42!','eaλ42') AS value", true, false},
				{"RETURN size(apoc.xml.prettify('<a>v</a>')) > 0 AS value", true, false},
			} {
				t.Run(tc.query, func(t *testing.T) {
					got, err := nor282ExecuteOnce(t, tc.query)
					if tc.failure {
						require.Error(t, err)
						return
					}
					require.NoError(t, err)
					require.Equal(t, tc.want, got)
				})
			}
		})
	}
}
