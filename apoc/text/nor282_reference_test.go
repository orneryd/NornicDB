// Author: Codex unit-test lane for NOR-282 reproduction, 2026-11-18.
// Oracle correction: 2026-11-19, after the parent's fresh Neo4j 5.26.30 /
// APOC 5.26.30 reference run recorded in fresh-parent-reference.json.
//
// This file asserts the direct apoc.text owner values against the recorded
// NOR-282 reference corpus (openspec/changes/nor282-v151-reproduction/
// query-cases.json) and proves that corpus oracle is the fresh reference. It is
// scoped non-regex acceptance. The untouched full RED fixture is preserved in
// the local task archive (OpenSpec original-source-hashes.json); regex rows remain open.
package text_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	apoctext "github.com/orneryd/nornicdb/apoc/text"
)

const (
	nor282CorpusPath         = "../../openspec/changes/nor282-v151-reproduction/query-cases.json"
	nor282FreshReferencePath = "../../openspec/changes/nor282-v151-reproduction/fresh-parent-reference.json"
)

type nor282Corpus struct {
	Reference nor282ReferenceMetadata `json:"reference"`
	Cases     []nor282Case            `json:"cases"`
}

type nor282ReferenceMetadata struct {
	Neo4j          string `json:"neo4j"`
	APOC           string `json:"apoc"`
	FreshReference string `json:"fresh_reference"`
}

type nor282Case struct {
	ID               string            `json:"id"`
	Owner            string            `json:"owner"`
	Origin           string            `json:"origin"`
	Query            string            `json:"query"`
	Direct           *nor282DirectCall `json:"direct"`
	Reference        nor282Expectation `json:"reference"`
	Confidence       string            `json:"confidence"`
	Flags            []string          `json:"flags"`
	FreshReferenceID string            `json:"fresh_reference_id"`
	Observed         nor282Expectation `json:"nornicdb_observed"`
}

type nor282DirectCall struct {
	Package  string   `json:"package"`
	Function string   `json:"function"`
	Args     []string `json:"args"`
}

type nor282Expectation struct {
	ID    string          `json:"id"`
	Kind  string          `json:"kind"`
	Value json.RawMessage `json:"value"`
}

type nor282FreshReferenceFile struct {
	Metadata nor282FreshMetadata `json:"metadata"`
	Cases    []nor282FreshCase   `json:"cases"`
}

type nor282FreshMetadata struct {
	Neo4jVersion string `json:"neo4j_version"`
	APOCVersion  string `json:"apoc_version"`
	ImageDigest  string `json:"image_digest"`
	APOCJarSHA   string `json:"apoc_jar_sha256"`
}

type nor282FreshCase struct {
	ID    string            `json:"id"`
	Query string            `json:"query"`
	Fresh nor282Expectation `json:"fresh_reference"`
}

func loadNOR282Corpus(t *testing.T) nor282Corpus {
	t.Helper()
	data, err := os.ReadFile(nor282CorpusPath)
	if err != nil {
		t.Fatalf("read NOR-282 case corpus: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var corpus nor282Corpus
	if err := decoder.Decode(&corpus); err != nil {
		t.Fatalf("decode NOR-282 case corpus: %v", err)
	}
	if len(corpus.Cases) == 0 {
		t.Fatal("NOR-282 case corpus is empty")
	}
	return corpus
}

func loadNOR282FreshReference(t *testing.T) nor282FreshReferenceFile {
	t.Helper()
	data, err := os.ReadFile(nor282FreshReferencePath)
	if err != nil {
		t.Fatalf("read NOR-282 fresh parent reference: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var fresh nor282FreshReferenceFile
	if err := decoder.Decode(&fresh); err != nil {
		t.Fatalf("decode NOR-282 fresh parent reference: %v", err)
	}
	if len(fresh.Cases) == 0 {
		t.Fatal("NOR-282 fresh parent reference is empty")
	}
	return fresh
}

// nor282TextOwners maps a corpus direct call to the exported Go owner in this
// package. Every case names its owner so a renamed function fails loudly
// instead of silently skipping.
var nor282TextOwners = map[string]func([]string) (interface{}, error){
	"Clean":          func(a []string) (interface{}, error) { return nor282StringArg(a, 0, apoctext.Clean) },
	"Split":          func(a []string) (interface{}, error) { return nor282TwoStringArgs(a, apoctext.Split) },
	"Replace":        func(a []string) (interface{}, error) { return nor282ThreeStringArgs(a, apoctext.Replace) },
	"Urlencode":      func(a []string) (interface{}, error) { return nor282StringArg(a, 0, apoctext.Urlencode) },
	"Urldecode":      func(a []string) (interface{}, error) { return apoctext.UrldecodeChecked(a[0]) },
	"IndexOf":        func(a []string) (interface{}, error) { return nor282TwoStringIntArgs(a, apoctext.IndexOf) },
	"CamelCase":      func(a []string) (interface{}, error) { return nor282StringArg(a, 0, apoctext.CamelCase) },
	"UpperCamelCase": func(a []string) (interface{}, error) { return nor282StringArg(a, 0, apoctext.UpperCamelCase) },
	"Base64Encode":   func(a []string) (interface{}, error) { return nor282StringArg(a, 0, apoctext.Base64Encode) },
	"Base64Decode":   func(a []string) (interface{}, error) { return apoctext.Base64DecodeChecked(a[0]) },
}

func nor282StringArg(args []string, index int, owner func(string) string) (interface{}, error) {
	if len(args) != index+1 {
		return nil, fmt.Errorf("want %d direct argument(s), got %d", index+1, len(args))
	}
	return owner(args[index]), nil
}

func nor282TwoStringArgs(args []string, owner func(string, string) []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("want 2 direct arguments, got %d", len(args))
	}
	return owner(args[0], args[1]), nil
}

func nor282ThreeStringArgs(args []string, owner func(string, string, string) string) (interface{}, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("want 3 direct arguments, got %d", len(args))
	}
	return owner(args[0], args[1], args[2]), nil
}

func nor282TwoStringIntArgs(args []string, owner func(string, string) int) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("want 2 direct arguments, got %d", len(args))
	}
	return owner(args[0], args[1]), nil
}

// TestNOR282TextOracleMatchesFreshReference proves the corpus oracle is the
// parent's fresh APOC 5.26.30 result rather than a derived or historical guess:
// every apoc.text case resolves to a fresh case with the same reference kind and
// value, and the corpus citation names that run's image digest and bundled JAR.
func TestNOR282TextOracleMatchesFreshReference(t *testing.T) {
	corpus := loadNOR282Corpus(t)
	fresh := loadNOR282FreshReference(t)

	if fresh.Metadata.Neo4jVersion != "5.26.30" || fresh.Metadata.APOCVersion != "5.26.30" {
		t.Fatalf("fresh reference versions = neo4j %q apoc %q, want 5.26.30 / 5.26.30", fresh.Metadata.Neo4jVersion, fresh.Metadata.APOCVersion)
	}
	if !strings.Contains(corpus.Reference.Neo4j, fresh.Metadata.ImageDigest) {
		t.Fatalf("corpus neo4j citation %q must carry the fresh image digest %q", corpus.Reference.Neo4j, fresh.Metadata.ImageDigest)
	}
	if !strings.Contains(corpus.Reference.APOC, fresh.Metadata.APOCJarSHA) {
		t.Fatalf("corpus apoc citation %q must carry the fresh bundled jar sha256 %q", corpus.Reference.APOC, fresh.Metadata.APOCJarSHA)
	}
	if !strings.Contains(corpus.Reference.FreshReference, "fresh-parent-reference.json") {
		t.Fatalf("corpus must cite fresh-parent-reference.json, got %q", corpus.Reference.FreshReference)
	}

	freshByID := make(map[string]nor282FreshCase, len(fresh.Cases))
	textCases := 0
	for _, freshCase := range fresh.Cases {
		freshByID[freshCase.ID] = freshCase
		if freshCase.Fresh.ID != "" {
			freshByID[freshCase.Fresh.ID] = freshCase
		}
		if freshCase.Fresh.Kind != "missing_function" {
			textCases++
		}
	}
	if textCases != 59 {
		t.Fatalf("fresh reference holds %d apoc.text cases, want 59", textCases)
	}

	checked := 0
	for _, tc := range corpus.Cases {
		if !strings.HasPrefix(tc.Owner, "apoc.text.") {
			continue
		}
		checked++
		if tc.Confidence != "fresh_confirmed" {
			t.Fatalf("%s confidence = %q, want fresh_confirmed", tc.ID, tc.Confidence)
		}
		if tc.FreshReferenceID == "" {
			t.Fatalf("%s has no fresh_reference_id", tc.ID)
		}
		freshCase, ok := freshByID[tc.FreshReferenceID]
		if !ok {
			t.Fatalf("%s fresh_reference_id %q is not in fresh-parent-reference.json", tc.ID, tc.FreshReferenceID)
		}
		if freshCase.ID != tc.ID {
			t.Fatalf("%s fresh_reference_id %q resolves to fresh case %q", tc.ID, tc.FreshReferenceID, freshCase.ID)
		}
		if tc.Reference.Kind != freshCase.Fresh.Kind {
			t.Fatalf("%s reference kind = %q, fresh reference kind = %q", tc.ID, tc.Reference.Kind, freshCase.Fresh.Kind)
		}
		want := nor282ExpectationValue(t, tc.Reference)
		freshValue := nor282ExpectationValue(t, freshCase.Fresh)
		if !reflect.DeepEqual(want, freshValue) {
			t.Fatalf("%s reference value = %#v, fresh reference value = %#v", tc.ID, want, freshValue)
		}
	}
	if checked != textCases {
		t.Fatalf("corpus holds %d apoc.text cases, fresh reference holds %d", checked, textCases)
	}
}

// TestNOR282DirectReferenceParity asserts the reference expectation for every
// direct case the corpus establishes. A failure here is the reproduced NOR-282
// defect on the Go owner.
func TestN15268NonRegexDirectReferenceParity(t *testing.T) {
	corpus := loadNOR282Corpus(t)
	ran := 0
	for _, tc := range corpus.Cases {
		if tc.Direct == nil || tc.Direct.Package != "text" || tc.Owner == "apoc.text.split" || tc.Owner == "apoc.text.replace" {
			continue
		}
		ran++
		t.Run(tc.ID, func(t *testing.T) {
			if tc.Query == "" {
				t.Fatalf("case %s has no public query", tc.ID)
			}
			owner, ok := nor282TextOwners[tc.Direct.Function]
			if !ok {
				t.Fatalf("corpus names unknown apoc/text owner %q", tc.Direct.Function)
			}
			got, err := owner(tc.Direct.Args)
			nor282RequireExpectation(t, "apoc/text "+tc.Direct.Function, tc, got, err)
		})
	}
	if ran != 40 {
		t.Fatalf("scoped direct rows=%d want40", ran)
	}
}

// nor282RequireExpectation compares a call result with the recorded reference
// expectation. A value must match exactly; a function_error must make the call
// fail as the fresh APOC reference does.
func nor282RequireExpectation(t *testing.T, label string, tc nor282Case, got interface{}, err error) {
	t.Helper()
	want := nor282ExpectationValue(t, tc.Reference)
	switch tc.Reference.Kind {
	case "value":
		if err != nil {
			t.Fatalf("%s returned error %v, want APOC 5.26.30 value %#v (confidence %s, flags %v)", label, err, want, tc.Confidence, tc.Flags)
		}
		if !reflect.DeepEqual(nor282Normalize(got), want) {
			t.Fatalf("%s = %#v, want APOC 5.26.30 value %#v (confidence %s, flags %v)", label, nor282Normalize(got), want, tc.Confidence, tc.Flags)
		}
	case "function_error":
		if err == nil {
			t.Fatalf("%s = %#v, want the APOC 5.26.30 function error for the same call (confidence %s, flags %v)", label, nor282Normalize(got), tc.Confidence, tc.Flags)
		}
	default:
		t.Fatalf("%s has unsupported reference kind %q", label, tc.Reference.Kind)
	}
}

func nor282RequireObserved(t *testing.T, label string, tc nor282Case, got interface{}, err error) {
	t.Helper()
	switch tc.Observed.Kind {
	case "value":
		want := nor282ExpectationValue(t, tc.Observed)
		if err != nil {
			t.Fatalf("%s returned error %v, recorded NornicDB value %#v", label, err, want)
		}
		if !reflect.DeepEqual(nor282Normalize(got), want) {
			t.Fatalf("%s = %#v, recorded NornicDB value %#v", label, nor282Normalize(got), want)
		}
	case "error":
		if err == nil {
			t.Fatalf("%s = %#v, recorded NornicDB failure kind", label, nor282Normalize(got))
		}
	default:
		t.Fatalf("%s has unsupported observed kind %q", label, tc.Observed.Kind)
	}
}

// nor282ExpectationValue decodes an expectation into the comparison domain:
// strings, int64, nil and []interface{} of those.
func nor282ExpectationValue(t *testing.T, expectation nor282Expectation) interface{} {
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
	return nor282Normalize(value)
}

// nor282Normalize converts Go owner results and JSON values into one domain.
func nor282Normalize(value interface{}) interface{} {
	switch v := value.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i
		}
	case int:
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
			out[i] = nor282Normalize(v[i])
		}
		return out
	}
	rv := reflect.ValueOf(value)
	if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
		out := make([]interface{}, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out[i] = nor282Normalize(rv.Index(i).Interface())
		}
		return out
	}
	return value
}
