// Author: Codex unit-test lane for NOR-282 reproduction, 2026-11-18.
// Oracle correction: 2026-11-19, after the parent's fresh Neo4j 5.26.30 /
// APOC 5.26.30 reference run recorded in fresh-parent-reference.json.
//
// Direct apoc.util Base64/URL peer assertions over the shared NOR-282 case
// corpus. These owners are NornicDB extensions; only their advertised contract
// is asserted, never APOC parity. The fresh reference records APOC 5.26.30 as
// having no counterpart for these names, which must not turn the advertised
// values into expected errors.
package util

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

const (
	nor282CorpusPath         = "../../openspec/changes/nor282-v151-reproduction/query-cases.json"
	nor282FreshReferencePath = "../../openspec/changes/nor282-v151-reproduction/fresh-parent-reference.json"
)

type nor282Corpus struct {
	Cases []nor282Case `json:"cases"`
}

type nor282Case struct {
	ID         string            `json:"id"`
	Owner      string            `json:"owner"`
	Query      string            `json:"query"`
	Direct     *nor282DirectCall `json:"direct"`
	Reference  nor282Expectation `json:"reference"`
	Confidence string            `json:"confidence"`
	Flags      []string          `json:"flags"`
	Observed   nor282Expectation `json:"nornicdb_observed"`
}

type nor282DirectCall struct {
	Package  string   `json:"package"`
	Function string   `json:"function"`
	Args     []string `json:"args"`
}

type nor282Expectation struct {
	Kind  string          `json:"kind"`
	Value json.RawMessage `json:"value"`
}

type nor282FreshReferenceFile struct {
	Metadata struct {
		APOCVersion string `json:"apoc_version"`
	} `json:"metadata"`
	Cases []struct {
		ID    string            `json:"id"`
		Fresh nor282Expectation `json:"fresh_reference"`
	} `json:"cases"`
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

var nor282UtilOwners = map[string]func(string) string{
	"EncodeBase64": EncodeBase64,
	"DecodeBase64": DecodeBase64,
	"EncodeURL":    EncodeURL,
	"DecodeURL":    DecodeURL,
}

// TestNOR282UtilOracleContract pins the contract decision for the six apoc.util
// peers: APOC 5.26.30 has no such function, so the fresh reference records
// missing_function, while the corpus keeps the NornicDB advertised value. It
// fails if a future oracle change replaces an advertised value with an expected
// APOC unknown-function error.
func TestNOR282UtilOracleContract(t *testing.T) {
	corpus := loadNOR282Corpus(t)
	fresh := loadNOR282FreshReference(t)
	if fresh.Metadata.APOCVersion != "5.26.30" {
		t.Fatalf("fresh reference APOC version = %q, want 5.26.30", fresh.Metadata.APOCVersion)
	}

	freshKinds := make(map[string]string, len(fresh.Cases))
	for _, freshCase := range fresh.Cases {
		freshKinds[freshCase.ID] = freshCase.Fresh.Kind
	}

	checked := 0
	for _, tc := range corpus.Cases {
		if tc.Direct == nil || tc.Direct.Package != "util" {
			continue
		}
		checked++
		freshKind, ok := freshKinds[tc.ID]
		if !ok {
			t.Fatalf("%s is missing from fresh-parent-reference.json", tc.ID)
		}
		if freshKind != "missing_function" {
			t.Fatalf("%s fresh reference kind = %q, want missing_function (APOC has no counterpart)", tc.ID, freshKind)
		}
		switch tc.Confidence {
		case "advertised_nornicdb":
			if tc.Reference.Kind != "value" {
				t.Fatalf("%s advertised extension expectation kind = %q, want value: the APOC absence must not replace it", tc.ID, tc.Reference.Kind)
			}
			if _, ok := nor282UtilOwners[tc.Direct.Function]; !ok {
				t.Fatalf("%s names unknown apoc/util owner %q", tc.ID, tc.Direct.Function)
			}
		case "pending":
			if tc.Reference.Kind != "ambiguous" {
				t.Fatalf("%s is pending, so its expectation must stay ambiguous", tc.ID)
			}
		default:
			t.Fatalf("%s confidence = %q, want advertised_nornicdb or pending", tc.ID, tc.Confidence)
		}
	}
	if checked != 6 {
		t.Fatalf("checked %d apoc/util cases, want 6", checked)
	}
}

// TestNOR282UtilReferenceParity asserts the advertised contract of the
// NornicDB extension peers. A failure is an advertised-contract defect, not an
// APOC parity claim.
func TestNOR282UtilReferenceParity(t *testing.T) {
	corpus := loadNOR282Corpus(t)
	ran := 0
	for _, tc := range corpus.Cases {
		if tc.Direct == nil || tc.Direct.Package != "util" {
			continue
		}
		ran++
		t.Run(tc.ID, func(t *testing.T) {
			if tc.Confidence == "pending" {
				t.Skipf("advertised contract is explicitly undecided: %v", tc.Flags)
			}
			if len(tc.Direct.Args) != 1 {
				t.Fatalf("want 1 direct argument, got %d", len(tc.Direct.Args))
			}
			owner, ok := nor282UtilOwners[tc.Direct.Function]
			if !ok {
				t.Fatalf("corpus names unknown apoc/util owner %q", tc.Direct.Function)
			}
			got := owner(tc.Direct.Args[0])
			want, wantKind := nor282ExpectedValue(t, tc.Reference)
			if wantKind != "value" {
				t.Fatalf("unsupported expectation kind %q", wantKind)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("apoc/util %s(%q) = %#v, want advertised %#v (confidence %s, flags %v)", tc.Direct.Function, tc.Direct.Args[0], got, want, tc.Confidence, tc.Flags)
			}
		})
	}
	if ran == 0 {
		t.Fatal("no direct apoc/util cases ran")
	}
}

// TestNOR282UtilObserved pins the current placeholder values.
func TestNOR282UtilObserved(t *testing.T) {
	corpus := loadNOR282Corpus(t)
	for _, tc := range corpus.Cases {
		if tc.Direct == nil || tc.Direct.Package != "util" {
			continue
		}
		t.Run(tc.ID, func(t *testing.T) {
			if len(tc.Direct.Args) != 1 {
				t.Fatalf("want 1 direct argument, got %d", len(tc.Direct.Args))
			}
			owner, ok := nor282UtilOwners[tc.Direct.Function]
			if !ok {
				t.Fatalf("corpus names unknown apoc/util owner %q", tc.Direct.Function)
			}
			got := owner(tc.Direct.Args[0])
			want, wantKind := nor282ExpectedValue(t, tc.Observed)
			if wantKind == "" {
				t.Fatalf("no nornicdb_observed recorded for %s: owner %s returned %#v", tc.ID, tc.Owner, got)
			}
			if wantKind != "value" || !reflect.DeepEqual(got, want) {
				t.Fatalf("apoc/util %s(%q) = %#v, recorded NornicDB value %#v", tc.Direct.Function, tc.Direct.Args[0], got, want)
			}
		})
	}
}

func nor282ExpectedValue(t *testing.T, expectation nor282Expectation) (interface{}, string) {
	t.Helper()
	if expectation.Kind == "" {
		return nil, ""
	}
	if expectation.Kind != "value" {
		return nil, expectation.Kind
	}
	if len(expectation.Value) == 0 || string(expectation.Value) == "null" {
		return nil, "value"
	}
	var value interface{}
	if err := json.Unmarshal(expectation.Value, &value); err != nil {
		t.Fatalf("decode expectation value %s: %v", expectation.Value, err)
	}
	return value, "value"
}
