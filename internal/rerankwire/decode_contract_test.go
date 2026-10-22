package rerankwire

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"
)

type rerankWireFailingReader struct{}

func (rerankWireFailingReader) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

type rerankWireBrokenRequired struct{}

func (rerankWireBrokenRequired) rerankRequired() {}

func TestWireAcceptanceSupportBranches(t *testing.T) {
	var null Required[int]
	if err := json.Unmarshal([]byte("null"), &null); err != nil {
		t.Fatal(err)
	}
	if _, err := null.get(); err == nil {
		t.Fatal("null required value was accepted")
	}

	accepted, err := AcceptTyped([]TypedResult{{Index: 128, Score: -0.5}}, 129)
	if err != nil || len(accepted) != 1 || accepted[0].Index != 128 {
		t.Fatalf("large typed population: accepted=%+v err=%v", accepted, err)
	}
	for name, values := range map[string][]TypedResult{
		"negative_index": {{Index: -1, Score: 0.5}},
		"duplicate":      {{Index: 0, Score: 0.5}, {Index: 0, Score: 0.4}},
		"nan":            {{Index: 0, Score: math.NaN()}},
		"inf":            {{Index: 0, Score: math.Inf(1)}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := AcceptTyped(values, 2); err == nil {
				t.Fatal("invalid typed result was accepted")
			}
		})
	}
	if _, err := AcceptTyped(nil, 0); err == nil {
		t.Fatal("empty population was accepted")
	}
	if _, err := validateAcceptedResults(nil, 1, errors.New("provider failed")); err == nil {
		t.Fatal("provider error was discarded")
	}
	if _, err := accept(input{kind: inputKind(255)}); err == nil {
		t.Fatal("unknown provider representation was accepted")
	}
	if _, err := accept(ordered([]Required[int]{requiredValue(0)}, nil, true, 1)); err == nil {
		t.Fatal("mismatched scores were accepted")
	}
	if _, err := acceptEntries([]acceptanceEntry{{score: requiredValue(0.5), requireScore: true}}); err == nil {
		t.Fatal("missing index was accepted")
	}
	if _, err := acceptEntries([]acceptanceEntry{{index: requiredValue(0), requireScore: true}}); err == nil {
		t.Fatal("missing score was accepted")
	}
	synthesized, err := acceptEntries([]acceptanceEntry{{index: requiredValue(0)}})
	if err != nil || len(synthesized) != 1 || synthesized[0].Score != 1 {
		t.Fatalf("synthesized score: %+v err=%v", synthesized, err)
	}
	if _, err := acceptExclusive(
		typed([]TypedResult{{Index: 0, Score: 0.5}}),
		typed([]TypedResult{{Index: 1, Score: 0.4}}),
	); !errors.Is(err, errMultipleRepresentations) {
		t.Fatalf("multiple representations: %v", err)
	}
	if _, err := acceptExclusive(indexed([]IndexedResult{{}}, scoreFieldScore)); err == nil {
		t.Fatal("presence error was discarded")
	}
}

func TestLLMStructuredAndNonEnvelopeErrorBranches(t *testing.T) {
	for name, input := range map[string]string{
		"array_type":   `[null]`,
		"ranked_null":  `{"ranked":null}`,
		"order_null":   `{"order":null}`,
		"scores_null":  `{"order":[0],"scores":null}`,
		"scalar":       `null`,
		"dash":         `-`,
		"huge_integer": `999999999999999999999999999999999999999999999999999999999`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := AcceptLLM(input, 1); err == nil {
				t.Fatal("invalid model output was accepted")
			}
		})
	}
}

func TestLLMResponsesOutsideEnvelopeAreRejected(t *testing.T) {
	for _, input := range llmOutsideEnvelopeSeeds {
		t.Run(input, func(t *testing.T) {
			if _, err := AcceptLLM(input, 3); err == nil {
				t.Fatalf("response outside the one-value envelope grammar was accepted: %q", input)
			}
		})
	}
}

func TestEnvelopeAndBindingSupportBranches(t *testing.T) {
	var target struct {
		Order []int `json:"order"`
	}
	if err := DecodeResponse(rerankWireFailingReader{}, &target); err == nil {
		t.Fatal("reader error was discarded")
	}
	for name, input := range map[string]string{
		"empty":                 "",
		"fence_without_newline": "```json",
		"invalid_fence_tag":     "```json extra\n{}\n```",
		"invalid_fence_body":    "```json\n{\n```",
		"missing_close":         "```json\n{}",
		"invalid_prefix":        "null\n```json\n{}\n```",
		"invalid_suffix":        "```json\n{}\n``` []",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeEnvelope([]byte(input)); err == nil {
				t.Fatal("invalid envelope was accepted")
			}
		})
	}
	if raw, err := decodeExactJSON([]byte(`{"order":[0]}`)); err != nil || string(raw) != `{"order":[0]}` {
		t.Fatalf("exact JSON: %q err=%v", raw, err)
	}
	for _, input := range []string{`{"order":[0]} null`, `{"order":[0]} ,`} {
		if _, err := decodeExactJSON([]byte(input)); err == nil {
			t.Fatalf("invalid exact JSON accepted: %q", input)
		}
	}
	if _, _, err := decodeLeadingJSONValue([]byte("{")); err == nil {
		t.Fatal("truncated leading value was accepted")
	}
	for _, tag := range [][]byte{[]byte("json extra"), []byte("json`")} {
		if validFenceTag(tag) {
			t.Fatalf("invalid fence tag accepted: %q", tag)
		}
	}
	for _, prose := range [][]byte{[]byte("}"), []byte("42"), []byte("tru"), jsonBOM} {
		if validateEnvelopeProse(prose) == nil {
			t.Fatalf("invalid prose accepted: %q", prose)
		}
	}
	if err := validateEnvelopeProse([]byte("version2 is truly prose")); err != nil {
		t.Fatalf("ordinary prose rejected: %v", err)
	}

	if err := decodeBoundJSON([]byte(`{}`), nil); err == nil {
		t.Fatal("nil target was accepted")
	}
	if err := decodeBoundJSON([]byte(`{}`), target); err == nil {
		t.Fatal("non-pointer target was accepted")
	}
	var nilTarget *struct{}
	if err := decodeBoundJSON([]byte(`{}`), nilTarget); err == nil {
		t.Fatal("nil pointer target was accepted")
	}
	if err := decodeBoundJSON([]byte(`{} {}`), &target); err == nil {
		t.Fatal("second bound value was accepted")
	}
	if err := decodeBoundJSON([]byte(`{"order":"wrong"}`), &target); err == nil {
		t.Fatal("wrong bound type was accepted")
	}

	type bindingFixture struct {
		Hidden int
		Skip   int `json:"-"`
		Empty  int `json:",omitempty"`
		Value  int `json:"value"`
	}
	binding := bindingForType(reflect.TypeOf((**bindingFixture)(nil)))
	if binding == nil {
		t.Fatal("pointer binding was not built")
	}
	if _, _, ok := binding.resolveField("VALUE"); !ok {
		t.Fatal("folded field was not resolved")
	}
	if _, _, ok := binding.resolveField("unknown"); ok {
		t.Fatal("unknown field was resolved")
	}
	var nilBinding *jsonBinding
	if _, _, ok := nilBinding.resolveField("value"); ok {
		t.Fatal("nil binding resolved a field")
	}
	if buildBindingForType(reflect.TypeOf(rerankWireBrokenRequired{})) != nil {
		t.Fatal("broken required marker produced a binding")
	}
	if buildBindingForType(reflect.TypeOf(json.RawMessage{})) != nil {
		t.Fatal("custom JSON unmarshaler produced a structural binding")
	}
	if buildBindingForType(reflect.TypeOf([1]bindingFixture{})) == nil {
		t.Fatal("array binding was not built")
	}

	decoder := json.NewDecoder(bytes.NewReader([]byte("{")))
	if err := validateBoundJSONValue(decoder, binding); err == nil {
		t.Fatal("token error was discarded")
	}
}
