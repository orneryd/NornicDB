package convert_test

import (
	"encoding/json"
	"reflect"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/convert"
)

// marshalDomains lists the public writers that share the compiled field plan and
// the opaque-value error path, so the contract is proven for every consumer of
// the owner rather than for one entry point.
var marshalDomains = []struct {
	name    string
	marshal func(interface{}) ([]byte, error)
}{
	{"property", convert.PropertyJSON.Marshal},
	{"input", convert.CypherInputJSON.Marshal},
}

func TestJSONUnsupportedFormatPriority(t *testing.T) {
	type Left struct {
		Deep int `json:"deep,format:bad"`
	}
	type Right struct {
		Peer int `json:"peer,format:bad"`
	}
	type Deeper struct{ Left }
	for _, value := range []any{
		struct {
			Left
			Root int `json:"root,format:bad"`
		}{},
		struct {
			Left
			Right
		}{},
		struct {
			Deeper
			Right
		}{},
	} {
		_, expected := json.Marshal(value)
		if expected == nil {
			t.Fatal("fixture must reject its format tag")
		}
		for _, domain := range marshalDomains {
			_, actual := domain.marshal(value)
			if actual == nil || reflect.TypeOf(actual) != reflect.TypeOf(expected) || actual.Error() != expected.Error() {
				t.Fatalf("%s %T: got %T %v, want %T %v", domain.name, value, actual, actual, expected, expected)
			}
			want := expected.(*json.UnsupportedValueError)
			got := actual.(*json.UnsupportedValueError)
			if got.Value.IsValid() != want.Value.IsValid() {
				t.Fatal("format error changed its reflected-value contract")
			}
		}
	}
}

// taggedIntEnvelope returns a pointer to a one-field struct whose exported int
// field V carries text as its decoded `json` tag value. The type is built at run
// time so one table can cover the tag grammar encoding/json accepts, recovers
// from, and ignores without declaring a type per tag spelling.
func taggedIntEnvelope(text string, value int64) interface{} {
	typ := reflect.StructOf([]reflect.StructField{{
		Name: "V",
		Type: reflect.TypeOf(value),
		Tag:  reflect.StructTag("json:" + strconv.Quote(text)),
	}})
	envelope := reflect.New(typ)
	envelope.Elem().Field(0).SetInt(value)
	return envelope.Interface()
}

// TestJSONFieldTagMatchesStandardLibrary holds the compiled field plan to
// encoding/json's own struct-tag rule, which owns the JSON object name and the
// omitempty/omitzero/string options. A malformed tag is not a private decision:
// encoding/json's default (v1) options recover from it, so the plan must emit
// the same member name and the same options for valid, recoverable, and
// rejected tag text. want pins the canonical output where the release depends on
// a specific name rather than only on parity with the control.
func TestJSONFieldTagMatchesStandardLibrary(t *testing.T) {
	cases := []struct {
		name, tag string
		value     int64
		want      string
	}{
		{name: "empty-tag", tag: "", value: 1, want: `{"V":1}`},
		{name: "ignored", tag: "-", value: 1, want: `{}`},
		{name: "plain", tag: "name", value: 1, want: `{"name":1}`},
		{name: "punctuation", tag: "$%-/", value: 1, want: `{"$%-/":1}`},
		{name: "inner-space", tag: "with space", value: 1, want: `{"with space":1}`},
		{name: "leading-space", tag: " name", value: 1, want: `{" name":1}`},
		{name: "trailing-space", tag: "name ", value: 1, want: `{"name ":1}`},
		{name: "non-ascii-letter", tag: "é", value: 1, want: `{"é":1}`},
		{name: "non-ascii-symbol", tag: "€", value: 1, want: `{"€":1}`},
		{name: "tab", tag: "a\tb", value: 1, want: `{"a\tb":1}`},
		{name: "invalid-utf8", tag: "\xff", value: 1, want: "{\"\ufffd\":1}"},
		{name: "comma-only", tag: ",", value: 1, want: `{"V":1}`},
		{name: "dash-comma", tag: "-,", value: 1, want: `{"-":1}`},
		{name: "trailing-comma", tag: "name,", value: 1, want: `{"name":1}`},
		{name: "superfluous-comma", tag: "name,,omitempty", value: 1, want: `{"name":1}`},
		{name: "single-quoted", tag: "'name'", value: 1, want: `{"V":1}`},
		// The Go 1.27 regression: encoding/json re-reads a name that stops at a
		// reserved character as a tag option, so the leading identifier names the
		// member. A private validator instead fell back to the Go field name.
		{name: "malformed-backslash", tag: `invalid\name`, value: 1, want: `{"invalid":1}`},
		{name: "malformed-backslash-zero", tag: `invalid\name`, value: 0, want: `{"invalid":0}`},
		{name: "malformed-double-quote", tag: `a"b`, value: 1, want: `{"a":1}`},
		{name: "malformed-quote-only", tag: `"b`, value: 1, want: `{"V":1}`},
		{name: "malformed-backtick", tag: "a`b", value: 1, want: `{"a":1}`},
		{name: "malformed-single-quote", tag: "a'b", value: 1, want: `{"a":1}`},
		// Options survive the same recovery: the malformed span is skipped and a
		// later supported option still applies.
		{name: "malformed-backslash-omitempty", tag: `invalid\name,omitempty`, value: 1, want: `{"invalid":1}`},
		{name: "malformed-backslash-omitempty-zero", tag: `invalid\name,omitempty`, value: 0, want: `{}`},
		{name: "malformed-backslash-before-comma", tag: `invalid\,omitempty`, value: 0, want: `{}`},
		{name: "malformed-backslash-string", tag: `invalid\name,string`, value: 1, want: `{"invalid":"1"}`},
		{name: "omitempty", tag: "name,omitempty", value: 0, want: `{}`},
		{name: "omitzero", tag: "name,omitzero", value: 0, want: `{}`},
		{name: "string", tag: "name,string", value: 1, want: `{"name":"1"}`},
		{name: "string-omitempty", tag: "name,string,omitempty", value: 0, want: `{}`},
		{name: "unknown-option", tag: "name,unknownoption", value: 0, want: `{"name":0}`},
		{name: "invalid-option-appearance", tag: "name,omitEmpty", value: 0, want: `{"name":0}`},
		// Options this writer does not implement stay unimplemented; they must not
		// change the member name or the supported options around them.
		{name: "unimplemented-inline", tag: "name,inline", value: 0, want: `{"name":0}`},
		{name: "unimplemented-case", tag: "name,case:ignore", value: 0, want: `{"name":0}`},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			value := taggedIntEnvelope(test.tag, test.value)
			expected, err := json.Marshal(value)
			if err != nil {
				t.Fatalf("control failed for tag %q: %v", test.tag, err)
			}
			for _, domain := range marshalDomains {
				actual, err := domain.marshal(value)
				if err != nil {
					t.Fatalf("%s writer failed for tag %q: %v", domain.name, test.tag, err)
				}
				if string(actual) != string(expected) {
					t.Fatalf("%s tag %q: owned %s, encoding/json %s", domain.name, test.tag, actual, expected)
				}
				if test.want != "" && string(actual) != test.want {
					t.Fatalf("%s tag %q: owned %s, want %s", domain.name, test.tag, actual, test.want)
				}
			}
		})
	}
}

// TestJSONFieldTagOptionSemantics pins the supported option semantics on a
// declared envelope, where the name, omission and string selection of each field
// are visible together, and holds the whole document to the standard control.
func TestJSONFieldTagOptionSemantics(t *testing.T) {
	type envelope struct {
		Kept      int     `json:"kept"`
		Empty     int     `json:"empty,omitempty"`
		Zero      int     `json:"zero,omitzero"`
		Both      int     `json:"both,omitempty,omitzero"`
		Quoted    int     `json:"quoted,string"`
		Whole     float64 `json:"whole,string"`
		Text      string  `json:"text,string"`
		Unknown   int     `json:"unknown,unknownoption"`
		Trailing  int     `json:"trailing,"`
		Hidden    int     `json:"-"`
		Malformed int     `json:"invalid\\name,omitempty"`
	}
	zero := envelope{}
	wantZero := `{"kept":0,"quoted":"0","whole":"0","text":"\"\"","unknown":0,"trailing":0}`
	populated := envelope{Kept: 1, Empty: 2, Zero: 3, Both: 4, Quoted: 5, Whole: 2, Text: "x", Unknown: 6, Trailing: 7, Hidden: 8, Malformed: 9}
	wantPopulated := `{"kept":1,"empty":2,"zero":3,"both":4,"quoted":"5","whole":"2","text":"\"x\"","unknown":6,"trailing":7,"invalid":9}`
	for _, test := range []struct {
		name  string
		value envelope
		want  string
	}{
		{"zero", zero, wantZero},
		{"populated", populated, wantPopulated},
	} {
		t.Run(test.name, func(t *testing.T) {
			expected, err := json.Marshal(test.value)
			if err != nil {
				t.Fatal(err)
			}
			if string(expected) != test.want {
				t.Fatalf("control changed: encoding/json wrote %s, want %s", expected, test.want)
			}
			for _, domain := range marshalDomains {
				actual, err := domain.marshal(test.value)
				if err != nil {
					t.Fatal(err)
				}
				if string(actual) != test.want {
					t.Fatalf("%s envelope: owned %s, want %s", domain.name, actual, test.want)
				}
			}
		})
	}
}

// numberEnvelope carries json.Number at the positions a document reaches: root
// value, object member, nested member, list element and map value.
type numberEnvelope struct {
	Number  json.Number
	Nested  struct{ Number json.Number }
	List    []json.Number
	Map     map[string]json.Number
	Pointer *json.Number
}

func numberEnvelopeFor(number json.Number) numberEnvelope {
	return numberEnvelope{
		Number:  number,
		Nested:  struct{ Number json.Number }{Number: number},
		List:    []json.Number{number},
		Map:     map[string]json.Number{"number": number},
		Pointer: &number,
	}
}

// TestJSONNumberErrorMatchesStandardLibrary pins the numeric rejection of an
// unusable json.Number lexeme to the standard numeric owner at every position
// the writer reaches. The caller sees one numeric error contract: the standard
// *json.MarshalerError, not a private alternative.
func TestJSONNumberErrorMatchesStandardLibrary(t *testing.T) {
	const wantError = `json: error calling MarshalJSONTo for type *json.Number: cannot parse "invalid" as JSON number: invalid syntax`
	invalid := json.Number("invalid")
	for _, test := range []struct {
		name  string
		value interface{}
	}{
		{"root", invalid},
		{"nested-interface", []interface{}{invalid}},
		{"envelope", numberEnvelopeFor(invalid)},
		{"list", []json.Number{invalid}},
		{"map", map[string]json.Number{"number": invalid}},
		{"pointer", &invalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, expected := json.Marshal(test.value)
			if expected == nil {
				t.Fatalf("control accepted %T", test.value)
			}
			for _, domain := range marshalDomains {
				_, actual := domain.marshal(test.value)
				if actual == nil {
					t.Fatalf("%s writer accepted %T", domain.name, test.value)
				}
				if reflect.TypeOf(actual) != reflect.TypeOf(expected) || actual.Error() != expected.Error() {
					t.Fatalf("%s owned %T %v, encoding/json %T %v", domain.name, actual, actual, expected, expected)
				}
				if test.name == "root" {
					if _, ok := actual.(*json.MarshalerError); !ok || actual.Error() != wantError {
						t.Fatalf("%s root json.Number: owned %T %v, want *json.MarshalerError %s", domain.name, actual, actual, wantError)
					}
				}
			}
		})
	}
}

// TestJSONNumberLexemesMatchStandardLibrary proves the correction is confined to
// the rejection: every usable lexeme, including a whole FLOAT, keeps the exact
// text it had before, at the same positions.
func TestJSONNumberLexemesMatchStandardLibrary(t *testing.T) {
	lexemes := []string{"0", "-0", "2.0", "0.125", "1E2", "-1.5e-8", "9007199254740993", "1e1000", ""}
	for _, lexeme := range lexemes {
		number := json.Number(lexeme)
		for _, test := range []struct {
			name  string
			value interface{}
		}{
			{"root", number},
			{"envelope", numberEnvelopeFor(number)},
			{"list", []json.Number{number}},
			{"map", map[string]json.Number{"number": number}},
		} {
			t.Run(lexeme+"/"+test.name, func(t *testing.T) {
				expected, err := json.Marshal(test.value)
				if err != nil {
					t.Fatalf("control rejected %q: %v", lexeme, err)
				}
				for _, domain := range marshalDomains {
					actual, err := domain.marshal(test.value)
					if err != nil {
						t.Fatalf("%s writer rejected %q: %v", domain.name, lexeme, err)
					}
					if string(actual) != string(expected) {
						t.Fatalf("%s %q: owned %s, encoding/json %s", domain.name, lexeme, actual, expected)
					}
				}
			})
		}
	}
}

// TestJSONFormatTagOptionBoundaries holds the `format` tag value's own span to
// encoding/json: the v1 options accept a single-quoted Go string, validate its
// escapes with strconv, and use the decoded value as the format. A non-empty
// value makes the standard encoder reject the whole struct before writing any
// member, so the owned writer must return the same error type and message; an
// empty, unterminated or invalidly escaped value is an ignored malformed tag
// and must not change the name or any other option. Commas and quotes inside
// the value never become field options.
func TestJSONFormatTagOptionBoundaries(t *testing.T) {
	const unsupported = "json: unsupported value: Go struct field V has unsupported \x60format\x60 tag option"
	cases := []struct {
		name, tag, want, wantError string
		value                      int64
	}{
		// Quoted values own their commas: omitempty inside the value is not a flag.
		{name: "quoted-comma-omitempty", tag: `v,format:'foo,omitempty'`, value: 0, wantError: unsupported},
		{name: "quoted-comma-omitzero-string", tag: `v,format:'a,omitzero',string`, value: 0, wantError: unsupported},
		// An unquoted value is the whole option; it is non-empty and rejected.
		{name: "unquoted", tag: "v,format:omitempty", value: 0, wantError: unsupported},
		// Options after a non-empty format are still consumed, but the rejection
		// precedes field serialization, so omission and quoting never apply.
		{name: "format-then-omitempty", tag: `v,format:'a',omitempty`, value: 0, wantError: unsupported},
		{name: "format-then-string", tag: `v,format:'a',string`, value: 0, wantError: unsupported},
		{name: "omitzero-then-format", tag: `v,omitzero,format:'a'`, value: 0, wantError: unsupported},
		// Go escape validation belongs to strconv.Unquote, including escaped quotes.
		{name: "escaped-single-quote", tag: `v,format:'a\'b'`, value: 0, wantError: unsupported},
		{name: "escaped-double-quote", tag: `v,format:'a\"b'`, value: 0, wantError: unsupported},
		// Malformed format values are ignored by the v1 options, exactly like every
		// other malformed tag span: the value is skipped and the surviving options
		// still apply.
		{name: "empty-value", tag: `v,format:''`, value: 0, want: `{"v":0}`},
		{name: "empty-value-then-string", tag: `v,format:'',string`, value: 0, want: `{"v":"0"}`},
		{name: "missing-value", tag: "v,format:", value: 0, want: `{"v":0}`},
		{name: "missing-colon", tag: "v,format", value: 0, want: `{"v":0}`},
		{name: "invalid-escape", tag: `v,format:'a\q'`, value: 0, want: `{"v":0}`},
		{name: "unterminated-quote", tag: `v,format:'abc`, value: 0, want: `{"v":0}`},
		{name: "invalid-escape-comma", tag: `v,format:'a\q,omitempty'`, value: 0, want: `{}`},
		// `case` takes a value too; the value is consumed, never a field flag, and
		// later options still apply.
		{name: "case-value", tag: "v,case:omitempty", value: 0, want: `{"v":0}`},
		{name: "case-then-omitzero", tag: "v,case:ignore,omitzero", value: 0, want: `{}`},
		{name: "case-then-string", tag: "v,case:strict,string", value: 1, want: `{"v":"1"}`},
		{name: "quoted-case-value", tag: `v,case:'ignore',omitempty`, value: 0, want: `{}`},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			value := taggedIntEnvelope(test.tag, test.value)
			expected, expectedErr := json.Marshal(value)
			if test.wantError != "" {
				if expectedErr == nil || expectedErr.Error() != test.wantError {
					t.Fatalf("control for tag %q returned %v, want %s", test.tag, expectedErr, test.wantError)
				}
			} else if expectedErr != nil {
				t.Fatalf("control for tag %q failed: %v", test.tag, expectedErr)
			}
			for _, domain := range marshalDomains {
				actual, err := domain.marshal(value)
				if (err == nil) != (expectedErr == nil) {
					t.Fatalf("%s tag %q: owned %s err=%v, encoding/json %s err=%v", domain.name, test.tag, actual, err, expected, expectedErr)
				}
				if err != nil {
					if reflect.TypeOf(err) != reflect.TypeOf(expectedErr) || err.Error() != expectedErr.Error() {
						t.Fatalf("%s tag %q: owned %T %v, encoding/json %T %v", domain.name, test.tag, err, err, expectedErr, expectedErr)
					}
					continue
				}
				if string(actual) != string(expected) {
					t.Fatalf("%s tag %q: owned %s, encoding/json %s", domain.name, test.tag, actual, expected)
				}
				if test.want != "" && string(actual) != test.want {
					t.Fatalf("%s tag %q: owned %s, want %s", domain.name, test.tag, actual, test.want)
				}
			}
		})
	}
}

// TestJSONUnsupportedFormatPrecedesFieldSerialization proves the rejection is the
// standard one and happens before any member, not merely at the offending field:
// a preceding custom marshaler must not run, matching makeStructArshaler's check
// of fields.errUnsupportedFormat before its field loop.
func TestJSONUnsupportedFormatPrecedesFieldSerialization(t *testing.T) {
	calls := 0
	type envelope struct {
		First countingJSONMarshaler `json:"first"`
		V     int                   `json:"v,format:'a,b'"`
	}
	value := envelope{First: countingJSONMarshaler{calls: &calls}, V: 1}
	expected, expectedErr := json.Marshal(value)
	if expectedErr == nil {
		t.Fatal("control accepted an unsupported format")
	}
	for _, domain := range marshalDomains {
		calls = 0
		actual, err := domain.marshal(value)
		if err == nil || err.Error() != expectedErr.Error() || reflect.TypeOf(err) != reflect.TypeOf(expectedErr) {
			t.Fatalf("%s: owned %s err=%v, encoding/json %s err=%v", domain.name, actual, err, expected, expectedErr)
		}
		if calls != 0 {
			t.Fatalf("%s: %d custom marshal calls before the unsupported format rejection", domain.name, calls)
		}
	}
}

// countingJSONMarshaler records that the standard owner reached a member.
type countingJSONMarshaler struct{ calls *int }

func (marshaler countingJSONMarshaler) MarshalJSON() ([]byte, error) {
	(*marshaler.calls)++
	return []byte(`"counted"`), nil
}

var tagMarshalSink []byte

// BenchmarkJSONFieldTagMarshal measures the compiled field-plan hot path with a
// tag the plan implements, so the format-value correction is shown to add no
// allocation to an accepted struct.
func BenchmarkJSONFieldTagMarshal(b *testing.B) {
	type envelope struct {
		Count int `json:"count,omitzero"`
	}
	value := envelope{Count: 1}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, err := convert.PropertyJSON.Marshal(value)
		if err != nil {
			b.Fatal(err)
		}
		tagMarshalSink = data
	}
}
