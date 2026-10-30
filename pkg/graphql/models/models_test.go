package models

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestJSONMarshalAndUnmarshal(t *testing.T) {
	var buf bytes.Buffer
	JSON(nil).MarshalGQL(&buf)
	if got := buf.String(); got != "null" {
		t.Fatalf("expected null for nil JSON, got %q", got)
	}

	buf.Reset()
	j := JSON{"k": "v", "n": float64(1)}
	j.MarshalGQL(&buf)
	if !strings.Contains(buf.String(), "\"k\":\"v\"") {
		t.Fatalf("expected marshaled json object, got %q", buf.String())
	}

	var parsed JSON
	if err := (&parsed).UnmarshalGQL(map[string]interface{}{"ok": true}); err != nil {
		t.Fatalf("unmarshal map failed: %v", err)
	}
	if parsed["ok"] != true {
		t.Fatalf("expected parsed map value true, got %#v", parsed["ok"])
	}

	if err := (&parsed).UnmarshalGQL(`{"name":"alice"}`); err != nil {
		t.Fatalf("unmarshal string json failed: %v", err)
	}
	if parsed["name"] != "alice" {
		t.Fatalf("expected parsed name alice, got %#v", parsed["name"])
	}

	if err := (&parsed).UnmarshalGQL("not-json"); err == nil {
		t.Fatalf("expected invalid json string error")
	}
	if err := (&parsed).UnmarshalGQL(123); err == nil {
		t.Fatalf("expected invalid type error")
	}
	if err := (&parsed).UnmarshalGQL(nil); err != nil {
		t.Fatalf("unmarshal nil should succeed: %v", err)
	}
	if parsed != nil {
		t.Fatalf("expected parsed JSON to be nil after nil input")
	}
}

func TestFloatArrayMarshalAndUnmarshal(t *testing.T) {
	var buf bytes.Buffer
	FloatArray(nil).MarshalGQL(&buf)
	if got := buf.String(); got != "null" {
		t.Fatalf("expected null for nil FloatArray, got %q", got)
	}

	buf.Reset()
	FloatArray{1.5, 2}.MarshalGQL(&buf)
	if got := buf.String(); got != "[1.5,2]" {
		t.Fatalf("expected marshaled float array, got %q", got)
	}

	var arr FloatArray
	items := []interface{}{float64(1.25), float32(2.5), int(3), int64(4), json.Number("5.75")}
	if err := (&arr).UnmarshalGQL(items); err != nil {
		t.Fatalf("unmarshal mixed numeric types failed: %v", err)
	}
	if len(arr) != 5 || arr[4] != float32(5.75) {
		t.Fatalf("unexpected parsed float array: %#v", arr)
	}

	if err := (&arr).UnmarshalGQL(`[0.1,0.2,0.3]`); err != nil {
		t.Fatalf("unmarshal json string failed: %v", err)
	}
	if len(arr) != 3 {
		t.Fatalf("expected 3 values after string parse, got %d", len(arr))
	}

	if err := (&arr).UnmarshalGQL([]interface{}{"bad"}); err == nil {
		t.Fatalf("expected invalid element type error")
	}
	if err := (&arr).UnmarshalGQL([]interface{}{json.Number("nope")}); err == nil {
		t.Fatalf("expected invalid json.Number error")
	}
	if err := (&arr).UnmarshalGQL(true); err == nil {
		t.Fatalf("expected invalid top-level type error")
	}
	if err := (&arr).UnmarshalGQL(nil); err != nil {
		t.Fatalf("unmarshal nil should succeed: %v", err)
	}
	if arr != nil {
		t.Fatalf("expected nil array after nil input")
	}
}

func TestDateTimeScalars(t *testing.T) {
	var buf bytes.Buffer
	MarshalDateTime(time.Time{}, &buf)
	if got := buf.String(); got != "null" {
		t.Fatalf("expected null for zero time, got %q", got)
	}

	buf.Reset()
	tm := time.Unix(1700000000, 0).UTC()
	MarshalDateTime(tm, &buf)
	if got := buf.String(); got != `"2023-11-14T22:13:20Z"` {
		t.Fatalf("unexpected marshaled datetime: %q", got)
	}

	if got, err := UnmarshalDateTime("2023-11-14T22:13:20Z"); err != nil || !got.Equal(tm) {
		t.Fatalf("expected parsed RFC3339 time, got %v, err %v", got, err)
	}
	if got, err := UnmarshalDateTime(tm); err != nil || !got.Equal(tm) {
		t.Fatalf("expected passthrough time, got %v, err %v", got, err)
	}
	if got, err := UnmarshalDateTime(int64(1700000000)); err != nil || !got.Equal(tm) {
		t.Fatalf("expected unix time parse, got %v, err %v", got, err)
	}
	if _, err := UnmarshalDateTime(false); err == nil {
		t.Fatalf("expected invalid datetime type error")
	}
	if got, err := UnmarshalDateTime(nil); err != nil || !got.IsZero() {
		t.Fatalf("expected nil datetime to return zero time, got %v, err %v", got, err)
	}
}

func TestEnumsValidationUnmarshalAndMarshal(t *testing.T) {
	tests := []struct {
		name         string
		validValue   string
		invalidValue string
		newEnum      func() interface {
			IsValid() bool
			String() string
			MarshalGQL(w interface{ Write([]byte) (int, error) })
		}
		unmarshal func(v interface{}) error
		marshal   func() string
	}{
		{
			name:         "RelationshipDirection",
			validValue:   "BOTH",
			invalidValue: "SIDEWAYS",
			newEnum: func() interface {
				IsValid() bool
				String() string
				MarshalGQL(w interface{ Write([]byte) (int, error) })
			} {
				e := RelationshipDirection("BOTH")
				return &enumAdapter{isValid: e.IsValid, str: e.String, marshal: func(w interface{ Write([]byte) (int, error) }) { e.MarshalGQL(w) }}
			},
			unmarshal: func(v interface{}) error {
				var e RelationshipDirection
				return (&e).UnmarshalGQL(v)
			},
			marshal: func() string {
				var b bytes.Buffer
				RelationshipDirectionIncoming.MarshalGQL(&b)
				return b.String()
			},
		},
		{
			name:         "SearchSortBy",
			validValue:   "RELEVANCE",
			invalidValue: "RANDOM",
			newEnum: func() interface {
				IsValid() bool
				String() string
				MarshalGQL(w interface{ Write([]byte) (int, error) })
			} {
				e := SearchSortBy("RELEVANCE")
				return &enumAdapter{isValid: e.IsValid, str: e.String, marshal: func(w interface{ Write([]byte) (int, error) }) { e.MarshalGQL(w) }}
			},
			unmarshal: func(v interface{}) error {
				var e SearchSortBy
				return (&e).UnmarshalGQL(v)
			},
			marshal: func() string {
				var b bytes.Buffer
				SearchSortByDecayScore.MarshalGQL(&b)
				return b.String()
			},
		},
		{
			name:         "SearchMethod",
			validValue:   "VECTOR",
			invalidValue: "FUZZY",
			newEnum: func() interface {
				IsValid() bool
				String() string
				MarshalGQL(w interface{ Write([]byte) (int, error) })
			} {
				e := SearchMethod("VECTOR")
				return &enumAdapter{isValid: e.IsValid, str: e.String, marshal: func(w interface{ Write([]byte) (int, error) }) { e.MarshalGQL(w) }}
			},
			unmarshal: func(v interface{}) error {
				var e SearchMethod
				return (&e).UnmarshalGQL(v)
			},
			marshal: func() string {
				var b bytes.Buffer
				SearchMethodBm25.MarshalGQL(&b)
				return b.String()
			},
		},
		{
			name:         "SortOrder",
			validValue:   "ASC",
			invalidValue: "UP",
			newEnum: func() interface {
				IsValid() bool
				String() string
				MarshalGQL(w interface{ Write([]byte) (int, error) })
			} {
				e := SortOrder("ASC")
				return &enumAdapter{isValid: e.IsValid, str: e.String, marshal: func(w interface{ Write([]byte) (int, error) }) { e.MarshalGQL(w) }}
			},
			unmarshal: func(v interface{}) error {
				var e SortOrder
				return (&e).UnmarshalGQL(v)
			},
			marshal: func() string {
				var b bytes.Buffer
				SortOrderDesc.MarshalGQL(&b)
				return b.String()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.unmarshal(tc.validValue); err != nil {
				t.Fatalf("expected valid unmarshal for %s: %v", tc.validValue, err)
			}
			if err := tc.unmarshal(tc.invalidValue); err == nil {
				t.Fatalf("expected invalid value %q to fail", tc.invalidValue)
			}
			if err := tc.unmarshal(123); err == nil {
				t.Fatalf("expected non-string enum unmarshal to fail")
			}
			if got := tc.marshal(); !strings.HasPrefix(got, "\"") || !strings.HasSuffix(got, "\"") {
				t.Fatalf("expected marshaled enum to be quoted, got %q", got)
			}
		})
	}
}

type enumAdapter struct {
	isValid func() bool
	str     func() string
	marshal func(w interface{ Write([]byte) (int, error) })
}

func (e *enumAdapter) IsValid() bool  { return e.isValid() }
func (e *enumAdapter) String() string { return e.str() }
func (e *enumAdapter) MarshalGQL(w interface{ Write([]byte) (int, error) }) {
	e.marshal(w)
}
