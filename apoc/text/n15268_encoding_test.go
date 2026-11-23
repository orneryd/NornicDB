package text

import (
	"github.com/orneryd/nornicdb/internal/jsontext"
	"os"
	"testing"
)

// These rows are actual outputs from the pinned Neo4j/APOC 5.26.30 owner,
// including raw lone UTF-16 units surrounding percent-encoded byte runs.
func TestN15268EncodingReference(t *testing.T) {
	data, err := os.ReadFile("testdata/n15268-encoding-reference.json")
	if err != nil {
		t.Fatal(err)
	}
	var rows []struct {
		ID        string   `json:"id"`
		Function  string   `json:"function"`
		Args      []string `json:"args"`
		Reference struct {
			Kind  string      `json:"kind"`
			Value interface{} `json:"value"`
		} `json:"reference"`
	}
	if err = jsontext.Unmarshal(data, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 91 {
		t.Fatalf("rows=%d want91", len(rows))
	}
	for _, row := range rows {
		t.Run(row.ID, func(t *testing.T) {
			var got interface{}
			var err error
			switch row.Function {
			case "urldecode":
				got, err = UrldecodeChecked(row.Args[0])
			case "base64Decode":
				got, err = Base64DecodeChecked(row.Args[0])
			case "urlencode":
				got = Urlencode(row.Args[0])
			case "base64Encode":
				got = Base64Encode(row.Args[0])
			case "compareCleaned":
				got = CompareCleaned(row.Args[0], row.Args[1])
			default:
				t.Fatalf("unknown function %s", row.Function)
			}
			if row.Reference.Kind == "function_error" {
				if err == nil {
					t.Fatalf("got %q without required error", got)
				}
				return
			}
			if err != nil || got != row.Reference.Value {
				t.Fatalf("got %q err=%v want %q", got, err, row.Reference.Value)
			}
		})
	}
}

func TestN15268ConvenienceSignatures(t *testing.T) {
	if CamelCase("") != "" || UpperCamelCase("") != "" {
		t.Fatal("empty legacy name conversion changed")
	}
	if Capitalize("") != "" {
		t.Fatal("legacy empty capitalize changed")
	}
	if _, err := CapitalizeChecked(""); err == nil {
		t.Fatal("registered empty capitalize must fail")
	}
	if Urldecode("%zz") != "" || Base64Decode("%%") != "" {
		t.Fatal("unchecked adapters must not return partial malformed results")
	}
	value, err := DecapitalizeNullable(nil)
	if value != nil || err != nil {
		t.Fatalf("null=%v,%v", value, err)
	}
	if _, err := DecapitalizeNullable(1); err == nil {
		t.Fatal("nonstring accepted")
	}
	for _, s := range []string{"", "plain", "a b+~/é😀"} {
		got, err := UrldecodeChecked(Urlencode(s))
		if err != nil || got != s {
			t.Fatalf("URL roundtrip %q=%q,%v", s, got, err)
		}
		got, err = Base64DecodeChecked(Base64Encode(s))
		if err != nil || got != s {
			t.Fatalf("base64 roundtrip %q=%q,%v", s, got, err)
		}
	}
}
