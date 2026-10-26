package cyphertext

import (
	"reflect"
	"testing"
)

func TestReaderFoundationParameterContract(t *testing.T) {
	for _, test := range []struct {
		text string
		want []string
	}{
		{"RETURN $\u00a0x, $\u2003x, $\u0085x", nil},
		{"RETURN $café\u00a0AS x, $0, $a$b, $`a``b`", []string{"$café", "$0", "$a$b", "$`a``b`"}},
		{"RETURN $all(['A']), $any(['B']), $(x), a$b, $p", []string{"$p"}},
		{"RETURN '$hidden', `$name`, /* $comment */ $界 // $tail", []string{"$界"}},
		{"RETURN $`open", nil},
	} {
		t.Run(test.text, func(t *testing.T) {
			var got []string
			reader := NewReader(test.text, 0)
			previous := 0
			for token, ok := reader.Next(); ok; token, ok = reader.Next() {
				if token.Start < previous || token.End <= token.Start || token.End > len(test.text) {
					t.Fatalf("invalid source span: %+v after %d", token, previous)
				}
				previous = token.End
				if token.Kind == Parameter {
					got = append(got, token.Text(test.text))
				}
			}
			if !reflect.DeepEqual(test.want, got) {
				t.Fatalf("parameters %q, want %q", got, test.want)
			}
		})
	}
}

func TestReaderFoundationNameExtents(t *testing.T) {
	for _, text := range []string{"café\u00a0x", "界$9\u2003x", "` a``b ` rest", "_a$0,"} {
		name, ok := ScanIdentifier(text, 0)
		reader := NewReader(text, 0)
		token, present := reader.Next()
		if !ok || !present || token.Start != name.Start || token.End != name.End || !BraceEntryName(text, token) {
			t.Fatalf("%q: identifier %+v and token %+v disagree", text, name, token)
		}
	}
}
