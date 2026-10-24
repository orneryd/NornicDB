package cyphertext

import (
	"testing"
)

// Parameter references are found outside string literals, quoted names and
// comments; a '$' inside a name, and a dynamic label, are not references.
func TestParameterReferences(t *testing.T) {
	for _, tt := range []struct {
		text string
		want []string
	}{
		{"RETURN 1", nil},
		{"RETURN $a, $b1 + $_c", []string{"a:$a", "b1:$b1", "_c:$_c"}},
		{"RETURN '$a', \"$b\", `$c`, /* $d */ $e // $f", []string{"e:$e"}},
		{"RETURN $`my p`, $`a``b`", []string{"my p:$`my p`", "a`b:$`a``b`"}},
		{"MATCH (n:$all(['A'])) RETURN a$b, $0", []string{"0:$0"}},
		{"RETURN $", nil},
		{"RETURN $`open", nil},
	} {
		var got []string
		ParameterReferences(tt.text, func(name string, start, end int) {
			got = append(got, name+":"+tt.text[start:end])
		})
		if len(got) != len(tt.want) {
			t.Fatalf("%q: got %v, want %v", tt.text, got, tt.want)
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Fatalf("%q: got %v, want %v", tt.text, got, tt.want)
			}
		}
	}
}
