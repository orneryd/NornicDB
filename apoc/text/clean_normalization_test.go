package text

import "testing"

func TestCleanPreservesWholeTextCaseMapping(t *testing.T) {
	for _, test := range []struct {
		name  string
		input string
		want  string
	}{
		{name: "ASCII controls", input: " A\tZ 42! ", want: "az42"},
		{name: "German replacements", input: "Ä Ö Ü ß É", want: "aeoeuesse"},
		{name: "final Greek sigma", input: "Ο-Σ!", want: "ος"},
		{name: "interior Greek sigma", input: "Ο-Σ Α", want: "οσα"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if actual := Clean(test.input); actual != test.want {
				t.Errorf("Clean(%q) = %q, want %q", test.input, actual, test.want)
			}
		})
	}
}
