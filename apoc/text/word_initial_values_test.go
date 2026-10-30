// Author: Codex implementation lane for NOR-207, 2026-10-26.
package text_test

import (
	"strings"
	"testing"

	apoctext "github.com/orneryd/nornicdb/apoc/text"
)

// Expected values follow APOC 5.26.30 Strings.java: split on ASCII space,
// titlecase/uncapitalize the first code point, then Java String.trim().
func TestWordInitialValuesNOR207(t *testing.T) {
	for _, tc := range []struct{ name, input, capitalized, decapitalized string }{
		{"lower", "hello world", "Hello World", "hello world"},
		{"upper", "HELLO WORLD", "HELLO WORLD", "hELLO wORLD"},
		{"mixed", "hELLO wORLD", "HELLO WORLD", "hELLO wORLD"},
		{"spaces", "  hello   World  ", "Hello   World", "hello   world"},
		{"empty", "", "", ""},
		{"only_spaces", "   ", "", ""},
		{"tab_inside", "hello\tworld", "Hello\tworld", "hello\tworld"},
		{"line_inside", "HELLO\nWORLD", "HELLO\nWORLD", "hELLO\nWORLD"},
		{"leading_control", "\thello world\n", "hello World", "hello world"},
		{"control_segment", "hello \tworld", "Hello \tworld", "hello \tworld"},
		{"outer_controls", "\x00hello world\x1f", "hello World", "hello world"},
		{"punctuation", "hello-world foo.bar 'word", "Hello-world Foo.bar 'word", "hello-world foo.bar 'word"},
		{"nonbreaking_space", "hello\u00a0world", "Hello\u00a0world", "hello\u00a0world"},
		{"nonbreaking_outer", "\u00a0hello world\u00a0", "\u00a0hello World\u00a0", "\u00a0hello world\u00a0"},
		{"latin", "élan Über", "Élan Über", "élan über"},
		{"titlecase", "ǳungla ǆungla", "ǲungla ǅungla", "ǳungla ǆungla"},
		{"simple_mapping", "ßeta İSTANBUL", "ßeta İSTANBUL", "ßeta iSTANBUL"},
		{"supplementary", "𐐨abc 𐐀XYZ", "𐐀abc 𐐀XYZ", "𐐨abc 𐐨XYZ"},
		{"combining_mark", "\u0301hello World", "\u0301hello World", "\u0301hello world"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := apoctext.CapitalizeAll(tc.input); got != tc.capitalized {
				t.Errorf("CapitalizeAll(%q) = %q, want %q", tc.input, got, tc.capitalized)
			}
			if got := apoctext.DecapitalizeAll(tc.input); got != tc.decapitalized {
				t.Errorf("DecapitalizeAll(%q) = %q, want %q", tc.input, got, tc.decapitalized)
			}
		})
	}
}

var wordInitialBenchmarkValue string

func TestDecapitalizeValuesNOR207(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"HELLO WORLD", "hELLO WORLD"}, {"ǅungla World", "ǆungla World"},
		{"𐐀XYZ", "𐐨XYZ"}, {"", ""},
	} {
		t.Run(tc.input, func(t *testing.T) {
			if got := apoctext.Decapitalize(tc.input); got != tc.want {
				t.Errorf("Decapitalize(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

// Plain capitalize uses the upper-case string mapping of the first UTF-16
// code unit, unlike capitalizeAll's code-point titlecase mapping.
func TestCapitalizeValuesNOR207(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"hello world", "Hello world"},
		{"hELLO wORLD", "HELLO wORLD"},
		{" hello world", " hello world"},
		{"élan Über", "Élan Über"},
		{"ǳungla world", "Ǳungla world"},
		{"ßeta world", "SSeta world"},
		{"ﬃeld world", "FFIeld world"},
		{"𐐨abc world", "𐐨abc world"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			if got := apoctext.Capitalize(tc.input); got != tc.want {
				t.Errorf("Capitalize(%q) = %q, want %q (APOC 5.26.30 Strings.java:514)", tc.input, got, tc.want)
			}
		})
	}
}

func BenchmarkWordInitialValuesNOR207(b *testing.B) {
	for _, tc := range []struct{ name, input string }{
		{"short", "hello World"}, {"large", strings.Repeat("élan HELLO world  ", 4096)},
	} {
		for _, operation := range []struct {
			name string
			fn   func(string) string
		}{
			{"capitalize", apoctext.Capitalize}, {"capitalizeAll", apoctext.CapitalizeAll}, {"decapitalizeAll", apoctext.DecapitalizeAll},
		} {
			b.Run(tc.name+"/"+operation.name, func(b *testing.B) {
				b.SetBytes(int64(len(tc.input)))
				b.ReportAllocs()
				for b.Loop() {
					wordInitialBenchmarkValue = operation.fn(tc.input)
				}
			})
		}
	}
}
