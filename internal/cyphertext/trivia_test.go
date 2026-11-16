package cyphertext

import (
	"strings"
	"testing"
)

func TestTriviaAdmissionSpans(t *testing.T) {
	for _, tc := range []struct {
		text     string
		end, bad int
	}{
		{" /* incomplete", 14, 1}, {"/*/", 3, 0}, {"/* ok */", 8, -1}, {"// EOF", 6, -1}, {"\u2003/* ok */RETURN", 11, -1},
	} {
		t.Run(tc.text, func(t *testing.T) {
			end, bad := GapEnd(tc.text, 0)
			if end != tc.end || bad != tc.bad {
				t.Fatalf("gap=(%d,%d), want (%d,%d)", end, bad, tc.end, tc.bad)
			}
		})
	}
	for _, tc := range []struct {
		text  string
		found bool
		bad   int
	}{
		{"RETURN '/* incomplete;';", true, -1}, {"RETURN `/* incomplete;`", false, -1}, {"RETURN 1 /* incomplete", false, 9}, {"RETURN 1 // /* incomplete", false, -1},
	} {
		t.Run(tc.text, func(t *testing.T) {
			index, bad := FindOutside(tc.text, ";")
			found := index >= 0
			if found != tc.found || bad != tc.bad {
				t.Fatalf("outside=(%v,%d), want (%v,%d)", found, bad, tc.found, tc.bad)
			}
		})
	}
	source := " \t/* ok */ // line\nRETURN 1"
	if end, bad := GapEnd(source, 0); end != strings.Index(source, "RETURN") || bad != -1 {
		t.Fatalf("mixed trivia=(%d,%d)", end, bad)
	}
}

func TestFindOutsideReturnsMatchPosition(t *testing.T) {
	for _, tc := range []struct {
		text, needle string
		want, bad    int
	}{
		{"(n:`a[b`) /* [ */ [r:T]", "[", 18, -1},
		{"'[ignored]'", "[", -1, -1},
		{"// [\nRETURN 1", "[", -1, -1},
		{"RETURN 1 /* incomplete", "[", -1, 9},
		{"[ok] /* incomplete", "[", 0, -1},
	} {
		got, bad := FindOutside(tc.text, tc.needle)
		if got != tc.want || bad != tc.bad {
			t.Errorf("%q got(%d,%d), want(%d,%d)", tc.text, got, bad, tc.want, tc.bad)
		}
	}
}
