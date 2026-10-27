package cyphertext

import (
	"fmt"
	"testing"
	"unicode/utf8"
)

func TestLexicalUnicodeCursors(t *testing.T) {
	for _, r := range []rune{'\t', '\n', '\v', '\f', '\r', ' ', '\u0085', '\u00a0', '\u1680', '\u2000', '\u2001', '\u2002', '\u2003', '\u2004', '\u2005', '\u2006', '\u2007', '\u2008', '\u2009', '\u200a', '\u2028', '\u2029', '\u202f', '\u205f', '\u3000'} {
		t.Run(fmt.Sprintf("U%04X", r), func(t *testing.T) {
			gap := string(r)
			before, after, found := CutWhitespace("left" + gap + "right")
			if !found || before != "left" || after != "right" {
				t.Fatal("source separator split incorrectly")
			}
			if ValidNameStart(r) || ValidNamePart(r) {
				t.Fatal("whitespace admitted as a valid name")
			}
			for _, text := range []string{gap + "PASSWORD" + gap + "value", "é" + gap + "PASSWORD" + gap + "`value`"} {
				start := PasswordBoundary(text)
				if start < 0 || !KeywordBoundary(text, start, start+len("PASSWORD")) {
					t.Fatal("keyword boundary missing")
				}
			}
			if WhitespaceAt(gap, 0) != len(gap) {
				t.Fatal("incorrect whitespace width")
			}
			for index := range len(gap) {
				if !IsWhitespaceAt(gap, index) || NameStartAt(gap, index) || NamePartAt(gap, index) {
					t.Fatal("byte cursor split whitespace")
				}
			}
		})
	}
	for _, name := range []string{"a", "_", "é", "界", "𐐀", "a$b"} {
		for index := range len(name) {
			if !NamePartAt(name, index) {
				t.Fatal("name continuation missing")
			}
		}
	}
	for _, text := range []string{"éPASSWORD x", "PASSWORDé x", "xPASSWORD x", "PASSWORD$x"} {
		if PasswordBoundary(text) >= 0 {
			t.Fatal("keyword matched inside a name")
		}
	}
	for _, index := range []int{-1, 0, 1} {
		if NamePartAt("", index) || NameStartAt("", index) || IsWhitespaceAt("", index) || WhitespaceAt("", index) != 0 {
			t.Fatal("invalid cursor classified")
		}
	}
	if NameStart('1') || !NamePart('1') || NameStart('$') || !NamePart('$') || NameStart(utf8.RuneSelf-1) {
		t.Fatal("name class mismatch")
	}
}
