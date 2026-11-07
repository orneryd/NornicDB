package label

import (
	"testing"
	"unicode/utf8"
)

func TestReleaseNOR290UnicodeNormalize(t *testing.T) {
	got := Normalize("élan")
	if !utf8.ValidString(got) || got != "Élan" {
		t.Fatalf("got %q bytes=%x", got, got)
	}
}
