package cyphertext

import (
	"strings"
	"testing"
)

func TestCommentReplacementPolicies(t *testing.T) {
	for _, input := range []struct{ source, want string }{
		{"// line", ""}, {"// line\r\n", ""}, {"/* plain */", " "},
		{"/* a\r\nb\rc\n */", " \r\n\r\n"}, {"/* π\u2028\u2029 */", " "},
		{"/* unfinished\n", " \n"}, {"", " "},
	} {
		for _, interrupted := range []func() bool{nil, func() bool { return false }} {
			got, ok := CommentReplacement(input.source, interrupted)
			if !ok || got != input.want {
				t.Fatalf("comment projection = (%q,%v), want (%q,true)", got, ok, input.want)
			}
		}
	}
	for _, source := range []string{"// private", "/* private */", "/*" + strings.Repeat("x", 8192) + "\n*/"} {
		if got, ok := CommentReplacement(source, func() bool { return true }); ok || got != "" {
			t.Fatal("canceled projection exposed partial text")
		}
	}
	checks := 0
	got, ok := CommentReplacement("/*"+strings.Repeat("x\n", 8192)+"*/", func() bool { checks++; return checks >= 3 })
	if ok || got != "" || checks != 3 {
		t.Fatal("large comment did not stop at its bounded cancellation checkpoint")
	}
}
