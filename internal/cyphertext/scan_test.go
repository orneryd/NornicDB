package cyphertext

import "testing"

func TestSharedQuotedAndCommentTokens(t *testing.T) {
	for _, token := range []string{"'it\\'s }'", "'it''s }'", "\"say \\\"}\"", "`a``}b`", "/* ' } */", "// ' }"} {
		end, skipped := SkipToken(token+"\n RETURN 1", 0)
		if !skipped || end != len(token) {
			t.Errorf("%q: end=%d skipped=%t", token, end, skipped)
		}
	}
	for _, text := range []string{"'", "\"", "`", "'unterminated\\x"} {
		if end, closed := ScanQuotedText(text, 0); closed || end != len(text) {
			t.Errorf("%q: end=%d closed=%t", text, end, closed)
		}
	}
	if end, skipped := SkipToken("x", 0); skipped || end != 0 {
		t.Fatalf("plain code: %d %t", end, skipped)
	}
	if end, skipped := SkipToken("", 0); skipped || end != 0 {
		t.Fatalf("end of input: %d %t", end, skipped)
	}
	if end, closed := ScanQuotedText("x", 0); closed || end != 0 {
		t.Fatalf("plain code quote: %d %t", end, closed)
	}
	if end := CommentEnd("/* unclosed", 0); end != len("/* unclosed") {
		t.Fatalf("unclosed comment: %d", end)
	}
	if end := CommentEnd("/x", 0); end != -1 {
		t.Fatalf("division: %d", end)
	}
}
