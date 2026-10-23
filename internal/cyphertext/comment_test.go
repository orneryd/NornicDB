package cyphertext

import (
	"fmt"
	"strings"
	"testing"
)

func TestCommentCompletionFacts(t *testing.T) {
	for _, test := range []struct {
		name, source string
		start, end   int
		unfinished   bool
	}{
		{"empty block", "/**/", 0, 4, false},
		{"closed at EOF", "x /* complete */", 2, 16, false},
		{"block before token", "/* ' ` // */RETURN", 0, 12, false},
		{"unfinished at EOF", "x /* incomplete", 2, 15, true},
		{"short unfinished", "/*", 0, 2, true},
		{"overlapping delimiters", "/*/", 0, 3, true},
		{"non nesting", "/* outer /* inner */ RETURN", 0, 20, false},
		{"line at EOF", "// /* ' `", 0, 9, false},
		{"line before CRLF", "// end\r\nRETURN", 0, 6, false},
		{"line before LF", "// end\nRETURN", 0, 6, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			span, ok := ScanComment(test.source, test.start)
			if !ok || span != (Token{Start: test.start, End: test.end, Kind: Comment, Unterminated: test.unfinished}) {
				t.Fatalf("comment=%+v found=%v", span, ok)
			}
			if got := CommentEnd(test.source, test.start); got != span.End {
				t.Fatalf("compatibility endpoint=%d span=%+v", got, span)
			}
			reader := NewReader(test.source, test.start)
			if token, ok := reader.NextRaw(); !ok || token != span {
				t.Fatalf("reader=%+v found=%v comment=%+v", token, ok, span)
			}
			at, bad := reader.UnterminatedAt()
			if bad != test.unfinished || bad && at != span.Start {
				t.Fatalf("reader completion=%d,%v span=%+v", at, bad, span)
			}
		})
	}
	for _, test := range []struct {
		source string
		at     int
	}{{"", 0}, {"/", 0}, {"/x", 0}, {"/**/", -1}, {"/**/", 4}, {"x", 0}} {
		if span, ok := ScanComment(test.source, test.at); ok || span != (Token{}) || CommentEnd(test.source, test.at) != -1 {
			t.Fatalf("non-comment %q at %d classified as %+v,%v", test.source, test.at, span, ok)
		}
	}
}

func TestTriviaCompletionAndLosslessSpans(t *testing.T) {
	for _, test := range []struct {
		source                 string
		start, end, unfinished int
	}{
		{"", 0, 0, -1},
		{"RETURN", 0, 0, -1},
		{" \t/**/ // end\r\nRETURN", 0, 15, -1},
		{" \t/**/ // end", 0, 13, -1},
		{" /* closed */ /* open", 0, 21, 14},
		{"CALL \t/* open", 4, 13, 6},
		{" \u2003'/* open'", 0, 4, -1},
		{" \u2003`/* open`", 0, 4, -1},
	} {
		span := ScanTrivia(test.source, test.start)
		if span != (TriviaSpan{Start: test.start, End: test.end, UnterminatedComment: test.unfinished}) {
			t.Fatalf("trivia %q=%+v", test.source, span)
		}
		if got := SkipTrivia(test.source, test.start); got != span.End {
			t.Fatalf("compatibility endpoint=%d span=%+v", got, span)
		}
	}
	for _, tail := range []string{"// complete", "/* unfinished", "/* complete */"} {
		source := "\u2003RETURN '/* unfinished' AS `// unfinished` /* trivia */ " + tail
		reader := NewReader(source, 0)
		var rebuilt strings.Builder
		end, comments, stringsSeen, names := 0, 0, 0, 0
		for span, ok := reader.NextRaw(); ok; span, ok = reader.NextRaw() {
			if span.Start != end || span.End <= span.Start || span.End > len(source) {
				t.Fatalf("non-contiguous span=%+v after %d", span, end)
			}
			rebuilt.WriteString(span.Text(source))
			end = span.End
			switch span.Kind {
			case Comment:
				comments++
			case String:
				stringsSeen++
			case QuotedName:
				names++
			}
		}
		if rebuilt.String() != source || end != len(source) || comments != 2 || stringsSeen != 1 || names != 1 {
			t.Fatalf("lossless/opacity failed: source=%q rebuilt=%q comments=%d strings=%d names=%d", source, rebuilt.String(), comments, stringsSeen, names)
		}
		at, unfinished := reader.UnterminatedAt()
		wantUnfinished := tail == "/* unfinished"
		if unfinished != wantUnfinished || unfinished && at != len(source)-len(tail) {
			t.Fatalf("comment completion=%d,%v tail=%q", at, unfinished, tail)
		}
	}
}

func ExampleScanTrivia() {
	source := " /* complete */ /* unfinished"
	trivia := ScanTrivia(source, 0)
	fmt.Println(trivia.End == len(source), trivia.UnterminatedComment)
	// Output: true 16
}
