package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var groupWalkInputs = []string{
	"[1, 2,]", "[, 1]", "[1, /* c */ ]", "[/* c */, 1]", "[1, [2, ], 3]", "[[1,2],[3,]]", "[1, (2, ), 3]",
	"[1 , ]", "['a', ]", "[1, 2", "[1, 2)]", "{a: [1,], b: 2}", "[1,,2]", "[(a), (b)]",
	"(a), (b)", "$('A'), $any(['B']), (c)", "(a {s:') , ('})-[:R]->(b)", "/* (fake), */ (a), // (fake),\n(b)", "(a", "x",
	"[x], (a)", "(a)-[r]->(b), $(x) (c)",
	"n.a = 1, n.b = [1, 2], n.c = f(1, 2),", "n.a = 1,, n.b = 2", " , ", "", "a /* , */ b, c",
	"x[0]", "x[0].y", "x.y[0]", "f(x)[1]", "x[0] [1]", "x.`y`", "1.5", "x[0].1", "x.y + 1", "a[0] /* c */", "(a.b).c",
	"[x IN xs | x]", "reduce(s = 0, x IN xs | s + x)", "[x IN xs WHERE x > 1 | x] + 1", "{a: [x IN xs | x]}",
	"__nornic_haslabels(n, ['A'], 1) AND $(x)",
}

func groupWalkTestInputs() []string {
	inputs := append(append([]string{}, groupWalkInputs...), callCursorTestInputs()...)
	return inputs
}

func splitNodePatternsReference(pattern string) []string {
	syntax := uncachedFunctionSyntax(pattern)
	parents := groupParents(syntax)
	var patterns []string
	punctuation := 0
	dynamic := false
	for index, group := range syntax.groups[1:] {
		if parents[index+1] != 0 {
			continue
		}
		for punctuation < len(syntax.rootPunctuation) && syntax.rootPunctuation[punctuation] < group.open {
			dynamic = dynamic || dynamicLabelStartsAt(pattern, syntax.rootPunctuation[punctuation])
			punctuation++
		}
		if pattern[group.open] != '(' {
			continue
		}
		if dynamic {
			dynamic = false
			continue
		}
		end := group.close + 1
		if group.close < 0 {
			end = len(pattern)
		}
		patterns = append(patterns, pattern[group.open:end])
	}
	return patterns
}

// groupParents recovers each retained group's enclosing group: the latest
// earlier group still open at its opening.
func groupParents(syntax *functionSyntax) []int {
	parents := make([]int, len(syntax.groups))
	parents[0] = -1
	for index := 1; index < len(syntax.groups); index++ {
		parents[index] = 0
		for outer := index - 1; outer > 0; outer-- {
			if group := syntax.groups[outer]; group.close < 0 || group.close > syntax.groups[index].open {
				parents[index] = outer
				break
			}
		}
	}
	return parents
}

func containsTrailingListCommaReference(cypher string) bool {
	syntax := uncachedFunctionSyntax(cypher)
	for index, group := range syntax.groups {
		if group.open < 0 || group.close < 0 || cypher[group.open] != '[' || group.argumentEnd-group.argumentStart <= 1 {
			continue
		}
		spans := syntax.argumentSpans(index)
		if syntax.render(spans[0]) == "" || syntax.render(spans[len(spans)-1]) == "" {
			return true
		}
	}
	return false
}

// The group walk and its readers replace lookups in the retained groups,
// root punctuation and argument spans. Each must give the facts' answer.
func TestFunctionSyntaxGroupWalkMatchesFacts(t *testing.T) {
	executor := &StorageExecutor{}
	for _, input := range groupWalkTestInputs() {
		for _, text := range []string{input, paddedReaderInput(input)} {
			syntax := uncachedFunctionSyntax(text)
			// Every group's close, and the first opening from each code offset.
			for _, group := range syntax.groups[1:] {
				require.Equal(t, group.close, lexicalGroupClose(text, group.open), "close %d: %q", group.open, input)
				want, got := []string(nil), []string(nil)
				for _, keepEmpty := range []bool{false, true} {
					if group.close >= 0 {
						want = syntax.appendArguments(nil, syntax.groupIndex(group.open), keepEmpty)
					}
					var closed bool
					got, closed = lexicalGroupArguments(nil, text, group.open, keepEmpty)
					require.Equal(t, group.close >= 0, closed, "closed %d: %q", group.open, input)
					require.Equal(t, want, got, "arguments %d %v: %q", group.open, keepEmpty, input)
				}
				for _, opening := range []byte{'(', '[', '{'} {
					require.Equal(t, syntax.firstGroup(opening, group.open), lexicalFirstOpening(text, opening, group.open), "first %c %d: %q", opening, group.open, input)
				}
			}
			for _, keepEmpty := range []bool{false, true} {
				got, closed := lexicalGroupArguments(nil, text, -1, keepEmpty)
				require.True(t, closed)
				require.Equal(t, syntax.appendArguments(nil, 0, keepEmpty), got, "root arguments %v: %q", keepEmpty, input)
			}
			require.Equal(t, syntax.firstTopLevelByte('|'), rowTopLevelPipeIndex(text), "pipe: %q", input)
			require.Equal(t, splitNodePatternsReference(text), executor.splitNodePatterns(text), "nodes: %q", input)
			require.Equal(t, containsTrailingListCommaReference(text), containsTrailingListComma(text), "list comma: %q", input)
		}
	}
}

// groupIndex is the retained group opening at open.
func (syntax *functionSyntax) groupIndex(open int) int {
	for index, group := range syntax.groups {
		if group.open == open {
			return index
		}
	}
	return -1
}
