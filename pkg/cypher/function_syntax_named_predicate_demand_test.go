package cypher

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil/functionsyntaxmatrix"
	"github.com/stretchr/testify/require"
)

func TestFunctionSyntaxNamedPredicateDemandAllocations(t *testing.T) {
	text := strings.Repeat("/* don't parse unused arguments , ( */ ", 600) + "coalesce(null,1)"
	require.Greater(t, len(text), 16384)
	for _, check := range []struct {
		name string
		run  func() bool
	}{
		{"predicate", func() bool { return isFunctionCallWS(text, "rand") }},
		{"arguments", func() bool { _, open := extractFuncArgsLen(text, "rand"); return open >= 0 }},
		{"suffix", func() bool { _, _, ok := extractFuncArgsWithSuffix(text, "rand"); return ok }},
	} {
		t.Run(check.name, func(t *testing.T) {
			require.False(t, check.run())
			require.Zero(t, testing.AllocsPerRun(10, func() {
				if check.run() {
					t.Fatal("different function matched")
				}
			}))
		})
	}
}

func TestFunctionSyntaxNamedPredicateDemand(t *testing.T) {
	texts := []string{"`coalesce` /* gap */ (null,1)", "db /* gap */ . labels()", "`a``b`(1)", "coalesce(null,1) + 2", "coalesce(null,1)[0]", "count (1) /* tail */", "count\u00a0(1)", "'count(1)'", "coalesce(1", "CALL count(1)"}
	for _, sample := range functionsyntaxmatrix.Cases() {
		texts = append(texts, sample.Query)
	}
	for _, prefix := range []string{"", "/* " + strings.Repeat("trivia ", 2400) + " */ "} {
		for _, sample := range texts {
			text := prefix + sample
			for _, name := range []string{"coalesce", "count", "db.labels", "a`b", "rand"} {
				call, found := functionCallAt(text, queryGapEnd(text, 0))
				matched := found && call.asciiNameSpacing() && strings.EqualFold(call.name, name)
				closed := matched && call.close(text) >= 0
				standalone := closed && queryGapEnd(text, call.close(text)+1) == len(text)
				require.Equal(t, standalone, isFunctionCallWS(text, name), text+" / "+name)
				_, open := extractFuncArgsLen(text, name)
				require.Equal(t, standalone, open >= 0, text+" / "+name)
				_, _, suffix := extractFuncArgsWithSuffix(text, name)
				require.Equal(t, closed, suffix, text+" / "+name)
			}
		}
	}
}

func TestFunctionSyntaxNamedSetDemand(t *testing.T) {
	for _, prefix := range []string{"", "/* " + strings.Repeat("padding ", 2400) + " */ "} {
		for _, text := range []string{
			"RETURN coalesce(null,1)", "COUNT { RETURN sum(1) AS n }", "RETURN `SUM`(1)",
			"RETURN 'count(1)' /* SUM(2) */", "RETURN custom.sum(1)", "RETURN ſum(1)",
			"RETURN count(1", "CALL sum(1) YIELD x RETURN x", "RETURN count /* gap */ (1)",
		} {
			text = prefix + text
			names := []string{"COUNT", "SUM", "AVG", "MIN", "MAX", "COLLECT"}
			want := false
			for _, name := range names {
				want = want || containsFunctionCallNamed(text, name)
			}
			require.Equal(t, want, lexicalHasAnyFunction(text, names), text)
		}
	}
}

func TestFunctionSyntaxArgumentsAtDemand(t *testing.T) {
	for _, prefix := range []string{"", "/* " + strings.Repeat("padding ", 2400) + " */ "} {
		for _, sample := range []string{
			"RETURN date.truncate('day',date('2020-01-02'))",
			"CALL db.labels() YIELD label RETURN label",
			"RETURN coalesce(/* don't split , ) */ null, [1,2], {x:'a,b'})",
			"RETURN coalesce(null,1", "RETURN coalesce([1,2),3)",
		} {
			text := prefix + sample
			syntax := readFunctionSyntax(text)
			for _, call := range syntax.calls {
				if call.open < 0 {
					continue
				}
				got, closed := functionArgumentsAt(text, call.open)
				require.Equal(t, call.close(text) >= 0, closed, text)
				if closed {
					require.Equal(t, call.arguments(nil), got, text)
				}
			}
		}
	}
}
