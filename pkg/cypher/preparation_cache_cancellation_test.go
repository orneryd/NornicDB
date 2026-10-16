package cypher

import (
	"context"
	"hash/maphash"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCanonicalRewriteMemoCancellation(t *testing.T) {
	query := "RETURN  '" + strings.Repeat("value", 8192) + "' AS value"
	_, rewrite := canonicalizeQueryText(query)
	require.NotNil(t, rewrite)
	for _, stage := range []string{"load", "store", "scan_hit", "scan_miss"} {
		t.Run(stage, func(t *testing.T) {
			runPipelineCancelPointStage(t, func(ctx context.Context) error {
				probe := pipelineCancellationForContext(ctx)
				memo := canonicalRewriteMemo{seed: maphash.MakeSeed()}
				switch stage {
				case "load":
					memo.storeWithCancellation(query, rewrite, nil)
					memo.loadWithCancellation(query, probe)
				case "store":
					memo.storeWithCancellation(query, rewrite, probe)
				case "scan_hit":
					canonicalRewrites.storeWithCancellation(query, rewrite, nil)
					canonicalizeQueryTextWithCancellation(query, probe)
				case "scan_miss":
					canonicalRewrites.slot(query).Store(nil)
					canonicalizeQueryTextWithCancellation(query, probe)
				}
				return probe.err
			})
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe := pipelineCancellationForContext(ctx)
	memo := canonicalRewriteMemo{seed: maphash.MakeSeed()}
	require.Nil(t, memo.loadWithCancellation(query, probe))
	memo.storeWithCancellation(query, rewrite, probe)
	require.Same(t, rewrite, memo.loadWithCancellation(query, probe))
	require.Same(t, rewrite, memo.loadWithCancellation(query, nil))
	memo.slot("RETURN other").Store(&queryRewrite{original: "different"})
	require.Nil(t, memo.loadWithCancellation("RETURN other", probe))
	require.Nil(t, memo.loadWithCancellation("RETURN other", nil))
	require.NoError(t, probe.err)
}

func TestRewriteFreeCacheCancellation(t *testing.T) {
	query := "RETURN " + strings.Repeat("name ", 16384)
	for _, stage := range []string{"get", "put"} {
		t.Run(stage, func(t *testing.T) {
			runPipelineCancelPointStage(t, func(ctx context.Context) error {
				probe := pipelineCancellationForContext(ctx)
				if stage == "get" {
					markRewriteFree(rewriteFreeFunctionAliases, false, query)
					rewriteFreeWithCancellation(rewriteFreeFunctionAliases, false, query, probe)
				} else {
					markRewriteFreeWithCancellation(rewriteFreeCypher25Expressions, true, query, probe)
				}
				return probe.err
			})
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe := pipelineCancellationForContext(ctx)
	for _, version := range []bool{false, true} {
		for _, pass := range []rewriteFreePass{rewriteFreeFunctionAliases, rewriteFreeCypher25Expressions, rewriteFreeQueryStructure} {
			markRewriteFreeWithCancellation(pass, version, "RETURN cache_mask", probe)
			require.True(t, rewriteFreeWithCancellation(pass, version, "RETURN cache_mask", probe))
		}
	}
	require.False(t, rewriteFreeWithCancellation(rewriteFreeQueryStructure, false, "RETURN absent_mask", probe))
	require.NoError(t, probe.err)
}

func TestFunctionAliasReaderCancellation(t *testing.T) {
	for _, query := range []string{
		"RETURN " + strings.Repeat("x", 65536),
		"RETURN _" + strings.Repeat("x", 65536),
		"RETURN '" + strings.Repeat("x", 65536) + "',ln(2)",
		"RETURN /*" + strings.Repeat("x", 65536) + "*/ ln(2)",
		"RETURN ln" + strings.Repeat(" ", 65536) + "(2)",
		"RETURN " + strings.Repeat("ln(2),", 1025) + "ln(2)",
		"RETURN property_exists(n,`" + strings.Repeat("a'\\", 4096) + "`)",
	} {
		runPipelineCancelPointStage(t, func(ctx context.Context) error {
			probe := pipelineCancellationForContext(ctx)
			canonicalizeFunctionAliasesWithCancellation(query, probe)
			return probe.err
		})
	}
}

func TestFunctionAliasReaderHealthyParity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for alias := range cypherFunctionAliases {
		require.LessOrEqual(t, len(alias), len("duration_between"), "keep native alias-map keys bounded")
	}
	for _, query := range []string{"RETURN 1", "RETURN ceiling(2), ln(2)", "RETURN property_exists(n,foo)", "RETURN property_exists(n,`a'\\b`)", "RETURN property_exists(n)", "RETURN property_exists(1,x)", "RETURN property_exists(n,x+1)", "RETURN n.ln(2),$ln,x.ln", "RETURN _not_alias(2),ln_value,ln", "RETURN 'ln(2)', `ln`, /* ln(2) */ ln(2)", "RETURN " + strings.Repeat("x", 8192) + "(1)"} {
		want, wantRewrite := canonicalizeFunctionAliases(query)
		probe := pipelineCancellationForContext(ctx)
		actual, rewrite := canonicalizeFunctionAliasesWithCancellation(query, probe)
		require.NoError(t, probe.err)
		require.Equal(t, want, actual)
		if wantRewrite == nil {
			require.Nil(t, rewrite)
		} else {
			require.Equal(t, wantRewrite.original, rewrite.original)
			require.Equal(t, wantRewrite.canonical, rewrite.canonical)
			require.Equal(t, wantRewrite.edits, rewrite.edits)
		}
	}
}
