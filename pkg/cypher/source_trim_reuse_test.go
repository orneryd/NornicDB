package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestUnchangedSourceTrimReusesRetainedMapping(t *testing.T) {
	for _, raw := range []string{"RETURN $value AS value", "RETURN \uFF11 AS value"} {
		ctx, source := cyphertext.WithSource(context.Background(), raw)
		for _, policy := range []sourceTrimPolicy{trimSourceWhitespace, trimSourceTerminator} {
			got, text := trimSourceText(ctx, source.Text, policy)
			require.Equal(t, source.Text, text)
			retained, ok := cyphertext.RetainedSource(got, text, cyphertext.AcceptedSource)
			require.True(t, ok)
			for offset := 0; offset <= len(text); offset++ {
				want, valid := source.OriginalOffset(offset, true, cyphertext.SourceExtent)
				actual, mapped := retained.OriginalOffset(offset, true, cyphertext.SourceExtent)
				require.Equal(t, valid, mapped)
				require.Equal(t, want, actual)
			}
			require.Zero(t, testing.AllocsPerRun(100, func() { _, _ = trimSourceText(ctx, source.Text, policy) }))
		}
	}
}

func TestStatementClockReadReusesOwnedInstant(t *testing.T) {
	exec, _ := newTestExecutor(t)
	instant := time.Date(2026, 10, 29, 12, 34, 56, 789, time.UTC)
	ctx := exec.newStatementExecutionContext(context.Background(), instant, nil)
	require.Equal(t, instant, exec.temporalClockTime(ctx, "statement"))
	require.Zero(t, testing.AllocsPerRun(100, func() {
		if got := exec.temporalClockTime(ctx, "statement"); !got.Equal(instant) {
			t.Fatal(got)
		}
	}))
}
