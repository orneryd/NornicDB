package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPR1682CallScopeOffsetPreservesAdmission(t *testing.T) {
	for _, suffix := range []string{
		"CALL (x) { RETURN x AS value } RETURN value",
		"CALL /* gap */ (*) { RETURN x AS value } RETURN value",
		"CALL db.labels() YIELD label RETURN label",
		"CALL custom.proc({value: 1}) YIELD value RETURN value",
		"CALL (x + 1) { RETURN 1 AS value } RETURN value",
		"CALL (x",
		"CALL (x) { RETURN x AS value } RETURN +",
		"OPTIONAL CALL (x) { RETURN x AS value } RETURN value",
	} {
		t.Run(suffix, func(t *testing.T) {
			const prefix = "WITH 1 AS x "
			source := prefix + suffix
			want := readCallSubqueryScope(t.Context(), suffix, 0)
			if want.bodyOffset != 0 {
				want.bodyOffset += len(prefix)
			}
			got := readCallSubqueryScope(t.Context(), source, len(prefix))
			require.Equal(t, want, got, "whole-source admission must preserve the former suffix decision")
		})
	}
	for _, source := range []string{
		"WITH 1 AS x CALL (x) { RETURN x AS value } RETURN value",
		"WITH 1 AS x OPTIONAL CALL (x) { RETURN x AS value } RETURN value",
	} {
		from := strings.Index(source, "CALL")
		require.Equal(t, from, firstTopLevelCallSubquery(t.Context(), source))
		frame := readCallSubqueryScope(t.Context(), source, from)
		require.True(t, frame.scoped)
		require.Equal(t, byte('{'), source[frame.bodyOffset])
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.Equal(t, -1, firstTopLevelCallSubquery(ctx, "WITH 1 AS x CALL (x) { RETURN x AS value } RETURN value"))
}
