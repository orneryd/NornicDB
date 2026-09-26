package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStripComments: comments go, quoted text stays (a URL, a /* in a string
// or a quoted name); a statement without comments is returned as is.
func TestStripComments(t *testing.T) {
	for query, want := range map[string]string{
		"RETURN 'http://x.test/a' AS u":              "RETURN 'http://x.test/a' AS u",
		"RETURN 1 // one":                            "RETURN 1 ",
		"RETURN /* c */ 1":                           "RETURN   1",
		"RETURN 'a/*b*/c' AS `x//y` // end\nLIMIT 1": "RETURN 'a/*b*/c' AS `x//y` \nLIMIT 1",
		"// only a comment":                          "",
	} {
		require.Equal(t, want, StripComments(query), query)
	}
	query := "MATCH (n) RETURN n"
	require.Zero(t, testing.AllocsPerRun(100, func() { _ = StripComments(query) }))
}

// TestFabricLocalShardExecutorsWithoutExplicitTransaction: outside an
// explicit transaction a Fabric statement keeps its sub-transaction
// executors itself; inside one they are the transaction's.
func TestFabricLocalShardExecutorsWithoutExplicitTransaction(t *testing.T) {
	c := &cypherFabricExecutor{base: NewStorageExecutor(newTestMemoryEngine(t))}
	own := c.localShardTxExecutors()
	require.NotNil(t, own)
	own["s"] = c.base
	require.Same(t, c.base, c.localShardTxExecutors()["s"])

	c.base.txContext = &TransactionContext{active: true}
	shared := c.localShardTxExecutors()
	require.Empty(t, shared)
	require.NotNil(t, c.base.txContext.fabricLocalTxExec)
}
