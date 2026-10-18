package cypher

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// A comprehension owns what its elements built: a map's list counts in the
// comprehension, whose builder gave its own reservation back. A value the
// elements share, or one the input holds, counts once (#1343).
func TestComprehensionsCountWhatTheirElementsHold(t *testing.T) {
	resetTransactionMemory(t)
	executor := newTransactionMemoryExecutor(t)
	ctx := context.Background()
	big := make([]interface{}, 100000)
	for i := range big {
		big[i] = int64(i)
	}
	list := make([]interface{}, 2000)
	for i := range list {
		list[i] = []interface{}{int64(i), "element"}
	}
	for _, compiled := range []bool{true, false} {
		name := "text"
		if compiled {
			name = "compiled"
		}
		t.Run(name, func(t *testing.T) {
			rowValueProgramsDisabled.Store(!compiled)
			t.Cleanup(func() { rowValueProgramsDisabled.Store(false) })

			ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 20})
			for _, query := range []string{
				"RETURN size([i IN range(1, 2000) | {xs: range(i, i + 9999)}]) AS n",
				// Fewer elements than the old check interval of 1,024.
				"RETURN size([i IN range(1, 1000) | {xs: range(i, i + 9999)}]) AS n",
				"RETURN size([i IN range(1, 1000) | [range(i, i + 9999)]]) AS n",
			} {
				_, err := executor.Execute(ctx, query, nil)
				requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
				requirePoolsEmpty(t, "nornic")
			}

			// Shared and input values count once: these fit 4 MiB.
			ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 4 << 20})
			for query, want := range map[string]int64{
				"RETURN size([i IN range(1, 2000) | {xs: $big}]) AS n": 2000,
				"RETURN size([x IN $list | x]) AS n":                   2000,
				"RETURN size([x IN $list | {pair: x}]) AS n":           2000,
			} {
				result, err := executor.Execute(ctx, query, map[string]interface{}{"big": big, "list": list})
				require.NoError(t, err, query)
				require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
				requirePoolsEmpty(t, "nornic")
			}
		})
	}
}

// A MATCH's candidate nodes count while the match holds them, from an index
// seek or a label scan, with one cached list per property value, also when
// the rows built from them don't stay (count(*)); they are given back when
// the statement completes, fails or is cancelled (#1343).
func TestNodeMatchCandidatesCountWhileHeld(t *testing.T) {
	resetTransactionMemory(t)
	executor := newTransactionMemoryExecutor(t)
	ctx := context.Background()
	for _, setup := range []string{
		"CREATE INDEX item_p FOR (n:Item) ON (n.p)",
		"UNWIND range(1, 50000) AS i CREATE (:Item {p: 1})",
		"UNWIND range(1, 50000) AS i CREATE (:Scan {p: 1})",
		"UNWIND range(1, 30000) AS i CREATE (:Multi {p: 1 + i % 2})",
	} {
		_, err := executor.Execute(ctx, setup, nil)
		require.NoError(t, err, setup)
	}

	for _, test := range []struct {
		name, query string
		count       int64
	}{
		{"index seek", "UNWIND [1] AS i MATCH (n:Item {p: i}) RETURN count(*) AS n", 50000},
		{"label scan", "UNWIND [1] AS i MATCH (n:Scan {p: i}) RETURN count(*) AS n", 50000},
	} {
		t.Run(test.name, func(t *testing.T) {
			ConfigureTransactionMemory(0, TransactionMemoryLimits{})
			result, err := executor.Execute(ctx, test.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{test.count}}, result.Rows)

			ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 20})
			_, err = executor.Execute(ctx, test.query, nil)
			requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
			requirePoolsEmpty(t, "nornic")

			// Within the limit, what the candidates took goes back when the
			// statement completes.
			ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 64 << 20})
			result, err = executor.Execute(ctx, test.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{test.count}}, result.Rows)
			requirePoolsEmpty(t, "nornic")
		})
	}

	t.Run("cached property values", func(t *testing.T) {
		// 15,000 candidates per value: one list fits 1.5 MiB, two don't.
		ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 3 << 19})
		result, err := executor.Execute(ctx, "UNWIND [1] AS i MATCH (n:Multi {p: i}) RETURN count(*) AS n", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(15000)}}, result.Rows)
		requirePoolsEmpty(t, "nornic")
		_, err = executor.Execute(ctx, "UNWIND [1, 2] AS i MATCH (n:Multi {p: i}) RETURN count(*) AS n", nil)
		requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
		requirePoolsEmpty(t, "nornic")
	})

	t.Run("cancelled", func(t *testing.T) {
		ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 64 << 20})
		cancelled, cancel := context.WithCancel(ctx)
		defer cancel()
		var once sync.Once
		sizingObserver = func() { once.Do(cancel) }
		t.Cleanup(func() { sizingObserver = nil })
		_, err := executor.Execute(cancelled, "UNWIND [1] AS i MATCH (n:Item {p: i}) RETURN count(*) AS n", nil)
		sizingObserver = nil
		require.Error(t, err)
		requirePoolsEmpty(t, "nornic")
	})
}

// The other builders that keep what their elements hold count it too: an
// UNWIND over a list the expression built, a reduce map accumulator, and
// collect() of values that share a list (#1343).
func TestBuildersCountWhatTheirElementsHold(t *testing.T) {
	resetTransactionMemory(t)
	executor := newTransactionMemoryExecutor(t)
	ctx := context.Background()
	big := make([]interface{}, 100000)
	for i := range big {
		big[i] = int64(i)
	}

	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 20})
	for _, query := range []string{
		// Each range fits the limit alone; the list literal's three don't.
		"UNWIND [{xs: range(1, 20000)}, {xs: range(2, 20001)}, {xs: range(3, 20002)}] AS m RETURN count(*) AS n",
		"RETURN size(keys(reduce(m = {}, i IN range(1, 20) | apoc.map.setKey(m, toString(i), range(i, i + 9999))))) AS n",
	} {
		_, err := executor.Execute(ctx, query, nil)
		requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
		requirePoolsEmpty(t, "nornic")
	}

	// A list the row or a parameter holds streams as its slots, and values
	// that share a list count it once.
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 32 << 20})
	for query, want := range map[string]int64{
		"UNWIND $big AS x RETURN count(*) AS n":                                          100000,
		"WITH $big AS xs UNWIND xs AS x RETURN count(*) AS n":                            100000,
		"UNWIND range(1, 1000) AS i WITH collect({xs: $big}) AS ms RETURN size(ms) AS n": 1000,
	} {
		result, err := executor.Execute(ctx, query, map[string]interface{}{"big": big})
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
		requirePoolsEmpty(t, "nornic")
	}
}
