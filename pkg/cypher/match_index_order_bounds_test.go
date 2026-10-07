package cypher

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestImpliedPropertyBounds(t *testing.T) {
	params := map[string]interface{}{"t": int64(5), "id": "x", "s": "m", "list": []interface{}{1}, "nan": math.NaN()}
	lower := func(value interface{}, inclusive bool) storage.PropertyIndexBounds {
		return storage.PropertyIndexBounds{Lower: value, HasLower: true, LowerInclusive: inclusive}
	}
	upper := func(value interface{}, inclusive bool) storage.PropertyIndexBounds {
		return storage.PropertyIndexBounds{Upper: value, HasUpper: true, UpperInclusive: inclusive}
	}
	none := storage.PropertyIndexBounds{}
	for where, want := range map[string]storage.PropertyIndexBounds{
		"n.t > $t":                              lower(int64(5), false),
		"n.t >= 3":                              lower(int64(3), true),
		"n.t < 3":                               upper(int64(3), false),
		"3 >= n.t":                              upper(int64(3), true),
		"n.t = 'a'":                             {Lower: "a", HasLower: true, LowerInclusive: true, Upper: "a", HasUpper: true, UpperInclusive: true},
		"2 < n.t <= 9":                          {Lower: int64(2), HasLower: true, Upper: int64(9), HasUpper: true, UpperInclusive: true},
		"n.t > 2 AND n.t > 4 AND n.k = 1":       lower(int64(4), false),
		"n.t >= 4 AND n.t > 4":                  lower(int64(4), false),
		"n.t > $t OR (n.t = $t AND n.id > $id)": lower(int64(5), true),
		"(n.t < $t) OR (n.t = $t AND n.id < $id)": upper(int64(5), true),
		"n.t > 7 OR n.t > 3":                      lower(int64(3), false),
		"n.t > 3 OR n.k = 1":                      none,
		"n.t > 3 OR n.t > 'a'":                    none,
		"n.t > 3 XOR n.k = 1":                     none,
		"NOT n.t > 3":                             none,
		"n.k > 3":                                 none,
		"m.t > 3":                                 none,
		"toInteger(n.t) > 3":                      none,
		"n.t > null":                              none,
		"n.t > true":                              none,
		"n.t > $list":                             none,
		"n.t > $missing":                          none,
		"n.t >= $s AND n.t < 'z'":                 {Lower: "m", HasLower: true, LowerInclusive: true, Upper: "z", HasUpper: true},
		"":                                        none,
		"n.t > 1.5":                               lower(1.5, false),
		"n.t > $nan":                              none,
		"n.t > 'a' AND n.t > 'c'":                 lower("c", false),
		"n.t > 3 AND n.t > 'a'":                   lower(int64(3), false),
		"n.t < 5 AND n.t < 3":                     upper(int64(3), false),
		"n.t > 7 AND n.t > 3":                     lower(int64(7), false),
		"n.t > 3 OR n.t > 7":                      lower(int64(3), false),
		"n.t < 3 OR n.t < 7":                      upper(int64(7), false),
	} {
		require.Equal(t, want, impliedPropertyBounds("n", "t", where, params), where)
	}
}

// TestKeysetPagingSeeksPastTheBound pages an indexed list with the keyset
// predicate and checks the pages against the full ordered result, ties
// included, both ways; and that a page near the end of the list costs about
// what a page near the start does (#939).
func TestKeysetPagingSeeksPastTheBound(t *testing.T) {
	// No result cache: every page below executes its query.
	exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"), 0, 0)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE INDEX item_t FOR (n:Item) ON (n.t)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "UNWIND range(0, 1999) AS i CREATE (:Item {t: i / 4, id: 'id' + toString(1999 - i)})", nil)
	require.NoError(t, err)

	for _, direction := range []struct{ order, after string }{
		{"ASC", "n.t > $t OR (n.t = $t AND n.id > $id)"},
		{"DESC", "n.t < $t OR (n.t = $t AND n.id < $id)"},
	} {
		full, err := exec.Execute(ctx, fmt.Sprintf("MATCH (n:Item) WHERE n.t IS NOT NULL RETURN n.t AS t, n.id AS id ORDER BY n.t %[1]s, n.id %[1]s", direction.order), nil)
		require.NoError(t, err)
		require.Len(t, full.Rows, 2000)
		page := fmt.Sprintf("MATCH (n:Item) WHERE %s RETURN n.t AS t, n.id AS id ORDER BY n.t %s, n.id %s LIMIT 7", direction.after, direction.order, direction.order)
		var paged [][]interface{}
		params := map[string]interface{}{"t": full.Rows[0][0], "id": full.Rows[0][1]}
		paged = append(paged, full.Rows[0])
		for {
			result, err := exec.Execute(ctx, page, params)
			require.NoError(t, err)
			paged = append(paged, result.Rows...)
			if len(result.Rows) < 7 {
				break
			}
			last := result.Rows[len(result.Rows)-1]
			params = map[string]interface{}{"t": last[0], "id": last[1]}
		}
		require.Equal(t, full.Rows, paged, direction.order)
	}

	allocations := func(row int) float64 {
		full, err := exec.Execute(ctx, "MATCH (n:Item) WHERE n.t IS NOT NULL RETURN n.t AS t, n.id AS id ORDER BY n.t, n.id", nil)
		require.NoError(t, err)
		params := map[string]interface{}{"t": full.Rows[row][0], "id": full.Rows[row][1]}
		return testing.AllocsPerRun(5, func() {
			result, err := exec.Execute(ctx, "MATCH (n:Item) WHERE n.t > $t OR (n.t = $t AND n.id > $id) RETURN n.t AS t, n.id AS id ORDER BY n.t, n.id LIMIT 10", params)
			require.NoError(t, err)
			require.Len(t, result.Rows, 10)
		})
	}
	early, late := allocations(10), allocations(1980)
	require.Less(t, late, 2*early, "allocations of a page near the end: %.0f, near the start: %.0f", late, early)
}

func TestUnwrapOuterParensOnlyMatchingPair(t *testing.T) {
	for clause, want := range map[string]string{
		"(n.t < 1) OR (n.t = 1)":    "(n.t < 1) OR (n.t = 1)",
		"((n.t < 1))":               "n.t < 1",
		" ( n.t < 1 OR n.t > 3 ) ":  "n.t < 1 OR n.t > 3",
		"(n.s = ')') AND (n.t = 1)": "(n.s = ')') AND (n.t = 1)",
		"(n.s = ')')":               "n.s = ')'",
		"n.t = 1":                   "n.t = 1",
	} {
		require.Equal(t, want, unwrapOuterParens(clause), clause)
	}
}
