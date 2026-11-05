package cypher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nestedHeadReduceQuery nests depth head([v IN [...] | ...]) list
// comprehensions in a reduce step, each reading the previous level's
// variable, as a matcher that folds candidates level by level does. inner is
// the innermost value; it may read v<depth-1>.
func nestedHeadReduceQuery(depth int, inner string) string {
	for level := depth - 1; level >= 0; level-- {
		source := "pool"
		if level > 0 {
			source = fmt.Sprintf("v%d", level-1)
		}
		inner = fmt.Sprintf("head([v%d IN [[w%d IN %s WHERE any(name IN m.names WHERE name IN w%d.named)]] | %s])",
			level, level, source, level, inner)
	}
	return "WITH [] AS known RETURN reduce(acc = {created: []}, m IN $mentions | head([pool IN [known + acc.created] | " +
		inner + "])) AS matched"
}

// TestStaticTypeCheckOfNestedLocalScopesGrowsLinearly pins the cost of the
// compile-time type check of nested local scopes (NOR-150). Each nested
// comprehension used to be typed again by every enclosing scope, so the
// work doubled or more with each level: a 31 KB statement with eight such
// levels spent two seconds in validation on every new executor, which every
// explicit transaction is. Allocations are deterministic, so their growth
// from 5 to 10 levels separates linear (about 2x) from exponential (about
// 30x and more) without a timing bound.
func TestStaticTypeCheckOfNestedLocalScopesGrowsLinearly(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	ctx := context.Background()
	params := map[string]interface{}{"mentions": []interface{}{}}
	validate := func(depth int) float64 {
		query := nestedHeadReduceQuery(depth, fmt.Sprintf("{created: acc.created + [b IN v%d | b.id]}", depth-1))
		run := func() {
			// A new executor validates the statement again, as an explicit
			// transaction's executor does.
			exec := NewStorageExecutor(store)
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, query, params)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{map[string]interface{}{"created": []interface{}{}}}}, result.Rows)
			_, err = exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		}
		run() // statement-text caches shared by every executor
		return testing.AllocsPerRun(3, run)
	}
	shallow, deep := validate(5), validate(10)
	t.Logf("allocations per statement: 5 levels %.0f, 10 levels %.0f", shallow, deep)
	require.Less(t, deep, 4*shallow, "validation work grows faster than the statement's nesting")
}

// TestStaticTypeCheckOfNestedLocalScopesStillRejects keeps the check of a
// function argument that reads a comprehension's variable: the comprehension
// types it with its own variables, at any depth.
func TestStaticTypeCheckOfNestedLocalScopesStillRejects(t *testing.T) {
	for _, depth := range []int{1, 3, 10} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			exec, ctx := newUnitExecutor(t)
			query := nestedHeadReduceQuery(depth, fmt.Sprintf("{created: [toUpper(size(v%d))]}", depth-1))
			_, err := exec.Execute(ctx, query, map[string]interface{}{"mentions": []interface{}{}})
			require.Error(t, err)
			var semantic *SemanticError
			require.True(t, errors.As(err, &semantic), "%v", err)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semantic.Code)
			require.ErrorContains(t, err, "Type mismatch: expected String but was Integer")
		})
	}
}

// TestExplicitTransactionNestedReduceMatcherCost runs a 31 KB matcher read
// (a captured statement whose last WITH folds its candidates through eight
// nested head([...]) levels) in fresh explicit transactions. Each
// transaction gets a new executor, which validates the statement again; that
// validation must cost about what the statement's evaluation costs, not
// several times more (NOR-150: 2.2 s against 0.11 s on a reused executor).
func TestExplicitTransactionNestedReduceMatcherCost(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "library")
	ctx := context.Background()
	setup := NewStorageExecutor(store)
	for _, statement := range []string{
		"UNWIND range(0, 44) AS n CREATE (:LibDefinition {id: 'definition-' + toString(n)})",
		"UNWIND range(0, 23) AS n CREATE (:LibDocument:LibGraphNode {id: toString(n), current_version_id: toString(n)})",
	} {
		_, err := setup.Execute(ctx, statement, nil)
		require.NoError(t, err)
	}
	query, err := os.ReadFile(filepath.Join("testdata", "explicit_tx_nested_reduce_matcher.cypher"))
	require.NoError(t, err)
	encoded, err := os.ReadFile(filepath.Join("testdata", "explicit_tx_nested_reduce_matcher.params.json"))
	require.NoError(t, err)
	var params map[string]interface{}
	require.NoError(t, json.Unmarshal(encoded, &params))
	want := [][]interface{}{{[]interface{}{}, []interface{}{}, []interface{}{}, []interface{}{}, []interface{}{}}}

	read := func(exec *StorageExecutor) time.Duration {
		start := time.Now()
		_, err := exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		result, err := exec.Execute(ctx, string(query), params)
		require.NoError(t, err)
		require.Equal(t, want, result.Rows)
		_, err = exec.Execute(ctx, "ROLLBACK", nil)
		require.NoError(t, err)
		return time.Since(start)
	}
	median := func(next func() *StorageExecutor) time.Duration {
		times := make([]time.Duration, 5)
		for i := range times {
			times[i] = read(next())
		}
		sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
		return times[len(times)/2]
	}
	reused := NewStorageExecutor(store)
	read(reused)
	read(NewStorageExecutor(store)) // statement-text caches shared by every executor
	warm := median(func() *StorageExecutor { return reused })
	fresh := median(func() *StorageExecutor { return NewStorageExecutor(store) })
	t.Logf("explicit transaction read: reused executor %v, new executor %v", warm, fresh)
	require.Less(t, fresh, 6*warm, "a new transaction's validation dominates the read")
}
