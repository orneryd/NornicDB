package cypher

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Statement text a procedure runs is held to the limit: text over it is
// refused before anything reads it, whichever procedure takes it.
func TestDynamicStatementTextLimit(t *testing.T) {
	executor := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "dynlimit"), 0, 0)
	defer SetDynamicStatementLimit(0)
	SetDynamicStatementLimit(64)
	over := "RETURN 1 AS v //" + strings.Repeat("x", 64)
	for _, query := range []string{
		"CALL apoc.cypher.run($q, {}) YIELD value RETURN value",
		"CALL apoc.cypher.runMany($q, {}) YIELD row RETURN row",
		"CALL apoc.periodic.iterate($q, 'RETURN 1', {}) YIELD batches RETURN batches",
		"CALL apoc.periodic.iterate('RETURN 1 AS x', $q, {}) YIELD batches RETURN batches",
		"CALL apoc.periodic.commit($q, {}) YIELD updates RETURN updates",
		"CALL apoc.export.csv.query($q, '', {}) YIELD rows RETURN rows",
		"CALL apoc.export.json.query($q, '', {}) YIELD nodes RETURN nodes",
	} {
		_, err := executor.Execute(context.Background(), query, map[string]interface{}{"q": over})
		require.ErrorContains(t, err, "exceeds the limit of 64 bytes", query)
	}
	_, err := executor.Execute(context.Background(), "CALL apoc.cypher.run($q, {}) YIELD value RETURN value", map[string]interface{}{"q": "RETURN 1 AS v"})
	require.NoError(t, err)
}

// The bound the limit gives: statement text at the limit, of the shapes that
// cost its parse the most, ends promptly after its statement is canceled
// during the parse. Logged per shape (DYNLIMIT) for the stated bound.
func TestDynamicStatementTextAtTheLimitAfterCancellation(t *testing.T) {
	if os.Getenv("NORNICDB_DYNLIMIT_AT_LIMIT") == "" {
		t.Skip("NORNICDB_DYNLIMIT_AT_LIMIT measures statement text at the limit (minutes)")
	}
	executor := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "dynbound"), 0, 0)
	limit := int(DynamicStatementLimit())
	fill := func(prefix, unit, suffix string) string {
		n := (limit - len(prefix) - len(suffix)) / len(unit)
		return prefix + strings.Repeat(unit, n) + suffix
	}
	shapes := []struct{ name, text string }{
		{"list_literal", strings.TrimSuffix(fill("RETURN [", "1,", "1] AS v"), "")},
		{"string_literal", fill("RETURN '", "a", "' AS v")},
		{"comment", fill("RETURN 1 AS v //", "x", "")},
		{"or_chain", fill("MATCH (n:DynBound) WHERE n.a = 0", " OR n.a = 1", " RETURN n")},
		{"map_literal", fill("RETURN {k0: 0", ", k: 1", "} AS v")},
		{"union", fill("RETURN 1 AS v", " UNION ALL RETURN 1 AS v", "")},
	}
	for _, shape := range shapes {
		require.LessOrEqual(t, len(shape.text), limit, shape.name)
		ctx, cancel := context.WithCancel(context.Background())
		finished := make(chan error, 1)
		started := time.Now()
		go func() {
			_, err := executor.Execute(ctx, "CALL apoc.cypher.run($q, {}) YIELD value RETURN count(value) AS n", map[string]interface{}{"q": shape.text})
			finished <- err
		}()
		time.Sleep(20 * time.Millisecond)
		canceledAt := time.Now()
		cancel()
		err := <-finished
		ended := time.Now()
		t.Logf("DYNLIMIT shape=%s bytes=%d total=%v after_cancel=%v outcome=%v", shape.name, len(shape.text), ended.Sub(started), ended.Sub(canceledAt), err)
		if err == nil {
			continue
		}
		require.ErrorIs(t, err, context.Canceled, shape.name)
	}
}

// How the work after a cancellation during the parse of statement text a
// procedure runs grows with the text, per shape (DYNSCALE): the measurement
// the limit is chosen from. Sizes come from NORNICDB_DYNLIMIT_SIZES; a shape
// stops growing once one size takes longer than 20s.
func TestDynamicStatementTextParseScaling(t *testing.T) {
	sizes := os.Getenv("NORNICDB_DYNLIMIT_SIZES")
	if sizes == "" {
		t.Skip("NORNICDB_DYNLIMIT_SIZES lists the text sizes to measure")
	}
	executor := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "dynscale"), 0, 0)
	shapes := []struct{ name, prefix, unit, suffix string }{
		{"list_literal", "RETURN [", "1,", "1] AS v"},
		{"string_literal", "RETURN '", "a", "' AS v"},
		{"or_chain", "MATCH (n:DynScale) WHERE n.a = 0", " OR n.a = 1", " RETURN n"},
		{"and_chain", "MATCH (n:DynScale) WHERE n.a = 0", " AND n.a = 1", " RETURN n"},
		{"plus_chain", "RETURN 0", " + 1", " AS v"},
		{"map_literal", "RETURN {k0: 0", ", k: 1", "} AS v"},
		{"union", "RETURN 1 AS v", " UNION ALL RETURN 1 AS v", ""},
		{"nested_list", "RETURN ", "[", ""},
	}
	for _, shape := range shapes {
		for _, field := range strings.Split(sizes, ",") {
			size, err := strconv.Atoi(strings.TrimSpace(field))
			require.NoError(t, err)
			n := (size - len(shape.prefix) - len(shape.suffix)) / len(shape.unit)
			text := shape.prefix + strings.Repeat(shape.unit, n) + shape.suffix
			if shape.name == "nested_list" {
				text = "RETURN " + strings.Repeat("[", n/2) + strings.Repeat("]", n/2) + " AS v"
			}
			ctx, cancel := context.WithCancel(context.Background())
			finished := make(chan error, 1)
			started := time.Now()
			go func() {
				_, err := executor.Execute(ctx, "CALL apoc.cypher.run($q, {}) YIELD value RETURN count(value) AS n", map[string]interface{}{"q": text})
				finished <- err
			}()
			time.Sleep(5 * time.Millisecond)
			canceledAt := time.Now()
			cancel()
			err = <-finished
			ended := time.Now()
			late := ended.Sub(canceledAt)
			t.Logf("DYNSCALE shape=%s bytes=%d total=%v after_cancel=%v outcome=%v", shape.name, len(text), ended.Sub(started), late, err)
			if late > 20*time.Second {
				break
			}
		}
	}
}
