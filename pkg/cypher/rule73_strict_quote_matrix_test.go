package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRule73EscapedQuoteConjunctionMatrix(t *testing.T) {
	for _, tc := range []struct {
		name, literal string
		value         string
	}{
		{"escaped-single", `'O\'Reilly AND Sons'`, "O'Reilly AND Sons"},
		{"escaped-double", `"Say \"AND\" now"`, `Say "AND" now`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r73_and_"+tc.name))
			_, err := exec.Execute(context.Background(), "CREATE (:R73And {text:"+tc.literal+", id:1})", nil)
			require.NoError(t, err)
			query := "MATCH (n:R73And) WHERE n.text = " + tc.literal + " AND n.id = 1 RETURN n.text, n.id"
			got, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err, query)
			require.Equal(t, [][]interface{}{{tc.value, int64(1)}}, got.Rows)
		})
	}

	for _, expression := range []string{
		`n.text = 'a\' AND b' AND n.id = 1`,
		`n.text = "a\" AND b" AND n.id = 1`,
		`n.text = 'a'' AND b' AND n.id = 1`,
		`n.text = "a"" AND b" AND n.id = 1`,
		"n.`a`` AND b` = 1 AND n.id = 1",
	} {
		t.Run("split/"+expression, func(t *testing.T) {
			parts := splitTopLevelAndConjuncts(originalBraceView(expression))
			require.Len(t, parts, 2)
			require.Equal(t, "n.id = 1", parts[1])
		})
	}
	// Doubled string quotes are adjacent tokens in the pinned Cypher grammar,
	// unlike doubled backticks in symbolic names. Public writers must reject
	// them without retaining the node created earlier in the statement.
	for _, literal := range []string{`'a'' AND b'`, `"a"" AND b"`} {
		t.Run("reject/"+literal, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r73_doubled"))
			_, err := exec.Execute(context.Background(), "CREATE (n:R73Doubled {text:"+literal+"}) RETURN n", nil)
			require.Error(t, err)
			got, err := exec.Execute(context.Background(), "MATCH (n) RETURN count(n)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, got.Rows)
		})
	}
}

func TestRule73ConstraintRequireQuotationMatrix(t *testing.T) {
	for index, key := range []string{"owner's", `owner"name`, "a`b", "owner;and", "owner\nand"} {
		t.Run(fmt.Sprintf("%d/%s", index, key), func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), fmt.Sprintf("r73_contract_%d", index)))
			written := "`" + strings.ReplaceAll(key, "`", "``") + "`"
			query := fmt.Sprintf("CREATE CONSTRAINT r73q%d FOR (n:R73Quoted%d) REQUIRE { n.%s IS NOT NULL }", index, index, written)
			_, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err, query)
		})
	}
}
