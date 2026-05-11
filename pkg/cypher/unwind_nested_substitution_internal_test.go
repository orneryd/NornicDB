package cypher

// Internal unit tests that probe the variable-substitution path used when a
// nested UNWIND references a property of the outer row map.
//
// These tests feed the exact input that `executeUnwind`'s fallback hits for
// the benchmark seeder and assert on the substituted query text, so when a
// fix is applied we know exactly what the desired output looks like.

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// outerRowForSeeder mirrors the Bolt-decoded row map the Northwind seeder
// sends for an Order row with a nested product list.
func outerRowForSeeder() map[string]interface{} {
	return map[string]interface{}{
		"customerID": int64(1),
		"orderID":    int64(9001),
		"notes":      "test",
		"products": []interface{}{
			map[string]interface{}{"productID": int64(1), "quantity": int64(3)},
			map[string]interface{}{"productID": int64(2), "quantity": int64(5)},
		},
	}
}

// The input query deliberately contains:
//   - row.customerID (property access — must be replaced with int literal)
//   - row.orderID    (property access — must be replaced with int literal)
//   - WITH o, row    (bare `row` in a WITH list — must not inject raw map)
//   - UNWIND row.products AS prodRef (property access on list — must be
//     replaced with a valid list literal)
const seederOrderQueryForSubstTest = `
MATCH (c:Customer {customerID: row.customerID})
CREATE (o:Order {orderID: row.orderID})
CREATE (c)-[:PURCHASED]->(o)
WITH o, row
UNWIND row.products AS prodRef
MATCH (p:Product {productID: prodRef.productID})
CREATE (o)-[:ORDERS {quantity: prodRef.quantity}]->(p)`

func TestReplaceVariableInQuery_NestedUnwindSeederRow(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	got := exec.replaceVariableInQuery(seederOrderQueryForSubstTest, "row", outerRowForSeeder())

	t.Logf("substituted query:\n%s", got)

	// row.customerID / row.orderID must be concrete int literals.
	require.Contains(t, got, "customerID: 1", "row.customerID must be replaced with 1")
	require.Contains(t, got, "orderID: 9001", "row.orderID must be replaced with 9001")

	// row.products must be replaced with a valid Cypher list literal. The
	// exact ordering of map keys within each item is not asserted (Go map
	// iteration order is non-deterministic), only the list shape.
	require.Contains(t, got, "UNWIND", "UNWIND clause must survive substitution")
	// The list literal should start with `[` and contain the expected keys.
	unwindIdx := strings.Index(got, "UNWIND ")
	require.Greater(t, unwindIdx, 0, "expected UNWIND keyword present")
	afterUnwind := got[unwindIdx+len("UNWIND "):]
	afterUnwind = strings.TrimSpace(afterUnwind)
	require.True(t, strings.HasPrefix(afterUnwind, "["),
		"UNWIND operand must be a list literal starting with '[', got %q", afterUnwind[:min(60, len(afterUnwind))])

	// Bare `row` must NOT be substituted with the full map literal. That
	// corruption produces the classic "unsupported property value type" error
	// where the parser reads a following keyword as a property key (e.g.
	// "{customerID", "{}UNWIND[{productID").
	// Expect one of these forms for the bare `row` reference:
	//   - the token `row` preserved (if the substituter leaves it alone)
	//   - replaced with "{}" (the WITH/UNWIND-aware placeholder)
	// What it must NOT do: inject the full map literal containing nested
	// product-list literals inline.
	// Specifically, after "WITH o, " we want "row" or "{}".
	withIdx := strings.Index(got, "WITH o, ")
	require.Greater(t, withIdx, 0, "expected 'WITH o, ' clause to be preserved")
	afterWithComma := strings.TrimLeft(got[withIdx+len("WITH o, "):], " \t")
	acceptable := strings.HasPrefix(afterWithComma, "row") ||
		strings.HasPrefix(afterWithComma, "{}")
	assert.Truef(t, acceptable,
		"`row` in `WITH o, row` must stay as `row` or `{}` placeholder; found: %q",
		afterWithComma[:min(80, len(afterWithComma))])

	// The substituted query must not contain a dangling `{productID` token
	// floating outside an UNWIND list / MATCH property map. Concretely:
	// `WITH o, {customerID:...,products:[{productID:...}]}` is the corruption
	// that yields the error. Assert the substring pattern that would cause
	// the downstream parser to treat `{productID` as a property key on Order.
	assert.NotContains(t, got, "WITH o, {customerID",
		"bare-row substitution must not inject the outer row map into WITH — this causes downstream property-map parsing to collapse")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
