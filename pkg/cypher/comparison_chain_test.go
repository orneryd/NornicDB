package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

var benchmarkComparisonScan comparisonChainScan
var benchmarkComparisonIndex int

func TestWhereComparisonChainsUseAdjacentOperands(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(baseStore, "test"))
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		CREATE (a:A {prop1: 3, prop2: 4})
		CREATE (b:B {prop1: 4, prop2: 5})
		CREATE (c:C {prop1: 4, prop2: 4})
		CREATE (a)-[:R]->(b)
		CREATE (b)-[:R]->(c)
		CREATE (c)-[:R]->(a)
	`, nil)
	if err != nil {
		t.Fatalf("setup query failed: %v", err)
	}

	result, err := exec.Execute(ctx, `
		MATCH (n)-->(m)
		WHERE n.prop1 < m.prop1 = n.prop2 <> m.prop2
		RETURN labels(m)
	`, nil)
	if err != nil {
		t.Fatalf("comparison-chain query failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected one matching relationship, got %#v", result.Rows)
	}
	labels, ok := result.Rows[0][0].([]interface{})
	if !ok || len(labels) != 1 || labels[0] != "B" {
		t.Fatalf("expected labels [B], got %#v", result.Rows[0][0])
	}
}

func TestComparisonChainsSupportArbitraryLength(t *testing.T) {
	values := map[string]interface{}{
		"a": int64(1),
		"b": int64(2),
		"c": float64(2),
		"d": int64(3),
		"e": int64(3),
	}
	resolutionCount := make(map[string]int)
	resolve := func(operand string) interface{} {
		resolutionCount[operand]++
		return values[operand]
	}

	result, ok := evaluateComparisonChain("a < b = c <> d <= e", resolve, compareWithOperator)
	if !ok || result != true {
		t.Fatalf("expected an arbitrary-length comparison chain to match, got %#v, %v", result, ok)
	}
	for operand := range values {
		if resolutionCount[operand] != 1 {
			t.Fatalf("expected %q to be resolved once, got %d", operand, resolutionCount[operand])
		}
	}
}

func TestComparisonChainParserIgnoresNestedOperators(t *testing.T) {
	operands, operators, ok := splitComparisonChain("'a=b' = value <> coalesce(other, 1 < 2)")
	if !ok {
		t.Fatal("expected a comparison chain")
	}
	wantOperands := []string{"'a=b'", "value", "coalesce(other, 1 < 2)"}
	wantOperators := []string{"=", "<>"}
	if len(operands) != len(wantOperands) || len(operators) != len(wantOperators) {
		t.Fatalf("unexpected chain split: operands=%#v operators=%#v", operands, operators)
	}
	for index := range wantOperands {
		if operands[index] != wantOperands[index] {
			t.Fatalf("operand %d: got %q, want %q", index, operands[index], wantOperands[index])
		}
	}
	for index := range wantOperators {
		if operators[index] != wantOperators[index] {
			t.Fatalf("operator %d: got %q, want %q", index, operators[index], wantOperators[index])
		}
	}
}

func BenchmarkComparisonChainSplit(b *testing.B) {
	b.Run("legacy_traversal_binary", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for _, operator := range []string{"<>", "<=", ">=", "=", "<", ">"} {
				benchmarkComparisonIndex = strings.Index("n.prop1 < m.prop1", operator)
				if benchmarkComparisonIndex > 0 {
					break
				}
			}
		}
	})
	for _, benchmark := range []struct {
		name       string
		expression string
	}{
		{name: "binary_scan", expression: "n.prop1 < m.prop1"},
		{name: "n_ary_scan", expression: "a < b = c <> d <= e"},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchmarkComparisonScan, _ = scanComparisonChain(benchmark.expression)
			}
		})
	}
}
