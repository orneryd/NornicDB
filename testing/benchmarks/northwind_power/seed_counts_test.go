package main

import (
	"strings"
	"testing"
)

func TestVerifySeedCountsRejectsMissingOrDuplicatedOrderLines(t *testing.T) {
	for _, lines := range []int64{8, 10} {
		counts := SeedCounts{Categories: 2, Suppliers: 2, Customers: 3, Products: 4, Orders: 3, PartOfEdges: 4, SuppliesEdges: 4, PurchasedEdges: 3, OrdersEdges: lines}
		errors := verifySeedCounts(counts, 2, 2, 3, 4, 3, 20)
		if len(errors) != 1 || !strings.Contains(errors[0], "ORDERS count=") {
			t.Fatalf("%d order lines: errors=%v; want rejected ORDERS count", lines, errors)
		}
	}
}

func TestVerifySeedCountsAcceptsExactGeneratedPopulations(t *testing.T) {
	counts := SeedCounts{Categories: 2, Suppliers: 2, Customers: 3, Products: 4, Orders: 3, PartOfEdges: 4, SuppliesEdges: 4, PurchasedEdges: 3, OrdersEdges: 9}
	if errors := verifySeedCounts(counts, 2, 2, 3, 4, 3, 20); len(errors) != 0 {
		t.Fatal(errors)
	}
	if errors := verifySeedCounts(SeedCounts{}, 0, 0, 0, 0, 0, 0); len(errors) != 0 {
		t.Fatal(errors)
	}
}
