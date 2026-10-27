package contextslot

import (
	"context"
	"testing"
)

func TestStateLineageAndTypeIsolation(t *testing.T) {
	type first struct{ value int }
	type second struct{ value int }
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	if Get[first](parent) != nil {
		t.Fatal("uninitialized slot")
	}
	ctx := Ensure[first](parent)
	Get[first](ctx).value = 7
	if next := Ensure[first](ctx); next != ctx || Get[first](next).value != 7 {
		t.Fatal("nested operation replaced state")
	}
	sibling := Ensure[first](parent)
	if Get[first](sibling) == Get[first](ctx) {
		t.Fatal("independent lineages share state")
	}
	both := Ensure[second](ctx)
	Get[second](both).value = 11
	if Get[first](both).value != 7 || Get[second](both).value != 11 {
		t.Fatal("typed slots collided")
	}
	cancel()
	if both.Err() != context.Canceled {
		t.Fatal("lost parent cancellation")
	}
}
