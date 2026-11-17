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

func TestBoundSlotPreservesAbsentAndInheritedState(t *testing.T) {
	type state struct{ value int }
	type sibling struct{}
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	empty := Bind[state](parent)
	if Get[state](empty) != nil {
		t.Fatal("binding initialized absent state")
	}
	initialized := Ensure[state](empty)
	Get[state](initialized).value = 42
	bound := Bind[state](initialized)
	if Get[state](bound) != Get[state](initialized) {
		t.Fatal("binding replaced inherited state")
	}
	if Ensure[state](bound) != bound {
		t.Fatal("ensure replaced the bound state")
	}
	if Get[state](empty) != nil {
		t.Fatal("child initialization changed its parent")
	}
	if Get[sibling](bound) != nil {
		t.Fatal("binding crossed type boundaries")
	}
	cancel()
	if bound.Err() != context.Canceled || empty.Err() != context.Canceled {
		t.Fatal("binding lost cancellation")
	}
	select {
	case <-bound.Done():
	default:
		t.Fatal("binding lost Done")
	}
}
