package fabric

import (
	"context"
	"testing"
)

func TestWithSubTransaction_NilCtx(t *testing.T) {
	sub := &SubTransaction{ShardName: "s1"}
	ctx := WithSubTransaction(nil, sub)
	if ctx == nil {
		t.Fatal("expected non-nil context from nil input")
	}
	got, ok := SubTransactionFromContext(ctx)
	if !ok || got != sub {
		t.Fatalf("expected sub-transaction round-trip through nil ctx, got ok=%v sub=%v", ok, got)
	}
}

func TestSubTransactionFromContext_NilCtx(t *testing.T) {
	sub, ok := SubTransactionFromContext(nil)
	if ok || sub != nil {
		t.Fatalf("expected nil/false from nil ctx, got ok=%v sub=%v", ok, sub)
	}
}

func TestSubTransactionFromContext_MissingKey(t *testing.T) {
	ctx := context.Background()
	sub, ok := SubTransactionFromContext(ctx)
	if ok || sub != nil {
		t.Fatalf("expected nil/false for missing key, got ok=%v sub=%v", ok, sub)
	}
}

func TestSubTransactionFromContext_NilValue(t *testing.T) {
	// Store a nil *SubTransaction in the context.
	ctx := context.WithValue(context.Background(), subTransactionContextKey{}, (*SubTransaction)(nil))
	sub, ok := SubTransactionFromContext(ctx)
	if ok || sub != nil {
		t.Fatalf("expected false for nil stored value, got ok=%v sub=%v", ok, sub)
	}
}

func TestWithFabricTransaction_NilCtx(t *testing.T) {
	tx := NewFabricTransaction("tx-1")
	ctx := WithFabricTransaction(nil, tx)
	if ctx == nil {
		t.Fatal("expected non-nil context from nil input")
	}
	got, ok := FabricTransactionFromContext(ctx)
	if !ok || got != tx {
		t.Fatalf("expected fabric tx round-trip through nil ctx, got ok=%v", ok)
	}
}

func TestFabricTransactionFromContext_NilCtx(t *testing.T) {
	tx, ok := FabricTransactionFromContext(nil)
	if ok || tx != nil {
		t.Fatalf("expected nil/false from nil ctx, got ok=%v", ok)
	}
}

func TestFabricTransactionFromContext_MissingKey(t *testing.T) {
	ctx := context.Background()
	tx, ok := FabricTransactionFromContext(ctx)
	if ok || tx != nil {
		t.Fatalf("expected nil/false for missing key, got ok=%v", ok)
	}
}

func TestFabricTransactionFromContext_NilValue(t *testing.T) {
	ctx := context.WithValue(context.Background(), fabricTransactionContextKey{}, (*FabricTransaction)(nil))
	tx, ok := FabricTransactionFromContext(ctx)
	if ok || tx != nil {
		t.Fatalf("expected false for nil stored value, got ok=%v", ok)
	}
}

func TestWithRecordBindings_NilCtx(t *testing.T) {
	bindings := map[string]interface{}{"k": "v"}
	ctx := WithRecordBindings(nil, bindings)
	if ctx == nil {
		t.Fatal("expected non-nil context from nil input")
	}
	got, ok := RecordBindingsFromContext(ctx)
	if !ok || got["k"] != "v" {
		t.Fatalf("expected record bindings round-trip through nil ctx, got ok=%v bindings=%v", ok, got)
	}
}

func TestRecordBindingsFromContext_NilCtx(t *testing.T) {
	bindings, ok := RecordBindingsFromContext(nil)
	if ok || bindings != nil {
		t.Fatalf("expected nil/false from nil ctx, got ok=%v", ok)
	}
}

func TestRecordBindingsFromContext_MissingKey(t *testing.T) {
	ctx := context.Background()
	bindings, ok := RecordBindingsFromContext(ctx)
	if ok || bindings != nil {
		t.Fatalf("expected nil/false for missing key, got ok=%v", ok)
	}
}

func TestRecordBindingsFromContext_NilValue(t *testing.T) {
	ctx := context.WithValue(context.Background(), recordBindingsContextKey{}, (map[string]interface{})(nil))
	bindings, ok := RecordBindingsFromContext(ctx)
	if ok || bindings != nil {
		t.Fatalf("expected false for nil stored value, got ok=%v", ok)
	}
}

func TestWithSubTransaction_NonNilCtx(t *testing.T) {
	sub := &SubTransaction{ShardName: "s2"}
	parent := context.WithValue(context.Background(), "someKey", "someValue")
	ctx := WithSubTransaction(parent, sub)
	got, ok := SubTransactionFromContext(ctx)
	if !ok || got != sub {
		t.Fatal("expected sub-transaction to be retrievable from non-nil parent ctx")
	}
}

func TestWithFabricTransaction_NonNilCtx(t *testing.T) {
	tx := NewFabricTransaction("tx-2")
	parent := context.WithValue(context.Background(), "someKey", "someValue")
	ctx := WithFabricTransaction(parent, tx)
	got, ok := FabricTransactionFromContext(ctx)
	if !ok || got != tx {
		t.Fatal("expected fabric tx to be retrievable from non-nil parent ctx")
	}
}

func TestWithRecordBindings_NonNilCtx(t *testing.T) {
	bindings := map[string]interface{}{"x": 42}
	parent := context.WithValue(context.Background(), "someKey", "someValue")
	ctx := WithRecordBindings(parent, bindings)
	got, ok := RecordBindingsFromContext(ctx)
	if !ok || got["x"] != 42 {
		t.Fatal("expected record bindings to be retrievable from non-nil parent ctx")
	}
}
