package antlr

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"

	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestValidateContextCancellationAndReuse(t *testing.T) {
	if err := Validate("RETURN 1"); err != nil {
		t.Fatal(err)
	}
	query := strings.Repeat("WITH 1 AS x ", 10000) + "RETURN x"
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := ValidateContext(ctx, query, cyphertext.RawSource); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled native validation: %v", err)
	}
	if err := ValidateContext(context.Background(), "RETURN 7 AS k", cyphertext.RawSource); err != nil {
		t.Fatalf("pooled validator after cancellation: %v", err)
	}
	if err := ValidateContext(context.Background(), "RETURN (", cyphertext.RawSource); err == nil {
		t.Fatal("pooled validator must still report syntax errors")
	}
}
