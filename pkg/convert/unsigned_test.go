package convert

import (
	"testing"

	"github.com/orneryd/nornicdb/testing/cypher/listinputs"
)

func TestToInt64UnsignedBoundaries(t *testing.T) {
	for _, input := range listinputs.UnsignedInputs() {
		t.Run(input.Name, func(t *testing.T) {
			got, ok := ToInt64(input.Value)
			if got != input.Integer || ok != input.Valid {
				t.Fatalf("ToInt64(%T(%v)) = %d,%t; want %d,%t", input.Value, input.Value, got, ok, input.Integer, input.Valid)
			}
		})
	}
}
