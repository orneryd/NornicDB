package convert_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/convert"
)

// FLOAT preservation must not require allocating a second object graph. The
// standard encoder is an allocation control, not a typing oracle: the public
// round-trip contracts separately require the whole FLOATs which it erases.
// One fixed allocation of slack accommodates independent sync.Pool discards
// under the race runtime. This guards against materializing an object graph;
// the separate paired speed gate still rejects significant allocation growth.
func TestNumericWireWriterAllocationContract(t *testing.T) {
	type envelope struct {
		ID         string
		Created    time.Time
		Properties map[string]any
		Vector     []float32
		Binary     []byte
	}
	for name, value := range map[string]any{
		"integer-envelope": struct {
			Signed   int64
			Unsigned uint64
		}{math.MinInt64, math.MaxUint64},
		"property-envelope": envelope{
			ID: "n:1", Created: time.Date(2026, 1, 2, 3, 4, 5, 123, time.UTC),
			Properties: map[string]any{"integer": int64(9007199254740993), "whole": float64(2), "fraction": 0.125, "nested": map[string]any{"count": int64(11)}},
			Vector:     []float32{1, 2, 1.2}, Binary: []byte{0, 1, 2},
		},
		"typed-float-array": []float32{1, 2, 1.2, math.SmallestNonzeroFloat32},
	} {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			standard := json.NewEncoder(&out)
			baseline := testing.AllocsPerRun(100, func() {
				out.Reset()
				if err := standard.Encode(value); err != nil {
					t.Fatal(err)
				}
			})
			for _, domain := range []struct {
				name string
				make func(io.Writer) *convert.JSONEncoder
			}{{"property", convert.PropertyJSON.NewEncoder}, {"input", convert.CypherInputJSON.NewEncoder}} {
				encoder := domain.make(&out)
				actual := testing.AllocsPerRun(100, func() {
					out.Reset()
					if err := encoder.Encode(value); err != nil {
						t.Fatal(err)
					}
				})
				if actual > baseline+1 {
					t.Errorf("%s: %.0f allocations, standard control %.0f; numeric ownership must not copy the envelope", domain.name, actual, baseline)
				}
			}
		})
	}
}

func TestNumericWireTypedDecoderAllocationContract(t *testing.T) {
	type control struct {
		Database string
		Names    map[string]string
		Binary   []byte
		Vector   []float32
	}
	value := control{Database: "nornic", Names: map[string]string{}, Binary: []byte{0, 1, 2}, Vector: []float32{1, 2, 1.2}}
	for i := 0; i < 64; i++ {
		value.Names[fmt.Sprint(i)] = "name"
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	baseline := testing.AllocsPerRun(100, func() {
		var target control
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		if err := decoder.Decode(&target); err != nil {
			t.Fatal(err)
		}
	})
	for _, domain := range []struct {
		name   string
		decode func([]byte, any) error
	}{{"property", convert.PropertyJSON.Unmarshal}, {"input", convert.CypherInputJSON.Unmarshal}} {
		actual := testing.AllocsPerRun(100, func() {
			var target control
			if err := domain.decode(data, &target); err != nil {
				t.Fatal(err)
			}
		})
		if actual > baseline+1 {
			t.Errorf("%s: %.0f allocations, typed decoder control %.0f; interface-free data must not be re-materialized", domain.name, actual, baseline)
		}
	}
}
