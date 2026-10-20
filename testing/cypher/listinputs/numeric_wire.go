package listinputs

import (
	"math"
	"reflect"
)

// NumericWireParameter retains the driver list carriers exercised by these
// protocol tests. Other runtime widths are read from their API-stored property
// fixtures; embedded tests separately cover their native parameter carriers.
// JSON cannot encode IEEE nonfinite numbers; Bolt can carry those floats.
func NumericWireParameter(input NumericInput, json bool) bool {
	switch values := input.List.(type) {
	case []int64, []string:
		return true
	case []float64:
		if json {
			for _, value := range values {
				if math.IsNaN(value) || math.IsInf(value, 0) {
					return false
				}
			}
		}
		return true
	default:
		return false
	}
}

// HTTP's persistent fixture uses a JSON WAL, which cannot store IEEE nonfinite
// properties. The HTTP test constructs those runtime values with Cypher instead;
// finite native property carriers and numeric strings retain their full matrix.
func NumericHTTPInputs() []NumericInput {
	var inputs []NumericInput
	for _, input := range NumericInputs() {
		scalar := reflect.ValueOf(input.Scalar)
		if scalar.Kind() == reflect.Float32 || scalar.Kind() == reflect.Float64 {
			value := scalar.Float()
			if math.IsNaN(value) || math.IsInf(value, 0) {
				continue
			}
		}
		inputs = append(inputs, input)
	}
	return inputs
}

// NumericWireChecks has explicit boundary expectations. String parity checks
// are added by each harness using the ordinary head reader as an independent
// path control; no production numeric converter supplies these expectations.
func NumericWireChecks(input NumericInput) []struct {
	Expression string
	Want       interface{}
} {
	checks := []struct {
		Expression string
		Want       interface{}
	}{
		{"toInteger(head(%s))", input.Core},
		{"head(toIntegerList(%s))", input.Core},
		{"apoc.map.setKey({},'v',toInteger(coalesce(%s,[])[0]))", map[string]interface{}{"v": input.Core}},
		{"head(toIntegerList(coalesce(%s,[])[-1..]))", input.Core},
	}
	if scalar := reflect.ValueOf(input.Scalar); scalar.Kind() != reflect.String {
		coordinate := scalar.Convert(reflect.TypeOf(float64(0))).Float()
		// Neo4j Euclidean's coordinate contract is finite float32. Identical
		// vectors have distance0/score1, including a zero vector and uint64s
		// above signed range; no integer conversion supplies this oracle.
		if !math.IsNaN(coordinate) && !math.IsInf(coordinate, 0) && math.Abs(coordinate) <= math.MaxFloat32 {
			checks = append(checks, struct {
				Expression string
				Want       interface{}
			}{"vector.similarity.euclidean(%s,%s)", float64(1)})
		}
	}
	return checks
}
