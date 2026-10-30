package cypher

import (
	"strconv"
	"strings"
)

// CypherVector is a Cypher 25 VECTOR value (#907): a fixed number of
// coordinates of one numeric type. As in Neo4j:
//   - the coordinate types are INTEGER64 (INTEGER, INT, INT64), INTEGER32
//     (INT32), INTEGER16 (INT16), INTEGER8 (INT8), FLOAT64 (FLOAT) and
//     FLOAT32;
//   - an integer vector truncates float coordinates toward zero, and a
//     coordinate outside the type's range is an ArithmeticError; a float32
//     vector rounds to float32, and a coordinate that isn't finite is an
//     error;
//   - two vectors are equal only with the same type and coordinates, a
//     vector is never equal to a list, and vectors have no < or >;
//   - a vector is not a list: indexing and iterating it are type errors,
//     toIntegerList / toFloatList / size read it.
//
// Ints holds an integer vector's coordinates and Floats a float vector's.
type CypherVector struct {
	Type   VectorCoordinateType
	Ints   []int64
	Floats []float64
}

// VectorCoordinateType is a vector's coordinate type.
type VectorCoordinateType uint8

const (
	VectorInteger64 VectorCoordinateType = iota
	VectorInteger32
	VectorInteger16
	VectorInteger8
	VectorFloat64
	VectorFloat32
)

// vectorCoordinateTypeNames are the coordinate types' names: canonical (as
// toString writes them) and as valueType() writes them.
var vectorCoordinateTypeNames = [...]struct{ canonical, valueType string }{
	VectorInteger64: {"INTEGER64", "INTEGER"},
	VectorInteger32: {"INTEGER32", "INTEGER32"},
	VectorInteger16: {"INTEGER16", "INTEGER16"},
	VectorInteger8:  {"INTEGER8", "INTEGER8"},
	VectorFloat64:   {"FLOAT64", "FLOAT"},
	VectorFloat32:   {"FLOAT32", "FLOAT32"},
}

// String is the coordinate type's canonical name (INTEGER64, FLOAT32, …).
func (t VectorCoordinateType) String() string {
	return vectorCoordinateTypeNames[t].canonical
}

// IsFloat reports whether the coordinates are floats.
func (t VectorCoordinateType) IsFloat() bool {
	return t == VectorFloat64 || t == VectorFloat32
}

// parseVectorCoordinateType reads a coordinate type name, in any case:
// INTEGER64 or its aliases INTEGER, INT, INT64, … .
func parseVectorCoordinateType(name string) (VectorCoordinateType, bool) {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "INTEGER64", "INTEGER", "INT", "INT64", "SIGNED INTEGER":
		return VectorInteger64, true
	case "INTEGER32", "INT32":
		return VectorInteger32, true
	case "INTEGER16", "INT16":
		return VectorInteger16, true
	case "INTEGER8", "INT8":
		return VectorInteger8, true
	case "FLOAT64", "FLOAT":
		return VectorFloat64, true
	case "FLOAT32":
		return VectorFloat32, true
	}
	return 0, false
}

// vectorDimensionLimit is the largest dimension Neo4j allows.
const vectorDimensionLimit = 4096

// Dimension is the number of coordinates.
func (v CypherVector) Dimension() int {
	if v.Type.IsFloat() {
		return len(v.Floats)
	}
	return len(v.Ints)
}

// CypherSize is size(vector): the dimension (cypherfn.Sized).
func (v CypherVector) CypherSize() int64 {
	return int64(v.Dimension())
}

// coordinates returns the coordinates as Cypher values: integers or floats.
func (v CypherVector) coordinates() []interface{} {
	out := make([]interface{}, v.Dimension())
	for i := range out {
		if v.Type.IsFloat() {
			out[i] = v.Floats[i]
		} else {
			out[i] = v.Ints[i]
		}
	}
	return out
}

func (v CypherVector) floatAt(i int) float64 {
	if v.Type.IsFloat() {
		return v.Floats[i]
	}
	return float64(v.Ints[i])
}

// Equal reports whether v and other have the same type and coordinates.
func (v CypherVector) Equal(other CypherVector) bool {
	if v.Type != other.Type || v.Dimension() != other.Dimension() {
		return false
	}
	for i := 0; i < v.Dimension(); i++ {
		if v.Type.IsFloat() {
			if v.Floats[i] != other.Floats[i] {
				return false
			}
		} else if v.Ints[i] != other.Ints[i] {
			return false
		}
	}
	return true
}

// String is the vector as toString writes it: vector([1, 2], 2, INTEGER64).
func (v CypherVector) String() string {
	var b strings.Builder
	b.WriteString("vector([")
	for i := 0; i < v.Dimension(); i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		if v.Type.IsFloat() {
			b.WriteString(vectorFloatText(v.Floats[i], v.Type == VectorFloat32))
		} else {
			b.WriteString(strconv.FormatInt(v.Ints[i], 10))
		}
	}
	b.WriteString("], ")
	b.WriteString(strconv.Itoa(v.Dimension()))
	b.WriteString(", ")
	b.WriteString(v.Type.String())
	b.WriteString(")")
	return b.String()
}

// vectorFloatText writes a coordinate as toString writes a float (1.0,
// 0.1, 1.0E20); a float32 coordinate in its shortest float32 form.
func vectorFloatText(value float64, float32Coordinate bool) string {
	if float32Coordinate {
		return formatCypherValueString(float32(value))
	}
	return formatCypherValueString(value)
}

// vectorCoordinateTypeOrder is the coordinate types' sort order:
// INTEGER8 < INTEGER16 < INTEGER32 < INTEGER64 < FLOAT32 < FLOAT64.
var vectorCoordinateTypeOrder = [...]int{
	VectorInteger8: 0, VectorInteger16: 1, VectorInteger32: 2, VectorInteger64: 3, VectorFloat32: 4, VectorFloat64: 5,
}

// compareVectors orders vectors as Neo4j does: by coordinate type, then
// dimension, then coordinates.
func compareVectors(left, right CypherVector) int {
	if order := compareOrderedInts(vectorCoordinateTypeOrder[left.Type], vectorCoordinateTypeOrder[right.Type]); order != 0 {
		return order
	}
	if order := compareOrderedInts(left.Dimension(), right.Dimension()); order != 0 {
		return order
	}
	for i := 0; i < left.Dimension(); i++ {
		var order int
		if left.Type.IsFloat() {
			order = compareOrderedFloats(left.Floats[i], right.Floats[i])
		} else if left.Ints[i] < right.Ints[i] {
			order = -1
		} else if left.Ints[i] > right.Ints[i] {
			order = 1
		}
		if order != 0 {
			return order
		}
	}
	return 0
}

// compareOrderedFloats orders two finite coordinates.
func compareOrderedFloats(left, right float64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	}
	return 0
}
