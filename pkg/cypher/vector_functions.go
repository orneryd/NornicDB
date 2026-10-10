package cypher

import (
	"math"
	"strconv"
	"strings"
	"time"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// The Cypher 25 VECTOR and UUID functions (#907). vector(), vector_distance
// and vector_norm take a coordinate type or metric as a bare name; the
// statement rewrite writes them as the internal __nornic_vector,
// __nornic_vector_distance and __nornic_vector_norm with the canonical name
// quoted (vector_call_rewrite.go), so these read a string literal there.

func init() {
	// The public names reach here only with an argument count the
	// statement rewrite leaves alone: the functions report it.
	for _, name := range []string{vectorFunction, "vector"} {
		cypherfn.Register(name, fnVector)
	}
	for _, name := range []string{vectorDistanceFunction, "vector_distance"} {
		cypherfn.Register(name, fnVectorDistance)
	}
	for _, name := range []string{vectorNormFunction, "vector_norm"} {
		cypherfn.Register(name, fnVectorNorm)
	}
	cypherfn.Register("vector_dimension_count", fnVectorDimensionCount)
	cypherfn.Register("uuid", fnUUID)
	cypherfn.Register("uuid.mostsignificantbits", fnUUIDHalf(true))
	cypherfn.Register("uuid.leastsignificantbits", fnUUIDHalf(false))
}

const (
	vectorFunction         = "__nornic_vector"
	vectorDistanceFunction = "__nornic_vector_distance"
	vectorNormFunction     = "__nornic_vector_norm"
)

// fnVector is vector(value, dimension, coordinateType): value a list of
// numbers or its text ('[1, 2]'); null in, null out.
func fnVector(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, argumentCountError("vector", "3", len(args))
	}
	coordinateType, _ := parseVectorCoordinateType(strings.Trim(strings.TrimSpace(args[2]), "'"))
	values, err := evalArgs(ctx, args[:2])
	if err != nil || values[0] == nil || values[1] == nil {
		return nil, err
	}
	dimension, ok := cypherIntegerValue(values[1])
	if !ok {
		return nil, &cypherfn.TypeMismatchError{Function: "vector", Expected: "Integer", Value: values[1]}
	}
	if dimension < 1 || dimension > vectorDimensionLimit {
		return nil, vectorDimensionRangeError(strconv.FormatInt(dimension, 10))
	}
	items, err := vectorItems(values[0])
	if err != nil {
		return nil, err
	}
	vector, err := newCypherVector(items, coordinateType)
	if err != nil {
		return nil, err
	}
	if int64(vector.Dimension()) != dimension {
		return nil, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
			localization.CypherCoreVectorDimensionMismatch(vectorTypeText(coordinateType, int(dimension)), vectorTypeText(coordinateType, vector.Dimension())))
	}
	return vector, nil
}

// vectorDimensionRangeError is Neo4j's SyntaxError for a dimension outside 1
// to 4096.
func vectorDimensionRangeError(dimension string) error {
	return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidArgumentValue", localization.CypherCoreVectorDimensionRange(dimension))
}

// vectorTypeText is VECTOR<INTEGER64>(3), as Neo4j writes a vector type in
// its dimension error.
func vectorTypeText(coordinateType VectorCoordinateType, dimension int) string {
	return "VECTOR<" + coordinateType.String() + ">(" + strconv.Itoa(dimension) + ")"
}

// vectorItems reads vector()'s value: a list of numbers, or a string that
// writes one.
func vectorItems(value interface{}) ([]interface{}, error) {
	if text, ok := value.(string); ok {
		trimmed := strings.TrimSpace(text)
		if !strings.HasPrefix(trimmed, "[") || !strings.HasSuffix(trimmed, "]") {
			return nil, vectorCoordinateTypeError("vector", text)
		}
		var items []interface{}
		for _, part := range strings.Split(trimmed[1:len(trimmed)-1], ",") {
			if part = strings.TrimSpace(part); part == "" {
				continue
			}
			number, err := strconv.ParseFloat(part, 64)
			if err != nil {
				return nil, vectorCoordinateTypeError("vector", part)
			}
			items = append(items, number)
		}
		return items, nil
	}
	items, ok := cypherListValue(value)
	if !ok {
		return nil, &cypherfn.TypeMismatchError{Function: "vector", Expected: "String, List<Float>, List<Integer> or List<Number>", Value: value}
	}
	return items, nil
}

// vectorCoordinateTypeError is Neo4j's TypeError for a coordinate that isn't
// a number ("Expected a NUMBER, got: NO_VALUE"); function is vector() for a
// list's coordinate and vector for a string's, as Neo4j writes them.
func vectorCoordinateTypeError(function, got string) error {
	return localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType", localization.CypherCoreVectorCoordinateType(function, got))
}

// newCypherVector builds a vector of coordinateType from numbers: integer
// coordinates truncate toward zero and must fit the type; float32 ones round
// and, like float64 ones, must be finite.
func newCypherVector(items []interface{}, coordinateType VectorCoordinateType) (CypherVector, error) {
	vector := CypherVector{Type: coordinateType}
	limits := map[VectorCoordinateType]float64{VectorInteger64: math.MaxInt64, VectorInteger32: math.MaxInt32, VectorInteger16: math.MaxInt16, VectorInteger8: math.MaxInt8}
	for _, item := range items {
		if item == nil {
			return vector, vectorCoordinateTypeError("vector()", "NO_VALUE")
		}
		var number float64
		var integer int64
		isInteger := false
		switch typed := item.(type) {
		case float64:
			number = typed
		case float32:
			number = float64(typed)
		default:
			value, ok := cypherIntegerValue(item)
			if !ok {
				return vector, &cypherfn.TypeMismatchError{Function: "vector", Expected: "String, List<Float>, List<Integer> or List<Number>", Value: items}
			}
			integer, number, isInteger = value, float64(value), true
		}
		if coordinateType.IsFloat() {
			if coordinateType == VectorFloat32 {
				number = float64(float32(number))
			}
			if math.IsInf(number, 0) || math.IsNaN(number) {
				return vector, localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgumentValue", localization.CypherCoreVectorCoordinatesNotFinite())
			}
			vector.Floats = append(vector.Floats, number)
			continue
		}
		if !isInteger {
			truncated := math.Trunc(number)
			if math.IsNaN(truncated) || truncated > limits[coordinateType] || truncated < -limits[coordinateType]-1 {
				return vector, vectorRangeError()
			}
			integer = int64(truncated)
		}
		if limit := int64(limits[coordinateType]); coordinateType != VectorInteger64 && (integer > limit || integer < -limit-1) {
			return vector, vectorRangeError()
		}
		vector.Ints = append(vector.Ints, integer)
	}
	return vector, nil
}

// vectorRangeError is Neo4j's ArithmeticError for a coordinate outside its
// integer type.
func vectorRangeError() error {
	return localizedStatusError("Neo.ClientError.Statement.ArithmeticError", "NumericOutOfRange", localization.CypherCoreVectorCoordinateRange())
}

// fnVectorDimensionCount is vector_dimension_count(vector).
func fnVectorDimensionCount(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, argumentCountError("vector_dimension_count", "1", len(args))
	}
	values, err := evalArgs(ctx, args)
	if err != nil || values[0] == nil {
		return nil, err
	}
	vector, err := vectorArgument(values[0])
	if err != nil {
		return nil, err
	}
	return int64(vector.Dimension()), nil
}

// vectorArgument is a VECTOR argument, or the TypeError for another value.
func vectorArgument(value interface{}) (CypherVector, error) {
	switch typed := value.(type) {
	case CypherVector:
		return typed, nil
	case *CypherVector:
		return *typed, nil
	}
	return CypherVector{}, &cypherfn.TypeMismatchError{Function: "vector", Expected: "Vector", Value: value}
}

// fnVectorDistance is vector_distance(a, b, metric).
func fnVectorDistance(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, argumentCountError("vector_distance", "3", len(args))
	}
	metric := strings.Trim(strings.TrimSpace(args[2]), "'")
	values, err := evalArgs(ctx, args[:2])
	if err != nil || values[0] == nil || values[1] == nil {
		return nil, err
	}
	a, err := vectorArgument(values[0])
	if err != nil {
		return nil, err
	}
	b, err := vectorArgument(values[1])
	if err != nil {
		return nil, err
	}
	if a.Dimension() != b.Dimension() {
		return nil, localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgumentValue", localization.CypherCoreVectorDistanceDimensions())
	}
	// Neo4j computes every metric in float32: each coordinate as a float32,
	// each step rounded to float32 (explicit conversions keep the compiler
	// from fusing them), whatever the vectors' coordinate types.
	var sum, dot, normA, normB float32
	for i := 0; i < a.Dimension(); i++ {
		x, y := float32(a.floatAt(i)), float32(b.floatAt(i))
		switch metric {
		case "EUCLIDEAN", "EUCLIDEAN_SQUARED":
			d := x - y
			sum += float32(d * d)
		case "MANHATTAN":
			sum += float32(math.Abs(float64(x - y)))
		case "HAMMING":
			if x != y {
				sum++
			}
		default: // COSINE, DOT
			dot += float32(x * y)
			normA += float32(x * x)
			normB += float32(y * y)
		}
	}
	switch metric {
	case "EUCLIDEAN":
		return float64(float32(math.Sqrt(float64(sum)))), nil
	case "COSINE":
		return float64(1 - dot/float32(math.Sqrt(float64(float32(normA*normB))))), nil
	case "DOT":
		return float64(-dot), nil
	}
	return float64(sum), nil
}

// fnVectorNorm is vector_norm(vector, metric): EUCLIDEAN or MANHATTAN.
func fnVectorNorm(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, argumentCountError("vector_norm", "2", len(args))
	}
	metric := strings.Trim(strings.TrimSpace(args[1]), "'")
	values, err := evalArgs(ctx, args[:1])
	if err != nil || values[0] == nil {
		return nil, err
	}
	vector, err := vectorArgument(values[0])
	if err != nil {
		return nil, err
	}
	// In float32, as vector_distance (see there).
	var sum float32
	for i := 0; i < vector.Dimension(); i++ {
		x := float32(vector.floatAt(i))
		if metric == "MANHATTAN" {
			sum += float32(math.Abs(float64(x)))
		} else {
			sum += float32(x * x)
		}
	}
	if metric == "MANHATTAN" {
		return float64(sum), nil
	}
	return float64(float32(math.Sqrt(float64(sum)))), nil
}

// fnUUID is uuid() (a random version 7 UUID), uuid(text) or
// uuid(msb, lsb); null in, null out.
func fnUUID(ctx cypherfn.Context, args []string) (interface{}, error) {
	switch len(args) {
	case 0:
		return newVersion7UUID(time.Now()), nil
	case 1, 2:
	default:
		return nil, argumentCountError("uuid", "0, 1 or 2", len(args))
	}
	values, err := evalArgs(ctx, args)
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		if value == nil {
			return nil, nil
		}
	}
	if len(values) == 1 {
		text, ok := values[0].(string)
		if !ok {
			return nil, &cypherfn.TypeMismatchError{Function: "uuid", Expected: "String", Value: values[0]}
		}
		parsed, ok := parseCypherUUID(text)
		if !ok {
			return nil, localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgumentValue", localization.CypherCoreUUIDInvalidText())
		}
		return parsed, nil
	}
	msb, okMSB := cypherIntegerValue(values[0])
	lsb, okLSB := cypherIntegerValue(values[1])
	if !okMSB {
		return nil, &cypherfn.TypeMismatchError{Function: "uuid", Expected: "Integer", Value: values[0]}
	}
	if !okLSB {
		return nil, &cypherfn.TypeMismatchError{Function: "uuid", Expected: "Integer", Value: values[1]}
	}
	return uuidFromHalves(msb, lsb), nil
}

// fnUUIDHalf is uuid.mostSignificantBits (most) or
// uuid.leastSignificantBits.
func fnUUIDHalf(most bool) cypherfn.Func {
	name := "uuid.leastSignificantBits"
	if most {
		name = "uuid.mostSignificantBits"
	}
	return func(ctx cypherfn.Context, args []string) (interface{}, error) {
		if len(args) != 1 {
			return nil, argumentCountError(name, "1", len(args))
		}
		values, err := evalArgs(ctx, args)
		if err != nil || values[0] == nil {
			return nil, err
		}
		u, ok := values[0].(CypherUUID)
		if !ok {
			return nil, &cypherfn.TypeMismatchError{Function: name, Expected: "UUID", Value: values[0]}
		}
		if most {
			return u.MostSignificantBits(), nil
		}
		return u.LeastSignificantBits(), nil
	}
}
