package cypher

import (
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/math/vector"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)

// vector.similarity.cosine(a, b) and vector.similarity.euclidean(a, b), as
// Neo4j 5.26 and 2026.09 compute them (#907): a score in [0, 1], bit for bit
// (vector.Neo4jCosineSimilarity, vector.Neo4jEuclideanSimilarity). Null in,
// null out. An argument may be a VECTOR value or a list; one that is neither
// is a TypeError; a list that isn't a
// valid vector for the function (empty, a coordinate that isn't a finite
// number, a zero vector for cosine) and vectors of different dimensions are
// Neo4j's ArgumentErrors.

func init() {
	cypherfn.Register("vector.similarity.cosine", fnVectorSimilarity("cosine", vector.Neo4jCosineVectorValid[float64], vector.Neo4jCosineSimilarity[float64]))
	cypherfn.Register("vector.similarity.euclidean", fnVectorSimilarity("euclidean", vector.Neo4jEuclideanVectorValid[float64], vector.Neo4jEuclideanSimilarity[float64]))
}

// fnVectorSimilarity checks a, then b, then their dimensions, as Neo4j does.
func fnVectorSimilarity(function string, valid func([]float64) bool, similarity func(a, b []float64) (float64, bool)) cypherfn.Func {
	return func(ctx cypherfn.Context, args []string) (interface{}, error) {
		if len(args) != 2 {
			return nil, argumentCountError("vector.similarity."+function, "2", len(args))
		}
		values, err := evalArgs(ctx, args)
		if err != nil || values[0] == nil || values[1] == nil {
			return nil, err
		}
		a, err := similarityVectorArgument(function, "a", values[0], valid)
		if err != nil {
			return nil, err
		}
		b, err := similarityVectorArgument(function, "b", values[1], valid)
		if err != nil {
			return nil, err
		}
		if len(a) != len(b) {
			return nil, localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgumentValue",
				localization.CypherCoreVectorSimilarityDimensions(function))
		}
		// Valid vectors of one length always have a score.
		score, _ := similarity(a, b)
		return score, nil
	}
}

// similarityVectorArgument reads a vector.similarity argument: a VECTOR, or
// a list of numbers, that is a valid vector for the function. A list holding
// anything else isn't a valid vector; a value that is neither is a
// TypeError.
func similarityVectorArgument(function, argument string, value interface{}, valid func([]float64) bool) ([]float64, error) {
	if typed, isVector := value.(CypherVector); isVector {
		coordinates := make([]float64, typed.Dimension())
		for i := range coordinates {
			coordinates[i] = typed.floatAt(i)
		}
		if !valid(coordinates) {
			return nil, invalidSimilarityVector(function, argument)
		}
		return coordinates, nil
	}
	items, isList := cypherListValue(value)
	if !isList {
		return nil, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
			localization.CypherCoreFunctionArgumentInvalid("vector.similarity."+function, "LIST<INTEGER | FLOAT>", neo4jValueRepr(value)))
	}
	coordinates := make([]float64, len(items))
	for i, item := range items {
		switch number := item.(type) {
		case int64:
			coordinates[i] = float64(number)
		case float64:
			coordinates[i] = number
		default:
			converted, isNumber := numericCoordinate(item)
			if !isNumber {
				return nil, invalidSimilarityVector(function, argument)
			}
			coordinates[i] = converted
		}
	}
	if !valid(coordinates) {
		return nil, invalidSimilarityVector(function, argument)
	}
	return coordinates, nil
}

// numericCoordinate is a Go number of another width as a float64 (stored
// float32 embeddings, ints); ok is false for anything that isn't a number.
func numericCoordinate(value interface{}) (float64, bool) {
	switch number := value.(type) {
	case float32:
		return float64(number), true
	case int:
		return float64(number), true
	case int32:
		return float64(number), true
	}
	return 0, false
}

func invalidSimilarityVector(function, argument string) error {
	return localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgumentValue",
		localization.CypherCoreVectorSimilarityInvalidVector(function, argument))
}
