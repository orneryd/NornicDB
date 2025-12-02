// Package examples demonstrates how to create an APOC plugin.
//
// To build this as a plugin:
//  1. Change "package examples" back to "package main"
//  2. Run: go build -buildmode=plugin -o apoc-ml.so plugin_example.go
//
// Load it in your application:
//
//	apoc.LoadPlugin("./plugins/apoc-ml.so")
//
// Note: This file is kept as a non-main package to avoid build errors
// during normal builds. Change to "package main" only when building as plugin.
package examples

import (
	"math"

	"github.com/orneryd/nornicdb/apoc"
)

// Plugin is the exported symbol that NornicDB will load.
var Plugin MLPlugin

// MLPlugin provides machine learning functions.
type MLPlugin struct{}

func (p MLPlugin) Name() string {
	return "ml"
}

func (p MLPlugin) Version() string {
	return "1.0.0"
}

func (p MLPlugin) Functions() map[string]apoc.PluginFunction {
	return map[string]apoc.PluginFunction{
		"sigmoid": {
			Handler:     Sigmoid,
			Description: "Sigmoid activation function",
			Examples:    []string{"apoc.ml.sigmoid(0) => 0.5", "apoc.ml.sigmoid(1) => 0.73"},
		},
		"relu": {
			Handler:     ReLU,
			Description: "ReLU activation function",
			Examples:    []string{"apoc.ml.relu(-5) => 0", "apoc.ml.relu(5) => 5"},
		},
		"softmax": {
			Handler:     Softmax,
			Description: "Softmax function for classification",
			Examples:    []string{"apoc.ml.softmax([1,2,3]) => [0.09, 0.24, 0.67]"},
		},
		"cosineSimilarity": {
			Handler:     CosineSimilarity,
			Description: "Cosine similarity between vectors",
			Examples:    []string{"apoc.ml.cosineSimilarity([1,2,3], [4,5,6]) => 0.97"},
		},
		"euclideanDistance": {
			Handler:     EuclideanDistance,
			Description: "Euclidean distance between vectors",
			Examples:    []string{"apoc.ml.euclideanDistance([0,0], [3,4]) => 5.0"},
		},
	}
}

// Sigmoid activation function.
func Sigmoid(x float64) float64 {
	return 1.0 / (1.0 + math.Exp(-x))
}

// ReLU activation function.
func ReLU(x float64) float64 {
	if x < 0 {
		return 0
	}
	return x
}

// Softmax function for classification.
func Softmax(values []float64) []float64 {
	if len(values) == 0 {
		return []float64{}
	}

	// Find max for numerical stability
	max := values[0]
	for _, v := range values[1:] {
		if v > max {
			max = v
		}
	}

	// Compute exp(x - max)
	expValues := make([]float64, len(values))
	sum := 0.0
	for i, v := range values {
		expValues[i] = math.Exp(v - max)
		sum += expValues[i]
	}

	// Normalize
	result := make([]float64, len(values))
	for i, exp := range expValues {
		result[i] = exp / sum
	}

	return result
}

// CosineSimilarity calculates cosine similarity between two vectors.
func CosineSimilarity(a, b []float64) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}

	var dotProduct, normA, normB float64
	for i := 0; i < len(a); i++ {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 0
	}

	return dotProduct / (math.Sqrt(normA) * math.Sqrt(normB))
}

// EuclideanDistance calculates Euclidean distance between two vectors.
func EuclideanDistance(a, b []float64) float64 {
	if len(a) != len(b) {
		return 0
	}

	var sum float64
	for i := 0; i < len(a); i++ {
		diff := a[i] - b[i]
		sum += diff * diff
	}

	return math.Sqrt(sum)
}
