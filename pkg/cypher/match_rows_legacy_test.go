package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
	"strconv"
	"strings"
)

// evaluateSumArithmetic is retained only for the legacy helper tests.
// evaluateSumArithmetic handles expressions like SUM(n.a) + SUM(n.b)
func (e *StorageExecutor) evaluateSumArithmetic(expr string, nodes []*storage.Node, variable string) float64 {
	// Split by + and - operators (respecting parentheses)
	parts := splitArithmeticExpression(expr)
	result := float64(0)
	currentOp := "+"

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "+" {
			currentOp = "+"
			continue
		}
		if part == "-" {
			currentOp = "-"
			continue
		}

		// Evaluate this part
		var value float64
		upperPart := upperASCII(part)

		if strings.HasPrefix(upperPart, "SUM(") {
			if agg := ParseAggregation(part); agg != nil && agg.Function == "SUM" && agg.Property != "" {
				for _, node := range nodes {
					if val, exists := node.Properties[agg.Property]; exists {
						if num, ok := toFloat64(val); ok {
							value += num
						}
					}
				}
			}
		} else if num, err := strconv.ParseFloat(part, 64); err == nil {
			value = num
		}

		// Apply operator
		if currentOp == "+" {
			result += value
		} else {
			result -= value
		}
	}

	return result
}

// splitArithmeticExpression is a legacy, test-only arithmetic adapter.
// Live expression grammar belongs to the expression owners, not this helper.
func splitArithmeticExpression(expr string) []string {
	var parts []string
	start, depth := 0, 0
	for i := 0; i < len(expr); i++ {
		if end, opaque := cyphertext.SkipToken(expr, i); opaque {
			i = end - 1
			continue
		}
		if number, ok := cyphertext.ScanNumber(expr, i); ok {
			i = number.TailEnd - 1
			continue
		}
		switch expr[i] {
		case '(':
			depth++
		case ')':
			depth--
		case '+', '-':
			unary := i == 0 || expr[i-1] == '+' || expr[i-1] == '-' || expr[i-1] == '('
			if depth == 0 && !unary {
				if start < i {
					parts = append(parts, expr[start:i])
				}
				parts = append(parts, expr[i:i+1])
				start = i + 1
			}
		}
	}
	if start < len(expr) {
		parts = append(parts, expr[start:])
	}
	return parts
}
