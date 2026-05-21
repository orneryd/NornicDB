package cypher

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) evaluateExpressionWithContextFullMath(
	ctx context.Context,
	expr string,
	lowerExpr string,
	nodes map[string]*storage.Node,
	rels map[string]*storage.Edge,
	paths map[string]*PathResult,
	allPathEdges []*storage.Edge,
	allPathNodes []*storage.Node,
	pathLength int,
) interface{} {
	// ========================================
	// Trigonometric Functions
	// ========================================

	// sin(x) - sine of x (radians)
	if matchFuncStartAndSuffix(expr, "sin") {
		inner := extractFuncArgs(expr, "sin")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Sin(f)
		}
		return nil
	}

	// cos(x) - cosine of x (radians)
	if matchFuncStartAndSuffix(expr, "cos") {
		inner := extractFuncArgs(expr, "cos")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Cos(f)
		}
		return nil
	}

	// tan(x) - tangent of x (radians)
	if matchFuncStartAndSuffix(expr, "tan") {
		inner := extractFuncArgs(expr, "tan")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Tan(f)
		}
		return nil
	}

	// cot(x) - cotangent of x (radians)
	if matchFuncStartAndSuffix(expr, "cot") {
		inner := extractFuncArgs(expr, "cot")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return 1.0 / math.Tan(f)
		}
		return nil
	}

	// asin(x) - arc sine
	if matchFuncStartAndSuffix(expr, "asin") {
		inner := extractFuncArgs(expr, "asin")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Asin(f)
		}
		return nil
	}

	// acos(x) - arc cosine
	if matchFuncStartAndSuffix(expr, "acos") {
		inner := extractFuncArgs(expr, "acos")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Acos(f)
		}
		return nil
	}

	// atan(x) - arc tangent
	if matchFuncStartAndSuffix(expr, "atan") {
		inner := extractFuncArgs(expr, "atan")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Atan(f)
		}
		return nil
	}

	// atan2(y, x) - arc tangent of y/x
	if matchFuncStartAndSuffix(expr, "atan2") {
		inner := extractFuncArgs(expr, "atan2")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			y, ok1 := toFloat64(e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[0]), nodes, rels))
			x, ok2 := toFloat64(e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[1]), nodes, rels))
			if ok1 && ok2 {
				return math.Atan2(y, x)
			}
		}
		return nil
	}

	// ========================================
	// Exponential and Logarithmic Functions
	// ========================================

	// exp(x) - e^x
	if matchFuncStartAndSuffix(expr, "exp") {
		inner := extractFuncArgs(expr, "exp")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Exp(f)
		}
		return nil
	}

	// log(x) - natural logarithm
	if matchFuncStartAndSuffix(expr, "log") {
		inner := extractFuncArgs(expr, "log")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Log(f)
		}
		return nil
	}

	// log10(x) - base-10 logarithm
	if matchFuncStartAndSuffix(expr, "log10") {
		inner := extractFuncArgs(expr, "log10")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Log10(f)
		}
		return nil
	}

	// sqrt(x) - square root
	if matchFuncStartAndSuffix(expr, "sqrt") {
		inner := extractFuncArgs(expr, "sqrt")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Sqrt(f)
		}
		return nil
	}

	// ========================================
	// Angle Conversion Functions
	// ========================================

	// radians(degrees) - convert degrees to radians
	if matchFuncStartAndSuffix(expr, "radians") {
		inner := extractFuncArgs(expr, "radians")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return f * math.Pi / 180.0
		}
		return nil
	}

	// degrees(radians) - convert radians to degrees
	if matchFuncStartAndSuffix(expr, "degrees") {
		inner := extractFuncArgs(expr, "degrees")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return f * 180.0 / math.Pi
		}
		return nil
	}

	// haversin(x) - half of versine = (1 - cos(x))/2
	if matchFuncStartAndSuffix(expr, "haversin") {
		inner := extractFuncArgs(expr, "haversin")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return (1 - math.Cos(f)) / 2
		}
		return nil
	}

	// sinh(x) - hyperbolic sine (Neo4j 2025.06+)
	if matchFuncStartAndSuffix(expr, "sinh") {
		inner := extractFuncArgs(expr, "sinh")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Sinh(f)
		}
		return nil
	}

	// cosh(x) - hyperbolic cosine (Neo4j 2025.06+)
	if matchFuncStartAndSuffix(expr, "cosh") {
		inner := extractFuncArgs(expr, "cosh")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Cosh(f)
		}
		return nil
	}

	// tanh(x) - hyperbolic tangent (Neo4j 2025.06+)
	if matchFuncStartAndSuffix(expr, "tanh") {
		inner := extractFuncArgs(expr, "tanh")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Tanh(f)
		}
		return nil
	}

	// coth(x) - hyperbolic cotangent (Neo4j 2025.06+)
	if matchFuncStartAndSuffix(expr, "coth") {
		inner := extractFuncArgs(expr, "coth")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			sinh := math.Sinh(f)
			if sinh == 0 {
				return math.NaN()
			}
			return math.Cosh(f) / sinh
		}
		return nil
	}

	// power(base, exponent) - raise base to power of exponent
	if matchFuncStartAndSuffix(expr, "power") {
		inner := extractFuncArgs(expr, "power")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			base, ok1 := toFloat64(e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[0]), nodes, rels))
			exp, ok2 := toFloat64(e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[1]), nodes, rels))
			if ok1 && ok2 {
				return math.Pow(base, exp)
			}
		}
		return nil
	}

	// ========================================
	// Mathematical Constants
	// ========================================

	// pi() - mathematical constant π
	if lowerExpr == "pi()" {
		return math.Pi
	}

	// e() - mathematical constant e
	if lowerExpr == "e()" {
		return math.E
	}

	// ========================================
	// Relationship Functions
	// ========================================

	// startNode(r) - return start node of relationship
	if matchFuncStartAndSuffix(expr, "startnode") {
		inner := extractFuncArgs(expr, "startnode")
		if rel, ok := rels[inner]; ok {
			node, err := e.storage.GetNode(rel.StartNode)
			if err == nil {
				return node
			}
		}
		return nil
	}

	// endNode(r) - return end node of relationship
	if matchFuncStartAndSuffix(expr, "endnode") {
		inner := extractFuncArgs(expr, "endnode")
		if rel, ok := rels[inner]; ok {
			node, err := e.storage.GetNode(rel.EndNode)
			if err == nil {
				return node
			}
		}
		return nil
	}

	// nodes(path) - return list of nodes in a path
	if matchFuncStartAndSuffix(expr, "nodes") {
		inner := extractFuncArgs(expr, "nodes")
		// First check if we have a full path by that variable name
		if paths != nil {
			if pathResult, ok := paths[inner]; ok && pathResult != nil {
				var result []interface{}
				for _, node := range pathResult.Nodes {
					result = append(result, node)
				}
				return result
			}
		}
		// Then check allPathNodes (for variable-length patterns without explicit path variable)
		if allPathNodes != nil && len(allPathNodes) > 0 {
			var result []interface{}
			for _, node := range allPathNodes {
				result = append(result, node)
			}
			return result
		}
		// Fallback: return single node from node context
		if node, ok := nodes[inner]; ok {
			return []interface{}{node}
		}
		return []interface{}{}
	}

	// relationships(path) - return list of relationships in a path
	if matchFuncStartAndSuffix(expr, "relationships") {
		inner := extractFuncArgs(expr, "relationships")
		// First check if we have a full path by that variable name
		if paths != nil {
			if pathResult, ok := paths[inner]; ok && pathResult != nil {
				var result []interface{}
				for _, edge := range pathResult.Relationships {
					result = append(result, map[string]interface{}{
						"_edgeId":    string(edge.ID),
						"type":       edge.Type,
						"properties": edge.Properties,
					})
				}
				return result
			}
		}
		// Then check allPathEdges (for variable-length patterns without explicit path variable)
		if allPathEdges != nil && len(allPathEdges) > 0 {
			var result []interface{}
			for _, edge := range allPathEdges {
				result = append(result, map[string]interface{}{
					"_edgeId":    string(edge.ID),
					"type":       edge.Type,
					"properties": edge.Properties,
				})
			}
			return result
		}
		// Fallback: return single relationship from rel context
		if rel, ok := rels[inner]; ok {
			return []interface{}{map[string]interface{}{
				"_edgeId":    string(rel.ID),
				"type":       rel.Type,
				"properties": rel.Properties,
			}}
		}
		return []interface{}{}
	}

	// ========================================
	// Null Check Functions
	// ========================================

	// isEmpty(list/map/string) - check if empty
	if matchFuncStartAndSuffix(expr, "isempty") {
		inner := extractFuncArgs(expr, "isempty")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		switch v := val.(type) {
		case nil:
			return true
		case string:
			return len(v) == 0
		case []interface{}:
			return len(v) == 0
		case map[string]interface{}:
			return len(v) == 0
		}
		return false
	}

	// isNaN(number) - check if not a number
	if matchFuncStartAndSuffix(expr, "isnan") {
		inner := extractFuncArgs(expr, "isnan")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.IsNaN(f)
		}
		return false
	}

	// nullIf(val1, val2) - return null if val1 = val2
	if matchFuncStartAndSuffix(expr, "nullif") {
		inner := extractFuncArgs(expr, "nullif")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			val1 := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[0]), nodes, rels)
			val2 := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[1]), nodes, rels)
			if fmt.Sprintf("%v", val1) == fmt.Sprintf("%v", val2) {
				return nil
			}
			return val1
		}
		return nil
	}

	// ========================================
	// String Functions (additional)
	// ========================================

	// btrim(string) / btrim(string, chars) - trim both sides
	if matchFuncStartAndSuffix(expr, "btrim") {
		inner := extractFuncArgs(expr, "btrim")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 1 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[0]), nodes, rels))
			if len(args) >= 2 {
				chars := fmt.Sprintf("%v", e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[1]), nodes, rels))
				return strings.Trim(str, chars)
			}
			return strings.TrimSpace(str)
		}
		return nil
	}

	// char_length(string)
	if matchFuncStartAndSuffix(expr, "char_length") {
		inner := extractFuncArgs(expr, "char_length")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if str, ok := val.(string); ok {
			return int64(len([]rune(str))) // Character count, not byte count
		}
		return nil
	}

	// character_length(string) - alias for char_length
	if matchFuncStartAndSuffix(expr, "character_length") {
		inner := extractFuncArgs(expr, "character_length")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if str, ok := val.(string); ok {
			return int64(len([]rune(str))) // Character count, not byte count
		}
		return nil
	}

	// normalize(string) - Unicode normalization
	if matchFuncStartAndSuffix(expr, "normalize") {
		inner := extractFuncArgs(expr, "normalize")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if str, ok := val.(string); ok {
			// Simple normalization - just return the string (full Unicode normalization would require unicode package)
			return str
		}
		return nil
	}

	// ========================================
	// Aggregation Functions (in expression context)
	// ========================================

	// percentileCont(expr, percentile) - continuous percentile
	if matchFuncStartAndSuffix(expr, "percentilecont") {
		inner := extractFuncArgs(expr, "percentilecont")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			// In single-row context, just return the value
			return e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[0]), nodes, rels)
		}
		return nil
	}

	// percentileDisc(expr, percentile) - discrete percentile
	if matchFuncStartAndSuffix(expr, "percentiledisc") {
		inner := extractFuncArgs(expr, "percentiledisc")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			// In single-row context, just return the value
			return e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[0]), nodes, rels)
		}
		return nil
	}

	// stDev(expr) - standard deviation
	if matchFuncStartAndSuffix(expr, "stdev") {
		inner := extractFuncArgs(expr, "stdev")
		// In single-row context, return 0
		_ = inner
		return float64(0)
	}

	// stDevP(expr) - population standard deviation
	if matchFuncStartAndSuffix(expr, "stdevp") {
		inner := extractFuncArgs(expr, "stdevp")
		// In single-row context, return 0
		_ = inner
		return float64(0)
	}

	// ========================================
	// Reduce Function
	// ========================================

	// reduce(acc = initial, x IN list | expr) - reduce a list
	if matchFuncStartAndSuffix(expr, "reduce") {
		inner := extractFuncArgs(expr, "reduce")

		// Parse: acc = initial, x IN list | expr
		eqIdx := strings.Index(inner, "=")
		commaIdx := strings.Index(inner, ",")
		inIdx := strings.Index(strings.ToUpper(inner), " IN ")
		pipeIdx := strings.Index(inner, "|")

		if eqIdx > 0 && commaIdx > eqIdx && inIdx > commaIdx && pipeIdx > inIdx {
			accName := strings.TrimSpace(inner[:eqIdx])
			initialExpr := strings.TrimSpace(inner[eqIdx+1 : commaIdx])
			varName := strings.TrimSpace(inner[commaIdx+1 : inIdx])
			listExpr := strings.TrimSpace(inner[inIdx+4 : pipeIdx])
			reduceExpr := strings.TrimSpace(inner[pipeIdx+1:])

			// Get initial value
			acc := e.evaluateExpressionWithContext(ctx, initialExpr, nodes, rels)

			// Get list
			list := e.evaluateExpressionWithContext(ctx, listExpr, nodes, rels)

			var items []interface{}
			switch v := list.(type) {
			case []interface{}:
				items = v
			default:
				items = []interface{}{list}
			}

			// Apply reduce using variable bindings in evaluation context.
			// Text replacement is incorrect for identifiers and nested expressions.
			for _, item := range items {
				// Create context with acc and item bound as scalar pseudo-nodes.
				tempNodes := make(map[string]*storage.Node)
				for k, v := range nodes {
					tempNodes[k] = v
				}
				tempNodes[accName] = &storage.Node{
					ID: storage.NodeID(accName),
					Properties: map[string]interface{}{
						"value": acc,
					},
				}
				tempNodes[varName] = &storage.Node{
					ID: storage.NodeID(varName),
					Properties: map[string]interface{}{
						"value": item,
					},
				}
				acc = e.evaluateExpressionWithContext(ctx, reduceExpr, tempNodes, rels)
			}

			return acc
		}
		return nil
	}

	// ========================================
	// Kalman Filter Functions
	// ========================================

	// kalman.init() or kalman.init({processNoise: 0.1, measurementNoise: 88.0})
	if matchFuncStartAndSuffix(expr, "kalman.init") {
		inner := extractFuncArgs(expr, "kalman.init")
		var configMap map[string]interface{}
		if inner != "" {
			val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
			if m, ok := val.(map[string]interface{}); ok {
				configMap = m
			}
		}
		return kalmanInit(configMap)
	}

	// kalman.process(measurement, stateJson) or kalman.process(measurement, stateJson, target)
	if matchFuncStartAndSuffix(expr, "kalman.process") {
		inner := extractFuncArgs(expr, "kalman.process")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			measurement, _ := toFloat64(e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[0]), nodes, rels))
			stateJSON, _ := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[1]), nodes, rels).(string)
			target := 0.0
			if len(args) >= 3 {
				target, _ = toFloat64(e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[2]), nodes, rels))
			}
			return kalmanProcess(measurement, stateJSON, target)
		}
		return nil
	}

	// kalman.predict(stateJson, steps)
	if matchFuncStartAndSuffix(expr, "kalman.predict") {
		inner := extractFuncArgs(expr, "kalman.predict")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			stateJSON, _ := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[0]), nodes, rels).(string)
			stepsFloat, _ := toFloat64(e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[1]), nodes, rels))
			steps := int(stepsFloat)
			return kalmanPredict(stateJSON, steps)
		}
		return nil
	}

	// kalman.state(stateJson) - get current state estimate
	if matchFuncStartAndSuffix(expr, "kalman.state") {
		inner := extractFuncArgs(expr, "kalman.state")
		stateJSON, _ := e.evaluateExpressionWithContext(ctx, inner, nodes, rels).(string)
		return kalmanStateValue(stateJSON)
	}

	// kalman.reset(stateJson) - reset to initial values
	if matchFuncStartAndSuffix(expr, "kalman.reset") {
		inner := extractFuncArgs(expr, "kalman.reset")
		stateJSON, _ := e.evaluateExpressionWithContext(ctx, inner, nodes, rels).(string)
		return kalmanReset(stateJSON)
	}

	// kalman.velocity.init() or kalman.velocity.init(initialPos, initialVel)
	if matchFuncStartAndSuffix(expr, "kalman.velocity.init") {
		inner := extractFuncArgs(expr, "kalman.velocity.init")
		if inner == "" {
			return kalmanVelocityInit(0, 0, false)
		}
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			pos, _ := toFloat64(e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[0]), nodes, rels))
			vel, _ := toFloat64(e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[1]), nodes, rels))
			return kalmanVelocityInit(pos, vel, true)
		}
		return kalmanVelocityInit(0, 0, false)
	}

	// kalman.velocity.process(measurement, stateJson)
	if matchFuncStartAndSuffix(expr, "kalman.velocity.process") {
		inner := extractFuncArgs(expr, "kalman.velocity.process")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			measurement, _ := toFloat64(e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[0]), nodes, rels))
			stateJSON, _ := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[1]), nodes, rels).(string)
			return kalmanVelocityProcess(measurement, stateJSON)
		}
		return nil
	}

	// kalman.velocity.predict(stateJson, steps)
	if matchFuncStartAndSuffix(expr, "kalman.velocity.predict") {
		inner := extractFuncArgs(expr, "kalman.velocity.predict")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			stateJSON, _ := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[0]), nodes, rels).(string)
			stepsFloat, _ := toFloat64(e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[1]), nodes, rels))
			steps := int(stepsFloat)
			return kalmanVelocityPredict(stateJSON, steps)
		}
		return nil
	}

	// kalman.adaptive.init() or kalman.adaptive.init({trendThreshold: 0.1, ...})
	if matchFuncStartAndSuffix(expr, "kalman.adaptive.init") {
		inner := extractFuncArgs(expr, "kalman.adaptive.init")
		var configMap map[string]interface{}
		if inner != "" {
			val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
			if m, ok := val.(map[string]interface{}); ok {
				configMap = m
			}
		}
		return kalmanAdaptiveInit(configMap)
	}

	// kalman.adaptive.process(measurement, stateJson)
	if matchFuncStartAndSuffix(expr, "kalman.adaptive.process") {
		inner := extractFuncArgs(expr, "kalman.adaptive.process")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			measurement, _ := toFloat64(e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[0]), nodes, rels))
			stateJSON, _ := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[1]), nodes, rels).(string)
			return kalmanAdaptiveProcess(measurement, stateJSON)
		}
		return nil
	}

	// ========================================
	// Vector Functions
	// ========================================

	// vector.similarity.cosine(v1, v2)
	if matchFuncStartAndSuffix(expr, "vector.similarity.cosine") {
		inner := extractFuncArgs(expr, "vector.similarity.cosine")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			v1 := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[0]), nodes, rels)
			v2 := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[1]), nodes, rels)

			vec1, ok1 := toFloat64Slice(v1)
			vec2, ok2 := toFloat64Slice(v2)

			if ok1 && ok2 && len(vec1) == len(vec2) {
				return vector.CosineSimilarityFloat64(vec1, vec2)
			}
		}
		return nil
	}

	// vector.similarity.euclidean(v1, v2)
	if matchFuncStartAndSuffix(expr, "vector.similarity.euclidean") {
		inner := extractFuncArgs(expr, "vector.similarity.euclidean")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			v1 := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[0]), nodes, rels)
			v2 := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[1]), nodes, rels)

			vec1, ok1 := toFloat64Slice(v1)
			vec2, ok2 := toFloat64Slice(v2)

			if ok1 && ok2 && len(vec1) == len(vec2) {
				return vector.EuclideanSimilarityFloat64(vec1, vec2)
			}
		}
		return nil
	}

	// ========================================
	// Point/Spatial Functions (basic support)
	// ========================================

	// point({x: val, y: val}) or point({latitude: val, longitude: val})
	if matchFuncStartAndSuffix(expr, "point") {
		inner := extractFuncArgs(expr, "point")
		// Return the point as a map
		if strings.HasPrefix(inner, "{") && strings.HasSuffix(inner, "}") {
			props := e.parseProperties(ctx, inner)
			return props
		}
		return nil
	}

	// distance(p1, p2) - Euclidean distance between two points
	if matchFuncStartAndSuffix(expr, "distance") {
		inner := extractFuncArgs(expr, "distance")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			p1 := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[0]), nodes, rels)
			p2 := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[1]), nodes, rels)

			m1, ok1 := p1.(map[string]interface{})
			m2, ok2 := p2.(map[string]interface{})

			if ok1 && ok2 {
				// Try x/y coordinates
				x1, y1, hasXY1 := getXY(m1)
				x2, y2, hasXY2 := getXY(m2)
				if hasXY1 && hasXY2 {
					return math.Sqrt((x2-x1)*(x2-x1) + (y2-y1)*(y2-y1))
				}

				// Try lat/long (haversine distance in meters)
				lat1, lon1, hasLatLon1 := getLatLon(m1)
				lat2, lon2, hasLatLon2 := getLatLon(m2)
				if hasLatLon1 && hasLatLon2 {
					return haversineDistance(lat1, lon1, lat2, lon2)
				}
			}
		}
		return nil
	}

	// withinBBox(point, lowerLeft, upperRight) - checks if point is within bounding box
	if matchFuncStartAndSuffix(expr, "withinbbox") {
		inner := extractFuncArgs(expr, "withinbbox")
		args := e.splitFunctionArgs(inner)
		if len(args) < 3 {
			return false
		}
		point := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[0]), nodes, rels)
		lowerLeft := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[1]), nodes, rels)
		upperRight := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[2]), nodes, rels)

		pm, ok1 := point.(map[string]interface{})
		llm, ok2 := lowerLeft.(map[string]interface{})
		urm, ok3 := upperRight.(map[string]interface{})
		if !ok1 || !ok2 || !ok3 {
			return false
		}

		// Try x/y coordinates
		px, py, hasXY := getXY(pm)
		llx, lly, hasLL := getXY(llm)
		urx, ury, hasUR := getXY(urm)

		if hasXY && hasLL && hasUR {
			return px >= llx && px <= urx && py >= lly && py <= ury
		}

		// Try lat/lon
		plat, plon, hasLatLon := getLatLon(pm)
		lllat, lllon, hasLLLatLon := getLatLon(llm)
		urlat, urlon, hasURLatLon := getLatLon(urm)

		if hasLatLon && hasLLLatLon && hasURLatLon {
			return plat >= lllat && plat <= urlat && plon >= lllon && plon <= urlon
		}

		return false
	}

	// point.x(point) - get x coordinate
	if matchFuncStartAndSuffix(expr, "point.x") {
		inner := extractFuncArgs(expr, "point.x")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			if x, ok := m["x"]; ok {
				if v, ok := toFloat64(x); ok {
					return v
				}
			}
		}
		return nil
	}

	// point.y(point) - get y coordinate
	if matchFuncStartAndSuffix(expr, "point.y") {
		inner := extractFuncArgs(expr, "point.y")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			if y, ok := m["y"]; ok {
				if v, ok := toFloat64(y); ok {
					return v
				}
			}
		}
		return nil
	}

	// point.z(point) - get z coordinate (3D points)
	if matchFuncStartAndSuffix(expr, "point.z") {
		inner := extractFuncArgs(expr, "point.z")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			if z, ok := m["z"]; ok {
				if v, ok := toFloat64(z); ok {
					return v
				}
			}
		}
		return nil
	}

	// point.latitude(point) - get latitude
	if matchFuncStartAndSuffix(expr, "point.latitude") {
		inner := extractFuncArgs(expr, "point.latitude")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			if lat, ok := m["latitude"]; ok {
				if v, ok := toFloat64(lat); ok {
					return v
				}
			}
		}
		return nil
	}

	// point.longitude(point) - get longitude
	if matchFuncStartAndSuffix(expr, "point.longitude") {
		inner := extractFuncArgs(expr, "point.longitude")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			if lon, ok := m["longitude"]; ok {
				if v, ok := toFloat64(lon); ok {
					return v
				}
			}
		}
		return nil
	}

	// point.srid(point) - get SRID (Spatial Reference System Identifier)
	if matchFuncStartAndSuffix(expr, "point.srid") {
		inner := extractFuncArgs(expr, "point.srid")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			if srid, ok := m["srid"]; ok {
				return srid
			}
			// Default SRID based on coordinate type
			if _, ok := m["latitude"]; ok {
				return int64(4326) // WGS84
			}
			return int64(7203) // Cartesian 2D
		}
		return nil
	}

	// point.distance(p1, p2) - alias for distance(p1, p2)
	if matchFuncStartAndSuffix(expr, "point.distance") {
		inner := extractFuncArgs(expr, "point.distance")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			p1 := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[0]), nodes, rels)
			p2 := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[1]), nodes, rels)

			m1, ok1 := p1.(map[string]interface{})
			m2, ok2 := p2.(map[string]interface{})

			if ok1 && ok2 {
				// Try x/y coordinates
				x1, y1, hasXY1 := getXY(m1)
				x2, y2, hasXY2 := getXY(m2)
				if hasXY1 && hasXY2 {
					return math.Sqrt((x2-x1)*(x2-x1) + (y2-y1)*(y2-y1))
				}

				// Try lat/long (haversine distance in meters)
				lat1, lon1, hasLatLon1 := getLatLon(m1)
				lat2, lon2, hasLatLon2 := getLatLon(m2)
				if hasLatLon1 && hasLatLon2 {
					return haversineDistance(lat1, lon1, lat2, lon2)
				}
			}
		}
		return nil
	}

	// point.withinBBox(point, lowerLeft, upperRight) - alias for withinBBox
	if matchFuncStartAndSuffix(expr, "point.withinbbox") {
		inner := extractFuncArgs(expr, "point.withinbbox")
		args := e.splitFunctionArgs(inner)
		if len(args) < 3 {
			return false
		}
		point := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[0]), nodes, rels)
		lowerLeft := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[1]), nodes, rels)
		upperRight := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[2]), nodes, rels)

		pm, ok1 := point.(map[string]interface{})
		llm, ok2 := lowerLeft.(map[string]interface{})
		urm, ok3 := upperRight.(map[string]interface{})
		if !ok1 || !ok2 || !ok3 {
			return false
		}

		px, py, hasXY := getXY(pm)
		llx, lly, hasLL := getXY(llm)
		urx, ury, hasUR := getXY(urm)

		if hasXY && hasLL && hasUR {
			return px >= llx && px <= urx && py >= lly && py <= ury
		}

		plat, plon, hasLatLon := getLatLon(pm)
		lllat, lllon, hasLLLatLon := getLatLon(llm)
		urlat, urlon, hasURLatLon := getLatLon(urm)

		if hasLatLon && hasLLLatLon && hasURLatLon {
			return plat >= lllat && plat <= urlat && plon >= lllon && plon <= urlon
		}

		return false
	}

	// point.withinDistance(point, center, distance) - check if point is within distance of center
	if matchFuncStartAndSuffix(expr, "point.withindistance") {
		inner := extractFuncArgs(expr, "point.withindistance")
		args := e.splitFunctionArgs(inner)
		if len(args) < 3 {
			return false
		}
		point := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[0]), nodes, rels)
		center := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[1]), nodes, rels)
		maxDist := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[2]), nodes, rels)

		pm, ok1 := point.(map[string]interface{})
		cm, ok2 := center.(map[string]interface{})
		dist, ok3 := toFloat64(maxDist)
		if !ok1 || !ok2 || !ok3 {
			return false
		}

		// Calculate distance between point and center
		x1, y1, hasXY1 := getXY(pm)
		x2, y2, hasXY2 := getXY(cm)
		if hasXY1 && hasXY2 {
			actualDist := math.Sqrt((x2-x1)*(x2-x1) + (y2-y1)*(y2-y1))
			return actualDist <= dist
		}

		lat1, lon1, hasLatLon1 := getLatLon(pm)
		lat2, lon2, hasLatLon2 := getLatLon(cm)
		if hasLatLon1 && hasLatLon2 {
			actualDist := haversineDistance(lat1, lon1, lat2, lon2)
			return actualDist <= dist
		}

		return false
	}

	// point.height(point) - get height/altitude (alias for z coordinate)
	if matchFuncStartAndSuffix(expr, "point.height") {
		inner := extractFuncArgs(expr, "point.height")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			// Try z first (3D Cartesian)
			if z, ok := m["z"]; ok {
				if v, ok := toFloat64(z); ok {
					return v
				}
			}
			// Try height (geographic)
			if h, ok := m["height"]; ok {
				if v, ok := toFloat64(h); ok {
					return v
				}
			}
			// Try altitude (alternative name)
			if alt, ok := m["altitude"]; ok {
				if v, ok := toFloat64(alt); ok {
					return v
				}
			}
		}
		return nil
	}

	// point.crs(point) - get Coordinate Reference System name
	if matchFuncStartAndSuffix(expr, "point.crs") {
		inner := extractFuncArgs(expr, "point.crs")
		val := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			// Check if CRS is explicitly set
			if crs, ok := m["crs"]; ok {
				return crs
			}
			// Infer CRS from coordinate type
			if _, ok := m["latitude"]; ok {
				if _, ok := m["height"]; ok {
					return "wgs-84-3d"
				}
				return "wgs-84"
			}
			if _, ok := m["z"]; ok {
				return "cartesian-3d"
			}
			return "cartesian"
		}
		return nil
	}

	// polygon(points) - create a polygon geometry from a list of points
	if matchFuncStartAndSuffix(expr, "polygon") {
		inner := extractFuncArgs(expr, "polygon")

		// Check if inner is a list literal [...]
		if strings.HasPrefix(inner, "[") && strings.HasSuffix(inner, "]") {
			// Parse and evaluate list elements manually
			listContent := inner[1 : len(inner)-1]
			pointExprs := e.splitFunctionArgs(listContent)

			pointList := make([]interface{}, 0, len(pointExprs))
			for _, pointExpr := range pointExprs {
				evalPoint := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(pointExpr), nodes, rels)
				pointList = append(pointList, evalPoint)
			}

			// Validate that we have at least 3 points for a valid polygon
			if len(pointList) < 3 {
				return nil
			}

			// Return a polygon structure
			return map[string]interface{}{
				"type":   "polygon",
				"points": pointList,
			}
		}

		// Otherwise try evaluating as variable or expression
		pointsVal := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if pointList, ok := pointsVal.([]interface{}); ok {
			if len(pointList) < 3 {
				return nil
			}
			return map[string]interface{}{
				"type":   "polygon",
				"points": pointList,
			}
		}
		return nil
	}

	// lineString(points) - create a lineString geometry from a list of points
	if matchFuncStartAndSuffix(expr, "linestring") {
		inner := extractFuncArgs(expr, "linestring")

		// Check if inner is a list literal [...]
		if strings.HasPrefix(inner, "[") && strings.HasSuffix(inner, "]") {
			// Parse and evaluate list elements manually
			listContent := inner[1 : len(inner)-1]
			pointExprs := e.splitFunctionArgs(listContent)

			pointList := make([]interface{}, 0, len(pointExprs))
			for _, pointExpr := range pointExprs {
				evalPoint := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(pointExpr), nodes, rels)
				pointList = append(pointList, evalPoint)
			}

			// Validate that we have at least 2 points for a valid lineString
			if len(pointList) < 2 {
				return nil
			}

			// Return a lineString structure
			return map[string]interface{}{
				"type":   "linestring",
				"points": pointList,
			}
		}

		// Otherwise try evaluating as variable or expression
		pointsVal := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if pointList, ok := pointsVal.([]interface{}); ok {
			if len(pointList) < 2 {
				return nil
			}
			return map[string]interface{}{
				"type":   "linestring",
				"points": pointList,
			}
		}
		return nil
	}

	// point.intersects(point, polygon) - check if point intersects with polygon
	if matchFuncStartAndSuffix(expr, "point.intersects") {
		inner := extractFuncArgs(expr, "point.intersects")
		args := e.splitFunctionArgs(inner)
		if len(args) < 2 {
			return false
		}

		pointVal := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[0]), nodes, rels)
		polygonVal := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[1]), nodes, rels)

		pm, ok1 := pointVal.(map[string]interface{})
		polygonMap, ok2 := polygonVal.(map[string]interface{})
		if !ok1 || !ok2 {
			return false
		}

		// Extract polygon points
		polygonPoints := extractPolygonPoints(polygonMap)
		if polygonPoints == nil {
			return false
		}

		// Get point coordinates
		px, py, hasXY := getXY(pm)
		if !hasXY {
			// Try lat/lon
			var hasLatLon bool
			px, py, hasLatLon = getLatLon(pm)
			if !hasLatLon {
				return false
			}
		}

		// Use point-in-polygon algorithm
		return pointInPolygon(px, py, polygonPoints)
	}

	// point.contains(polygon, point) - check if polygon contains point
	if matchFuncStartAndSuffix(expr, "point.contains") {
		inner := extractFuncArgs(expr, "point.contains")
		args := e.splitFunctionArgs(inner)
		if len(args) < 2 {
			return false
		}

		polygonVal := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[0]), nodes, rels)
		pointVal := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(args[1]), nodes, rels)

		polygonMap, ok1 := polygonVal.(map[string]interface{})
		pm, ok2 := pointVal.(map[string]interface{})
		if !ok1 || !ok2 {
			return false
		}

		// Extract polygon points
		polygonPoints := extractPolygonPoints(polygonMap)
		if polygonPoints == nil {
			return false
		}

		// Get point coordinates
		px, py, hasXY := getXY(pm)
		if !hasXY {
			// Try lat/lon
			var hasLatLon bool
			px, py, hasLatLon = getLatLon(pm)
			if !hasLatLon {
				return false
			}
		}

		// Use point-in-polygon algorithm
		return pointInPolygon(px, py, polygonPoints)
	}

	// ========================================
	// List Predicate Functions
	// ========================================

	// all(variable IN list WHERE predicate) - check if all elements match
	if matchFuncStartAndSuffix(expr, "all") {
		inner := extractFuncArgs(expr, "all")
		// Parse "variable IN list WHERE predicate"
		inIdx := strings.Index(strings.ToLower(inner), " in ")
		if inIdx == -1 {
			return false
		}
		varName := strings.TrimSpace(inner[:inIdx])
		rest := inner[inIdx+4:]
		whereIdx := strings.Index(strings.ToLower(rest), " where ")
		if whereIdx == -1 {
			return false
		}
		listExpr := strings.TrimSpace(rest[:whereIdx])
		predicate := strings.TrimSpace(rest[whereIdx+7:])

		list := e.evaluateExpressionWithContext(ctx, listExpr, nodes, rels)
		listVal, ok := list.([]interface{})
		if !ok {
			return false
		}

		for _, item := range listVal {
			// Create temporary context with variable
			tempNodes := make(map[string]*storage.Node)
			for k, v := range nodes {
				tempNodes[k] = v
			}
			// For simple values, we need to substitute in the predicate
			predWithVal := strings.ReplaceAll(predicate, varName, fmt.Sprintf("%v", item))
			result := e.evaluateExpressionWithContext(ctx, predWithVal, tempNodes, rels)
			if result != true {
				return false
			}
		}
		return true
	}

	// any(variable IN list WHERE predicate) - check if any element matches
	if matchFuncStartAndSuffix(expr, "any") {
		inner := extractFuncArgs(expr, "any")
		inIdx := strings.Index(strings.ToLower(inner), " in ")
		if inIdx == -1 {
			return false
		}
		varName := strings.TrimSpace(inner[:inIdx])
		rest := inner[inIdx+4:]
		whereIdx := strings.Index(strings.ToLower(rest), " where ")
		if whereIdx == -1 {
			return false
		}
		listExpr := strings.TrimSpace(rest[:whereIdx])
		predicate := strings.TrimSpace(rest[whereIdx+7:])

		list := e.evaluateExpressionWithContext(ctx, listExpr, nodes, rels)
		listVal, ok := list.([]interface{})
		if !ok {
			return false
		}

		for _, item := range listVal {
			predWithVal := strings.ReplaceAll(predicate, varName, fmt.Sprintf("%v", item))
			result := e.evaluateExpressionWithContext(ctx, predWithVal, nodes, rels)
			if result == true {
				return true
			}
		}
		return false
	}

	// none(variable IN list WHERE predicate) - check if no element matches
	if matchFuncStartAndSuffix(expr, "none") {
		inner := extractFuncArgs(expr, "none")
		inIdx := strings.Index(strings.ToLower(inner), " in ")
		if inIdx == -1 {
			return true
		}
		varName := strings.TrimSpace(inner[:inIdx])
		rest := inner[inIdx+4:]
		whereIdx := strings.Index(strings.ToLower(rest), " where ")
		if whereIdx == -1 {
			return true
		}
		listExpr := strings.TrimSpace(rest[:whereIdx])
		predicate := strings.TrimSpace(rest[whereIdx+7:])

		list := e.evaluateExpressionWithContext(ctx, listExpr, nodes, rels)
		listVal, ok := list.([]interface{})
		if !ok {
			return true
		}

		for _, item := range listVal {
			predWithVal := strings.ReplaceAll(predicate, varName, fmt.Sprintf("%v", item))
			result := e.evaluateExpressionWithContext(ctx, predWithVal, nodes, rels)
			if result == true {
				return false
			}
		}
		return true
	}

	// single(variable IN list WHERE predicate) - check if exactly one element matches
	if matchFuncStartAndSuffix(expr, "single") {
		inner := extractFuncArgs(expr, "single")
		inIdx := strings.Index(strings.ToLower(inner), " in ")
		if inIdx == -1 {
			return false
		}
		varName := strings.TrimSpace(inner[:inIdx])
		rest := inner[inIdx+4:]
		whereIdx := strings.Index(strings.ToLower(rest), " where ")
		if whereIdx == -1 {
			return false
		}
		listExpr := strings.TrimSpace(rest[:whereIdx])
		predicate := strings.TrimSpace(rest[whereIdx+7:])

		list := e.evaluateExpressionWithContext(ctx, listExpr, nodes, rels)
		listVal, ok := list.([]interface{})
		if !ok {
			return false
		}

		matchCount := 0
		for _, item := range listVal {
			predWithVal := strings.ReplaceAll(predicate, varName, fmt.Sprintf("%v", item))
			result := e.evaluateExpressionWithContext(ctx, predWithVal, nodes, rels)
			if result == true {
				matchCount++
				if matchCount > 1 {
					return false
				}
			}
		}
		return matchCount == 1
	}

	// ========================================
	// Additional List Functions
	// ========================================

	// filter(variable IN list WHERE predicate) - filter list elements
	if matchFuncStartAndSuffix(expr, "filter") {
		inner := extractFuncArgs(expr, "filter")
		inIdx := strings.Index(strings.ToLower(inner), " in ")
		if inIdx == -1 {
			return []interface{}{}
		}
		varName := strings.TrimSpace(inner[:inIdx])
		rest := inner[inIdx+4:]
		whereIdx := strings.Index(strings.ToLower(rest), " where ")
		if whereIdx == -1 {
			return []interface{}{}
		}
		listExpr := strings.TrimSpace(rest[:whereIdx])
		predicate := strings.TrimSpace(rest[whereIdx+7:])

		list := e.evaluateExpressionWithContext(ctx, listExpr, nodes, rels)
		listVal, ok := list.([]interface{})
		if !ok {
			return []interface{}{}
		}

		result := make([]interface{}, 0)
		for _, item := range listVal {
			predWithVal := strings.ReplaceAll(predicate, varName, fmt.Sprintf("%v", item))
			res := e.evaluateExpressionWithContext(ctx, predWithVal, nodes, rels)
			if res == true {
				result = append(result, item)
			}
		}
		return result
	}

	// extract(variable IN list | expression) - transform list elements
	if matchFuncStartAndSuffix(expr, "extract") {
		inner := extractFuncArgs(expr, "extract")
		inIdx := strings.Index(strings.ToLower(inner), " in ")
		if inIdx == -1 {
			return []interface{}{}
		}
		varName := strings.TrimSpace(inner[:inIdx])
		rest := inner[inIdx+4:]
		pipeIdx := strings.Index(rest, " | ")
		if pipeIdx == -1 {
			return []interface{}{}
		}
		listExpr := strings.TrimSpace(rest[:pipeIdx])
		transform := strings.TrimSpace(rest[pipeIdx+3:])

		list := e.evaluateExpressionWithContext(ctx, listExpr, nodes, rels)
		listVal, ok := list.([]interface{})
		if !ok {
			return []interface{}{}
		}

		result := make([]interface{}, len(listVal))
		for i, item := range listVal {
			// Simple variable substitution for primitive values
			transformWithVal := strings.ReplaceAll(transform, varName, fmt.Sprintf("%v", item))
			result[i] = e.evaluateExpressionWithContext(ctx, transformWithVal, nodes, rels)
		}
		return result
	}

	// [x IN list WHERE condition] - list comprehension with filter
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") && strings.Contains(expr, " IN ") && strings.Contains(strings.ToUpper(expr), " WHERE ") {
		inner := strings.TrimSpace(expr[1 : len(expr)-1])
		upperInner := strings.ToUpper(inner)
		inIdx := strings.Index(upperInner, " IN ")
		whereIdx := strings.Index(upperInner, " WHERE ")

		if inIdx > 0 && whereIdx > inIdx {
			varName := strings.TrimSpace(inner[:inIdx])
			listExpr := strings.TrimSpace(inner[inIdx+4 : whereIdx])
			condition := strings.TrimSpace(inner[whereIdx+7:])

			// Evaluate the list expression
			list := e.evaluateExpressionWithContext(ctx, listExpr, nodes, rels)

			// Convert to []interface{} if needed
			var items []interface{}
			switch v := list.(type) {
			case []interface{}:
				items = v
			case []string:
				items = make([]interface{}, len(v))
				for i, s := range v {
					items[i] = s
				}
			default:
				return []interface{}{}
			}

			// Filter by condition
			result := make([]interface{}, 0, len(items))
			for _, item := range items {
				// Evaluate condition with item substituted
				itemStr := fmt.Sprintf("%v", item)

				// Handle different condition patterns
				matches := true
				if strings.Contains(condition, "<>") {
					parts := strings.SplitN(condition, "<>", 2)
					condVar := strings.TrimSpace(parts[0])
					condVal := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
					if condVar == varName {
						matches = itemStr != condVal
					}
				} else if strings.Contains(condition, "!=") {
					parts := strings.SplitN(condition, "!=", 2)
					condVar := strings.TrimSpace(parts[0])
					condVal := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
					if condVar == varName {
						matches = itemStr != condVal
					}
				} else if strings.Contains(condition, ">=") {
					parts := strings.SplitN(condition, ">=", 2)
					condVar := strings.TrimSpace(parts[0])
					condVal := strings.TrimSpace(parts[1])
					if condVar == varName {
						if itemNum, ok := toFloat64(item); ok {
							if condNum, ok := toFloat64(e.parseValue(ctx, condVal)); ok {
								matches = itemNum >= condNum
							}
						}
					}
				} else if strings.Contains(condition, ">") {
					parts := strings.SplitN(condition, ">", 2)
					condVar := strings.TrimSpace(parts[0])
					condVal := strings.TrimSpace(parts[1])
					if condVar == varName {
						if itemNum, ok := toFloat64(item); ok {
							if condNum, ok := toFloat64(e.parseValue(ctx, condVal)); ok {
								matches = itemNum > condNum
							}
						}
					}
				} else if strings.Contains(condition, "=") {
					parts := strings.SplitN(condition, "=", 2)
					condVar := strings.TrimSpace(parts[0])
					condVal := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
					if condVar == varName {
						matches = itemStr == condVal
					}
				}

				if matches {
					result = append(result, item)
				}
			}
			return result
		}
	}

	// [x IN list | expression] - list comprehension with transformation
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") && strings.Contains(expr, " IN ") && strings.Contains(expr, " | ") {
		inner := strings.TrimSpace(expr[1 : len(expr)-1])
		inIdx := strings.Index(strings.ToUpper(inner), " IN ")
		if inIdx > 0 {
			varName := strings.TrimSpace(inner[:inIdx])
			rest := inner[inIdx+4:]
			pipeIdx := strings.Index(rest, " | ")
			if pipeIdx > 0 {
				listExpr := strings.TrimSpace(rest[:pipeIdx])
				transform := strings.TrimSpace(rest[pipeIdx+3:])

				// Use full context to properly evaluate path functions like relationships(path)
				list := e.evaluateExpressionWithContextFull(ctx, listExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
				listVal, ok := list.([]interface{})
				if !ok {
					return []interface{}{}
				}

				result := make([]interface{}, len(listVal))
				for i, item := range listVal {
					// For simple function calls like type(r), handle the item directly
					// instead of string replacement which breaks for map types
					if matchFuncStartAndSuffix(transform, "type") {
						// Extract type from relationship map
						if mapItem, ok := item.(map[string]interface{}); ok {
							if relType, ok := mapItem["type"]; ok {
								result[i] = relType
								continue
							}
						}
						// Fallback: try to get type from storage.Edge
						if edge, ok := item.(*storage.Edge); ok {
							result[i] = edge.Type
							continue
						}
					}

					if matchFuncStartAndSuffix(transform, "id") {
						// Extract id from relationship map or *storage.Edge
						if mapItem, ok := item.(map[string]interface{}); ok {
							if id, ok := mapItem["_edgeId"]; ok {
								result[i] = id
								continue
							}
						}
						if edge, ok := item.(*storage.Edge); ok {
							result[i] = string(edge.ID)
							continue
						}
					}

					// Fallback to string replacement (may not work for complex types)
					transformWithVal := strings.ReplaceAll(transform, varName, fmt.Sprintf("%v", item))
					result[i] = e.evaluateExpressionWithContextFull(ctx, transformWithVal, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
				}
				return result
			}
		}
	}

	// [x IN list] - simple list comprehension (identity)
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") && strings.Contains(expr, " IN ") {
		inner := strings.TrimSpace(expr[1 : len(expr)-1])
		upperInner := strings.ToUpper(inner)
		inIdx := strings.Index(upperInner, " IN ")
		// Only if no WHERE or | (those are handled above)
		if inIdx > 0 && !strings.Contains(upperInner, " WHERE ") && !strings.Contains(inner, " | ") {
			listExpr := strings.TrimSpace(inner[inIdx+4:])
			list := e.evaluateExpressionWithContext(ctx, listExpr, nodes, rels)

			switch v := list.(type) {
			case []interface{}:
				return v
			case []string:
				result := make([]interface{}, len(v))
				for i, s := range v {
					result[i] = s
				}
				return result
			default:
				return []interface{}{list}
			}
		}
	}

	// ========================================
	// CASE WHEN Expressions (must be before operators)
	// ========================================
	if strings.HasPrefix(lowerExpr, "case") && strings.HasSuffix(lowerExpr, "end") {
		return e.evaluateCaseExpression(ctx, expr, nodes, rels)
	}

	// ========================================

	return e.evaluateExpressionWithContextFullOperators(ctx, expr, lowerExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
}
