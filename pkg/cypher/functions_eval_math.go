package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/math/angle"
	math "github.com/orneryd/nornicdb/pkg/math/libm"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) evaluateExpressionWithContextFullMath(
	ctx context.Context,
	view braceView,
	lowerExpr string,
	nodes map[string]*storage.Node,
	rels map[string]*storage.Edge,
	paths map[string]*PathResult,
	allPathEdges []*storage.Edge,
	allPathNodes []*storage.Node,
	pathLength int,
) interface{} {
	expr := view.text()
	ctx = withBraceView(ctx, view)
	// ========================================
	// Trigonometric Functions
	// ========================================

	// sin(x) - sine of x (radians)
	if matchFuncStartAndSuffix(expr, "sin") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return math.Sin(f)
		}
		return nil
	}

	// cos(x) - cosine of x (radians)
	if matchFuncStartAndSuffix(expr, "cos") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return math.Cos(f)
		}
		return nil
	}

	// tan(x) - tangent of x (radians)
	if matchFuncStartAndSuffix(expr, "tan") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return math.Tan(f)
		}
		return nil
	}

	// cot(x) - cotangent of x (radians)
	if matchFuncStartAndSuffix(expr, "cot") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return 1.0 / math.Tan(f)
		}
		return nil
	}

	// asin(x) - arc sine
	if matchFuncStartAndSuffix(expr, "asin") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return math.Asin(f)
		}
		return nil
	}

	// acos(x) - arc cosine
	if matchFuncStartAndSuffix(expr, "acos") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return math.Acos(f)
		}
		return nil
	}

	// atan(x) - arc tangent
	if matchFuncStartAndSuffix(expr, "atan") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return math.Atan(f)
		}
		return nil
	}

	// atan2(y, x) - arc tangent of y/x
	if matchFuncStartAndSuffix(expr, "atan2") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			y, ok1 := toFloat64(e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			x, ok2 := toFloat64(e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
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
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return math.Exp(f)
		}
		return nil
	}

	// log(x) - natural logarithm
	if matchFuncStartAndSuffix(expr, "log") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return math.Log(f)
		}
		return nil
	}

	// log10(x) - base-10 logarithm
	if matchFuncStartAndSuffix(expr, "log10") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return math.Log10(f)
		}
		return nil
	}

	// sqrt(x) - square root
	if matchFuncStartAndSuffix(expr, "sqrt") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return math.Sqrt(f)
		}
		return nil
	}

	// ========================================
	// Angle Conversion Functions
	// ========================================

	// degrees(radians) - convert radians to degrees
	if matchFuncStartAndSuffix(expr, "degrees") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return angle.ToDegrees(f)
		}
		return nil
	}

	// haversin(x) - half of versine = (1 - cos(x))/2
	if matchFuncStartAndSuffix(expr, "haversin") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return (1 - math.Cos(f)) / 2
		}
		return nil
	}

	// sinh(x) - hyperbolic sine (Neo4j 2025.06+)
	if matchFuncStartAndSuffix(expr, "sinh") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return math.Sinh(f)
		}
		return nil
	}

	// cosh(x) - hyperbolic cosine (Neo4j 2025.06+)
	if matchFuncStartAndSuffix(expr, "cosh") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return math.Cosh(f)
		}
		return nil
	}

	// tanh(x) - hyperbolic tangent (Neo4j 2025.06+)
	if matchFuncStartAndSuffix(expr, "tanh") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return math.Tanh(f)
		}
		return nil
	}

	// coth(x) - hyperbolic cotangent (Neo4j 2025.06+)
	if matchFuncStartAndSuffix(expr, "coth") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			base, ok1 := toFloat64(e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			exp, ok2 := toFloat64(e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
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

	// startNode(r) / endNode(r): relationshipEndpoint, as the row evaluator
	// reads them.
	for _, function := range []string{"startnode", "endnode"} {
		if matchFuncStartAndSuffix(expr, function) {
			value := e.evaluateExpressionWithContextFull(ctx, view.functionArguments().trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			if getExpressionFailure(ctx) != nil {
				return nil
			}
			result, err := e.relationshipEndpointValue(value, function == "endnode")
			if err != nil {
				recordExpressionFailure(ctx, err)
				return nil
			}
			return result
		}
	}

	// nodes(path) - return list of nodes in a path
	if matchFuncStartAndSuffix(expr, "nodes") {
		inner := view.functionArguments().trim()
		// First check if we have a full path by that variable name
		if paths != nil {
			if pathResult, ok := paths[inner.text()]; ok && pathResult != nil {
				var result []interface{}
				for _, node := range pathResult.Nodes {
					result = append(result, node)
				}
				return result
			}
		}
		// Then check allPathNodes (for variable-length patterns without explicit path variable)
		if len(allPathNodes) > 0 {
			var result []interface{}
			for _, node := range allPathNodes {
				result = append(result, node)
			}
			return result
		}
		// Fallback: return single node from node context
		if node, ok := nodes[inner.text()]; ok {
			return []interface{}{node}
		}
		// A path carried as a computed-row map (WITH p AS path).
		if innerVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength); innerVal != nil {
			if remote, ok := innerVal.(storage.RemotePathValue); ok {
				value, err := remotePathFunctionValue("nodes", remote)
				recordExpressionFailure(ctx, err)
				return value
			}
			if pathMap, ok := innerVal.(map[string]interface{}); ok {
				if nodesList, _, hasNodes, _ := pathValueParts(pathMap); hasNodes {
					return nodesList
				}
			}
		}
		return []interface{}{}
	}

	// relationships(path) - return list of relationships in a path
	if matchFuncStartAndSuffix(expr, "relationships") {
		inner := view.functionArguments().trim()
		// First check if we have a full path by that variable name
		if paths != nil {
			if pathResult, ok := paths[inner.text()]; ok && pathResult != nil {
				result := make([]interface{}, len(pathResult.Relationships))
				for index, edge := range pathResult.Relationships {
					result[index] = edge
				}
				return result
			}
		}
		// Then check allPathEdges (for variable-length patterns without explicit path variable)
		if len(allPathEdges) > 0 {
			result := make([]interface{}, len(allPathEdges))
			for index, edge := range allPathEdges {
				result[index] = edge
			}
			return result
		}
		// Fallback: return single relationship from rel context
		if rel, ok := rels[inner.text()]; ok {
			return []interface{}{rel}
		}
		// A path carried as a computed-row map (WITH p AS path).
		if innerVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength); innerVal != nil {
			if remote, ok := innerVal.(storage.RemotePathValue); ok {
				value, err := remotePathFunctionValue("relationships", remote)
				recordExpressionFailure(ctx, err)
				return value
			}
			if pathMap, ok := innerVal.(map[string]interface{}); ok {
				if _, relationshipsList, _, hasRelationships := pathValueParts(pathMap); hasRelationships {
					return relationshipsList
				}
			}
		}
		return []interface{}{}
	}

	// ========================================
	// Null Check Functions
	// ========================================

	// ========================================
	// String Functions (additional)
	// ========================================

	// ========================================
	// Aggregation Functions (in expression context)
	// ========================================

	// percentileCont(expr, percentile) - continuous percentile
	if matchFuncStartAndSuffix(expr, "percentilecont") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			// In single-row context, just return the value
			return e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		}
		return nil
	}

	// percentileDisc(expr, percentile) - discrete percentile
	if matchFuncStartAndSuffix(expr, "percentiledisc") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			// In single-row context, just return the value
			return e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		}
		return nil
	}

	// stDev(expr) - standard deviation
	if matchFuncStartAndSuffix(expr, "stdev") {
		inner := view.functionArguments().trim()
		// In single-row context, return 0
		_ = inner
		return float64(0)
	}

	// stDevP(expr) - population standard deviation
	if matchFuncStartAndSuffix(expr, "stdevp") {
		inner := view.functionArguments().trim()
		// In single-row context, return 0
		_ = inner
		return float64(0)
	}

	// ========================================
	// Reduce Function
	// ========================================

	// reduce(acc = initial, x IN list | step) and allReduce(…, predicate)
	if matchFuncStartAndSuffix(expr, "reduce") || matchFuncStartAndSuffix(expr, "allreduce") {
		var callFacts1 functionCallFacts
		if isCall := inspectFunctionCall(expr, qualifiedFunctionCall, "", &callFacts1); isCall && isReduceFormFunction(callFacts1.name) {
			function := callFacts1.name
			return e.evaluateReduceForm(ctx, function, view.functionArguments(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		}
	}

	// ========================================
	// Kalman Filter Functions
	// ========================================

	// kalman.init() or kalman.init({processNoise: 0.1, measurementNoise: 88.0})
	if matchFuncStartAndSuffix(expr, "kalman.init") {
		inner := view.functionArguments().trim()
		var configMap map[string]interface{}
		if inner.text() != "" {
			val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			if m, ok := val.(map[string]interface{}); ok {
				configMap = m
			}
		}
		return kalmanInit(configMap)
	}

	// kalman.process(measurement, stateJson) or kalman.process(measurement, stateJson, target)
	if matchFuncStartAndSuffix(expr, "kalman.process") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			measurement, _ := toFloat64(e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			stateJSON, _ := e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength).(string)
			target := 0.0
			if len(args) >= 3 {
				target, _ = toFloat64(e.evaluateExpressionWithContextFull(ctx, args[2].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			}
			return kalmanProcess(measurement, stateJSON, target)
		}
		return nil
	}

	// kalman.predict(stateJson, steps)
	if matchFuncStartAndSuffix(expr, "kalman.predict") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			stateJSON, _ := e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength).(string)
			steps, _ := toInt(e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			return kalmanPredict(stateJSON, steps)
		}
		return nil
	}

	// kalman.state(stateJson) - get current state estimate
	if matchFuncStartAndSuffix(expr, "kalman.state") {
		inner := view.functionArguments().trim()
		stateJSON, _ := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength).(string)
		return kalmanStateValue(stateJSON)
	}

	// kalman.reset(stateJson) - reset to initial values
	if matchFuncStartAndSuffix(expr, "kalman.reset") {
		inner := view.functionArguments().trim()
		stateJSON, _ := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength).(string)
		return kalmanReset(stateJSON)
	}

	// kalman.velocity.init() or kalman.velocity.init(initialPos, initialVel)
	if matchFuncStartAndSuffix(expr, "kalman.velocity.init") {
		inner := view.functionArguments().trim()
		if inner.text() == "" {
			return kalmanVelocityInit(0, 0, false)
		}
		args := inner.commaViews()
		if len(args) >= 2 {
			pos, _ := toFloat64(e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			vel, _ := toFloat64(e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			return kalmanVelocityInit(pos, vel, true)
		}
		return kalmanVelocityInit(0, 0, false)
	}

	// kalman.velocity.process(measurement, stateJson)
	if matchFuncStartAndSuffix(expr, "kalman.velocity.process") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			measurement, _ := toFloat64(e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			stateJSON, _ := e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength).(string)
			return kalmanVelocityProcess(measurement, stateJSON)
		}
		return nil
	}

	// kalman.velocity.predict(stateJson, steps)
	if matchFuncStartAndSuffix(expr, "kalman.velocity.predict") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			stateJSON, _ := e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength).(string)
			steps, _ := toInt(e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			return kalmanVelocityPredict(stateJSON, steps)
		}
		return nil
	}

	// kalman.adaptive.init() or kalman.adaptive.init({trendThreshold: 0.1, ...})
	if matchFuncStartAndSuffix(expr, "kalman.adaptive.init") {
		inner := view.functionArguments().trim()
		var configMap map[string]interface{}
		if inner.text() != "" {
			val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			if m, ok := val.(map[string]interface{}); ok {
				configMap = m
			}
		}
		return kalmanAdaptiveInit(configMap)
	}

	// kalman.adaptive.process(measurement, stateJson)
	if matchFuncStartAndSuffix(expr, "kalman.adaptive.process") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			measurement, _ := toFloat64(e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			stateJSON, _ := e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength).(string)
			return kalmanAdaptiveProcess(measurement, stateJSON)
		}
		return nil
	}

	// ========================================
	// Vector Functions
	// ========================================

	// vector.similarity.cosine and vector.similarity.euclidean are
	// registered functions (vector_similarity_functions.go).

	// ========================================
	// Point/Spatial Functions (basic support)
	// ========================================

	// point({x: val, y: val}) or point({latitude: val, longitude: val})
	if matchFuncStartAndSuffix(expr, "point") {
		inner := view.functionArguments().trim()
		var fields map[string]interface{}
		switch value := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength).(type) {
		case map[string]interface{}:
			fields = value
		case *storage.Node:
			if value != nil {
				fields = value.Properties
			}
		case *storage.Edge:
			if value != nil {
				fields = value.Properties
			}
		}
		if fields == nil {
			return nil
		}
		point, ok, err := newPointFromMap(fields)
		if err != nil {
			recordExpressionFailure(ctx, err)
			return nil
		}
		if !ok {
			return nil
		}
		return point
	}

	// distance(p1, p2) - Euclidean distance between two points
	if matchFuncStartAndSuffix(expr, "distance") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			p1 := e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			p2 := e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			if left, ok := pointValue(p1); ok {
				if right, ok := pointValue(p2); ok {
					if distance, ok := pointDistance(left, right); ok {
						return distance
					}
					return nil
				}
			}

			m1, ok1 := spatialMap(p1)
			m2, ok2 := spatialMap(p2)

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
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) < 3 {
			return false
		}
		point := e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		lowerLeft := e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		upperRight := e.evaluateExpressionWithContextFull(ctx, args[2].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if target, ok := pointValue(point); ok {
			low, lowOK := pointValue(lowerLeft)
			high, highOK := pointValue(upperRight)
			if lowOK && highOK {
				if within, ok := pointWithinBBox(target, low, high); ok {
					return within
				}
				return nil
			}
		}

		pm, ok1 := spatialMap(point)
		llm, ok2 := spatialMap(lowerLeft)
		urm, ok3 := spatialMap(upperRight)
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
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if m, ok := spatialMap(val); ok {
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
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if m, ok := spatialMap(val); ok {
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
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if m, ok := spatialMap(val); ok {
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
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if m, ok := spatialMap(val); ok {
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
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if m, ok := spatialMap(val); ok {
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
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if m, ok := spatialMap(val); ok {
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
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			p1 := e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			p2 := e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			if left, ok := pointValue(p1); ok {
				if right, ok := pointValue(p2); ok {
					if distance, ok := pointDistance(left, right); ok {
						return distance
					}
					return nil
				}
			}

			m1, ok1 := spatialMap(p1)
			m2, ok2 := spatialMap(p2)

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

	// point.withinBBox(point, lowerLeft, upperRight): whether point lies in
	// the box (pointWithinBBox). As in Neo4j, an argument that isn't a point
	// (null, a map, a string, …) or points of different systems give null.
	if matchFuncStartAndSuffix(expr, "point.withinbbox") {
		args := view.functionArguments().trim().commaViews()
		if len(args) != 3 {
			return nil
		}
		points := make([]CypherPoint, len(args))
		for index, argument := range args {
			point, ok := pointValue(e.evaluateExpressionWithContextFull(ctx, argument, nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			if !ok {
				return nil
			}
			points[index] = point
		}
		if within, ok := pointWithinBBox(points[0], points[1], points[2]); ok {
			return within
		}
		return nil
	}

	// point.withinDistance(point, center, distance) - check if point is within distance of center
	if matchFuncStartAndSuffix(expr, "point.withindistance") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) < 3 {
			return false
		}
		point := e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		center := e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		maxDist := e.evaluateExpressionWithContextFull(ctx, args[2].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)

		pm, ok1 := spatialMap(point)
		cm, ok2 := spatialMap(center)
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
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if m, ok := spatialMap(val); ok {
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
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if m, ok := spatialMap(val); ok {
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
		inner := view.functionArguments().trim()

		// Check if inner is a list literal [...]
		if isListLiteral(inner.text()) {
			// Parse and evaluate list elements manually
			listContent := inner.window(1, len(inner.text())-1)
			pointExprs := listContent.commaViews()

			pointList := make([]interface{}, 0, len(pointExprs))
			for _, pointExpr := range pointExprs {
				evalPoint := e.evaluateExpressionWithContextFull(ctx, pointExpr.trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		pointsVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if pointList, ok := cypherListValue(pointsVal); ok {
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
		inner := view.functionArguments().trim()

		// Check if inner is a list literal [...]
		if isListLiteral(inner.text()) {
			// Parse and evaluate list elements manually
			listContent := inner.window(1, len(inner.text())-1)
			pointExprs := listContent.commaViews()

			pointList := make([]interface{}, 0, len(pointExprs))
			for _, pointExpr := range pointExprs {
				evalPoint := e.evaluateExpressionWithContextFull(ctx, pointExpr.trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		pointsVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if pointList, ok := cypherListValue(pointsVal); ok {
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
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) < 2 {
			return false
		}

		pointVal := e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		polygonVal := e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)

		pm, ok1 := spatialMap(pointVal)
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
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) < 2 {
			return false
		}

		polygonVal := e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		pointVal := e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)

		polygonMap, ok1 := polygonVal.(map[string]interface{})
		pm, ok2 := spatialMap(pointVal)
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

	// all / any / none / single(variable IN list WHERE predicate)
	for _, function := range []string{"all", "any", "none", "single"} {
		if matchFuncStartAndSuffix(expr, function) {
			return e.evaluateQuantifierWithContext(ctx, function, view.functionArguments().trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		}
	}

	// ========================================
	// Additional List Functions
	// ========================================

	// filter(variable IN list WHERE predicate) and extract(variable IN list |
	// expression), kept from Cypher 3, are the list comprehensions
	// [variable IN list WHERE predicate] and [variable IN list | expression],
	// evaluated as one.
	for _, function := range []string{"filter", "extract"} {
		if matchFuncStartAndSuffix(expr, function) {
			comprehension := view.functionArguments().trim().wrap("[", "]")
			if list, ok := cypherListValue(e.evaluateExpressionWithContextFull(ctx, comprehension, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)); ok {
				return list
			}
			return []interface{}{}
		}
	}

	// [x IN list WHERE predicate | projection], each part but the list
	// optional; parseListComprehension finds the parts at the top level, so a
	// list literal holding a nested comprehension or reduce isn't one.
	if _, enclosed := stripEnclosingRowDelimiter(expr, '[', ']'); enclosed {
		innerView := view.window(1, len(expr)-1).trim()
		if parts, comprehension := parseListComprehensionSpans(innerView); comprehension {
			return e.evaluateListComprehension(ctx, parts.variable, innerView.spanWindow(parts.list), innerView.spanWindow(parts.predicate), innerView.spanWindow(parts.projection), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		}
	}

	// ========================================
	// CASE WHEN Expressions (must be before operators)
	// ========================================
	if isCaseExpression(view) {
		return e.evaluateCaseExpression(ctx, view, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	}

	// ========================================

	return e.evaluateExpressionWithContextFullOperators(ctx, view, lowerExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
}

// evaluateQuantifierWithContext evaluates the list predicate function
// (all / any / none / single) with arguments inner, "variable IN list WHERE
// predicate" (parseQuantifierArguments), folding the element results with
// quantifierFold. A null list is null (#736), and a value that isn't a list
// is a list of that one value (coerceToUnwindItems). Semantic validation rejects
// arguments of any other shape before evaluation; here they, and a list
// expression this evaluator can't evaluate, are null.
func (e *StorageExecutor) evaluateQuantifierWithContext(ctx context.Context, function string, inner braceView, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult, allPathEdges []*storage.Edge, allPathNodes []*storage.Node, pathLength int) interface{} {
	parts, parsed := parseQuantifierSpans(inner)
	if !parsed {
		return nil
	}
	varName, listExpr, predicate := parts.variable, inner.spanWindow(parts.list), inner.spanWindow(parts.predicate)
	list := e.evaluateExpressionWithContextFull(ctx, listExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	if list == nil {
		return nil
	}
	// The predicate sees the path variables too (r[i], nodes(p)[i], #882),
	// converted once for every item.
	var pathValues map[string]interface{}
	if len(paths) > 0 {
		pathValues = make(map[string]interface{}, len(paths))
		for name, path := range paths {
			if path != nil {
				pathValues[name] = e.pathContextRowValue(path)
			}
		}
	}
	// One row for every element, the variable rebound per element, with
	// the context's value bindings, parameters and statement context bound
	// once: evaluateRowExpressionWithContext binds the ones a row lacks,
	// and copied the row to do it for every element (#1214).
	values := make(map[string]interface{}, len(pathValues)+len(nodes)+len(rels)+1)
	for name, value := range pathValues {
		values[name] = value
	}
	for name, node := range nodes {
		values[name] = node
	}
	for name, relationship := range rels {
		values[name] = relationship
	}
	values[varName] = nil
	bindAbsent := func(name string, value interface{}) {
		if _, exists := values[name]; !exists {
			values[name] = value
		}
	}
	for name, value := range valueBindingsFromContext(ctx) {
		bindAbsent(name, value)
	}
	for name, value := range parameterRowValues(ctx) {
		bindAbsent(name, value)
	}
	if needsRowStatementContext(predicate.text()) {
		bindAbsent(rowStatementContextKey, ctx)
	}
	value, _, err := rowQuantifierLoop(function, coerceToUnwindItems(list), func(item interface{}) { values[varName] = item }, func() (interface{}, bool, error) {
		value, ok := e.evaluateQuantifierPredicate(ctx, predicate, values)
		return value, ok, getExpressionFailure(ctx)
	})
	if err != nil {
		recordExpressionFailure(ctx, err)
	}
	return value
}

func (e *StorageExecutor) evaluateQuantifierPredicate(ctx context.Context, predicate braceView, values map[string]interface{}) (interface{}, bool) {
	return e.evaluateRowExpressionWithContext(ctx, predicate, values)
}

// evaluateListComprehension evaluates [x IN list WHERE predicate |
// projection] as Neo4j does: a null list is null, and [x IN v] is v itself;
// otherwise a value that isn't a list is a one-item list, and an item the
// predicate doesn't hold for is left out. A relationship item's type(x) /
// id(x) projection reads a relationship map's type / _edgeId as well.
func (e *StorageExecutor) evaluateListComprehension(ctx context.Context, varName string, listExpr, condition, projection braceView, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult, allPathEdges []*storage.Edge, allPathNodes []*storage.Node, pathLength int) interface{} {
	list := e.evaluateExpressionWithContextFull(ctx, listExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	if list == nil {
		return nil
	}
	if condition.text() == "" && projection.text() == "" {
		if texts, isTexts := list.([]string); isTexts {
			items := make([]interface{}, len(texts))
			for index, item := range texts {
				items[index] = item
			}
			return items
		}
		return list
	}
	items, ok := toInterfaceSlice(list)
	if !ok {
		items = []interface{}{list}
	}
	boundNodes := make(map[string]*storage.Node, len(nodes)+1)
	for name, node := range nodes {
		boundNodes[name] = node
	}
	boundRels := make(map[string]*storage.Edge, len(rels)+1)
	for name, rel := range rels {
		boundRels[name] = rel
	}
	values := valueBindingsLayer(ctx, 1)
	itemCtx := withValueBindings(ctx, values)
	var holds func() (bool, bool, error)
	if condition.text() != "" {
		holds = func() (bool, bool, error) {
			value := e.evaluateExpressionWithContextFull(itemCtx, condition, boundNodes, boundRels, paths, allPathEdges, allPathNodes, pathLength)
			return rowConditionHolds(value, true, getExpressionFailure(itemCtx))
		}
	}
	var project func() (interface{}, bool, error)
	if projection.text() != "" {
		project = func() (interface{}, bool, error) {
			value := e.evaluateExpressionWithContextFull(itemCtx, projection, boundNodes, boundRels, paths, allPathEdges, allPathNodes, pathLength)
			return value, true, getExpressionFailure(itemCtx)
		}
	}
	result, _, err := rowComprehensionLoop(items, func(item interface{}) {
		bindEvaluationValue(varName, item, boundNodes, boundRels, values)
	}, holds, project)
	if err != nil {
		recordExpressionFailure(ctx, err)
	}
	return result
}

// evaluateReduceForm evaluates reduce and allReduce (parseReduceForm) in the
// expression evaluator: a list that isn't one is a one-item list, and an
// error is recorded on ctx, giving null.
func (e *StorageExecutor) evaluateReduceForm(ctx context.Context, function string, inner braceView, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult, allPathEdges []*storage.Edge, allPathNodes []*storage.Node, pathLength int) interface{} {
	form, ok := parseReduceForm(function, inner)
	if !ok {
		return nil
	}
	acc := e.evaluateExpressionWithContextFull(ctx, inner.spanWindow(form.initialSpan), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	list := e.evaluateExpressionWithContextFull(ctx, inner.spanWindow(form.listSpan), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	if list == nil {
		return nil
	}
	items, isList := cypherListValue(list)
	if !isList {
		items = []interface{}{list}
	}
	// The accumulator and item are bound as variables (bindEvaluationValue):
	// nodes / relationships as entities, anything else in the value scope.
	tempNodes := make(map[string]*storage.Node, len(nodes)+2)
	for k, v := range nodes {
		tempNodes[k] = v
	}
	tempRels := make(map[string]*storage.Edge, len(rels)+2)
	for k, v := range rels {
		tempRels[k] = v
	}
	values := valueBindingsLayer(ctx, 2)
	itemCtx := withValueBindings(ctx, values)
	evaluate := func(expression braceView) func(accumulator, item interface{}) (interface{}, error) {
		return func(accumulator, item interface{}) (interface{}, error) {
			bindEvaluationValue(form.accumulator, accumulator, tempNodes, tempRels, values)
			bindEvaluationValue(form.variable, item, tempNodes, tempRels, values)
			return e.evaluateExpressionWithContextFull(itemCtx, expression, tempNodes, tempRels, paths, allPathEdges, allPathNodes, pathLength), nil
		}
	}
	var appender *reduceAppender
	if plan := form.appendPlan(); plan.usable(values) {
		appender = &reduceAppender{plan: plan, term: func(index int, accumulator, item interface{}) (interface{}, error) {
			return evaluate(plan.views[index])(accumulator, item)
		}}
	}
	result, err := runReduceForm(form, acc, items, evaluate(inner.spanWindow(form.stepSpan)), evaluate(inner.spanWindow(form.predicateSpan)), appender)
	if err != nil {
		recordExpressionFailure(ctx, err)
		return nil
	}
	return result
}
