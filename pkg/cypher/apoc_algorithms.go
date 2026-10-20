// Package cypher implements APOC graph algorithms for Neo4j compatibility.
package cypher

import (
	"container/heap"
	"context"
	"fmt"
	math "github.com/orneryd/nornicdb/pkg/math/libm"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Priority Queue for Dijkstra/A*
type pathItem struct {
	nodeID graphEntityKey
	cost   float64
	path   []graphEntityKey
	index  int
}

type priorityQueue []*pathItem

func (pq priorityQueue) Len() int           { return len(pq) }
func (pq priorityQueue) Less(i, j int) bool { return pq[i].cost < pq[j].cost }
func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}
func (pq *priorityQueue) Push(x interface{}) {
	item := x.(*pathItem)
	item.index = len(*pq)
	*pq = append(*pq, item)
}
func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// callApocAlgoDijkstra implements Dijkstra's shortest path.
func (e *StorageExecutor) callApocAlgoDijkstra(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	graph := e.algorithmGraph(ctx)
	const procedure = "apoc.algo.dijkstra"
	startID, endID, steps, err := graph.pathAlgorithmArguments(procedure, args)
	if err != nil {
		return nil, err
	}
	weightProp, err := optionalProcedureString(procedure, args, 3, "weightPropertyName", "")
	if err != nil {
		return nil, err
	}
	// Keep the legacy omitted fallback; an explicitly supplied fifth argument
	// selects the cost of relationships without the requested weight property.
	defaultWeight := 1.0
	if value := procedureArgument(args, 4); value != nil {
		var numeric bool
		defaultWeight, numeric = strictNumericValue(value)
		if !numeric {
			return nil, procedureArgumentTypeError(procedure, "defaultWeight", "FLOAT", value)
		}
	}
	path, weight := graph.dijkstra(startID, endID, steps, weightProp, defaultWeight)
	if path == nil {
		return &ExecuteResult{Columns: []string{"path", "weight"}, Rows: [][]interface{}{}}, nil
	}
	return &ExecuteResult{Columns: []string{"path", "weight"}, Rows: [][]interface{}{{graph.storage.outputPath(path), weight}}}, nil
}

func (e *algorithmGraphExecutor) dijkstra(startID, endID graphEntityKey, steps []apocRelationshipStep, weightProp string, defaultWeight float64) ([]graphEntityKey, float64) {
	if startID == "" || endID == "" {
		return nil, 0
	}
	dist := make(map[graphEntityKey]float64)
	dist[startID] = 0
	pq := &priorityQueue{}
	heap.Init(pq)
	heap.Push(pq, &pathItem{nodeID: startID, cost: 0, path: []graphEntityKey{startID}})
	visited := make(map[graphEntityKey]bool)

	for pq.Len() > 0 {
		current := heap.Pop(pq).(*pathItem)
		if visited[current.nodeID] {
			continue
		}
		visited[current.nodeID] = true
		if current.nodeID == endID {
			return current.path, current.cost
		}
		for _, edge := range e.apocSteps(current.nodeID, steps) {
			neighbor := algorithmOtherEnd(edge, current.nodeID)
			if visited[neighbor] {
				continue
			}
			weight := defaultWeight
			if weightProp != "" {
				if w, ok := edge.Properties[weightProp]; ok {
					if wf, ok := toFloat64(w); ok {
						weight = wf
					}
				}
			}
			newDist := dist[current.nodeID] + weight
			if oldDist, exists := dist[neighbor]; !exists || newDist < oldDist {
				dist[neighbor] = newDist
				newPath := append([]graphEntityKey{}, current.path...)
				newPath = append(newPath, neighbor)
				heap.Push(pq, &pathItem{nodeID: neighbor, cost: newDist, path: newPath})
			}
		}
	}
	return nil, 0
}

// callApocAlgoAStar implements A* pathfinding.
func (e *StorageExecutor) callApocAlgoAStar(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	graph := e.algorithmGraph(ctx)
	const procedure = "apoc.algo.aStar"
	startID, endID, steps, err := graph.pathAlgorithmArguments(procedure, args)
	if err != nil {
		return nil, err
	}
	weightProp, err := optionalProcedureString(procedure, args, 3, "weightPropertyName", "")
	if err != nil {
		return nil, err
	}
	latProp, err := optionalProcedureString(procedure, args, 4, "latPropertyName", "lat")
	if err != nil {
		return nil, err
	}
	lonProp, err := optionalProcedureString(procedure, args, 5, "lonPropertyName", "lon")
	if err != nil {
		return nil, err
	}
	path, weight := graph.astar(startID, endID, steps, weightProp, latProp, lonProp)
	if path == nil {
		return &ExecuteResult{Columns: []string{"path", "weight"}, Rows: [][]interface{}{}}, nil
	}
	return &ExecuteResult{Columns: []string{"path", "weight"}, Rows: [][]interface{}{{graph.storage.outputPath(path), weight}}}, nil
}

func (e *algorithmGraphExecutor) astar(startID, endID graphEntityKey, steps []apocRelationshipStep, weightProp, latProp, lonProp string) ([]graphEntityKey, float64) {
	endNode, err := e.storage.node(endID)
	if err != nil {
		return nil, 0
	}
	endLat, endLon := 0.0, 0.0
	if lat, ok := endNode.value.Properties[latProp]; ok {
		if f, ok := toFloat64(lat); ok {
			endLat = f
		}
	}
	if lon, ok := endNode.value.Properties[lonProp]; ok {
		if f, ok := toFloat64(lon); ok {
			endLon = f
		}
	}
	heuristic := func(nodeID graphEntityKey) float64 {
		node, err := e.storage.node(nodeID)
		if err != nil {
			return 0
		}
		lat, lon := 0.0, 0.0
		if l, ok := node.value.Properties[latProp]; ok {
			if f, ok := toFloat64(l); ok {
				lat = f
			}
		}
		if l, ok := node.value.Properties[lonProp]; ok {
			if f, ok := toFloat64(l); ok {
				lon = f
			}
		}
		return math.Sqrt((lat-endLat)*(lat-endLat) + (lon-endLon)*(lon-endLon))
	}
	gScore := make(map[graphEntityKey]float64)
	gScore[startID] = 0
	pq := &priorityQueue{}
	heap.Init(pq)
	heap.Push(pq, &pathItem{nodeID: startID, cost: heuristic(startID), path: []graphEntityKey{startID}})
	visited := make(map[graphEntityKey]bool)

	for pq.Len() > 0 {
		current := heap.Pop(pq).(*pathItem)
		if current.nodeID == endID {
			return current.path, gScore[endID]
		}
		if visited[current.nodeID] {
			continue
		}
		visited[current.nodeID] = true
		for _, edge := range e.apocSteps(current.nodeID, steps) {
			neighbor := algorithmOtherEnd(edge, current.nodeID)
			if visited[neighbor] {
				continue
			}
			weight := 1.0
			if weightProp != "" {
				if w, ok := edge.Properties[weightProp]; ok {
					if wf, ok := toFloat64(w); ok {
						weight = wf
					}
				}
			}
			tentativeG := gScore[current.nodeID] + weight
			if oldG, exists := gScore[neighbor]; !exists || tentativeG < oldG {
				gScore[neighbor] = tentativeG
				newPath := append([]graphEntityKey{}, current.path...)
				newPath = append(newPath, neighbor)
				heap.Push(pq, &pathItem{nodeID: neighbor, cost: tentativeG + heuristic(neighbor), path: newPath})
			}
		}
	}
	return nil, 0
}

// callApocAlgoAllSimplePaths finds all simple paths of at most maxNodes
// relationships, as in APOC (#1185): below 1, none.
func (e *StorageExecutor) callApocAlgoAllSimplePaths(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	graph := e.algorithmGraph(ctx)
	const procedure = "apoc.algo.allSimplePaths"
	startID, endID, steps, err := graph.pathAlgorithmArguments(procedure, args)
	if err != nil {
		return nil, err
	}
	maxNodes, err := requiredProcedureInteger(procedure, args, 3, "maxNodes")
	if err != nil {
		return nil, err
	}
	var paths [][]graphEntityKey
	if maxNodes > 0 {
		paths = graph.findAllSimplePaths(startID, endID, steps, int(maxNodes))
	}
	rows := make([][]interface{}, len(paths))
	for i, path := range paths {
		rows[i] = []interface{}{graph.storage.outputPath(path)}
	}
	return &ExecuteResult{Columns: []string{"path"}, Rows: rows}, nil
}

func (e *algorithmGraphExecutor) findAllSimplePaths(startID, endID graphEntityKey, steps []apocRelationshipStep, maxDepth int) [][]graphEntityKey {
	if startID == "" || endID == "" {
		return nil
	}
	var result [][]graphEntityKey
	visited := make(map[graphEntityKey]bool)
	var dfs func(current graphEntityKey, path []graphEntityKey, depth int)
	dfs = func(current graphEntityKey, path []graphEntityKey, depth int) {
		if depth > maxDepth {
			return
		}
		if current == endID {
			pathCopy := make([]graphEntityKey, len(path))
			copy(pathCopy, path)
			result = append(result, pathCopy)
			return
		}
		visited[current] = true
		defer func() { visited[current] = false }()
		for _, edge := range e.apocSteps(current, steps) {
			neighbor := algorithmOtherEnd(edge, current)
			if visited[neighbor] {
				continue
			}
			dfs(neighbor, append(path, neighbor), depth+1)
		}
	}
	dfs(startID, []graphEntityKey{startID}, 0)
	return result
}

// callApocAlgoPageRank computes PageRank.
func (e *StorageExecutor) callApocAlgoPageRank(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	const procedure = "apoc.algo.pageRank"
	graph, nodes, config, err := e.readAlgorithmGraph(procedure, args)
	if err != nil {
		return nil, err
	}
	// The config map: iterations (default 20) and dampingFactor (0.85).
	iterations, damping := 20, 0.85
	if n, ok := fastRPConfigInt(config["iterations"]); ok && n > 0 {
		iterations = n
	}
	if d, ok := toFloat64(config["dampingFactor"]); ok && d > 0 && d < 1 {
		damping = d
	}
	scores := graph.computePageRank(nodes, damping, iterations)
	rows := graphAlgorithmRows(nodes, scores)
	return &ExecuteResult{Columns: []string{"node", "score"}, Rows: rows}, nil
}

// computePageRank ranks the given nodes over the relationships between
// them: a relationship to a node outside the set carries no score, and no
// outside node is ranked (#1164).
func (e *algorithmGraphExecutor) computePageRank(nodes []algorithmGraphNode, damping float64, iterations int) map[graphEntityKey]float64 {
	if len(nodes) == 0 {
		return map[graphEntityKey]float64{}
	}
	n := float64(len(nodes))
	scores := make(map[graphEntityKey]float64, len(nodes))
	newScores := make(map[graphEntityKey]float64, len(nodes))
	for _, node := range nodes {
		scores[node.ID] = 1.0 / n
	}
	targets := make(map[graphEntityKey][]graphEntityKey, len(nodes))
	for _, node := range nodes {
		edges, _ := e.storage.GetOutgoingEdges(node.ID)
		for _, edge := range edges {
			if _, inSet := scores[edge.EndNode]; inSet {
				targets[node.ID] = append(targets[node.ID], edge.EndNode)
			}
		}
	}
	for i := 0; i < iterations; i++ {
		for _, node := range nodes {
			newScores[node.ID] = (1.0 - damping) / n
		}
		for _, node := range nodes {
			if len(targets[node.ID]) == 0 {
				share := damping * scores[node.ID] / n
				for _, other := range nodes {
					newScores[other.ID] += share
				}
				continue
			}
			share := damping * scores[node.ID] / float64(len(targets[node.ID]))
			for _, target := range targets[node.ID] {
				newScores[target] += share
			}
		}
		scores, newScores = newScores, scores
	}
	return scores
}

// callApocAlgoBetweenness computes betweenness centrality.
func (e *StorageExecutor) callApocAlgoBetweenness(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	graph, nodes, _, err := e.readAlgorithmGraph("apoc.algo.betweenness", args)
	if err != nil {
		return nil, err
	}
	scores := graph.computeBetweenness(nodes)
	rows := graphAlgorithmRows(nodes, scores)
	return &ExecuteResult{Columns: []string{"node", "score"}, Rows: rows}, nil
}

func (e *algorithmGraphExecutor) computeBetweenness(nodes []algorithmGraphNode) map[graphEntityKey]float64 {
	scores := make(map[graphEntityKey]float64)
	for _, node := range nodes {
		scores[node.ID] = 0
	}
	for _, s := range nodes {
		stack := []graphEntityKey{}
		pred := make(map[graphEntityKey][]graphEntityKey)
		sigma := make(map[graphEntityKey]float64)
		dist := make(map[graphEntityKey]int)
		for _, node := range nodes {
			pred[node.ID] = []graphEntityKey{}
			sigma[node.ID] = 0
			dist[node.ID] = -1
		}
		sigma[s.ID] = 1
		dist[s.ID] = 0
		queue := []graphEntityKey{s.ID}
		for len(queue) > 0 {
			v := queue[0]
			queue = queue[1:]
			stack = append(stack, v)
			edges, _ := e.storage.GetOutgoingEdges(v)
			for _, edge := range edges {
				w := edge.EndNode
				if dist[w] < 0 {
					queue = append(queue, w)
					dist[w] = dist[v] + 1
				}
				if dist[w] == dist[v]+1 {
					sigma[w] += sigma[v]
					pred[w] = append(pred[w], v)
				}
			}
		}
		delta := make(map[graphEntityKey]float64)
		for _, node := range nodes {
			delta[node.ID] = 0
		}
		for len(stack) > 0 {
			w := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			for _, v := range pred[w] {
				delta[v] += (sigma[v] / sigma[w]) * (1 + delta[w])
			}
			if w != s.ID {
				scores[w] += delta[w]
			}
		}
	}
	n := float64(len(nodes))
	if n > 2 {
		norm := 2.0 / ((n - 1) * (n - 2))
		for id := range scores {
			scores[id] *= norm
		}
	}
	return scores
}

// callApocAlgoCloseness computes closeness centrality.
func (e *StorageExecutor) callApocAlgoCloseness(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	graph, nodes, _, err := e.readAlgorithmGraph("apoc.algo.closeness", args)
	if err != nil {
		return nil, err
	}
	scores := graph.computeCloseness(nodes)
	rows := graphAlgorithmRows(nodes, scores)
	return &ExecuteResult{Columns: []string{"node", "score"}, Rows: rows}, nil
}

func (e *algorithmGraphExecutor) computeCloseness(nodes []algorithmGraphNode) map[graphEntityKey]float64 {
	scores := make(map[graphEntityKey]float64)
	n := len(nodes)
	for _, source := range nodes {
		dist := make(map[graphEntityKey]int)
		for _, node := range nodes {
			dist[node.ID] = -1
		}
		dist[source.ID] = 0
		queue := []graphEntityKey{source.ID}
		totalDist := 0
		reachable := 0
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			edges := e.getNodeEdges(current)
			for _, edge := range edges {
				neighbor := edge.EndNode
				if edge.StartNode != current {
					neighbor = edge.StartNode
				}
				if dist[neighbor] < 0 {
					dist[neighbor] = dist[current] + 1
					totalDist += dist[neighbor]
					reachable++
					queue = append(queue, neighbor)
				}
			}
		}
		if reachable > 0 && totalDist > 0 {
			scores[source.ID] = float64(reachable*reachable) / (float64(totalDist) * float64(n-1))
		} else {
			scores[source.ID] = 0
		}
	}
	return scores
}

// callApocNeighborsTohop gets neighbors up to N hops.
func (e *StorageExecutor) callApocNeighborsTohop(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	graph := e.algorithmGraph(ctx)
	startID, relType, maxHops, err := graph.parseNeighborArguments(args)
	if err != nil {
		return nil, err
	}
	neighbors, err := graph.getNeighborsByhop(ctx, startID, relType, maxHops)
	if err != nil {
		return nil, err
	}
	rows := make([][]interface{}, 0)
	for _, group := range neighbors {
		for _, nodeID := range group {
			if nodeID == startID {
				continue
			}
			node, err := graph.storage.node(nodeID)
			if err != nil {
				return nil, err
			}
			rows = append(rows, []interface{}{algorithmNodeValue(node)})
		}
	}
	return &ExecuteResult{Columns: []string{"node"}, Rows: rows}, nil
}

func (e *algorithmGraphExecutor) getNeighborsByhop(ctx context.Context, startID graphEntityKey, relType string, maxHops int) ([][]graphEntityKey, error) {
	if relType == "" || maxHops < 1 {
		return nil, nil
	}
	filters := parseApocRelationshipFilter(relType)
	store := e.storage
	visited := make(map[graphEntityKey]bool)
	visited[startID] = true
	current := []graphEntityKey{startID}
	var result [][]graphEntityKey
	for hop := 0; hop < maxHops && len(current) > 0; hop++ {
		var next []graphEntityKey
		includeStart := false
		for _, nodeID := range current {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			outgoing, err := store.GetOutgoingEdges(nodeID)
			if err != nil {
				return nil, err
			}
			incoming, err := store.GetIncomingEdges(nodeID)
			if err != nil {
				return nil, err
			}
			edges := make([]algorithmGraphEdge, 0, len(outgoing)+len(incoming))
			edges = append(edges, outgoing...)
			edges = append(edges, incoming...)
			for _, edge := range edges {
				allowed := false
				for _, filter := range filters {
					if filter.follows(edge, nodeID) {
						allowed = true
						break
					}
				}
				if !allowed {
					continue
				}
				neighbor := edge.EndNode
				if edge.StartNode != nodeID {
					neighbor = edge.StartNode
				}
				if neighbor == startID && hop == 0 {
					includeStart = true
				}
				if !visited[neighbor] {
					visited[neighbor] = true
					next = append(next, neighbor)
				}
			}
		}
		group := next
		if includeStart {
			group = append(group, startID)
		}
		result = append(result, group)
		current = next
	}
	return result, nil
}

// callApocNeighborsByhop gets neighbors grouped by hop.
func (e *StorageExecutor) callApocNeighborsByhop(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	graph := e.algorithmGraph(ctx)
	startID, relType, maxHops, err := graph.parseNeighborArguments(args)
	if err != nil {
		return nil, err
	}
	neighborsByHop, err := graph.getNeighborsByhop(ctx, startID, relType, maxHops)
	if err != nil {
		return nil, err
	}
	rows := make([][]interface{}, 0)
	if relType == "" {
		return &ExecuteResult{Columns: []string{"nodes"}, Rows: rows}, nil
	}
	for depth := 0; depth < maxHops; depth++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var nodeIDs []graphEntityKey
		if depth < len(neighborsByHop) {
			nodeIDs = neighborsByHop[depth]
		}
		nodesList := make([]interface{}, 0, len(nodeIDs))
		for _, nodeID := range nodeIDs {
			node, err := graph.storage.node(nodeID)
			if err != nil {
				return nil, err
			}
			nodesList = append(nodesList, algorithmNodeValue(node))
		}
		rows = append(rows, []interface{}{nodesList})
	}
	return &ExecuteResult{Columns: []string{"nodes"}, Rows: rows}, nil
}

func (e *algorithmGraphExecutor) parseNeighborArguments(args []interface{}) (graphEntityKey, string, int, error) {
	if len(args) < 1 || len(args) > 3 {
		return "", "", 0, fmt.Errorf("neighbor procedures require between one and three arguments")
	}
	start, ok := args[0].(*storage.Node)
	if !ok || start == nil {
		return "", "", 0, fmt.Errorf("neighbor procedures require a node")
	}
	filter := ""
	if len(args) > 1 && args[1] != nil {
		filter, ok = args[1].(string)
		if !ok {
			return "", "", 0, fmt.Errorf("neighbor procedures require a relationship filter string")
		}
	}
	maxHops := 1
	if len(args) > 2 {
		if !isIntegerProcedureValue(args[2]) {
			return "", "", 0, fmt.Errorf("neighbor procedures require an integer distance")
		}
		maxHops = int(toInt64(args[2]))
	}
	return e.storage.nodeKey(start), filter, maxHops, nil
}

// pathAlgorithmArguments reads the arguments every path algorithm starts
// with: (startNode, endNode[, relTypesAndDirections]). A node may also be
// given by its id, NornicDB's kept form. relTypesAndDirections is APOC's
// "TYPE>|<TYPE|TYPE" (#1185), read as apoc.path's relationshipFilter: a type
// without a direction, and an empty filter, follow both directions.
func (e *algorithmGraphExecutor) pathAlgorithmArguments(procedure string, args []interface{}) (start, end graphEntityKey, steps []apocRelationshipStep, err error) {
	if start, err = e.requiredNodeKey(procedure, args, 0, "startNode"); err != nil {
		return "", "", nil, err
	}
	if end, err = e.requiredNodeKey(procedure, args, 1, "endNode"); err != nil {
		return "", "", nil, err
	}
	filter, err := optionalProcedureString(procedure, args, 2, "relTypesAndDirections", "")
	if err != nil {
		return "", "", nil, err
	}
	return start, end, parseApocRelationshipFilter(filter), nil
}

// algorithmRows are a whole-graph algorithm's rows, {node, value}, one per
// given node that has a value, in the given nodes' order (a node listed twice
// once), so the same call on the same graph gives the same rows (#1183).
func algorithmRows[T any](nodes []algorithmGraphNode, values map[graphEntityKey]T) [][]interface{} {
	rows := make([][]interface{}, 0, len(values))
	seen := make(map[graphEntityKey]struct{}, len(values))
	for _, node := range nodes {
		value, ok := values[node.ID]
		if _, listed := seen[node.ID]; !ok || listed {
			continue
		}
		seen[node.ID] = struct{}{}
		rows = append(rows, []interface{}{algorithmNodeValue(node), value})
	}
	return rows
}

// renumberInNodeOrder numbers communities 0, 1, … by their first node in the
// given nodes' order, so community IDs don't depend on map order (#1183).
func renumberInNodeOrder(nodes []algorithmGraphNode, communities map[graphEntityKey]int) map[graphEntityKey]int {
	numbers := make(map[int]int)
	result := make(map[graphEntityKey]int, len(communities))
	for _, node := range nodes {
		community, ok := communities[node.ID]
		if !ok {
			continue
		}
		number, numbered := numbers[community]
		if !numbered {
			number = len(numbers)
			numbers[community] = number
		}
		result[node.ID] = number
	}
	return result
}

// algorithmNodes reads the nodes a whole-graph algorithm runs over from its
// first argument: a LIST<NODE> (APOC's form: MATCH (n:L) WITH collect(n) AS
// nodes CALL apoc.algo.pageRank(nodes)), a label (a STRING, or a list of
// one), or none, null, an empty string or an empty list for every node.
func (e *StorageExecutor) algorithmNodes(procedure string, args []interface{}) ([]*storage.Node, error) {
	value := procedureArgument(args, 0)
	typeError := func() error {
		return procedureArgumentTypeError(procedure, "nodes", "LIST<NODE> or STRING", procedureArgument(args, 0))
	}
	if items, isList := cypherListValue(value); isList {
		if len(items) == 0 {
			return e.storage.GetAllNodes(), nil
		}
		if _, isNode := items[0].(*storage.Node); isNode {
			nodes := make([]*storage.Node, 0, len(items))
			for _, item := range items {
				node, isNode := item.(*storage.Node)
				if !isNode || node == nil {
					return nil, typeError()
				}
				nodes = append(nodes, node)
			}
			return nodes, nil
		}
		if len(items) != 1 {
			return nil, typeError()
		}
		value = items[0]
	}
	switch label := value.(type) {
	case nil:
		return e.storage.GetAllNodes(), nil
	case string:
		if label == "" {
			return e.storage.GetAllNodes(), nil
		}
		nodes, _ := e.storage.GetNodesByLabel(label)
		return nodes, nil
	default:
		return nil, typeError()
	}
}
