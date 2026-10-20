// Package cypher implements APOC graph algorithms for Neo4j compatibility.
package cypher

import (
	"container/heap"
	"context"
	"fmt"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"sort"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Priority Queue for Dijkstra/A*
type pathItem struct {
	nodeID storage.NodeID
	cost   float64
	path   []storage.NodeID
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

// getNodeEdges returns all edges for a node.
func (run relationshipAccessExecution) getNodeEdges(nodeID storage.NodeID) []*storage.Edge {
	e := run.executor
	var result []*storage.Edge
	outgoing, _ := e.storage.GetOutgoingEdges(nodeID)
	incoming, _ := e.storage.GetIncomingEdges(nodeID)
	result = append(result, outgoing...)
	result = append(result, incoming...)
	return result
}

func (e *StorageExecutor) getNodeEdges(nodeID storage.NodeID) []*storage.Edge {
	return e.relationshipExecutionFor(nil, "", "").getNodeEdges(nodeID)
}

// callApocAlgoDijkstra implements Dijkstra's shortest path.
func (e *StorageExecutor) callApocAlgoDijkstra(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	const procedure = "apoc.algo.dijkstra"
	startID, endID, steps, err := pathAlgorithmArguments(procedure, args)
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
	path, weight := e.dijkstra(startID, endID, steps, weightProp, defaultWeight)
	if path == nil {
		return &ExecuteResult{Columns: []string{"path", "weight"}, Rows: [][]interface{}{}}, nil
	}
	return &ExecuteResult{Columns: []string{"path", "weight"}, Rows: [][]interface{}{{path, weight}}}, nil
}

func (e *StorageExecutor) dijkstra(startID, endID storage.NodeID, steps []apocRelationshipStep, weightProp string, defaultWeight float64) ([]storage.NodeID, float64) {
	dist := make(map[storage.NodeID]float64)
	dist[startID] = 0
	pq := &priorityQueue{}
	heap.Init(pq)
	heap.Push(pq, &pathItem{nodeID: startID, cost: 0, path: []storage.NodeID{startID}})
	visited := make(map[storage.NodeID]bool)

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
			neighbor := otherEnd(edge, current.nodeID)
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
				newPath := append([]storage.NodeID{}, current.path...)
				newPath = append(newPath, neighbor)
				heap.Push(pq, &pathItem{nodeID: neighbor, cost: newDist, path: newPath})
			}
		}
	}
	return nil, 0
}

// callApocAlgoAStar implements A* pathfinding.
func (e *StorageExecutor) callApocAlgoAStar(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	const procedure = "apoc.algo.aStar"
	startID, endID, steps, err := pathAlgorithmArguments(procedure, args)
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
	path, weight := e.astar(startID, endID, steps, weightProp, latProp, lonProp)
	if path == nil {
		return &ExecuteResult{Columns: []string{"path", "weight"}, Rows: [][]interface{}{}}, nil
	}
	return &ExecuteResult{Columns: []string{"path", "weight"}, Rows: [][]interface{}{{path, weight}}}, nil
}

func (e *StorageExecutor) astar(startID, endID storage.NodeID, steps []apocRelationshipStep, weightProp, latProp, lonProp string) ([]storage.NodeID, float64) {
	endNode, err := e.getQueryNode(e.storage, endID)
	if err != nil {
		return nil, 0
	}
	endLat, endLon := 0.0, 0.0
	if lat, ok := endNode.Properties[latProp]; ok {
		if f, ok := toFloat64(lat); ok {
			endLat = f
		}
	}
	if lon, ok := endNode.Properties[lonProp]; ok {
		if f, ok := toFloat64(lon); ok {
			endLon = f
		}
	}
	heuristic := func(nodeID storage.NodeID) float64 {
		node, err := e.getQueryNode(e.storage, nodeID)
		if err != nil {
			return 0
		}
		lat, lon := 0.0, 0.0
		if l, ok := node.Properties[latProp]; ok {
			if f, ok := toFloat64(l); ok {
				lat = f
			}
		}
		if l, ok := node.Properties[lonProp]; ok {
			if f, ok := toFloat64(l); ok {
				lon = f
			}
		}
		return math.Sqrt((lat-endLat)*(lat-endLat) + (lon-endLon)*(lon-endLon))
	}
	gScore := make(map[storage.NodeID]float64)
	gScore[startID] = 0
	pq := &priorityQueue{}
	heap.Init(pq)
	heap.Push(pq, &pathItem{nodeID: startID, cost: heuristic(startID), path: []storage.NodeID{startID}})
	visited := make(map[storage.NodeID]bool)

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
			neighbor := otherEnd(edge, current.nodeID)
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
				newPath := append([]storage.NodeID{}, current.path...)
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
	const procedure = "apoc.algo.allSimplePaths"
	startID, endID, steps, err := pathAlgorithmArguments(procedure, args)
	if err != nil {
		return nil, err
	}
	maxNodes, err := requiredProcedureInteger(procedure, args, 3, "maxNodes")
	if err != nil {
		return nil, err
	}
	var paths [][]storage.NodeID
	if maxNodes > 0 {
		paths = e.findAllSimplePaths(startID, endID, steps, int(maxNodes))
	}
	rows := make([][]interface{}, len(paths))
	for i, path := range paths {
		rows[i] = []interface{}{path}
	}
	return &ExecuteResult{Columns: []string{"path"}, Rows: rows}, nil
}

func (e *StorageExecutor) findAllSimplePaths(startID, endID storage.NodeID, steps []apocRelationshipStep, maxDepth int) [][]storage.NodeID {
	var result [][]storage.NodeID
	visited := make(map[storage.NodeID]bool)
	var dfs func(current storage.NodeID, path []storage.NodeID, depth int)
	dfs = func(current storage.NodeID, path []storage.NodeID, depth int) {
		if depth > maxDepth {
			return
		}
		if current == endID {
			pathCopy := make([]storage.NodeID, len(path))
			copy(pathCopy, path)
			result = append(result, pathCopy)
			return
		}
		visited[current] = true
		defer func() { visited[current] = false }()
		for _, edge := range e.apocSteps(current, steps) {
			neighbor := otherEnd(edge, current)
			if visited[neighbor] {
				continue
			}
			dfs(neighbor, append(path, neighbor), depth+1)
		}
	}
	dfs(startID, []storage.NodeID{startID}, 0)
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
	rows := algorithmRows(nodes, scores)
	return &ExecuteResult{Columns: []string{"node", "score"}, Rows: rows}, nil
}

// computePageRank ranks the given nodes over the relationships between
// them: a relationship to a node outside the set carries no score, and no
// outside node is ranked (#1164).
func (run relationshipAccessExecution) computePageRank(nodes []*storage.Node, damping float64, iterations int) map[storage.NodeID]float64 {
	e := run.executor
	if len(nodes) == 0 {
		return map[storage.NodeID]float64{}
	}
	n := float64(len(nodes))
	scores := make(map[storage.NodeID]float64, len(nodes))
	newScores := make(map[storage.NodeID]float64, len(nodes))
	for _, node := range nodes {
		scores[node.ID] = 1.0 / n
	}
	targets := make(map[storage.NodeID][]storage.NodeID, len(nodes))
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

func (e *StorageExecutor) computePageRank(nodes []*storage.Node, damping float64, iterations int) map[storage.NodeID]float64 {
	return e.relationshipExecutionFor(nil, "", "").computePageRank(nodes, damping, iterations)
}

// callApocAlgoBetweenness computes betweenness centrality.
func (e *StorageExecutor) callApocAlgoBetweenness(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	graph, nodes, _, err := e.readAlgorithmGraph("apoc.algo.betweenness", args)
	if err != nil {
		return nil, err
	}
	scores := graph.computeBetweenness(nodes)
	rows := algorithmRows(nodes, scores)
	return &ExecuteResult{Columns: []string{"node", "score"}, Rows: rows}, nil
}

func (run relationshipAccessExecution) computeBetweenness(nodes []*storage.Node) map[storage.NodeID]float64 {
	e := run.executor
	scores := make(map[storage.NodeID]float64)
	for _, node := range nodes {
		scores[node.ID] = 0
	}
	for _, s := range nodes {
		stack := []storage.NodeID{}
		pred := make(map[storage.NodeID][]storage.NodeID)
		sigma := make(map[storage.NodeID]float64)
		dist := make(map[storage.NodeID]int)
		for _, node := range nodes {
			pred[node.ID] = []storage.NodeID{}
			sigma[node.ID] = 0
			dist[node.ID] = -1
		}
		sigma[s.ID] = 1
		dist[s.ID] = 0
		queue := []storage.NodeID{s.ID}
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
		delta := make(map[storage.NodeID]float64)
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

func (e *StorageExecutor) computeBetweenness(nodes []*storage.Node) map[storage.NodeID]float64 {
	return e.relationshipExecutionFor(nil, "", "").computeBetweenness(nodes)
}

// callApocAlgoCloseness computes closeness centrality.
func (e *StorageExecutor) callApocAlgoCloseness(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	graph, nodes, _, err := e.readAlgorithmGraph("apoc.algo.closeness", args)
	if err != nil {
		return nil, err
	}
	scores := graph.computeCloseness(nodes)
	rows := algorithmRows(nodes, scores)
	return &ExecuteResult{Columns: []string{"node", "score"}, Rows: rows}, nil
}

func (e *StorageExecutor) computeCloseness(nodes []*storage.Node) map[storage.NodeID]float64 {
	scores := make(map[storage.NodeID]float64)
	n := len(nodes)
	for _, source := range nodes {
		dist := make(map[storage.NodeID]int)
		for _, node := range nodes {
			dist[node.ID] = -1
		}
		dist[source.ID] = 0
		queue := []storage.NodeID{source.ID}
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
	startID, relType, maxHops, err := parseNeighborArguments(args)
	if err != nil {
		return nil, err
	}
	neighbors, err := e.getNeighborsByhop(ctx, startID, relType, maxHops)
	if err != nil {
		return nil, err
	}
	rows := make([][]interface{}, 0)
	for _, group := range neighbors {
		for _, nodeID := range group {
			if nodeID == startID {
				continue
			}
			node, err := e.getQueryNode(e.getStorage(ctx), nodeID)
			if err != nil {
				return nil, err
			}
			rows = append(rows, []interface{}{node})
		}
	}
	return &ExecuteResult{Columns: []string{"node"}, Rows: rows}, nil
}

func (run relationshipAccessExecution) getNeighborsByhop(ctx context.Context, startID storage.NodeID, relType string, maxHops int) ([][]storage.NodeID, error) {
	e := run.executor
	if relType == "" || maxHops < 1 {
		return nil, nil
	}
	filters := parseApocRelationshipFilter(relType)
	store := e.getStorage(ctx)
	visited := make(map[storage.NodeID]bool)
	visited[startID] = true
	current := []storage.NodeID{startID}
	var result [][]storage.NodeID
	for hop := 0; hop < maxHops && len(current) > 0; hop++ {
		var next []storage.NodeID
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
			edges := make([]*storage.Edge, 0, len(outgoing)+len(incoming))
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

func (e *StorageExecutor) getNeighborsByhop(ctx context.Context, startID storage.NodeID, relType string, maxHops int) ([][]storage.NodeID, error) {
	return e.relationshipExecutionFor(nil, "", "").getNeighborsByhop(ctx, startID, relType, maxHops)
}

// callApocNeighborsByhop gets neighbors grouped by hop.
func (e *StorageExecutor) callApocNeighborsByhop(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	startID, relType, maxHops, err := parseNeighborArguments(args)
	if err != nil {
		return nil, err
	}
	neighborsByHop, err := e.getNeighborsByhop(ctx, startID, relType, maxHops)
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
		var nodeIDs []storage.NodeID
		if depth < len(neighborsByHop) {
			nodeIDs = neighborsByHop[depth]
		}
		nodesList := make([]interface{}, 0, len(nodeIDs))
		for _, nodeID := range nodeIDs {
			node, err := e.getQueryNode(e.getStorage(ctx), nodeID)
			if err != nil {
				return nil, err
			}
			nodesList = append(nodesList, node)
		}
		rows = append(rows, []interface{}{nodesList})
	}
	return &ExecuteResult{Columns: []string{"nodes"}, Rows: rows}, nil
}

func parseNeighborArguments(args []interface{}) (storage.NodeID, string, int, error) {
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
	return start.ID, filter, maxHops, nil
}

// pathAlgorithmArguments reads the arguments every path algorithm starts
// with: (startNode, endNode[, relTypesAndDirections]). A node may also be
// given by its id, NornicDB's kept form. relTypesAndDirections is APOC's
// "TYPE>|<TYPE|TYPE" (#1185), read as apoc.path's relationshipFilter: a type
// without a direction, and an empty filter, follow both directions.
func pathAlgorithmArguments(procedure string, args []interface{}) (start, end storage.NodeID, steps []apocRelationshipStep, err error) {
	if start, err = requiredProcedureNodeID(procedure, args, 0, "startNode"); err != nil {
		return "", "", nil, err
	}
	if end, err = requiredProcedureNodeID(procedure, args, 1, "endNode"); err != nil {
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
func algorithmRows[T any](nodes []*storage.Node, values map[storage.NodeID]T) [][]interface{} {
	rows := make([][]interface{}, 0, len(values))
	seen := make(map[storage.NodeID]struct{}, len(values))
	for _, node := range nodes {
		value, ok := values[node.ID]
		if _, listed := seen[node.ID]; !ok || listed {
			continue
		}
		seen[node.ID] = struct{}{}
		rows = append(rows, []interface{}{node, value})
	}
	return rows
}

// renumberInNodeOrder numbers communities 0, 1, … by their first node in the
// given nodes' order, so community IDs don't depend on map order (#1183).
func renumberInNodeOrder(nodes []*storage.Node, communities map[storage.NodeID]int) map[storage.NodeID]int {
	numbers := make(map[int]int)
	result := make(map[storage.NodeID]int, len(communities))
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
			return nodesInIDOrder(e.storage.GetAllNodes()), nil
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
		return nodesInIDOrder(e.storage.GetAllNodes()), nil
	case string:
		if label == "" {
			return nodesInIDOrder(e.storage.GetAllNodes()), nil
		}
		nodes, _ := e.storage.GetNodesByLabel(label)
		return nodesInIDOrder(nodes), nil
	default:
		return nil, typeError()
	}
}

// nodesInIDOrder sorts the nodes an algorithm reads from storage by ID, so
// the same graph gives the same order on every engine (#1183); a node list
// the caller passes keeps its order. It sorts a copy: storage may share the
// slice it returns.
func nodesInIDOrder(nodes []*storage.Node) []*storage.Node {
	sorted := append([]*storage.Node(nil), nodes...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	return sorted
}
