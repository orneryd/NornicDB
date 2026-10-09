// Package cypher implements APOC graph algorithms for Neo4j compatibility.
package cypher

import (
	"container/heap"
	"context"
	"fmt"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"strings"

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
func (e *StorageExecutor) getNodeEdges(nodeID storage.NodeID) []*storage.Edge {
	var result []*storage.Edge
	outgoing, _ := e.storage.GetOutgoingEdges(nodeID)
	incoming, _ := e.storage.GetIncomingEdges(nodeID)
	result = append(result, outgoing...)
	result = append(result, incoming...)
	return result
}

// callApocAlgoDijkstra implements Dijkstra's shortest path.
func (e *StorageExecutor) callApocAlgoDijkstra(ctx context.Context, cypher string) (*ExecuteResult, error) {
	startID, endID, relType, weightProp, err := e.parsePathAlgoParams(cypher, "APOC.ALGO.DIJKSTRA")
	if err != nil {
		return nil, err
	}
	path, weight := e.dijkstra(startID, endID, relType, weightProp)
	if path == nil {
		return &ExecuteResult{Columns: []string{"path", "weight"}, Rows: [][]interface{}{}}, nil
	}
	return &ExecuteResult{Columns: []string{"path", "weight"}, Rows: [][]interface{}{{path, weight}}}, nil
}

func (e *StorageExecutor) dijkstra(startID, endID storage.NodeID, relType, weightProp string) ([]storage.NodeID, float64) {
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
		edges, _ := e.storage.GetOutgoingEdges(current.nodeID)
		for _, edge := range edges {
			if relType != "" && edge.Type != relType {
				continue
			}
			neighbor := edge.EndNode
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
func (e *StorageExecutor) callApocAlgoAStar(ctx context.Context, cypher string) (*ExecuteResult, error) {
	startID, endID, relType, weightProp, err := e.parsePathAlgoParams(cypher, "APOC.ALGO.ASTAR")
	if err != nil {
		return nil, err
	}
	path, weight := e.astar(startID, endID, relType, weightProp, "lat", "lon")
	if path == nil {
		return &ExecuteResult{Columns: []string{"path", "weight"}, Rows: [][]interface{}{}}, nil
	}
	return &ExecuteResult{Columns: []string{"path", "weight"}, Rows: [][]interface{}{{path, weight}}}, nil
}

func (e *StorageExecutor) astar(startID, endID storage.NodeID, relType, weightProp, latProp, lonProp string) ([]storage.NodeID, float64) {
	endNode, err := e.storage.GetNode(endID)
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
		node, err := e.storage.GetNode(nodeID)
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
		edges, _ := e.storage.GetOutgoingEdges(current.nodeID)
		for _, edge := range edges {
			if relType != "" && edge.Type != relType {
				continue
			}
			neighbor := edge.EndNode
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

// callApocAlgoAllSimplePaths finds all simple paths.
func (e *StorageExecutor) callApocAlgoAllSimplePaths(ctx context.Context, cypher string) (*ExecuteResult, error) {
	startID, endID, relType, _, err := e.parsePathAlgoParams(cypher, "APOC.ALGO.ALLSIMPLEPATHS")
	if err != nil {
		return nil, err
	}
	paths := e.findAllSimplePaths(startID, endID, relType, 10)
	rows := make([][]interface{}, len(paths))
	for i, path := range paths {
		rows[i] = []interface{}{path}
	}
	return &ExecuteResult{Columns: []string{"path"}, Rows: rows}, nil
}

func (e *StorageExecutor) findAllSimplePaths(startID, endID storage.NodeID, relType string, maxDepth int) [][]storage.NodeID {
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
		edges, _ := e.storage.GetOutgoingEdges(current)
		for _, edge := range edges {
			if relType != "" && edge.Type != relType {
				continue
			}
			neighbor := edge.EndNode
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
func (e *StorageExecutor) callApocAlgoPageRank(ctx context.Context, cypher string) (*ExecuteResult, error) {
	label := e.extractLabelFromAlgoCall(cypher, "PAGERANK")
	scores := e.computePageRank(label, 0.85, 20)
	rows := make([][]interface{}, 0, len(scores))
	for nodeID, score := range scores {
		node, err := e.storage.GetNode(nodeID)
		if err != nil {
			continue
		}
		rows = append(rows, []interface{}{node, score})
	}
	return &ExecuteResult{Columns: []string{"node", "score"}, Rows: rows}, nil
}

func (e *StorageExecutor) computePageRank(label string, damping float64, iterations int) map[storage.NodeID]float64 {
	var nodes []*storage.Node
	if label != "" {
		nodes, _ = e.storage.GetNodesByLabel(label)
	} else {
		nodes = e.storage.GetAllNodes()
	}
	if len(nodes) == 0 {
		return map[storage.NodeID]float64{}
	}
	n := float64(len(nodes))
	scores := make(map[storage.NodeID]float64)
	newScores := make(map[storage.NodeID]float64)
	outDegree := make(map[storage.NodeID]int)
	for _, node := range nodes {
		scores[node.ID] = 1.0 / n
		outDegree[node.ID] = e.storage.GetOutDegree(node.ID)
	}
	for i := 0; i < iterations; i++ {
		for _, node := range nodes {
			newScores[node.ID] = (1.0 - damping) / n
		}
		for _, node := range nodes {
			if outDegree[node.ID] == 0 {
				share := damping * scores[node.ID] / n
				for _, other := range nodes {
					newScores[other.ID] += share
				}
			} else {
				edges, _ := e.storage.GetOutgoingEdges(node.ID)
				share := damping * scores[node.ID] / float64(outDegree[node.ID])
				for _, edge := range edges {
					newScores[edge.EndNode] += share
				}
			}
		}
		scores, newScores = newScores, scores
	}
	return scores
}

// callApocAlgoBetweenness computes betweenness centrality.
func (e *StorageExecutor) callApocAlgoBetweenness(ctx context.Context, cypher string) (*ExecuteResult, error) {
	label := e.extractLabelFromAlgoCall(cypher, "BETWEENNESS")
	scores := e.computeBetweenness(label)
	rows := make([][]interface{}, 0, len(scores))
	for nodeID, score := range scores {
		node, err := e.storage.GetNode(nodeID)
		if err != nil {
			continue
		}
		rows = append(rows, []interface{}{node, score})
	}
	return &ExecuteResult{Columns: []string{"node", "score"}, Rows: rows}, nil
}

func (e *StorageExecutor) computeBetweenness(label string) map[storage.NodeID]float64 {
	var nodes []*storage.Node
	if label != "" {
		nodes, _ = e.storage.GetNodesByLabel(label)
	} else {
		nodes = e.storage.GetAllNodes()
	}
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

// callApocAlgoCloseness computes closeness centrality.
func (e *StorageExecutor) callApocAlgoCloseness(ctx context.Context, cypher string) (*ExecuteResult, error) {
	label := e.extractLabelFromAlgoCall(cypher, "CLOSENESS")
	scores := e.computeCloseness(label)
	rows := make([][]interface{}, 0, len(scores))
	for nodeID, score := range scores {
		node, err := e.storage.GetNode(nodeID)
		if err != nil {
			continue
		}
		rows = append(rows, []interface{}{node, score})
	}
	return &ExecuteResult{Columns: []string{"node", "score"}, Rows: rows}, nil
}

func (e *StorageExecutor) computeCloseness(label string) map[storage.NodeID]float64 {
	var nodes []*storage.Node
	if label != "" {
		nodes, _ = e.storage.GetNodesByLabel(label)
	} else {
		nodes = e.storage.GetAllNodes()
	}
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
			node, err := e.getStorage(ctx).GetNode(nodeID)
			if err != nil {
				return nil, err
			}
			rows = append(rows, []interface{}{node})
		}
	}
	return &ExecuteResult{Columns: []string{"node"}, Rows: rows}, nil
}

func (e *StorageExecutor) getNeighborsByhop(ctx context.Context, startID storage.NodeID, relType string, maxHops int) ([][]storage.NodeID, error) {
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
			node, err := e.getStorage(ctx).GetNode(nodeID)
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

// Helper functions
func (e *StorageExecutor) parsePathAlgoParams(cypher, algoName string) (storage.NodeID, storage.NodeID, string, string, error) {
	upper := upperASCII(cypher)
	idx := strings.Index(upper, algoName)
	if idx < 0 {
		return "", "", "", "", fmt.Errorf("could not find %s", algoName)
	}
	remainder := cypher[idx:]
	openParen := strings.Index(remainder, "(")
	closeParen := strings.LastIndex(remainder, ")")
	if openParen < 0 || closeParen < 0 {
		return "", "", "", "", fmt.Errorf("invalid syntax for %s", algoName)
	}
	args := remainder[openParen+1 : closeParen]
	parts := strings.Split(args, ",")
	if len(parts) < 2 {
		return "", "", "", "", fmt.Errorf("%s requires at least 2 arguments", algoName)
	}
	startID := storage.NodeID(strings.Trim(strings.TrimSpace(parts[0]), "'\""))
	endID := storage.NodeID(strings.Trim(strings.TrimSpace(parts[1]), "'\""))
	relType := ""
	if len(parts) > 2 {
		relType = strings.Trim(strings.TrimSpace(parts[2]), "'\"")
	}
	weightProp := ""
	if len(parts) > 3 {
		weightProp = strings.Trim(strings.TrimSpace(parts[3]), "'\"")
	}
	return startID, endID, relType, weightProp, nil
}

func (e *StorageExecutor) extractLabelFromAlgoCall(cypher, algoName string) string {
	upper := upperASCII(cypher)
	idx := strings.Index(upper, algoName)
	if idx < 0 {
		return ""
	}
	remainder := cypher[idx:]
	openParen := strings.Index(remainder, "(")
	closeParen := strings.Index(remainder, ")")
	if openParen > 0 && closeParen > openParen {
		args := strings.TrimSpace(remainder[openParen+1 : closeParen])
		if args != "" && args != "''" && args != "[]" {
			return strings.Trim(args, "'\"[]")
		}
	}
	return ""
}
