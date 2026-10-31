//go:build nornic_copycount

package storage

import "sync"

var structuralCopies struct {
	sync.Mutex
	nodes map[NodeID]int
	edges map[EdgeID]int
}

var vectorCopies struct {
	sync.Mutex
	nodes map[NodeID]int
}

func countVectorCopy(node *Node) {
	vectorCopies.Lock()
	defer vectorCopies.Unlock()
	if vectorCopies.nodes == nil {
		vectorCopies.nodes = make(map[NodeID]int)
	}
	vectorCopies.nodes[node.ID]++
}

// CountVectorCopies runs read and returns how many times each node's vector
// payloads were copied while it ran. Test build only.
func CountVectorCopies(read func()) map[NodeID]int {
	vectorCopies.Lock()
	vectorCopies.nodes = make(map[NodeID]int)
	vectorCopies.Unlock()
	read()
	vectorCopies.Lock()
	defer vectorCopies.Unlock()
	nodes := vectorCopies.nodes
	vectorCopies.nodes = nil
	return nodes
}

func countNodeCopy(node *Node) {
	structuralCopies.Lock()
	defer structuralCopies.Unlock()
	if structuralCopies.nodes == nil {
		structuralCopies.nodes = make(map[NodeID]int)
	}
	structuralCopies.nodes[node.ID]++
}

func countEdgeCopy(edge *Edge) {
	structuralCopies.Lock()
	defer structuralCopies.Unlock()
	if structuralCopies.edges == nil {
		structuralCopies.edges = make(map[EdgeID]int)
	}
	structuralCopies.edges[edge.ID]++
}

// CountStructuralCopies runs read and returns how many times each node and
// edge was structurally copied while it ran. Test build only; callers must
// not run copying work concurrently.
func CountStructuralCopies(read func()) (nodes map[NodeID]int, edges map[EdgeID]int) {
	structuralCopies.Lock()
	structuralCopies.nodes, structuralCopies.edges = make(map[NodeID]int), make(map[EdgeID]int)
	structuralCopies.Unlock()
	read()
	structuralCopies.Lock()
	defer structuralCopies.Unlock()
	nodes, edges = structuralCopies.nodes, structuralCopies.edges
	structuralCopies.nodes, structuralCopies.edges = nil, nil
	return nodes, edges
}
