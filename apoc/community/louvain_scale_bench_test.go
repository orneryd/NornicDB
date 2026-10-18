package community

import "testing"

func louvainScaleGraph() ([]*Node, []*Relationship) {
	const blocks, width = 64, 16
	nodes := make([]*Node, blocks*width)
	for i := range nodes {
		nodes[i] = &Node{ID: int64(i)}
	}
	var rels []*Relationship
	add := func(i, j int) {
		rels = append(rels, &Relationship{ID: int64(len(rels)), StartNode: int64(i), EndNode: int64(j)})
	}
	for block := 0; block < blocks; block++ {
		start := block * width
		for i := 0; i < width; i++ {
			for j := i + 1; j < width; j++ {
				add(start+i, start+j)
			}
		}
		add(start+width-1, ((block+1)%blocks)*width)
	}
	return nodes, rels
}

func BenchmarkLouvainScale1024(b *testing.B) {
	nodes, rels := louvainScaleGraph()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Louvain(nodes, rels, DefaultLouvainConfig())
	}
}
