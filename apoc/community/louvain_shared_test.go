package community

import "testing"

func TestLouvainRingUsesSharedCommunities(t *testing.T) {
	nodes := make([]*Node, 4)
	relationships := make([]*Relationship, 4)
	for i := range nodes {
		nodes[i] = &Node{ID: int64(i)}
		relationships[i] = &Relationship{StartNode: int64(i), EndNode: int64((i + 1) % 4)}
	}
	result := Louvain(nodes, relationships, DefaultLouvainConfig())
	if len(result) != len(nodes) {
		t.Fatalf("got %d nodes, want %d", len(result), len(nodes))
	}
	for i, row := range result {
		if row.Node != nodes[i] || row.CommunityID != 0 {
			t.Errorf("node %d: got community %d, want 0", i, row.CommunityID)
		}
	}
}

func TestLouvainDefaultsResolutionAndExcludedEndpoints(t *testing.T) {
	nodes := []*Node{{ID: 10}, {ID: 20}}
	relationships := []*Relationship{{StartNode: 10, EndNode: 20}, {StartNode: 10, EndNode: 99}}
	for _, config := range []LouvainConfig{{}, {Resolution: 0.25}} {
		result := Louvain(nodes, relationships, config)
		if len(result) != 2 || result[0].Node != nodes[0] || result[1].Node != nodes[1] {
			t.Fatalf("unexpected result: %+v", result)
		}
		wantTogether := config.Resolution == 0
		if (result[0].CommunityID == result[1].CommunityID) != wantTogether {
			t.Fatalf("resolution %v: got communities %d and %d", config.Resolution, result[0].CommunityID, result[1].CommunityID)
		}
	}
}
