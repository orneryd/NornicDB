package storage

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
)

// BenchmarkCompositeEntityPointRead is the bounded point-read baseline for the
// composite's identity reads. One seeded source owns the entity, so an
// unqualified read walks every readable source while a qualified read routes
// straight to the owner. Every iteration asserts the resolved value, so a
// faster number cannot come from a read that silently stopped resolving.
func BenchmarkCompositeEntityPointRead(b *testing.B) {
	for _, count := range []int{2, 8} {
		for _, qualified := range []bool{false, true} {
			b.Run(fmt.Sprintf("constituents=%d/qualified=%t", count, qualified), func(b *testing.B) {
				composite, alias, nodeVersion, edgeVersion := newCompositePointReadBenchmark(b, count)
				nodeID, edgeID := NodeID(compositePointReadNodeID), EdgeID(compositePointReadEdgeID)
				if qualified {
					nodeID = NodeID(SourceNodeElementID(alias, compositePointReadNodeID))
					edgeID = EdgeID(SourceRelationshipElementID(alias, compositePointReadEdgeID))
				}
				ctx := context.Background()
				reads := []struct {
					name string
					read func() error
				}{
					{"GetNode", func() error {
						node, err := composite.GetNode(ctx, nodeID)
						if err == nil && node == nil {
							return fmt.Errorf("GetNode returned no node and no error")
						}
						return err
					}},
					{"GetEdge", func() error {
						edge, err := composite.GetEdge(ctx, edgeID)
						if err == nil && edge == nil {
							return fmt.Errorf("GetEdge returned no edge and no error")
						}
						return err
					}},
					{"GetNodeProjected", func() error {
						node, err := composite.GetNodeProjected(ctx, nodeID, []string{"k"})
						if err == nil && node == nil {
							return fmt.Errorf("GetNodeProjected returned no node and no error")
						}
						return err
					}},
					{"GetNodeLatestVisible", func() error {
						node, err := composite.GetNodeLatestVisible(ctx, nodeID)
						if err == nil && node == nil {
							return fmt.Errorf("GetNodeLatestVisible returned no node and no error")
						}
						return err
					}},
					{"GetNodeLatestEffective", func() error {
						node, err := composite.GetNodeLatestEffective(ctx, nodeID)
						if err == nil && node == nil {
							return fmt.Errorf("GetNodeLatestEffective returned no node and no error")
						}
						return err
					}},
					{"GetNodeVisibleAt", func() error {
						node, err := composite.GetNodeVisibleAt(ctx, nodeID, nodeVersion)
						if err == nil && node == nil {
							return fmt.Errorf("GetNodeVisibleAt returned no node and no error")
						}
						return err
					}},
					{"GetNodeCurrentHead", func() error {
						head, err := composite.GetNodeCurrentHead(ctx, nodeID)
						if err == nil && head.Version.Compare(nodeVersion) != 0 {
							return fmt.Errorf("GetNodeCurrentHead version %v, want %v", head.Version, nodeVersion)
						}
						return err
					}},
					{"GetEdgeLatestVisible", func() error {
						edge, err := composite.GetEdgeLatestVisible(ctx, edgeID)
						if err == nil && edge == nil {
							return fmt.Errorf("GetEdgeLatestVisible returned no edge and no error")
						}
						return err
					}},
					{"GetEdgeLatestEffective", func() error {
						edge, err := composite.GetEdgeLatestEffective(ctx, edgeID)
						if err == nil && edge == nil {
							return fmt.Errorf("GetEdgeLatestEffective returned no edge and no error")
						}
						return err
					}},
					{"GetEdgeVisibleAt", func() error {
						edge, err := composite.GetEdgeVisibleAt(ctx, edgeID, edgeVersion)
						if err == nil && edge == nil {
							return fmt.Errorf("GetEdgeVisibleAt returned no edge and no error")
						}
						return err
					}},
					{"GetEdgeCurrentHead", func() error {
						head, err := composite.GetEdgeCurrentHead(ctx, edgeID)
						if err == nil && head.Version.Compare(edgeVersion) != 0 {
							return fmt.Errorf("GetEdgeCurrentHead version %v, want %v", head.Version, edgeVersion)
						}
						return err
					}},
				}
				for _, read := range reads {
					b.Run(read.name, func(b *testing.B) {
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							if err := read.read(); err != nil {
								b.Fatal(err)
							}
						}
					})
				}
			})
		}
	}
}

const (
	compositePointReadNodeID = "bench:point"
	compositePointReadEdgeID = "bench:point-edge"
)

// newCompositePointReadBenchmark builds the requested number of in-memory
// constituents and seeds the entity in the last one so the unqualified walk is
// bounded by the constituent count. It returns the owning alias and the
// creation versions used by the visible-at and head reads.
func newCompositePointReadBenchmark(b *testing.B, constituentCount int) (*CompositeEngine, string, MVCCVersion, MVCCVersion) {
	b.Helper()
	constituents := make(map[string]Engine, constituentCount)
	names := make(map[string]string, constituentCount)
	modes := make(map[string]string, constituentCount)
	var owner Engine
	var alias string
	for index := 0; index < constituentCount; index++ {
		alias = fmt.Sprintf("bench-%d", index)
		engine, err := fixtureowner.OwnResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
		if err != nil {
			b.Fatal(err)
		}
		constituents[alias], names[alias], modes[alias] = engine, alias, "read"
		owner = engine
	}
	ctx := context.Background()
	if _, err := owner.CreateNode(ctx, &Node{ID: compositePointReadNodeID, Labels: []string{"Bench"}, Properties: map[string]any{"k": "v"}}); err != nil {
		b.Fatal(err)
	}
	if err := owner.CreateEdge(ctx, &Edge{ID: compositePointReadEdgeID, StartNode: compositePointReadNodeID, EndNode: compositePointReadNodeID, Type: "LOOP"}); err != nil {
		b.Fatal(err)
	}
	heads := owner.(MVCCHeadEngine)
	nodeHead, err := heads.GetNodeCurrentHead(ctx, compositePointReadNodeID)
	if err != nil {
		b.Fatal(err)
	}
	edgeHead, err := heads.GetEdgeCurrentHead(ctx, compositePointReadEdgeID)
	if err != nil {
		b.Fatal(err)
	}
	return NewCompositeEngine(constituents, names, modes), alias, nodeHead.Version, edgeHead.Version
}
