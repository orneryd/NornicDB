package multidb

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type legacyBulkObserved struct {
	id      string
	present bool
	err     error
}

type legacyBulkCase struct {
	name     string
	readName string
	entity   string
	prepare  func(*testing.T, *legacyTestEngine)
	invoke   func(*legacyTestEngine) error
	ids      []string
	wantErr  error
	readback func(*legacyTestEngine, string) legacyBulkObserved
}

func readLegacyBulkNode(engine *legacyTestEngine, id string) legacyBulkObserved {
	got, err := engine.GetNode(storage.NodeID(id))
	if err != nil {
		return legacyBulkObserved{id: id, present: false, err: err}
	}
	if got == nil {
		return legacyBulkObserved{id: id, present: false, err: nil}
	}
	return legacyBulkObserved{id: string(got.ID), present: true, err: nil}
}

func readLegacyBulkEdge(engine *legacyTestEngine, id string) legacyBulkObserved {
	got, err := engine.GetEdge(storage.EdgeID(id))
	if err != nil {
		return legacyBulkObserved{id: id, present: false, err: err}
	}
	if got == nil {
		return legacyBulkObserved{id: id, present: false, err: nil}
	}
	return legacyBulkObserved{id: string(got.ID), present: true, err: nil}
}

func TestLegacyBulkCallbacksPreserveSeedPolicy(t *testing.T) {
	cases := []legacyBulkCase{
		{
			name:     "BulkCreateNodes",
			readName: "GetNode", entity: "node",
			prepare: nil,
			invoke: func(engine *legacyTestEngine) error {
				first := &storage.Node{ID: "node-first"}
				later := &storage.Node{ID: "node-later"}
				return engine.BulkCreateNodes([]*storage.Node{nil, first, first, later})
			},
			ids:      []string{"node-first", "node-later"},
			wantErr:  nil,
			readback: readLegacyBulkNode,
		},
		{
			name:     "BulkCreateEdges",
			readName: "GetEdge", entity: "edge",
			prepare: func(t *testing.T, engine *legacyTestEngine) {
				t.Helper()
				if err := engine.BulkCreateNodes([]*storage.Node{
					{ID: "seed-a"},
					{ID: "seed-b"},
				}); err != nil {
					t.Fatalf("seeding nodes returned error: %v", err)
				}
			},
			invoke: func(engine *legacyTestEngine) error {
				first := &storage.Edge{ID: "edge-first", StartNode: "seed-a", EndNode: "seed-b"}
				later := &storage.Edge{ID: "edge-later", StartNode: "seed-b", EndNode: "seed-a"}
				return engine.BulkCreateEdges([]*storage.Edge{nil, first, first, later})
			},
			ids:      []string{"edge-first", "edge-later"},
			wantErr:  nil,
			readback: readLegacyBulkEdge,
		},
		{
			name:     "BulkDeleteNodes",
			readName: "GetNode", entity: "node",
			prepare: func(t *testing.T, engine *legacyTestEngine) {
				t.Helper()
				if err := engine.BulkCreateNodes([]*storage.Node{
					{ID: "node-existing-first"},
					{ID: "node-existing-later"},
				}); err != nil {
					t.Fatalf("seeding nodes returned error: %v", err)
				}
			},
			invoke: func(engine *legacyTestEngine) error {
				first := storage.NodeID("node-existing-first")
				later := storage.NodeID("node-existing-later")
				missingFirst := storage.NodeID("node-missing-first")
				missingMiddle := storage.NodeID("node-missing-middle")
				return engine.BulkDeleteNodes([]storage.NodeID{missingFirst, first, missingMiddle, later})
			},
			ids:      []string{"node-existing-first", "node-existing-later"},
			wantErr:  storage.ErrNotFound,
			readback: readLegacyBulkNode,
		},
		{
			name:     "BulkDeleteEdges",
			readName: "GetEdge", entity: "edge",
			prepare: func(t *testing.T, engine *legacyTestEngine) {
				t.Helper()
				if err := engine.BulkCreateNodes([]*storage.Node{
					{ID: "seed-a"},
					{ID: "seed-b"},
				}); err != nil {
					t.Fatalf("seeding nodes returned error: %v", err)
				}
				if err := engine.BulkCreateEdges([]*storage.Edge{
					{ID: "edge-existing-first", StartNode: "seed-a", EndNode: "seed-b"},
					{ID: "edge-existing-later", StartNode: "seed-b", EndNode: "seed-a"},
				}); err != nil {
					t.Fatalf("seeding edges returned error: %v", err)
				}
			},
			invoke: func(engine *legacyTestEngine) error {
				first := storage.EdgeID("edge-existing-first")
				later := storage.EdgeID("edge-existing-later")
				missingFirst := storage.EdgeID("edge-missing-first")
				missingMiddle := storage.EdgeID("edge-missing-middle")
				return engine.BulkDeleteEdges([]storage.EdgeID{missingFirst, first, missingMiddle, later})
			},
			ids:      []string{"edge-existing-first", "edge-existing-later"},
			wantErr:  storage.ErrNotFound,
			readback: readLegacyBulkEdge,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			engine := newLegacyTestEngine(t)

			if tc.prepare != nil {
				tc.prepare(t, engine)
			}

			if err := tc.invoke(engine); err != nil {
				t.Fatalf("%s returned error: %v", tc.name, err)
			}

			for _, id := range tc.ids {
				got := tc.readback(engine, id)
				if tc.wantErr == nil {
					if got.err != nil {
						t.Fatalf("%s(%q) returned error: %v", tc.readName, id, got.err)
					}
					if !got.present {
						t.Fatalf("%s(%q) returned nil %s", tc.readName, id, tc.entity)
					}
					if got.id != id {
						t.Fatalf("%s(%q) returned ID %q", tc.readName, id, got.id)
					}
				} else {
					if got.err != tc.wantErr {
						t.Fatalf("expected %s %q to be deleted", tc.entity, id)
					}
				}
			}
		})
	}
}

func TestLegacyCreatePolicyStopsWrappedDuplicateError(t *testing.T) {
	first := 1
	fail := 2
	later := 3
	input := []*int{nil, &first, &fail, &later}
	wrappedErr := fmt.Errorf("wrapped: %w", storage.ErrAlreadyExists)
	var visited []int
	callback := func(item *int) error {
		if item == nil {
			t.Fatal("nil item reached create callback")
		}
		visited = append(visited, *item)
		if *item == fail {
			return wrappedErr
		}
		return nil
	}
	err := applyLegacyCreates(input, callback)
	if err != wrappedErr {
		t.Fatalf("expected exact wrapped error identity, got %v", err)
	}
	if len(visited) != 2 || visited[0] != first || visited[1] != fail {
		t.Fatalf("expected visited [%d %d], got %v", first, fail, visited)
	}
}
