// Package storage provides the storage interface for APOC functions.
//
// This package defines the interface that APOC functions use to interact
// with the underlying graph database storage engine.
package storage

import (
	"context"
	nativestorage "github.com/orneryd/nornicdb/pkg/storage"
	"maps"
)

// Storage defines the interface for graph database operations.
//
// Every point, path and traversal method is context-first: the caller's context
// is the lifetime of that operation. It decides admission - a caller that has
// already stopped is refused before any state is read or written - and it
// decides whether a read may be published, so a read whose caller stopped
// returns that caller's own error instead of a value. Ordinary results are
// unchanged: a missing entity still reports its not-found error, an empty
// collection is still the empty collection the call documents, and a write that
// was accepted before the caller stopped keeps the completion it already had.
//
// Values carry the same lifetime. A read hands out caller-owned values: the
// node, relationship, path member or traversal visitor value a caller receives
// is that caller's own, so mutating it leaves the retained graph, and every
// later read of it, exactly as it was. A write accepts caller-owned values for
// the same reason: the store retains its own copy, so a caller that keeps
// mutating the containers it supplied cannot change the graph afterwards.
type Storage interface {
	// Node operations
	GetNode(ctx context.Context, id int64) (*Node, error)
	CreateNode(ctx context.Context, labels []string, properties map[string]interface{}) (*Node, error)
	UpdateNode(ctx context.Context, id int64, properties map[string]interface{}) error
	// DeleteNode also removes incident relationships, matching the native backend.
	DeleteNode(ctx context.Context, id int64) error
	AddLabels(ctx context.Context, id int64, labels []string) error
	RemoveLabels(ctx context.Context, id int64, labels []string) error

	// Bulk node operations
	//
	// A bulk read context is the read lifetime: a read whose context has already
	// stopped returns that context error instead of a collection, so a caller
	// never receives a partial whole-graph result. A completed read returns every
	// entry, and an empty store still returns a non-nil empty collection.
	AllNodes(ctx context.Context) ([]*Node, error)
	UpdateNodeLabels(ctx context.Context, id int64, labels []string) error

	// Relationship operations
	GetRelationship(ctx context.Context, id int64) (*Relationship, error)
	CreateRelationship(ctx context.Context, startID, endID int64, relType string, properties map[string]interface{}) (*Relationship, error)
	UpdateRelationship(ctx context.Context, id int64, properties map[string]interface{}) error
	DeleteRelationship(ctx context.Context, id int64) error
	UpdateRelationshipType(ctx context.Context, id int64, newType string) error

	// Bulk relationship operations
	//
	// AllRelationships is context-first like AllNodes and follows the same
	// contract.
	AllRelationships(ctx context.Context) ([]*Relationship, error)

	// Query operations
	GetNodeRelationships(ctx context.Context, nodeID int64, relType string, direction Direction) ([]*Relationship, error)
	GetNodeNeighbors(ctx context.Context, nodeID int64, relType string, direction Direction) ([]*Node, error)
	GetNodeDegree(ctx context.Context, nodeID int64, relType string, direction Direction) (int, error)

	// Path operations
	FindShortestPath(ctx context.Context, startID, endID int64, relType string, maxHops int) (*Path, error)
	FindAllPaths(ctx context.Context, startID, endID int64, relType string, maxHops int) ([]*Path, error)

	// Traversal operations
	BFS(ctx context.Context, startID int64, relType string, maxDepth int, visitor func(*Node) bool) error
	DFS(ctx context.Context, startID int64, relType string, maxDepth int, visitor func(*Node) bool) error
}

// Direction represents relationship direction.
type Direction int

const (
	DirectionBoth Direction = iota
	DirectionOutgoing
	DirectionIncoming
)

// Node represents a graph node.
type Node struct {
	ID         int64
	Labels     []string
	Properties map[string]interface{}
}

// Relationship represents a graph relationship.
type Relationship struct {
	ID         int64
	Type       string
	StartNode  int64
	EndNode    int64
	Properties map[string]interface{}
}

// Path represents a path through the graph.
type Path struct {
	Nodes         []*Node
	Relationships []*Relationship
	Length        int
}

// InMemoryStorage provides an in-memory implementation for testing/development.
type InMemoryStorage struct {
	nodes         map[int64]*Node
	relationships map[int64]*Relationship
	nodeRels      map[int64][]*Relationship
	nextNodeID    int64
	nextRelID     int64
}

// NewInMemoryStorage creates a new in-memory storage.
func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		nodes:         make(map[int64]*Node),
		relationships: make(map[int64]*Relationship),
		nodeRels:      make(map[int64][]*Relationship),
		nextNodeID:    1,
		nextRelID:     1,
	}
}

// retainedEntry owns admission and lookup for internal reads and mutations.
// Its value stays inside the store; public reads detach it before publication.
func retainedEntry[T any](ctx context.Context, entries map[int64]*T, id int64, notFound error) (*T, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if entry, ok := entries[id]; ok {
		return entry, nil
	}
	return nil, notFound
}

// ownedEntry publishes a detached point read only while its caller is active.
func ownedEntry[T any](ctx context.Context, entries map[int64]*T, id int64, notFound error) (*T, error) {
	entry, err := retainedEntry(ctx, entries, id, notFound)
	if err != nil {
		return nil, err
	}
	owned := nativestorage.CopyValue(entry, nil).(*T)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return owned, nil
}

func (s *InMemoryStorage) GetNode(ctx context.Context, id int64) (*Node, error) {
	return ownedEntry(ctx, s.nodes, id, ErrNodeNotFound)
}

func (s *InMemoryStorage) CreateNode(ctx context.Context, labels []string, properties map[string]interface{}) (*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// The accepted labels and properties stay the caller's: the store retains
	// its own copy so a later mutation of the supplied containers cannot reach
	// the graph.
	node := nativestorage.CopyValue(&Node{
		ID:         s.nextNodeID,
		Labels:     labels,
		Properties: properties,
	}, nil).(*Node)
	s.nodes[node.ID] = node
	s.nextNodeID++
	return nativestorage.CopyValue(node, nil).(*Node), nil
}

// mergeProperties owns update inputs and initializes an absent retained bag.
func mergeProperties(current, updates map[string]interface{}) map[string]interface{} {
	if len(updates) == 0 {
		return current
	}
	owned := nativestorage.CopyValue(updates, nil).(map[string]interface{})
	if current == nil {
		return owned
	}
	maps.Copy(current, owned)
	return current
}

func (s *InMemoryStorage) UpdateNode(ctx context.Context, id int64, properties map[string]interface{}) error {
	node, err := retainedEntry(ctx, s.nodes, id, ErrNodeNotFound)
	if err != nil {
		return err
	}
	node.Properties = mergeProperties(node.Properties, properties)
	return nil
}

func (s *InMemoryStorage) DeleteNode(ctx context.Context, id int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := s.nodes[id]; !ok {
		return ErrNodeNotFound
	}
	// Match the native backend's cascade contract. This mutation has passed
	// admission; incident removals share its completion, without readmitting
	// each child against a caller that may have stopped in the meantime.
	for _, rel := range s.nodeRels[id] {
		s.removeRelationship(rel)
	}
	delete(s.nodes, id)
	delete(s.nodeRels, id)
	return nil
}

func (s *InMemoryStorage) AddLabels(ctx context.Context, id int64, labels []string) error {
	node, err := retainedEntry(ctx, s.nodes, id, ErrNodeNotFound)
	if err != nil {
		return err
	}
	for _, label := range labels {
		if !hasLabel(node, label) {
			node.Labels = append(node.Labels, label)
		}
	}
	return nil
}

func (s *InMemoryStorage) RemoveLabels(ctx context.Context, id int64, labels []string) error {
	node, err := retainedEntry(ctx, s.nodes, id, ErrNodeNotFound)
	if err != nil {
		return err
	}
	labelSet := make(map[string]bool)
	for _, label := range labels {
		labelSet[label] = true
	}
	newLabels := make([]string, 0)
	for _, label := range node.Labels {
		if !labelSet[label] {
			newLabels = append(newLabels, label)
		}
	}
	node.Labels = newLabels
	return nil
}

func (s *InMemoryStorage) GetRelationship(ctx context.Context, id int64) (*Relationship, error) {
	return ownedEntry(ctx, s.relationships, id, ErrRelationshipNotFound)
}

func (s *InMemoryStorage) CreateRelationship(ctx context.Context, startID, endID int64, relType string, properties map[string]interface{}) (*Relationship, error) {
	if _, err := retainedEntry(ctx, s.nodes, startID, ErrNodeNotFound); err != nil {
		return nil, err
	}
	if _, err := retainedEntry(ctx, s.nodes, endID, ErrNodeNotFound); err != nil {
		return nil, err
	}

	// The accepted properties stay the caller's: the store retains its own copy.
	rel := nativestorage.CopyValue(&Relationship{
		ID:         s.nextRelID,
		Type:       relType,
		StartNode:  startID,
		EndNode:    endID,
		Properties: properties,
	}, nil).(*Relationship)
	s.relationships[rel.ID] = rel
	s.nodeRels[startID] = append(s.nodeRels[startID], rel)
	s.nodeRels[endID] = append(s.nodeRels[endID], rel)
	s.nextRelID++
	return nativestorage.CopyValue(rel, nil).(*Relationship), nil
}

func (s *InMemoryStorage) UpdateRelationship(ctx context.Context, id int64, properties map[string]interface{}) error {
	rel, err := retainedEntry(ctx, s.relationships, id, ErrRelationshipNotFound)
	if err != nil {
		return err
	}
	rel.Properties = mergeProperties(rel.Properties, properties)
	return nil
}

func (s *InMemoryStorage) DeleteRelationship(ctx context.Context, id int64) error {
	rel, err := retainedEntry(ctx, s.relationships, id, ErrRelationshipNotFound)
	if err != nil {
		return err
	}

	s.removeRelationship(rel)
	return nil
}

// removeRelationship applies the resolved relationship's accepted mutation.
// Public deletion admits and resolves first; a node cascade already owns both.
func (s *InMemoryStorage) removeRelationship(rel *Relationship) {
	s.nodeRels[rel.StartNode] = removeRel(s.nodeRels[rel.StartNode], rel.ID)
	s.nodeRels[rel.EndNode] = removeRel(s.nodeRels[rel.EndNode], rel.ID)
	delete(s.relationships, rel.ID)
}

// bulkReadInterval is how many entries an in-memory bulk read copies between
// checks of its caller context. Checking at an interval rather than on every
// entry keeps the copy tight while bounding how long a read that already lost
// its caller keeps working.
const bulkReadInterval = 256

// collectBulk copies one storage map into the collection a whole-graph read
// returns, detaching each entry through the shared structural value owner. The
// caller context decides the read lifetime: the collection is checked before
// the result is allocated, every bulkReadInterval entries, and once more
// before the result is published, so a stopped read returns its context error
// and no partial collection.
func collectBulk[T any](ctx context.Context, entries map[int64]*T) ([]*T, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	items := make([]*T, 0, len(entries))
	for _, entry := range entries {
		items = append(items, nativestorage.CopyValue(entry, nil).(*T))
		if len(items)%bulkReadInterval == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *InMemoryStorage) AllNodes(ctx context.Context) ([]*Node, error) {
	return collectBulk(ctx, s.nodes)
}

func (s *InMemoryStorage) UpdateNodeLabels(ctx context.Context, id int64, labels []string) error {
	node, err := retainedEntry(ctx, s.nodes, id, ErrNodeNotFound)
	if err != nil {
		return err
	}
	node.Labels = nativestorage.CopyValue(labels, nil).([]string)
	return nil
}

func (s *InMemoryStorage) AllRelationships(ctx context.Context) ([]*Relationship, error) {
	return collectBulk(ctx, s.relationships)
}

func (s *InMemoryStorage) UpdateRelationshipType(ctx context.Context, id int64, newType string) error {
	rel, err := retainedEntry(ctx, s.relationships, id, ErrRelationshipNotFound)
	if err != nil {
		return err
	}
	rel.Type = newType
	return nil
}

// walkRelationships visits the retained relationships incident to nodeID, in
// stored order, skipping those that do not match relType or direction. It is the
// one owner of relationship filtering: node existence is decided once here, the
// caller's context is checked every bulkReadInterval relationships and again
// before the walk returns, and a caller that stopped receives its own error
// instead of a partial walk.
//
// A relationship it hands the visitor stays inside the store. A public read
// detaches what it publishes; an internal read keeps the retained record so a
// traversal copies only the members it returns.
func (s *InMemoryStorage) walkRelationships(ctx context.Context, nodeID int64, relType string, direction Direction, visit func(*Relationship) error) error {
	if _, err := retainedEntry(ctx, s.nodes, nodeID, ErrNodeNotFound); err != nil {
		return err
	}

	for index, rel := range s.nodeRels[nodeID] {
		if index%bulkReadInterval == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		// Filter by type
		if relType != "" && rel.Type != relType {
			continue
		}
		// Filter by direction
		if !relMatchesDirection(rel, nodeID, direction) {
			continue
		}
		if err := visit(rel); err != nil {
			return err
		}
	}

	// A caller that stopped while this walk filtered keeps no collection.
	return ctx.Err()
}

// relMatchesDirection reports whether rel is incident to nodeID in the requested
// direction. An unrecognized direction matches nothing, which is the answer a
// relationship read has always given.
func relMatchesDirection(rel *Relationship, nodeID int64, direction Direction) bool {
	switch direction {
	case DirectionOutgoing:
		return rel.StartNode == nodeID
	case DirectionIncoming:
		return rel.EndNode == nodeID
	case DirectionBoth:
		return true
	default:
		return false
	}
}

// neighborEntry is one neighbor of a walk together with the first matching
// relationship that reached it. Both stay inside the store: a public read detaches
// the node it publishes, and an internal traversal queues the identity and keeps
// the relationship for a path it may return.
type neighborEntry struct {
	node *Node
	rel  *Relationship
}

// neighborEntries collects the distinct neighbors of nodeID in the order the walk
// reaches them, each with the first matching relationship. A relationship that
// points at a node the store does not hold yields no neighbor, as it always did,
// so the walk skips it and keeps looking.
func (s *InMemoryStorage) neighborEntries(ctx context.Context, nodeID int64, relType string, direction Direction) ([]neighborEntry, error) {
	entries := make([]neighborEntry, 0)
	seen := make(map[int64]bool)
	err := s.walkRelationships(ctx, nodeID, relType, direction, func(rel *Relationship) error {
		neighborID := rel.EndNode
		if rel.StartNode != nodeID {
			neighborID = rel.StartNode
		}
		if seen[neighborID] {
			return nil
		}
		neighbor, ok := s.nodes[neighborID]
		if !ok {
			return nil
		}
		seen[neighborID] = true
		entries = append(entries, neighborEntry{node: neighbor, rel: rel})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (s *InMemoryStorage) GetNodeRelationships(ctx context.Context, nodeID int64, relType string, direction Direction) ([]*Relationship, error) {
	result := make([]*Relationship, 0)
	err := s.walkRelationships(ctx, nodeID, relType, direction, func(rel *Relationship) error {
		result = append(result, nativestorage.CopyValue(rel, nil).(*Relationship))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *InMemoryStorage) GetNodeNeighbors(ctx context.Context, nodeID int64, relType string, direction Direction) ([]*Node, error) {
	entries, err := s.neighborEntries(ctx, nodeID, relType, direction)
	if err != nil {
		return nil, err
	}

	neighbors := make([]*Node, 0, len(entries))
	for _, entry := range entries {
		neighbors = append(neighbors, nativestorage.CopyValue(entry.node, nil).(*Node))
	}

	// A caller that stopped while this read resolved neighbors keeps no collection.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return neighbors, nil
}

func (s *InMemoryStorage) GetNodeDegree(ctx context.Context, nodeID int64, relType string, direction Direction) (int, error) {
	// A count needs the walk and nothing else: no record, and no property
	// payload, is materialized for a caller that asked only for a degree.
	degree := 0
	if err := s.walkRelationships(ctx, nodeID, relType, direction, func(*Relationship) error {
		degree++
		return nil
	}); err != nil {
		return 0, err
	}
	return degree, nil
}

func (s *InMemoryStorage) FindShortestPath(ctx context.Context, startID, endID int64, relType string, maxHops int) (*Path, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if startID == endID {
		start, err := s.GetNode(ctx, startID)
		if err != nil {
			return nil, err
		}
		return &Path{
			Nodes:         []*Node{start},
			Relationships: []*Relationship{},
			Length:        0,
		}, nil
	}

	visited := make(map[int64]bool)
	parent := make(map[int64]*pathParent)
	queue := []int64{startID}
	visited[startID] = true

	for len(queue) > 0 && len(parent) < maxHops*100 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		currentID := queue[0]
		queue = queue[1:]

		if currentID == endID {
			return s.reconstructPath(ctx, startID, endID, parent)
		}

		depth := 0
		if p, ok := parent[currentID]; ok {
			depth = p.depth + 1
		}

		if depth >= maxHops {
			continue
		}

		entries, err := s.neighborEntries(ctx, currentID, relType, DirectionBoth)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			// An ordinary read failure stays a dead end for this search, as it
			// was before the caller's lifetime decided the read.
		}
		for _, entry := range entries {
			if visited[entry.node.ID] {
				continue
			}
			visited[entry.node.ID] = true

			// The entry carries the first relationship that reached this
			// neighbor, which is the edge this search always chose among
			// parallel ones. The path detaches it when it is returned.
			parent[entry.node.ID] = &pathParent{
				nodeID: currentID,
				rel:    entry.rel,
				depth:  depth,
			}

			queue = append(queue, entry.node.ID)
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, ErrPathNotFound
}

func (s *InMemoryStorage) FindAllPaths(ctx context.Context, startID, endID int64, relType string, maxHops int) ([]*Path, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	paths := make([]*Path, 0)
	visited := make(map[int64]bool)
	var stopped error

	var dfs func(currentID int64, path *Path, depth int)
	dfs = func(currentID int64, path *Path, depth int) {
		if stopped != nil {
			return
		}
		if err := ctx.Err(); err != nil {
			stopped = err
			return
		}
		if currentID == endID {
			// Found a path. The walk collected retained members, so the
			// completed path is detached once through the shared copy owner.
			paths = append(paths, nativestorage.CopyValue(&Path{
				Nodes:         append([]*Node{}, path.Nodes...),
				Relationships: append([]*Relationship{}, path.Relationships...),
				Length:        path.Length,
			}, nil).(*Path))
			return
		}

		if depth >= maxHops {
			return
		}

		visited[currentID] = true

		entries, err := s.neighborEntries(ctx, currentID, relType, DirectionBoth)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				stopped = ctxErr
				return
			}
		}
		for _, entry := range entries {
			if visited[entry.node.ID] {
				continue
			}
			path.Nodes = append(path.Nodes, entry.node)
			path.Relationships = append(path.Relationships, entry.rel)
			path.Length++

			dfs(entry.node.ID, path, depth+1)

			path.Nodes = path.Nodes[:len(path.Nodes)-1]
			path.Relationships = path.Relationships[:len(path.Relationships)-1]
			path.Length--
		}

		visited[currentID] = false
	}

	start, err := retainedEntry(ctx, s.nodes, startID, ErrNodeNotFound)
	if err != nil {
		return nil, err
	}

	initialPath := &Path{
		Nodes:         []*Node{start},
		Relationships: []*Relationship{},
		Length:        0,
	}

	dfs(startID, initialPath, 0)

	if stopped != nil {
		return nil, stopped
	}
	// A caller that stopped while this read collected keeps no partial path list.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return paths, nil
}

func (s *InMemoryStorage) BFS(ctx context.Context, startID int64, relType string, maxDepth int, visitor func(*Node) bool) error {
	visited := make(map[int64]bool)
	queue := []nodeDepth{{nodeID: startID, depth: 0}}
	visited[startID] = true

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		node, err := s.GetNode(ctx, current.nodeID)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			continue
		}

		keepGoing := visitor(node)
		if err := ctx.Err(); err != nil {
			return err
		}
		if !keepGoing {
			return nil
		}

		if current.depth >= maxDepth {
			continue
		}

		entries, err := s.neighborEntries(ctx, current.nodeID, relType, DirectionBoth)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
		}
		for _, entry := range entries {
			if !visited[entry.node.ID] {
				visited[entry.node.ID] = true
				queue = append(queue, nodeDepth{nodeID: entry.node.ID, depth: current.depth + 1})
			}
		}
	}

	return ctx.Err()
}

func (s *InMemoryStorage) DFS(ctx context.Context, startID int64, relType string, maxDepth int, visitor func(*Node) bool) error {
	visited := make(map[int64]bool)
	var stopped error

	var dfs func(nodeID int64, depth int) bool
	dfs = func(nodeID int64, depth int) bool {
		if err := ctx.Err(); err != nil {
			stopped = err
			return false
		}
		visited[nodeID] = true

		node, err := s.GetNode(ctx, nodeID)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				stopped = ctxErr
				return false
			}
			return true
		}

		keepGoing := visitor(node)
		if err := ctx.Err(); err != nil {
			stopped = err
			return false
		}
		if !keepGoing {
			return false
		}

		if depth >= maxDepth {
			return true
		}

		entries, err := s.neighborEntries(ctx, nodeID, relType, DirectionBoth)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				stopped = ctxErr
				return false
			}
		}
		for _, entry := range entries {
			if !visited[entry.node.ID] {
				if !dfs(entry.node.ID, depth+1) {
					return false
				}
			}
		}

		return true
	}

	dfs(startID, 0)
	if stopped != nil {
		return stopped
	}
	return ctx.Err()
}

// Helper types and functions

type pathParent struct {
	nodeID int64
	rel    *Relationship
	depth  int
}

type nodeDepth struct {
	nodeID int64
	depth  int
}

func (s *InMemoryStorage) reconstructPath(ctx context.Context, startID, endID int64, parent map[int64]*pathParent) (*Path, error) {
	nodes := make([]*Node, 0)
	rels := make([]*Relationship, 0)

	currentID := endID
	for currentID != startID {
		node, err := s.GetNode(ctx, currentID)
		if err != nil {
			return nil, err
		}
		nodes = append([]*Node{node}, nodes...)

		if p, ok := parent[currentID]; ok {
			rels = append([]*Relationship{nativestorage.CopyValue(p.rel, nil).(*Relationship)}, rels...)
			currentID = p.nodeID
		} else {
			return nil, ErrPathNotFound
		}
	}

	start, err := s.GetNode(ctx, startID)
	if err != nil {
		return nil, err
	}
	nodes = append([]*Node{start}, nodes...)

	return &Path{
		Nodes:         nodes,
		Relationships: rels,
		Length:        len(rels),
	}, nil
}

func hasLabel(node *Node, label string) bool {
	for _, l := range node.Labels {
		if l == label {
			return true
		}
	}
	return false
}

func removeRel(rels []*Relationship, id int64) []*Relationship {
	result := make([]*Relationship, 0)
	for _, rel := range rels {
		if rel.ID != id {
			result = append(result, rel)
		}
	}
	return result
}

// Errors
var (
	ErrNodeNotFound         = &StorageError{Message: "node not found"}
	ErrRelationshipNotFound = &StorageError{Message: "relationship not found"}
	ErrPathNotFound         = &StorageError{Message: "path not found"}
)

type StorageError struct {
	Message string
}

func (e *StorageError) Error() string {
	return e.Message
}
