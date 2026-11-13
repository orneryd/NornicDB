package search

import (
	"context"
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/internal/queryprofile"
	"slices"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Vector index population states, as Neo4j 5.26 reports and enforces them.
const (
	VectorIndexPopulating = "POPULATING"
	VectorIndexOnline     = "ONLINE"
	VectorIndexFailed     = "FAILED"
)

// ErrVectorIndexFailed is a query of a vector index whose population failed:
// it answers nothing until it is dropped and created again.
var ErrVectorIndexFailed = errors.New("search: vector index population failed")

// ErrVectorIndexNotOnline is a query of a vector index still populating when
// its wait for the index ran out.
var ErrVectorIndexNotOnline = errors.New("search: vector index not online")

// errStoredVectorNotServed is a stored vector a population could not make its
// index serve, with no refusal to report.
var errStoredVectorNotServed = errors.New("search: the stored vector is not served")

// VectorIndexTarget is what a schema vector index reads: a vector property of
// the nodes with a label, or of the relationships of a type.
type VectorIndexTarget struct {
	Label        string // the nodes' label, or the relationships' type
	Property     string
	Relationship bool
}

// VectorIndexPopulation is the population of one schema vector index, from
// the commit that publishes the index to ONLINE or FAILED. The search service
// owns it. Populating an index means making it serve what storage holds:
// every stored node with the index's label (relationship of its type) whose
// property holds a vector the index can read is served with that vector.
//
// The service holds a record only while an index is POPULATING or FAILED. A
// population that succeeds removes its record, the committed drop of its
// index removes it (Retire), and a new population of the name replaces it.
// A name without one is the service's own build state (vectorIndexBuildState):
// an index the service found published when it started (a restart, a restore, a
// replacement) is served by that build, so it is ONLINE once the build is
// ready, POPULATING while the build has not completed, and FAILED, with that
// build's error, when the build failed. Cypher answers such an index from
// storage until the build is ready.
type VectorIndexPopulation struct {
	svc      *Service
	name     string
	target   VectorIndexTarget
	previous *VectorIndexPopulation
	done     chan struct{}
	state    string
	failure  error
}

type vectorIndexPopulations struct {
	mu      sync.Mutex
	records map[string]*VectorIndexPopulation
}

// BeginVectorIndexPopulation records that the index named name, reading
// target, is POPULATING. Its publisher calls it before the index becomes
// visible to readers (before the commit that publishes it), then Populate
// after the commit, or Abort when the commit fails.
func (s *Service) BeginVectorIndexPopulation(name string, target VectorIndexTarget) *VectorIndexPopulation {
	if s == nil {
		return nil
	}
	p := &VectorIndexPopulation{svc: s, name: name, target: target, done: make(chan struct{}), state: VectorIndexPopulating}
	p.transition(populationBegun, nil)
	return p
}

// populationEvent is a change of a population record.
type populationEvent int

const (
	populationBegun    populationEvent = iota // a publication began it, before its commit
	populationAborted                         // its publishing commit failed
	populationFinished                        // its population ended, with err as its failure
	populationRetired                         // the drop of its index committed
)

// transition is the one writer of the population records: the name's current
// record, a record's state and its link to the record it replaced, which an
// abort restores.
func (p *VectorIndexPopulation) transition(event populationEvent, err error) {
	populations := &p.svc.vectorIndexPopulations
	populations.mu.Lock()
	defer populations.mu.Unlock()
	current := populations.records[p.name]
	switch event {
	case populationBegun:
		if populations.records == nil {
			populations.records = make(map[string]*VectorIndexPopulation)
		}
		p.previous = current
		populations.records[p.name] = p
	case populationAborted:
		if current == p {
			if p.previous != nil {
				populations.records[p.name] = p.previous
			} else {
				delete(populations.records, p.name)
			}
		}
	case populationRetired:
		if current == p {
			delete(populations.records, p.name)
		} else if current != nil && current.previous == p {
			current.previous = nil
		}
	case populationFinished:
		if p.state != VectorIndexPopulating {
			return
		}
		p.previous = nil
		p.state, p.failure = VectorIndexOnline, nil
		if err != nil {
			p.state, p.failure = VectorIndexFailed, err
		} else if current == p {
			delete(populations.records, p.name)
		}
		close(p.done)
	}
}

// Populate makes the index serve what storage holds (populateVectorIndex) and
// records the outcome: ONLINE, or FAILED with what it could not serve as the
// cause. served is whether the index's queries read the service, decided by
// the queries' own source decision; an index they read from storage has
// nothing in the service to populate.
func (p *VectorIndexPopulation) Populate(ctx context.Context, served bool) error {
	if p == nil {
		return nil
	}
	var err error
	if served {
		err = p.svc.populateVectorIndex(ctx, p.target)
	}
	p.transition(populationFinished, err)
	return err
}

// Abort withdraws a population whose publishing commit failed: the name's
// previous record, if any, is current again.
func (p *VectorIndexPopulation) Abort() {
	p.end(populationAborted)
}

// VectorIndexPopulationOf is the population record of the index named name:
// nil while it is ONLINE.
func (s *Service) VectorIndexPopulationOf(name string) *VectorIndexPopulation {
	if s == nil {
		return nil
	}
	s.vectorIndexPopulations.mu.Lock()
	defer s.vectorIndexPopulations.mu.Unlock()
	return s.vectorIndexPopulations.records[name]
}

// Retire removes the record of an index whose drop has committed. A
// population of the name begun since then no longer restores it on Abort.
// A population still running is over, as an aborted one is: its waiters
// wake and find the name's current record, and its outcome is not recorded.
func (p *VectorIndexPopulation) Retire() {
	p.end(populationRetired)
}

// end applies event, which ends the population other than by its own
// outcome (Abort, Retire): its waiters wake, and its outcome is not
// recorded.
func (p *VectorIndexPopulation) end(event populationEvent) {
	if p == nil {
		return
	}
	p.transition(event, nil)
	p.transition(populationFinished, nil)
}

// VectorIndexState reports the population state of the index named name, and
// the cause of a FAILED population. An index this service holds no population
// record for takes the state of the service's own build
// (vectorIndexBuildState): a service that did not create the index serves it
// through that build, so its outcome is the index's population state.
func (s *Service) VectorIndexState(name string) (string, error) {
	if s == nil {
		return VectorIndexOnline, nil
	}
	s.vectorIndexPopulations.mu.Lock()
	p := s.vectorIndexPopulations.records[name]
	state, failure := VectorIndexOnline, error(nil)
	if p != nil {
		state, failure = p.state, p.failure
	}
	s.vectorIndexPopulations.mu.Unlock()
	if p != nil {
		return state, failure
	}
	return s.vectorIndexBuildState()
}

// vectorIndexBuildState is the state of an index this service holds no
// population record for, read from the build that serves it. The build
// publishes every published index the service did not populate itself (a
// restart, a restore, a replacement), so its outcome is that index's
// population state: ONLINE once its indexes are ready, POPULATING while the
// build has not completed, and FAILED, with the error that ended the last
// attempt, when that attempt failed. A later attempt is POPULATING again while
// it runs; a successful one is ONLINE.
func (s *Service) vectorIndexBuildState() (string, error) {
	if s.IsReady() {
		return VectorIndexOnline, nil
	}
	if s.BuildInProgress() {
		return VectorIndexPopulating, nil
	}
	if failure := s.buildFailureError(); failure != nil {
		return VectorIndexFailed, failure
	}
	return VectorIndexPopulating, nil
}

// buildFailureError is the error of the service's last completed build: nil
// when it succeeded, or when no build has completed.
func (s *Service) buildFailureError() error {
	if failure := s.buildFailure.Load(); failure != nil {
		return *failure
	}
	return nil
}

// recordBuildOutcome records the outcome of the build BuildIndexes just
// completed: the error that left the service not ready, or nil for a
// successful build. BuildIndexes is its only writer, and it holds buildMu, so
// the last completed attempt is the one reported.
func (s *Service) recordBuildOutcome(err error) {
	if err == nil {
		s.buildFailure.Store(nil)
		return
	}
	s.buildFailure.Store(&err)
}

// AwaitVectorIndexOnline waits, at most timeout, for the index named name to
// be ONLINE, as Neo4j's vector query procedures do before they read an index.
// A FAILED index is an ErrVectorIndexFailed error carrying its cause; an
// index still populating when the wait ends is ErrVectorIndexNotOnline. An
// index already ONLINE costs a lookup: the wait starts only for a population.
// An index this service holds no population record for waits on nothing: a
// failed build refuses it (vectorIndexBuildState) and an unfinished one leaves
// the read to storage.
func (s *Service) AwaitVectorIndexOnline(ctx context.Context, name string, timeout time.Duration) error {
	if s == nil {
		return nil
	}
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		s.vectorIndexPopulations.mu.Lock()
		p := s.vectorIndexPopulations.records[name]
		state, failure := VectorIndexOnline, error(nil)
		if p != nil {
			state, failure = p.state, p.failure
		}
		s.vectorIndexPopulations.mu.Unlock()
		if p == nil {
			// The service's own build is the population of an index whose
			// population this service did not record. A failed build is FAILED,
			// as a failed population is. A build that has not finished is not
			// waited for here: it has no completion this query joined, and a
			// schema-bound read answers from storage until the service is ready
			// (vectorIndexServiceAnswers).
			if buildState, buildFailure := s.vectorIndexBuildState(); buildState == VectorIndexFailed {
				return fmt.Errorf("%w: %w", ErrVectorIndexFailed, buildFailure)
			}
			return nil
		}
		switch state {
		case VectorIndexOnline:
			return nil
		case VectorIndexFailed:
			return fmt.Errorf("%w: %w", ErrVectorIndexFailed, failure)
		}
		if timer == nil {
			timer = time.NewTimer(timeout)
		}
		select {
		case <-p.done:
			// Its outcome, its retirement by the committed drop of its
			// index, or a newer population of the name begun while it ran,
			// which is waited for within the same time.
		case <-timer.C:
			return ErrVectorIndexNotOnline
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// populateVectorIndex makes the index of target serve what storage holds,
// and returns what it could not serve (serveStored). Refreshes still reserved for writes
// committed before the index was published are applied first: they also
// cover identities that have left the index's label or type, which the pass
// over storage cannot see. Then every stored node with the index's label
// (relationship of its type) is compared with what the service serves for it;
// one that differs is refreshed until the index serves its stored vector, a
// newer write's refresh is pending for it, or the refresh is refused (an
// index memory budget), which is the population's failure. A value of other
// dimensions is not one the index can read, as in Neo4j.
func (s *Service) populateVectorIndex(ctx context.Context, target VectorIndexTarget) error {
	if err := s.CatchUpPendingRefreshes(ctx); err != nil {
		return err
	}
	if !s.vectorEnabled.Load() {
		return nil
	}
	dims := s.VectorIndexDimensions()
	if dims <= 0 {
		return nil
	}
	if target.Relationship {
		return s.populateRelationshipVectors(ctx, target, dims)
	}
	return s.populateNodeVectors(ctx, target, dims)
}

func (s *Service) populateNodeVectors(ctx context.Context, target VectorIndexTarget, dims int) error {
	nodes := storedIdentities[storage.NodeID, *storage.Node]{
		kind: "node", read: func(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
			return s.readNodeImage(ctx, id, fullNodeImage)
		}, refresh: s.RefreshNode, registry: &s.nodeMutations,
		settled: func(ctx context.Context, node *storage.Node) (bool, error) {
			if !slices.Contains(node.Labels, target.Label) {
				return true, nil
			}
			// The installer's own decision whether it indexes the node: it
			// does not index a historical temporal version, so the index
			// serves nothing of one.
			indexed, err := s.shouldIndexNode(ctx, node)
			if err != nil {
				return false, err
			}
			return s.servesNodeVector(node, target, dims, indexed), nil
		},
	}
	var differing []storage.NodeID
	err := s.streamIndexNodes(ctx, target, func(node *storage.Node) error {
		if s.indexReadNeedsWholeNode(node, target, dims) {
			whole, err := s.readNodeImage(ctx, node.ID, fullNodeImage)
			if errors.Is(err, storage.ErrNotFound) {
				return nil // deleted since the stream read it
			}
			if err != nil {
				return err
			}
			node = whole
		}
		if settled, err := nodes.settled(ctx, node); err != nil || !settled {
			differing = append(differing, node.ID) // serveStored reports what it could not decide
		}
		return nil
	})
	if err != nil {
		return err
	}
	return serveStored(ctx, s, nodes, differing)
}

// streamIndexNodes visits the stored nodes with the index's label, read
// through the label index with only the properties the population reads
// decoded (storage.ProjectedLabelNodeReader): the index's property, and the
// properties of the temporal constraints, which the installer's decision
// whether it indexes a node reads (shouldIndexNode). A node's labels, named
// embeddings and embedding metadata come with it, its other properties and
// its separately stored chunk vectors do not. An engine without that reader
// streams whole nodes.
func (s *Service) streamIndexNodes(ctx context.Context, target VectorIndexTarget, visit func(*storage.Node) error) error {
	if projected, ok := s.engine.(storage.ProjectedLabelNodeReader); ok {
		properties := []string{target.Property}
		for _, constraint := range s.schemaConstraints(ctx) {
			if constraint.Type == storage.ConstraintTemporal {
				properties = append(properties, constraint.Properties...)
			}
		}
		return projected.StreamNodesByLabelProjected(ctx, target.Label, properties, queryprofile.EntityVisitor(ctx, queryprofile.KindNodeByLabelScan, target.Label, visit))
	}
	return s.engine.StreamNodesWithOptions(ctx, storage.StreamNodesOptions{Label: target.Label, WithEmbeddings: true}, queryprofile.EntityVisitor(ctx, queryprofile.KindNodeByLabelScan, target.Label, visit))
}

// indexReadNeedsWholeNode reports whether the index's read of node, as
// streamIndexNodes read it, falls to its managed chunks (it has no named
// embedding or property vector the index reads) and the stream did not load
// them: the node is then read whole.
func (s *Service) indexReadNeedsWholeNode(node *storage.Node, target VectorIndexTarget, dims int) bool {
	return len(node.ChunkEmbeddings) == 0 && node.HasChunkEmbeddings() && len(s.storedIndexVectors(node, target.Property, dims)) == 0
}

// storedIdentities is how a population reads one kind of stored identity, a
// node or a relationship: read from storage, refresh, its reservation
// registry, and settled, whether the index serves a stored identity as the
// installer serves it, or it is not the index's.
type storedIdentities[ID ~string, T any] struct {
	kind     string
	read     func(context.Context, ID) (T, error)
	refresh  func(context.Context, ID) error
	registry *mutationRegistry[ID]
	settled  func(context.Context, T) (bool, error)
}

// serveStored is the one decision of a population for each stored identity
// in ids that the index does not serve as stored: it refreshes the identity
// until it is settled (gone from storage, no longer the index's, served as
// stored, or with a newer write's refresh pending), or until the refresh is
// refused (a budget), which is final. A refresh or a read of storage that
// failed is applied again, a bounded number of times. It returns what it
// could not serve.
func serveStored[ID ~string, T any](ctx context.Context, s *Service, identities storedIdentities[ID, T], ids []ID) error {
	return eachFailure(identities.kind, ids, func(id ID) error {
		var cause error
		for range refreshAttempts {
			if cause = identities.refresh(ctx, id); cause != nil && !entryRefused(cause) {
				continue
			}
			stored, err := identities.read(ctx, id)
			if errors.Is(err, storage.ErrNotFound) {
				return nil
			}
			if err != nil {
				cause = err
				continue
			}
			settled, err := identities.settled(ctx, stored)
			if err != nil {
				cause = err
				continue
			}
			if settled || refreshPending(s, identities.registry, id) {
				return nil
			}
			if entryRefused(cause) {
				return cause
			}
		}
		if cause == nil {
			cause = errStoredVectorNotServed
		}
		return cause
	})
}

// servesNodeVector reports whether the index of target serves node, a stored
// node with its label, as the installer serves it: when it indexes the node
// (indexed), the vector a query of the index reads for the node (its named
// embedding of the property, else the property's vector, else its managed
// chunks) is the one the service holds, or neither has one; when it does not,
// the service holds none.
func (s *Service) servesNodeVector(node *storage.Node, target VectorIndexTarget, dims int, indexed bool) bool {
	var want []nodeVectorEntry
	if indexed {
		want = s.storedIndexVectors(node, target.Property, dims)
	}
	nodeID := string(node.ID)
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	labeled := slices.Contains(s.nodeLabels[nodeID], target.Label)
	held := s.indexVectorKeysLocked(nodeID, target.Property)
	if len(want) == 0 {
		return !labeled || len(held) == 0
	}
	if !labeled || len(held) != len(want) {
		return false
	}
	for i, entry := range want {
		if held[i] != entry.key || !s.storedVectorEqualsLocked(entry.key, entry.vector) {
			return false
		}
	}
	return true
}

// storedIndexVectors is the vector entries of node, as stored, that a query
// of an index on property reads: the query's own decision
// (VectorSourcePrecedence), over the node's stored entries of the index's
// dimensions.
func (s *Service) storedIndexVectors(node *storage.Node, property string, dims int) []nodeVectorEntry {
	var named, value, chunks []nodeVectorEntry
	s.forEachNodeVectorEntry(node, dims, func(item nodeVectorEntry) bool {
		switch {
		case len(item.vector) != dims:
		case item.entry.Kind == vectorEntryNamed && item.entry.Name == property:
			named = append(named, item)
		case item.entry.Kind == vectorEntryProperty && item.entry.Name == property:
			value = append(value, item)
		case item.entry.Kind == vectorEntryMain || item.entry.Kind == vectorEntryChunk:
			chunks = append(chunks, item)
		}
		return true
	})
	for _, source := range VectorSourcePrecedence(property) {
		var entries []nodeVectorEntry
		switch source {
		case VectorSourceNamed:
			entries = named
		case VectorSourceProperty:
			entries = value
		case VectorSourceChunks:
			entries = chunks
		}
		if len(entries) > 0 {
			return entries
		}
	}
	return nil
}

// indexVectorKeysLocked is the vector entries the service holds for nodeID
// that a query of an index on property reads: the query's own decision
// (VectorSourcePrecedence), over the entries the service holds. Caller holds
// s.indexMu.
func (s *Service) indexVectorKeysLocked(nodeID, property string) []string {
	for _, source := range VectorSourcePrecedence(property) {
		switch source {
		case VectorSourceNamed:
			if key, ok := s.nodeNamedVector[nodeID][property]; ok {
				return []string{key}
			}
		case VectorSourceProperty:
			if key, ok := s.nodePropVector[nodeID][property]; ok {
				return []string{key}
			}
		case VectorSourceChunks:
			if keys := s.nodeChunkVectors[nodeID]; len(keys) > 0 {
				return keys
			}
		}
	}
	return nil
}

// refreshPending reports whether a refresh of id is reserved in registry and
// not yet applied.
func refreshPending[ID ~string](s *Service, registry *mutationRegistry[ID], id ID) bool {
	s.nodeMutationMu.Lock()
	defer s.nodeMutationMu.Unlock()
	_, pending := registry.pending[id]
	return pending
}

func (s *Service) populateRelationshipVectors(ctx context.Context, target VectorIndexTarget, dims int) error {
	edges, err := s.engine.GetEdgesByType(ctx, target.Label)
	queryprofile.EntityPopulation(ctx, queryprofile.KindRelationshipByTypeScan, target.Label, edges)
	if err != nil {
		return err
	}
	relationships := storedIdentities[storage.EdgeID, *storage.Edge]{
		kind: "relationship", read: s.readEdgeImage, refresh: s.RefreshEdge, registry: &s.edgeMutations,
		settled: func(_ context.Context, edge *storage.Edge) (bool, error) {
			return edge.Type != target.Label || s.servesEdgeVector(edge, target, dims), nil
		},
	}
	var differing []storage.EdgeID
	for _, edge := range edges {
		if settled, _ := relationships.settled(ctx, edge); !settled {
			differing = append(differing, edge.ID)
		}
	}
	return serveStored(ctx, s, relationships, differing)
}

// servesEdgeVector reports whether the index of target serves edge, a stored
// relationship of its type, as storage holds it.
func (s *Service) servesEdgeVector(edge *storage.Edge, target VectorIndexTarget, dims int) bool {
	want, stored := vectorFromPropertyValue(edge.Properties[target.Property], dims)
	edgeID := string(edge.ID)
	s.mu.RLock()
	defer s.mu.RUnlock()
	held, holds := s.edgePropVector[edgeID][target.Property]
	if !stored {
		return !holds
	}
	return holds && s.edgeTypes[edgeID] == edge.Type && slices.Equal(held, want)
}
