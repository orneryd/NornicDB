package storage

import "context"

// StoredReadMode makes payload selection explicit. Metadata never carries a
// Node, Edge, property map, embedding metadata or vector buffer.
type StoredReadMode uint8

const (
	StoredMetadataOnly StoredReadMode = iota + 1
	StoredSelectedProperties
	StoredFullMaterialization
)

// StoredReadIdentity identifies one compatible physical/overlay capture.
// Frontier is reserved for the publication/retirement owner in #1676.
type StoredReadIdentity struct {
	Store       uint64
	Timestamp   uint64
	Transaction string
	Namespace   string
	Constituent string
	Frontier    uint64
}

// StoredReference binds a logical ID to the dictionary entry in this snapshot.
// Generation is the physical version of that entry, not a mutable live epoch.
// Zero means no dictionary entry exists in the captured physical snapshot;
// pending/ACK-only logical entities are distinguished by Pending/Buffered and
// their issued head, never numbered through a later live dictionary.
type StoredReference struct {
	NumericID  uint64
	Generation uint64
}

// StoredNodeFact is an owned, payload-free node before-image.
type StoredNodeFact struct {
	ID        NodeID
	Exists    bool
	Head      MVCCHead
	Reference StoredReference
	Labels    []string
	Pending   bool
	Buffered  bool
	View      StoredReadIdentity
}

// StoredEdgeFact is an owned, payload-free relationship before-image.
type StoredEdgeFact struct {
	ID                           EdgeID
	Exists                       bool
	Head                         MVCCHead
	Reference                    StoredReference
	Type                         string
	StartNode, EndNode           NodeID
	StartReference, EndReference StoredReference
	Pending                      bool
	Buffered                     bool
	View                         StoredReadIdentity
}

// StoredAdjacencyRequest asks for the complete stored incident set, including
// hidden relationships. Endpoint labels are returned in the same capture.
type StoredAdjacencyRequest struct {
	Node      NodeID
	Direction EdgeDirection
}

// StoredReadWork observes the actual selected decoder/copy work. It is optional
// and belongs to one operation; metadata must not count excluded payload work.
type StoredReadWork struct {
	Captures, EnvelopeParses, MetadataBytes, PropertyValues, PropertyBytes, VectorValues, FullMaterializations uint64
}

// StoredReadRequest describes one batched operation. Physical requests include
// persisted predecessors alongside pending/acknowledged buffered facts.
type StoredReadRequest struct {
	Mode                           StoredReadMode
	Nodes                          []NodeID
	Edges                          []EdgeID
	Adjacency                      []StoredAdjacencyRequest
	NodeProperties, EdgeProperties []string
	Physical                       bool
	Work                           *StoredReadWork
}

// StoredMetadata contains only bounded owned facts. Full and selected payload
// results are distinct types, rather than nullable body fields on metadata.
type StoredMetadata struct {
	Views         []StoredReadIdentity
	Nodes         []StoredNodeFact
	Edges         []StoredEdgeFact
	PhysicalNodes []StoredNodeFact
	PhysicalEdges []StoredEdgeFact
}

func (facts StoredMetadata) StoredFacts() StoredMetadata { return facts }

type StoredNodeProjection struct {
	Fact       StoredNodeFact
	Properties map[string]any
}
type StoredEdgeProjection struct {
	Fact       StoredEdgeFact
	Properties map[string]any
}
type StoredProjection struct {
	Facts         StoredMetadata
	Nodes         []StoredNodeProjection
	Edges         []StoredEdgeProjection
	PhysicalNodes []StoredNodeProjection
	PhysicalEdges []StoredEdgeProjection
}

func (result StoredProjection) StoredFacts() StoredMetadata { return result.Facts }

type StoredFullRead struct {
	Facts         StoredMetadata
	Nodes         []*Node
	Edges         []*Edge
	PhysicalNodes []*Node
	PhysicalEdges []*Edge
}

func (result StoredFullRead) StoredFacts() StoredMetadata { return result.Facts }

// StoredReadResult is one explicit mode's result. StoredMetadata has no payload
// carrier; selected/full callers pay only the contract they requested.
type StoredReadResult interface{ StoredFacts() StoredMetadata }

// StoredReadPolicy declares each real view's fact and materialization support.
// Refusal is explicit; read transparency cannot invent a stored contract.
type StoredReadPolicy struct {
	Metadata, Selected, Full          BindingDisposition
	Nodes, Edges, Adjacency, Physical BindingDisposition
}
type StoredReadDeclaration interface{ StoredReadPolicy() StoredReadPolicy }
type StoredReadProvider interface {
	StoredReadDeclaration
	ReadStored(context.Context, StoredReadRequest) (StoredReadResult, error)
}

// ReadStored executes one required stored operation. It never substitutes a
// visible read or a full body stripped after hydration.
func ReadStored(ctx context.Context, owner any, request StoredReadRequest) (StoredReadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Mode < StoredMetadataOnly || request.Mode > StoredFullMaterialization {
		return nil, ErrInvalidData
	}
	if err := MembershipForEngine(owner).ReadError(); err != nil {
		return nil, err
	}
	provider, ok := engineCapability[StoredReadProvider](owner, capabilityViewMetadata, nil)
	if !ok {
		return nil, ErrNotImplemented
	}
	policy := provider.StoredReadPolicy()
	if !policy.Admits(request) {
		return nil, ErrNotImplemented
	}
	return provider.ReadStored(ctx, request)
}

// ReadStoredMetadata requests the structurally payload-free result variant.
func ReadStoredMetadata(ctx context.Context, owner any, request StoredReadRequest) (StoredMetadata, error) {
	request.Mode = StoredMetadataOnly
	result, err := ReadStored(ctx, owner, request)
	if err != nil {
		return StoredMetadata{}, err
	}
	facts, ok := result.(StoredMetadata)
	if !ok {
		return StoredMetadata{}, ErrInvalidData
	}
	return facts, nil
}

// Admits checks the complete compound request before reading any child.
func (p StoredReadPolicy) Admits(r StoredReadRequest) bool {
	mode := p.Metadata
	if r.Mode == StoredSelectedProperties {
		mode = p.Selected
	}
	if r.Mode == StoredFullMaterialization {
		mode = p.Full
	}
	return mode == BindingOwned && (len(r.Nodes) == 0 || p.Nodes == BindingOwned) && (len(r.Edges) == 0 || p.Edges == BindingOwned) && (len(r.Adjacency) == 0 || p.Adjacency == BindingOwned) && (!r.Physical || p.Physical == BindingOwned)
}
func NativeStoredReadPolicy() StoredReadPolicy {
	return StoredReadPolicy{BindingOwned, BindingOwned, BindingOwned, BindingOwned, BindingOwned, BindingOwned, BindingOwned}
}
func RefusedStoredReadPolicy() StoredReadPolicy {
	return StoredReadPolicy{BindingRefused, BindingRefused, BindingRefused, BindingRefused, BindingRefused, BindingRefused, BindingRefused}
}

// StoredNodePresent is the stored-presence leaf consumed by integrity callers.
// A missing stronger contract is an error, never a visible/full-body fallback.
func StoredNodePresent(ctx context.Context, owner any, id NodeID) (bool, error) {
	facts, err := ReadStoredMetadata(ctx, owner, StoredReadRequest{Nodes: []NodeID{id}})
	if err != nil {
		return false, err
	}
	for _, fact := range facts.Nodes {
		if fact.Exists {
			return true, nil
		}
	}
	return false, nil
}

// StoredReadPolicyForEngine uses the same structural view cursor as dispatch.
// Transparent reads share their underlying capture; boundaries own or refuse.
func StoredReadPolicyForEngine(owner any) StoredReadPolicy {
	p, ok := engineCapability[StoredReadProvider](owner, capabilityViewMetadata, nil)
	if !ok {
		return RefusedStoredReadPolicy()
	}
	return p.StoredReadPolicy()
}

// visitStoredReadRequestIDs lets each existing view owner admit the complete
// compound request before a fact or payload is read. It does not bind or open
// another view, and edges are admitted by their own physical entity ID.
func visitStoredReadRequestIDs(request StoredReadRequest, admit func(string) error) error {
	for _, id := range request.Nodes {
		if err := admit(string(id)); err != nil {
			return err
		}
	}
	for _, id := range request.Edges {
		if err := admit(string(id)); err != nil {
			return err
		}
	}
	for _, adjacency := range request.Adjacency {
		if err := admit(string(adjacency.Node)); err != nil {
			return err
		}
	}
	return nil
}
