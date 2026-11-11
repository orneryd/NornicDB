package storage

import (
	"bytes"
	"fmt"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// EntityTransactionVersion identifies the newest semantic transaction format.
// Version 3 stores MessagePack timestamps, preserving unset and Unix epoch as
// distinct values without the range limit of version 2's Unix nanoseconds.
// Author: NOR-303 lane (Codex), 2026-10-29; codec change allocated by the user.
//
// Version 6 is version 3 plus node updates that omit the vectors they kept
// unchanged (NOR-412): the update names the omitted images in RetainedVectors
// and identifies each by its content, and replay restores an image from the
// stored predecessor only when the predecessor's has that identity. A writer
// stamps the lowest version that carries its record's meaning: a record that
// omits nothing stays byte-identical to version 3, and a record that omits
// kept vectors is version 6, which an older reader refuses instead of
// installing an image without vectors.
//
// Versions 4 and 5 were written only by builds of this change before it was
// merged. Version 4 named the omitted images without identifying them;
// version 5 identified them by an earlier definition of the identity, taken
// from the update's image after its body was prepared rather than from its
// predecessor. Both are still read and no longer written: replay cannot check
// what such an update kept, so it restores nothing for it
// (restoreRetainedVectorsLocked).
const EntityTransactionVersion uint16 = 6

const (
	semanticTransactionUnixNanoVersion      uint16 = 2
	semanticTransactionTimeVersion          uint16 = 3
	semanticTransactionUnidentifiedVersion  uint16 = 4
	semanticTransactionEarlyIdentityVersion uint16 = 5
)

// A pointer encoder avoids reflection boxing the two time.Time values in each
// entity. EncodeTime keeps exactly the same v3 timestamp bytes and range.
// Author: NOR-303 lane (Codex), 2026-10-29; measured structural cost correction.
type semanticTransactionTimestamp time.Time

func (at *semanticTransactionTimestamp) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.EncodeTime(time.Time(*at))
}

func (at *semanticTransactionTimestamp) DecodeMsgpack(dec *msgpack.Decoder) error {
	value, err := dec.DecodeTime()
	if err == nil {
		*at = semanticTransactionTimestamp(value)
	}
	return err
}

// semanticPropertyValues keeps the stored property type and temporal rules
// when a semantic transaction writes a map directly into its MessagePack body.
type semanticPropertyValues struct{ values map[string]any }

func (p semanticPropertyValues) EncodeMsgpack(enc *msgpack.Encoder) error {
	// Nil and empty maps already normalize to an empty MessagePack map. Let
	// the standard encoder write it without materializing a transient Go map.
	if len(p.values) == 0 {
		return enc.EncodeMapLen(0)
	}
	isolated := false
	for _, value := range p.values {
		needsIsolation, err := inspectPropertyEncoding(value, 0, maxTransportPropertyDepth)
		if err != nil {
			return err
		}
		isolated = isolated || needsIsolation
	}
	values := encodeStoredTemporalValue(p.values)
	if !isolated {
		return enc.Encode(values)
	}
	return encodeIsolatedPropertyValue(enc, values, -1, maxTransportPropertyDepth)
}

func (p *semanticPropertyValues) DecodeMsgpack(dec *msgpack.Decoder) error {
	value, err := decodeStrictTypedValue(dec)
	if err != nil {
		return err
	}
	if value == nil {
		p.values = nil
		return nil
	}
	properties, ok := restoreStoredTemporalValue(value).(map[string]any)
	if !ok {
		return fmt.Errorf("semantic properties must be a map")
	}
	p.values = properties
	return nil
}

// Metadata has the same encoding as properties but retains the body metadata
// decoder's scalar and array representations.
type semanticMetadataValues struct{ semanticPropertyValues }

// EncodeMsgpack preserves absent body metadata. Property maps retain their
// existing nil-to-empty contract; non-nil metadata uses those same value rules.
// Author: NOR-292 lane (Codex), 2026-10-29; metadata-only allocation by Nornic2.
func (p semanticMetadataValues) EncodeMsgpack(enc *msgpack.Encoder) error {
	if p.values == nil {
		return enc.EncodeNil()
	}
	return p.semanticPropertyValues.EncodeMsgpack(enc)
}

func (p *semanticMetadataValues) DecodeMsgpack(dec *msgpack.Decoder) error {
	value, err := dec.DecodeInterface()
	if err != nil {
		return err
	}
	if value == nil {
		p.values = nil
		return nil
	}
	metadata, ok := restoreStoredTemporalValue(value).(map[string]any)
	if !ok {
		return fmt.Errorf("semantic metadata must be a map")
	}
	p.values = metadata
	return nil
}

// The historical timestamp field names remain stable. Their value type is
// selected explicitly by the envelope version: int64 for v2, time.Time for v3.
type semanticTransactionNode[T int64 | semanticTransactionTimestamp] struct {
	ID                   string
	Labels               []string
	Properties           semanticPropertyValues
	NamedEmbeddings      map[string][]float32
	ChunkEmbeddings      [][]float32
	CreatedAtUnixNs      T
	UpdatedAtUnixNs      T
	EmbedMeta            semanticMetadataValues
	VisibilitySuppressed bool
}

type semanticTransactionEdge[T int64 | semanticTransactionTimestamp] struct {
	ID, StartNode, EndNode, Type string
	Properties                   semanticPropertyValues
	CreatedAtUnixNs              T
	UpdatedAtUnixNs              T
	Confidence                   float64
	AutoGenerated                bool
	VisibilitySuppressed         bool
}

type semanticTransactionChange[T int64 | semanticTransactionTimestamp] struct {
	Type   EntityCommandKind
	Node   *semanticTransactionNode[T]
	Edge   *semanticTransactionEdge[T]
	NodeID string
	EdgeID string
}

type semanticTransactionWire[T int64 | semanticTransactionTimestamp] struct {
	Mode        EntityCommitMode
	Version     uint16
	Namespace   string
	LogicalTxID string
	Changes     []semanticTransactionChange[T]
}

// The version-6 wire shape is the version-3 shape plus, on a node, the vector
// images an update kept from its predecessor and therefore omits (NOR-412):
// RetainedVectors names them, and RetainedManaged and RetainedNamed hold the
// identity of each (vectorImageID). The fields are absent when empty. Only a
// record that omits vectors is encoded from these types, so a record that
// omits nothing is encoded from the version-3 types above, at their cost and
// with their exact bytes. Every version decodes into these types: an older
// record simply has none of the fields, a version-4 record has only
// RetainedVectors, and a version-5 record has the version-6 fields.
type semanticTransactionMarkedNode[T int64 | semanticTransactionTimestamp] struct {
	ID                   string
	Labels               []string
	Properties           semanticPropertyValues
	NamedEmbeddings      map[string][]float32
	ChunkEmbeddings      [][]float32
	CreatedAtUnixNs      T
	UpdatedAtUnixNs      T
	EmbedMeta            semanticMetadataValues
	VisibilitySuppressed bool
	RetainedVectors      vectorRetention `msgpack:",omitempty"`
	RetainedManaged      []byte          `msgpack:",omitempty"`
	RetainedNamed        []byte          `msgpack:",omitempty"`
}

type semanticTransactionMarkedChange[T int64 | semanticTransactionTimestamp] struct {
	Type   EntityCommandKind
	Node   *semanticTransactionMarkedNode[T]
	Edge   *semanticTransactionEdge[T]
	NodeID string
	EdgeID string
}

type semanticTransactionMarkedWire[T int64 | semanticTransactionTimestamp] struct {
	Mode        EntityCommitMode
	Version     uint16
	Namespace   string
	LogicalTxID string
	Changes     []semanticTransactionMarkedChange[T]
}

// EntityTransactionRequest selects raw decoded operations or one immutable
// prepared transaction. Prepared must be used alone; combining both sources
// is rejected instead of silently choosing metadata or images from one of them.
type EntityTransactionRequest struct {
	Namespace  string
	LogicalID  string
	Operations []Operation
	Mode       EntityCommitMode
	Prepared   *PreparedEntityCommit
	// keptVectors is the staging transaction's record of the vector images
	// its marked updates kept (BadgerTransaction.keptVectors), for raw
	// operations a native commit publishes. A prepared transaction carries
	// its own.
	keptVectors map[NodeID]keptVectorImages
}

// EncodeEntityTransaction encodes the current semantic transaction format.
// Prepared input reads the accepted private snapshot without allocating the
// detached public Operations view. Raw input retains its original validation.
func EncodeEntityTransaction(request EntityTransactionRequest) ([]byte, error) {
	encoder := getValueEncoder()
	defer putValueEncoder(encoder)
	payload, err := encoder.entityTransaction(request)
	if err != nil {
		return nil, err
	}
	return bytes.Clone(payload), nil
}

// semanticTransactionScratch belongs to the existing encoder lease. It holds
// only transient version-3 write images; release clears every graph reference
// before pool reuse and drops unusually large backing arrays rather than
// retaining them.
const maxRetainedSemanticChanges = 256

type semanticTransactionScratch struct {
	nodes   []semanticTransactionNode[semanticTransactionTimestamp]
	edges   []semanticTransactionEdge[semanticTransactionTimestamp]
	changes []semanticTransactionChange[semanticTransactionTimestamp]
	wire    semanticTransactionWire[semanticTransactionTimestamp]
}

func (s *semanticTransactionScratch) clear() {
	clear(s.nodes)
	clear(s.edges)
	clear(s.changes)
	s.nodes = s.nodes[:0]
	s.edges = s.edges[:0]
	s.changes = s.changes[:0]
	s.wire = semanticTransactionWire[semanticTransactionTimestamp]{}
}

func (s *semanticTransactionScratch) release() {
	s.clear()
	if cap(s.nodes) > maxRetainedSemanticChanges {
		s.nodes = nil
	}
	if cap(s.edges) > maxRetainedSemanticChanges {
		s.edges = nil
	}
	if cap(s.changes) > maxRetainedSemanticChanges {
		s.changes = nil
	}
}

// entityTransaction borrows its raw bytes until this encoder's next use. The
// owned public API clones; synchronous native publication retains the encoder
// through finish instead of copying an already-owned immutable image again.
func (encoder *valueEncoder) entityTransaction(request EntityTransactionRequest) ([]byte, error) {
	scratch := &encoder.semantic
	scratch.clear()

	namespace, logicalID, ops, mode, kept := request.Namespace, request.LogicalID, request.Operations, request.Mode, request.keptVectors
	if prepared := request.Prepared; prepared != nil {
		if namespace != "" || logicalID != "" || ops != nil || mode != 0 || kept != nil {
			return nil, ErrInvalidData
		}
		namespace, logicalID, ops, mode, kept = prepared.namespace, prepared.logicalID, prepared.operations, prepared.mode, prepared.keptVectors
	}
	if !mode.valid() || namespace == "" || logicalID == "" || len(ops) == 0 {
		return nil, ErrInvalidData
	}
	var nodeCount, edgeCount int
	for _, op := range ops {
		switch op.Type {
		case OpCreateNode, OpUpdateNode:
			nodeCount++
		case OpCreateEdge, OpUpdateEdge:
			edgeCount++
		case OpDeleteNode, OpDeleteEdge:
		default:
			return nil, fmt.Errorf("not an entity operation: %s", op.Type)
		}
	}
	if cap(scratch.nodes) < nodeCount {
		scratch.nodes = make([]semanticTransactionNode[semanticTransactionTimestamp], nodeCount)
	} else {
		scratch.nodes = scratch.nodes[:nodeCount]
	}
	if cap(scratch.edges) < edgeCount {
		scratch.edges = make([]semanticTransactionEdge[semanticTransactionTimestamp], edgeCount)
	} else {
		scratch.edges = scratch.edges[:edgeCount]
	}
	if cap(scratch.changes) < len(ops) {
		scratch.changes = make([]semanticTransactionChange[semanticTransactionTimestamp], len(ops))
	} else {
		scratch.changes = scratch.changes[:len(ops)]
	}
	nodes, edges, changes := scratch.nodes, scratch.edges, scratch.changes
	nextNode, nextEdge := 0, 0
	// Set when an update kept vectors: its record is then version 6.
	retainsVectors := false
	for i, op := range ops {
		switch op.Type {
		case OpCreateNode, OpUpdateNode:
			if op.Node == nil {
				return nil, ErrInvalidData
			}
			n := op.Node
			nodes[nextNode] = semanticTransactionNode[semanticTransactionTimestamp]{
				ID: string(n.ID), Labels: n.Labels, Properties: semanticPropertyValues{values: n.Properties},
				NamedEmbeddings: n.NamedEmbeddings, ChunkEmbeddings: n.ChunkEmbeddings,
				CreatedAtUnixNs: semanticTransactionTimestamp(n.CreatedAt), UpdatedAtUnixNs: semanticTransactionTimestamp(n.UpdatedAt),
				EmbedMeta: semanticMetadataValues{semanticPropertyValues{values: n.EmbedMeta}}, VisibilitySuppressed: n.RuntimeMetadata().VisibilitySuppressed,
			}
			changes[i].Node = &nodes[nextNode]
			retainsVectors = retainsVectors || op.Type == OpUpdateNode && op.retainedVectors != 0
			nextNode++
			changes[i].Type = EntityCreateNode
			if op.Type == OpUpdateNode {
				changes[i].Type = EntityUpdateNode
			}
		case OpCreateEdge, OpUpdateEdge:
			if op.Edge == nil {
				return nil, ErrInvalidData
			}
			e := op.Edge
			image := semanticTransactionEdge[semanticTransactionTimestamp]{
				ID: string(e.ID), StartNode: string(e.StartNode), EndNode: string(e.EndNode), Type: e.Type,
				Properties: semanticPropertyValues{values: e.Properties}, Confidence: e.Confidence,
				CreatedAtUnixNs: semanticTransactionTimestamp(e.CreatedAt), UpdatedAtUnixNs: semanticTransactionTimestamp(e.UpdatedAt),
				AutoGenerated: e.AutoGenerated, VisibilitySuppressed: e.VisibilitySuppressed,
			}
			edges[nextEdge] = image
			changes[i].Edge = &edges[nextEdge]
			nextEdge++
			changes[i].Type = EntityCreateEdge
			if op.Type == OpUpdateEdge {
				changes[i].Type = EntityUpdateEdge
			}
		case OpDeleteNode:
			changes[i] = semanticTransactionChange[semanticTransactionTimestamp]{Type: EntityDeleteNode, NodeID: string(op.NodeID)}
		case OpDeleteEdge:
			changes[i] = semanticTransactionChange[semanticTransactionTimestamp]{Type: EntityDeleteEdge, EdgeID: string(op.EdgeID)}
		}
	}
	scratch.wire = semanticTransactionWire[semanticTransactionTimestamp]{Mode: mode, Version: semanticTransactionTimeVersion, Namespace: namespace, LogicalTxID: logicalID, Changes: changes}
	if retainsVectors {
		return encoder.markedEntityTransaction(&scratch.wire, ops, kept)
	}
	return encoder.encode(&scratch.wire, rawMessagePackFraming)
}

// markedEntityTransaction encodes wire, the version-3 image of ops, as a
// version-6 record: each node update that kept vectors leaves those images
// out, names them in RetainedVectors and gives the identity staging recorded
// for them (kept, from journalRetentionLocked), because they are the
// predecessor's and the record carries only the change (NOR-412). The encoder
// computes no identity: an operation's image here is the one its body
// preparation published, not the predecessor replay compares with. Every
// other change is carried as it is. A marked group with no recorded identity
// is carried whole when the image holds it; when the image does not (an
// operation decoded from a record that already omitted it), it cannot be
// claimed again and is refused.
func (encoder *valueEncoder) markedEntityTransaction(wire *semanticTransactionWire[semanticTransactionTimestamp], ops []Operation, kept map[NodeID]keptVectorImages) ([]byte, error) {
	nodes := make([]semanticTransactionMarkedNode[semanticTransactionTimestamp], 0, len(ops))
	changes := make([]semanticTransactionMarkedChange[semanticTransactionTimestamp], len(ops))
	// Room for both identities of every change, so the record's byte fields
	// point into one array that never moves.
	identities := make([]byte, 0, 2*len(vectorImageID{})*len(ops))
	for i, change := range wire.Changes {
		changes[i] = semanticTransactionMarkedChange[semanticTransactionTimestamp]{Type: change.Type, Edge: change.Edge, NodeID: change.NodeID, EdgeID: change.EdgeID}
		n := change.Node
		if n == nil {
			continue
		}
		image := semanticTransactionMarkedNode[semanticTransactionTimestamp]{
			ID: n.ID, Labels: n.Labels, Properties: n.Properties,
			NamedEmbeddings: n.NamedEmbeddings, ChunkEmbeddings: n.ChunkEmbeddings,
			CreatedAtUnixNs: n.CreatedAtUnixNs, UpdatedAtUnixNs: n.UpdatedAtUnixNs,
			EmbedMeta: n.EmbedMeta, VisibilitySuppressed: n.VisibilitySuppressed,
		}
		if retained := ops[i].retainedVectors; ops[i].Type == OpUpdateNode && retained != 0 {
			if retained&^allVectorRetention != 0 {
				return nil, ErrInvalidData
			}
			node, recorded := ops[i].Node, kept[ops[i].NodeID]
			if retained&retainedManagedEmbeddings != 0 {
				switch {
				case recorded.identified&retainedManagedEmbeddings != 0:
					identities = append(identities, recorded.ids.managed[:]...)
					image.RetainedManaged = identities[len(identities)-len(recorded.ids.managed):]
					image.ChunkEmbeddings = nil
					image.EmbedMeta = semanticMetadataValues{}
				case len(node.ChunkEmbeddings) == 0 && len(node.EmbedMeta) == 0:
					return nil, ErrInvalidData
				default:
					retained &^= retainedManagedEmbeddings
				}
			}
			if retained&retainedNamedEmbeddings != 0 {
				switch {
				case recorded.identified&retainedNamedEmbeddings != 0:
					identities = append(identities, recorded.ids.named[:]...)
					image.RetainedNamed = identities[len(identities)-len(recorded.ids.named):]
					image.NamedEmbeddings = nil
				case len(node.NamedEmbeddings) == 0:
					return nil, ErrInvalidData
				default:
					retained &^= retainedNamedEmbeddings
				}
			}
			image.RetainedVectors = retained
		}
		// nodes never grows past its capacity, so the pointers stay valid.
		nodes = append(nodes, image)
		changes[i].Node = &nodes[len(nodes)-1]
	}
	marked := semanticTransactionMarkedWire[semanticTransactionTimestamp]{Mode: wire.Mode, Version: EntityTransactionVersion, Namespace: wire.Namespace, LogicalTxID: wire.LogicalTxID, Changes: changes}
	return encoder.encode(&marked, rawMessagePackFraming)
}

// DecodeEntityTransaction validates and restores one semantic transaction.
func DecodeEntityTransaction(data []byte) (EntityTransactionEnvelope, []Operation, error) {
	var envelope EntityTransactionEnvelope
	// The envelope, change list, change, entity, and property map account for
	// six traversal frames around a nested property value.
	if err := validateMessagePackValue(data, maxTransportPropertyDepth+6); err != nil {
		return envelope, nil, fmt.Errorf("invalid semantic transaction payload: %w", err)
	}
	// Inspect only the version before choosing the typed timestamp reader. All
	// framing is validated above, and both versions share materialization below.
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	count, err := dec.DecodeMapLen()
	if err != nil {
		return envelope, nil, err
	}
	for range count {
		key, err := dec.DecodeString()
		if err != nil {
			return envelope, nil, err
		}
		if key == "Version" {
			version, err := dec.DecodeUint16()
			if err != nil {
				return envelope, nil, err
			}
			switch version {
			case semanticTransactionUnixNanoVersion:
				return decodeSemanticTransaction(data, version, func(nanos int64) time.Time {
					if nanos == 0 {
						return time.Time{}
					}
					return time.Unix(0, nanos)
				})
			case semanticTransactionTimeVersion, semanticTransactionUnidentifiedVersion, semanticTransactionEarlyIdentityVersion, EntityTransactionVersion:
				return decodeSemanticTransaction(data, version, func(at semanticTransactionTimestamp) time.Time { return time.Time(at) })
			default:
				return envelope, nil, ErrInvalidData
			}
		}
		if err := dec.Skip(); err != nil {
			return envelope, nil, err
		}
	}
	return envelope, nil, ErrInvalidData
}

func decodeSemanticTransaction[T int64 | semanticTransactionTimestamp](data []byte, version uint16, timestamp func(T) time.Time) (EntityTransactionEnvelope, []Operation, error) {
	var wire semanticTransactionMarkedWire[T]
	var envelope EntityTransactionEnvelope
	reader := bytes.NewReader(data)
	if err := msgpack.NewDecoder(reader).Decode(&wire); err != nil {
		return envelope, nil, err
	}
	if reader.Len() != 0 {
		return envelope, nil, fmt.Errorf("trailing semantic transaction payload")
	}
	envelope = EntityTransactionEnvelope{Mode: wire.Mode, Version: wire.Version, Namespace: wire.Namespace, LogicalTxID: wire.LogicalTxID}
	if !wire.Mode.valid() || wire.Version != version || wire.Namespace == "" || wire.LogicalTxID == "" || len(wire.Changes) == 0 {
		return envelope, nil, ErrInvalidData
	}
	sameNamespace := func(id string) bool {
		ns, _, ok := ParseDatabasePrefix(id)
		return ok && ns == wire.Namespace
	}
	ops := make([]Operation, len(wire.Changes))
	// The identities of what each marked update kept, one entry per change,
	// allocated for the first update that carries any.
	var ids []retainedVectorIDs
	for i, change := range wire.Changes {
		switch change.Type {
		case EntityCreateNode, EntityUpdateNode:
			if change.Node == nil || !sameNamespace(change.Node.ID) {
				return envelope, nil, ErrCrossNamespaceTransaction
			}
			n := change.Node
			if !validRetainedVectors(version, change.Type, n) {
				return envelope, nil, ErrInvalidData
			}
			node := &Node{ID: NodeID(n.ID), Labels: n.Labels, Properties: n.Properties.values, NamedEmbeddings: n.NamedEmbeddings, ChunkEmbeddings: n.ChunkEmbeddings, EmbedMeta: n.EmbedMeta.values}
			node.SetRuntimeMetadata(NodeRuntimeMetadata{VisibilitySuppressed: n.VisibilitySuppressed})
			node.CreatedAt = timestamp(n.CreatedAtUnixNs)
			node.UpdatedAt = timestamp(n.UpdatedAtUnixNs)
			kind := OpCreateNode
			if change.Type == EntityUpdateNode {
				kind = OpUpdateNode
			}
			ops[i] = Operation{Type: kind, NodeID: node.ID, Node: node, retainedVectors: n.RetainedVectors}
			// A version-5 identity is of an earlier definition: replay cannot
			// check it, as it cannot check a version-4 mark.
			if version == EntityTransactionVersion && (len(n.RetainedManaged) != 0 || len(n.RetainedNamed) != 0) {
				if ids == nil {
					ids = make([]retainedVectorIDs, len(wire.Changes))
				}
				copy(ids[i].managed[:], n.RetainedManaged)
				copy(ids[i].named[:], n.RetainedNamed)
			}
		case EntityCreateEdge, EntityUpdateEdge:
			if change.Edge == nil || !sameNamespace(change.Edge.ID) || !sameNamespace(change.Edge.StartNode) || !sameNamespace(change.Edge.EndNode) {
				return envelope, nil, ErrCrossNamespaceTransaction
			}
			e := change.Edge
			edge := &Edge{ID: EdgeID(e.ID), StartNode: NodeID(e.StartNode), EndNode: NodeID(e.EndNode), Type: e.Type, Properties: e.Properties.values, Confidence: e.Confidence, AutoGenerated: e.AutoGenerated, VisibilitySuppressed: e.VisibilitySuppressed}
			edge.CreatedAt = timestamp(e.CreatedAtUnixNs)
			edge.UpdatedAt = timestamp(e.UpdatedAtUnixNs)
			kind := OpCreateEdge
			if change.Type == EntityUpdateEdge {
				kind = OpUpdateEdge
			}
			ops[i] = Operation{Type: kind, EdgeID: edge.ID, Edge: edge}
		case EntityDeleteNode:
			if !sameNamespace(change.NodeID) {
				return envelope, nil, ErrCrossNamespaceTransaction
			}
			ops[i] = Operation{Type: OpDeleteNode, NodeID: NodeID(change.NodeID)}
		case EntityDeleteEdge:
			if !sameNamespace(change.EdgeID) {
				return envelope, nil, ErrCrossNamespaceTransaction
			}
			ops[i] = Operation{Type: OpDeleteEdge, EdgeID: EdgeID(change.EdgeID)}
		default:
			return envelope, nil, ErrInvalidData
		}
	}
	envelope.retainedVectorIDs = ids
	return envelope, ops, nil
}

// validRetainedVectors accepts a retention mark only where its writer can
// produce it: a node update of version 4 or later with known bits whose
// omitted images are in fact absent. A version-5 or version-6 update
// identifies exactly the images its mark names; a version-4 update identifies
// none. Anything else is malformed, never silently reinterpreted.
func validRetainedVectors[T int64 | semanticTransactionTimestamp](version uint16, kind EntityCommandKind, n *semanticTransactionMarkedNode[T]) bool {
	retained := n.RetainedVectors
	identity := func(group vectorRetention) int {
		if version < semanticTransactionEarlyIdentityVersion || retained&group == 0 {
			return 0
		}
		return len(vectorImageID{})
	}
	if len(n.RetainedManaged) != identity(retainedManagedEmbeddings) || len(n.RetainedNamed) != identity(retainedNamedEmbeddings) {
		return false
	}
	if retained == 0 {
		return true
	}
	if version < semanticTransactionUnidentifiedVersion || kind != EntityUpdateNode || retained&^allVectorRetention != 0 {
		return false
	}
	if retained&retainedManagedEmbeddings != 0 && (len(n.ChunkEmbeddings) > 0 || n.EmbedMeta.values != nil) {
		return false
	}
	return retained&retainedNamedEmbeddings == 0 || len(n.NamedEmbeddings) == 0
}
