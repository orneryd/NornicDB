package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"time"
)

// vectorRetention names the vector images of one node update that the update
// kept unchanged from the node's predecessor (NOR-412). Under WALRecordsLarge
// the semantic journal record omits those images, marks them retained and
// carries the identity of each (vectorImageID). Replay restores an omitted
// image from the stored predecessor only when the predecessor's image has that
// identity. An update that leaves a node's vectors alone then journals no
// vectors, however many the node holds.
type vectorRetention uint8

const (
	// retainedManagedEmbeddings: ChunkEmbeddings and EmbedMeta, the vectors
	// the embedding owner manages (kept by reconcileManagedEmbeddingsLocked).
	retainedManagedEmbeddings vectorRetention = 1 << iota
	// retainedNamedEmbeddings: NamedEmbeddings, carried unchanged in the
	// update's image.
	retainedNamedEmbeddings

	allVectorRetention = retainedManagedEmbeddings | retainedNamedEmbeddings
)

// ErrKeptVectorsNotStored reports a journal record whose update kept named
// vectors that replay cannot take from the store: the stored node's named
// vectors are not the ones the update kept, or the record does not say which
// ones it kept. Named vectors are supplied by clients and cannot be produced
// again, so replay rejects the record instead of installing other vectors.
var ErrKeptVectorsNotStored = errors.New("the named vectors this update kept are not the stored node's")

// vectorImageID identifies one vector image by its content: a SHA-256 over the
// image's canonical encoding (vectorImageEncoding). Two images have the same
// identity exactly when they are the same image bit for bit (up to a SHA-256
// collision). This is the one definition of "kept" (NOR-412): the commit marks
// an image retained when the committed image has the predecessor's identity,
// and replay restores it when the predecessor it finds has the identity the
// record carries. It reads no clock and does not depend on which path wrote
// the vectors.
type vectorImageID [sha256.Size]byte

// retainedVectorIDs identifies the images one marked node update left out of
// its record. A field is meaningful only for a group the update's mark names.
type retainedVectorIDs struct {
	managed vectorImageID
	named   vectorImageID
}

// managedVectorImageID identifies node's managed vector image: every chunk
// vector by its float bits, with the chunk boundaries, and the embedding
// metadata that is kept and restored with them. It reports false when the
// metadata holds a value with no canonical form; such an image is never
// marked retained, so its record carries it whole.
func managedVectorImageID(node *Node) (vectorImageID, bool) {
	var block [vectorImageBlock]byte
	e := beginVectorImage(&block, "nornicdb kept managed vectors 1")
	e = e.count(len(node.ChunkEmbeddings))
	for _, chunk := range node.ChunkEmbeddings {
		e = e.vector(chunk)
	}
	ok := true
	if len(node.EmbedMeta) == 0 {
		// Empty metadata reads as no embedding state (freshEmbeddingSidecarMeta).
		e = e.tag(canonicalNil)
	} else {
		e, ok = e.entries(node.EmbedMeta)
	}
	return e.identity(), ok
}

// namedVectorImageID identifies a node's named vector image: every vector by
// its float bits under its name, in name order.
func namedVectorImageID(named map[string][]float32) vectorImageID {
	var block [vectorImageBlock]byte
	e := beginVectorImage(&block, "nornicdb kept named vectors 1")
	e = e.count(len(named))
	var few [16]string
	names := few[:0]
	for name := range named {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		e = e.text(name).vector(named[name])
	}
	return e.identity()
}

// vectorImageEncoding is the canonical encoding of a vector image while it is
// being hashed. Lengths precede every variable-length part, so distinct images
// never share an encoding. The encoding is hashed as a chain of blocks: the
// slice holds the SHA-256 of all earlier blocks followed by the bytes written
// since, in one small fixed array of its caller, and a full block is replaced
// by its SHA-256 (room). The identity is the SHA-256 of the last block.
// Identifying an image therefore allocates at most that one block, whatever
// the image's size. Every method returns the encoding to continue with.
type vectorImageEncoding []byte

const vectorImageBlock = 256

const (
	canonicalNil byte = iota + 1
	canonicalFalse
	canonicalTrue
	canonicalInteger  // int64 two's complement: every number with an integral value in range
	canonicalUnsigned // uint64 above the int64 range
	canonicalFloat    // float64 bits: every other number
	canonicalNaN32    // a NaN keeps its width and bits (sameStoredValue)
	canonicalNaN64
	canonicalText
	canonicalInstant
	canonicalList
	canonicalMap
)

// beginVectorImage starts the encoding of an image of one kind in block; kind
// keeps the kinds' identities apart. The first block starts with the identity
// of nothing, 32 zero bytes.
func beginVectorImage(block *[vectorImageBlock]byte, kind string) vectorImageEncoding {
	return vectorImageEncoding(block[:sha256.Size]).text(kind)
}

func (e vectorImageEncoding) identity() vectorImageID {
	return sha256.Sum256(e)
}

// room returns the encoding with space for n more bytes in its block, folding
// the block when it has none.
func (e vectorImageEncoding) room(n int) vectorImageEncoding {
	if cap(e)-len(e) >= n {
		return e
	}
	sum := sha256.Sum256(e)
	return append(e[:0], sum[:]...)
}

func (e vectorImageEncoding) tag(tag byte) vectorImageEncoding {
	return append(e.room(1), tag)
}

func (e vectorImageEncoding) word(tag byte, word uint64) vectorImageEncoding {
	return binary.LittleEndian.AppendUint64(append(e.room(9), tag), word)
}

func (e vectorImageEncoding) count(n int) vectorImageEncoding {
	return binary.LittleEndian.AppendUint64(e.room(8), uint64(n))
}

func (e vectorImageEncoding) text(s string) vectorImageEncoding {
	e = e.count(len(s))
	for len(s) > 0 {
		e = e.room(1)
		n := copy(e[len(e):cap(e)], s)
		e, s = e[:len(e)+n], s[n:]
	}
	return e
}

// vector writes one vector by the bits of its elements, so positive and
// negative zero and every NaN payload are distinct.
func (e vectorImageEncoding) vector(vector []float32) vectorImageEncoding {
	e = e.count(len(vector))
	for _, element := range vector {
		e = binary.LittleEndian.AppendUint32(e.room(4), math.Float32bits(element))
	}
	return e
}

// value writes one embedding-metadata value as a stored value, the way
// sameStoredValue compares them: a number by its value whatever Go type holds
// it, a list or map element by element, a time as an instant. The Go types of
// metadata differ between a value held in memory, one read back from the
// store, one decoded from a journal record and one restored from a snapshot;
// its identity must not. It reports false for a value with no such form. The
// types metadata ordinarily holds are written directly; every other type goes
// through reflected, which writes the same encoding.
func (e vectorImageEncoding) value(value any) (vectorImageEncoding, bool) {
	switch v := value.(type) {
	case nil:
		return e.tag(canonicalNil), true
	case bool:
		return e.truth(v), true
	case string:
		return e.tag(canonicalText).text(v), true
	case int:
		return e.word(canonicalInteger, uint64(v)), true
	case int64:
		return e.word(canonicalInteger, uint64(v)), true
	case float64:
		return e.float(v, false), true
	case []any:
		e = e.tag(canonicalList).count(len(v))
		for _, element := range v {
			var ok bool
			if e, ok = e.value(element); !ok {
				return e, false
			}
		}
		return e, true
	case map[string]any:
		return e.entries(v)
	default:
		return e.reflected(reflect.ValueOf(value))
	}
}

// entries writes a map in the order of its keys.
func (e vectorImageEncoding) entries(entries map[string]any) (vectorImageEncoding, bool) {
	e = e.tag(canonicalMap).count(len(entries))
	var few [16]string
	names := few[:0]
	for name := range entries {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		var ok bool
		if e, ok = e.text(name).value(entries[name]); !ok {
			return e, false
		}
	}
	return e, true
}

func (e vectorImageEncoding) truth(v bool) vectorImageEncoding {
	if v {
		return e.tag(canonicalTrue)
	}
	return e.tag(canonicalFalse)
}

// float writes a floating-point number by its value: as the integer it equals
// when it is one, so 2.0 read back from JSON is the 2 that was written.
func (e vectorImageEncoding) float(f float64, single bool) vectorImageEncoding {
	switch {
	case math.IsNaN(f) && single:
		return e.word(canonicalNaN32, uint64(math.Float32bits(float32(f))))
	case math.IsNaN(f):
		return e.word(canonicalNaN64, math.Float64bits(f))
	case f == math.Trunc(f) && f >= -(1<<63) && f < 1<<63:
		return e.word(canonicalInteger, uint64(int64(f)))
	default:
		return e.word(canonicalFloat, math.Float64bits(f))
	}
}

func (e vectorImageEncoding) reflected(v reflect.Value) (vectorImageEncoding, bool) {
	for v.IsValid() && (v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer) {
		if v.IsNil() {
			return e.tag(canonicalNil), true
		}
		v = v.Elem()
	}
	if !v.IsValid() {
		return e.tag(canonicalNil), true
	}
	ok := true
	switch v.Kind() {
	case reflect.Bool:
		e = e.truth(v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		e = e.word(canonicalInteger, uint64(v.Int()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if u := v.Uint(); u <= math.MaxInt64 {
			e = e.word(canonicalInteger, u)
		} else {
			e = e.word(canonicalUnsigned, u)
		}
	case reflect.Float32, reflect.Float64:
		e = e.float(v.Float(), v.Kind() == reflect.Float32)
	case reflect.String:
		e = e.tag(canonicalText).text(v.String())
	case reflect.Slice, reflect.Array:
		e = e.tag(canonicalList).count(v.Len())
		for i := 0; ok && i < v.Len(); i++ {
			e, ok = e.reflected(v.Index(i))
		}
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return e, false
		}
		e = e.tag(canonicalMap).count(v.Len())
		keys := v.MapKeys()
		slices.SortFunc(keys, func(a, b reflect.Value) int {
			switch {
			case a.String() < b.String():
				return -1
			case a.String() > b.String():
				return 1
			}
			return 0
		})
		for i := 0; ok && i < len(keys); i++ {
			e, ok = e.text(keys[i].String()).reflected(v.MapIndex(keys[i]))
		}
	case reflect.Struct:
		at, isTime := v.Interface().(time.Time)
		if !isTime {
			return e, false
		}
		e = e.word(canonicalInstant, uint64(at.Unix())).count(at.Nanosecond())
	default:
		return e, false
	}
	return e, ok
}

// nodeVectorRetention is the one place a new journal record gets its
// retention mark. It reports which non-empty vector images of node are the
// predecessor's own, by the one definition of "kept": the image has the
// predecessor's identity. managedKept is the embedding owner's decision for
// the managed set, which keepManagedEmbeddingsLocked then put on node, so that
// image is the predecessor's by assignment; named vectors are compared because
// callers carry them in the image they write. An engine that writes standard
// records (the default) marks nothing and compares nothing, so its records
// carry every image whole. Replaying a record that carries marks does not
// depend on this policy (restoreRetainedVectorsLocked).
func (b *BadgerEngine) nodeVectorRetention(oldNode, node *Node, managedKept bool) vectorRetention {
	if b.walRecordPolicy != WALRecordsLarge {
		return 0
	}
	var retained vectorRetention
	if managedKept && (len(oldNode.ChunkEmbeddings) > 0 || len(oldNode.EmbedMeta) > 0) && identifiableEmbeddingMetadata(node.EmbedMeta) {
		retained |= retainedManagedEmbeddings
	}
	if len(oldNode.NamedEmbeddings) > 0 && namedVectorImageID(node.NamedEmbeddings) == namedVectorImageID(oldNode.NamedEmbeddings) {
		retained |= retainedNamedEmbeddings
	}
	return retained
}

// identifiableEmbeddingMetadata reports whether meta has a canonical form, so
// that a managed image holding it can be identified (managedVectorImageID).
func identifiableEmbeddingMetadata(meta map[string]any) bool {
	if len(meta) == 0 {
		return true
	}
	var block [vectorImageBlock]byte
	_, ok := beginVectorImage(&block, "").entries(meta)
	return ok
}

// keepManagedEmbeddingsLocked puts oldNode's managed embeddings on node and
// returns the body to store. When they live in the embedding sidecar the body
// is stored without them, the sidecar stays their source, and its freshness
// stamp follows the body's new version in the same transaction. meta and
// sidecar come from retainedSidecarMetadataLocked(oldNode).
func (tx *BadgerTransaction) keepManagedEmbeddingsLocked(oldNode, node *Node, meta map[string]any, sidecar bool) (*Node, error) {
	node.ChunkEmbeddings = oldNode.ChunkEmbeddings
	node.EmbedMeta = oldNode.EmbedMeta
	if !sidecar {
		return node, nil
	}
	// An unrelated user mutation advances the body version without
	// changing the embedded source. Publish the retained sidecar's new
	// freshness stamp atomically with that body, including repeated writes.
	if !oldNode.UpdatedAt.Equal(node.UpdatedAt) {
		meta[embeddingContentUpdatedAtKey] = node.UpdatedAt.UTC().Format(time.RFC3339Nano)
		data, err := encodeValue(meta)
		if err != nil {
			return nil, err
		}
		tx.bufferSet(embeddingMetaKey(node.ID), data)
	}
	body := *node
	body.ChunkEmbeddings = nil
	body.EmbedMeta = nil
	return &body, nil
}

// restoreRetainedVectorsLocked applies a journal image whose update kept
// vectors. It decides nothing about retention: the commit did, and the record
// says which images it kept (ids; nil for a record written before records
// identified them, whose claim cannot be checked). An image the record omitted
// is taken from the stored predecessor only when the predecessor's image is
// that one, so replay never presents other vectors as the kept ones:
//
//   - named vectors that are not the kept ones fail the update
//     (ErrKeptVectorsNotStored), which the replay reports for its record;
//   - managed vectors that are not the kept ones are not restored: node stays
//     without managed vectors and metadata, and the caller replaces the stored
//     range with that, the state of a node that was never embedded.
//
// Only a record omits an image: the decoder accepts a mark only on an image
// without the marked vectors. An image that still holds them (a prepared
// commit staged again in the same process) is whole, so there is nothing to
// restore and it is stored as it is.
//
// It reports whether the managed set was kept.
func (tx *BadgerTransaction) restoreRetainedVectorsLocked(oldNode, node *Node, retained vectorRetention, ids *retainedVectorIDs) (*Node, bool, error) {
	if retained&retainedNamedEmbeddings != 0 && len(node.NamedEmbeddings) == 0 {
		if ids == nil || namedVectorImageID(oldNode.NamedEmbeddings) != ids.named {
			return nil, false, fmt.Errorf("node %s: %w", node.ID, ErrKeptVectorsNotStored)
		}
		node.NamedEmbeddings = oldNode.NamedEmbeddings
	}
	if retained&retainedManagedEmbeddings == 0 || len(node.ChunkEmbeddings) > 0 || len(node.EmbedMeta) > 0 {
		return node, false, nil
	}
	stored := ids != nil
	if stored {
		id, ok := managedVectorImageID(oldNode)
		stored = ok && id == ids.managed
	}
	if !stored {
		node.ChunkEmbeddings, node.EmbedMeta = nil, nil
		return node, false, nil
	}
	meta, sidecar, err := tx.retainedSidecarMetadataLocked(oldNode)
	if err != nil {
		return nil, false, err
	}
	body, err := tx.keepManagedEmbeddingsLocked(oldNode, node, meta, sidecar)
	if err != nil {
		return nil, false, err
	}
	return body, true, nil
}
