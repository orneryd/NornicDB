package search

import (
	"context"
	"fmt"
	"math/bits"
	"slices"
	"strings"
	"unsafe"
)

// literalSignature is a lossless exclusion filter: a missing bit proves that
// the substring is absent; collisions admit extra documents for the existing
// exact matcher. The signature never supplies a match or a score by itself.
type literalSignature [32]uint64

func literalSignatureWidth(averageBytes int) int {
	target := 2048
	if averageBytes < 8192 {
		target = max(512, averageBytes/4)
	}
	return 1 << (bits.Len(uint(target)) - 1)
}

// Tabulation hashes serve only as lossy exclusion signatures, never as identity
// or security digests. Their fixed byte tables fit in L1 and avoid repeated
// integer avalanches for every gram of every document. Query/document hashing
// shares this exact owner; collisions only admit extra exact-matcher work.
var literalByteHashes = func() [3][256]uint32 {
	var table [3][256]uint32
	state := uint32(0x9e3779b9)
	for position := range table {
		for value := range table[position] {
			state += 0x9e3779b9
			mixed := state
			mixed = (mixed ^ (mixed >> 16)) * 0x7feb352d
			mixed = (mixed ^ (mixed >> 15)) * 0x846ca68b
			table[position][value] = mixed ^ (mixed >> 16)
		}
	}
	return table
}()

// addLiteralText is the shared byte-gram owner for documents and query terms.
// Three-byte grams preserve strings.Contains semantics for arbitrary byte text.
// Shorter terms impose no exclusion; their exact predicate still runs over the
// complete population. Normalization remains the dialect's responsibility.
func addLiteralText(ctx context.Context, mask *literalSignature, text string, width int, workspace map[string]*literalSignature) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Two bytes of overlap preserve every gram crossing a chunk boundary.
	// The constructor's bounded workspace can reuse common text chunks without
	// retaining a corpus/query cache or assuming equal field representations.
	for offset := 0; offset < len(text); offset += 256 {
		// Poll after at most 4 KiB of bounded in-memory work. Checking the
		// deeply scoped request context for every cached 256-byte chunk dominated
		// construction; byte-bounded polling preserves prompt cancellation.
		if offset != 0 && offset&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		chunk := text[max(0, offset-2):min(offset+256, len(text))]
		if cached, found := workspace[chunk]; found {
			for word := 0; word < width/64; word++ {
				mask[word] |= cached[word]
			}
			continue
		}
		var signature literalSignature
		target := mask
		if workspace != nil {
			target = &signature
		}
		for i := 2; i < len(chunk); i++ {
			value := literalByteHashes[0][chunk[i-2]] ^ literalByteHashes[1][chunk[i-1]] ^ literalByteHashes[2][chunk[i]]
			first := int(value) & (width - 1)
			target[first>>6] |= 1 << uint(first&63)
		}
		if workspace != nil {
			if len(workspace) < 32 {
				stored := new(literalSignature)
				*stored = signature
				workspace[chunk] = stored
			}
			for word := 0; word < width/64; word++ {
				mask[word] |= target[word]
			}
		}
	}
	return ctx.Err()
}

// fulltextLiteralIndex transposes document signatures into posting bitsets.
// Each block covers 64 document slots; a query intersects columns before it
// visits any document text. Memory is bounded by width times peak live slots,
// independent of vocabulary or the number of distinct queries.
type fulltextLiteralIndex struct {
	width       int
	columns     []uint64
	live        []uint64
	common      literalSignature
	commonReady bool
	slots       map[string]int
	ids         []string
	free        []int
	plans       []fulltextLiteralPlan // cleared, bounded scratch borrowed under the projection lease
}

func (index *fulltextLiteralIndex) put(ctx context.Context, slot int, signature *literalSignature) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	block := slot >> 6
	required := (block + 1) * index.width
	if required > len(index.columns) {
		index.columns = slices.Grow(index.columns, required-len(index.columns))
		index.columns = index.columns[:required]
	}
	if block >= len(index.live) {
		index.live = append(index.live, make([]uint64, block+1-len(index.live))...)
	}
	if !index.commonReady {
		index.common = *signature
		index.commonReady = true
	} else {
		for word := 0; word < index.width/64; word++ {
			index.common[word] &= signature[word]
		}
	}
	row := uint64(1) << uint(slot&63)
	// New and recycled slots already have zero columns. Clearing the complete
	// column width again would turn each new document into a dense-width write.
	if index.live[block]&row != 0 {
		index.clear(slot)
	}
	index.live[block] |= row
	for word := 0; word < index.width/64; word++ {
		remaining := signature[word]
		for remaining != 0 {
			bit := bits.TrailingZeros64(remaining)
			index.columns[block*index.width+word*64+bit] |= row
			remaining &= remaining - 1
		}
	}
	return nil
}

func (index *fulltextLiteralIndex) clear(slot int) {
	block := slot >> 6
	if block >= len(index.live) {
		return
	}
	row := ^(uint64(1) << uint(slot&63))
	index.live[block] &= row
	for bit := 0; bit < index.width; bit++ {
		index.columns[block*index.width+bit] &= row
	}
}

func (index *fulltextLiteralIndex) candidates(block int, signature *literalSignature) uint64 {
	result := index.live[block]
	for word := 0; word < index.width/64 && result != 0; word++ {
		remaining := signature[word]
		for remaining != 0 && result != 0 {
			bit := bits.TrailingZeros64(remaining)
			result &= index.columns[block*index.width+word*64+bit]
			remaining &= remaining - 1
		}
	}
	return result
}

// mayRestrict uses proven common bits first. Deletion can make additional bits
// common, so a missing proof is checked against the live posting columns and
// cached only after every block proves it. Churn therefore cannot permanently
// force broad queries through an unnecessary per-slot candidate traversal.
func (index *fulltextLiteralIndex) mayRestrict(ctx context.Context, signature *literalSignature) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	for word := 0; word < index.width/64; word++ {
		missing := signature[word] &^ index.common[word]
		for missing != 0 {
			bit := bits.TrailingZeros64(missing)
			for block, live := range index.live {
				if err := ctx.Err(); err != nil {
					return false, err
				}
				if index.columns[block*index.width+word*64+bit]&live != live {
					return true, nil
				}
			}
			index.common[word] |= uint64(1) << uint(bit)
			missing &= missing - 1
		}
	}
	return false, nil
}

func (index *fulltextLiteralIndex) retainedBytes() int64 {
	return int64(cap(index.columns)+cap(index.live)+cap(index.free))*8 + int64(cap(index.ids))*16 + int64(cap(index.ids))*64 + int64(unsafe.Sizeof(*index)) + int64(cap(index.plans))*int64(unsafe.Sizeof(fulltextLiteralPlan{}))
}

// literalBuildWorkspace bounds exact signature reuse to one constructor call;
// neither map becomes part of the retained projection or a query cache.
type literalBuildWorkspace struct {
	bodies map[string]literalSignature
	chunks map[string]*literalSignature
}

// replace builds the new signature before changing any visible slot. The view's
// projection lease owns the index, its slots and its immutable documents.
func (index *fulltextLiteralIndex) replace(ctx context.Context, id string, document *FulltextDocument, workspace *literalBuildWorkspace) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	slot, exists := index.slots[id]
	if document == nil {
		if exists {
			index.clear(slot)
			index.ids[slot] = ""
			delete(index.slots, id)
			index.free = append(index.free, slot)
		}
		return nil
	}
	var signature literalSignature
	var chunks map[string]*literalSignature
	cached := false
	if workspace != nil {
		if len(document.Text) <= 4096 {
			signature, cached = workspace.bodies[document.Text]
		}
		chunks = workspace.chunks
	}
	if !cached {
		if err := addLiteralText(ctx, &signature, document.Text, index.width, chunks); err != nil {
			return err
		}
		if workspace != nil && len(document.Text) <= 4096 && len(workspace.bodies) < 32 {
			workspace.bodies[document.Text] = signature
		}
	}

	for _, field := range document.MatchFields {
		// Most fields already occur verbatim in the body. Preserve the field's
		// untrimmed boundary bytes when the scoring body does not contain them.
		// Normalization already shares field slices with the body. Prefix/suffix
		// pointer identity proves coverage without rereading a potentially large
		// field. Contains is only a bounded small-body optimization; other fields
		// retain the cancellable chunk compiler.
		covered := len(field) == 0
		if !covered && len(field) <= len(document.Text) {
			pointer := unsafe.StringData(field)
			covered = pointer == unsafe.StringData(document.Text) || pointer == unsafe.StringData(document.Text[len(document.Text)-len(field):])
		}
		if covered || len(document.Text) <= 4096 && strings.Contains(document.Text, field) {
			continue
		}
		if err := addLiteralText(ctx, &signature, field, index.width, chunks); err != nil {
			return err
		}
	}
	if !exists {
		slot = len(index.ids)
		if len(index.free) > 0 {
			slot = index.free[len(index.free)-1]
		}
	}
	if err := index.put(ctx, slot, &signature); err != nil {
		return err
	}
	if !exists {
		if slot == len(index.ids) {
			index.ids = append(index.ids, id)
		} else {
			index.free = index.free[:len(index.free)-1]
			index.ids[slot] = id
		}
		index.slots[id] = slot
	}
	return nil
}

func (v *fulltextDocumentView) literalIndex(ctx context.Context) (*fulltextLiteralIndex, error) {
	if v.temporary() || len(v.documents) < 128 {
		return nil, nil
	}
	if v.literals != nil {
		return v.literals, nil
	}
	width := literalSignatureWidth(int(v.bodyBytes / int64(len(v.documents))))
	blocks := (len(v.documents) + 63) / 64
	index := &fulltextLiteralIndex{width: width, columns: make([]uint64, blocks*width), live: make([]uint64, blocks), slots: make(map[string]int, len(v.documents)), ids: make([]string, 0, len(v.documents))}
	workspace := &literalBuildWorkspace{bodies: make(map[string]literalSignature), chunks: make(map[string]*literalSignature)}
	for id, document := range v.documents {
		if err := index.replace(ctx, id, document, workspace); err != nil {
			return nil, err
		}
	}
	v.literals = index
	return index, nil
}

// FulltextLiteralOperation describes conservative positive candidate selection.
// Exact field predicates, exclusions and scoring remain in the query dialect.
type FulltextLiteralOperation uint8

const (
	FulltextAllCandidates FulltextLiteralOperation = iota
	FulltextLiteral
	FulltextAllOf
	FulltextAnyOf
)

// FulltextLiteralSelector is a conservative candidate expression. A literal
// refers to byte-substring presence in Text or MatchFields. AllOf intersects
// child populations; AnyOf unions them. Never complement a lossy candidate set
// for a prohibited clause. The zero value admits the complete corpus.
// For example, a mandatory exact term can use {Op: FulltextLiteral, Text: "otel"}.
type FulltextLiteralSelector struct {
	Op       FulltextLiteralOperation
	Text     string
	Children []FulltextLiteralSelector
}

// FulltextLiteralNode describes one source node without copying its children.
// Children is the source's indexed child range, including children Child skips.
type FulltextLiteralNode struct {
	Op       FulltextLiteralOperation
	Text     string
	Children int
}

// FulltextLiteralReader exposes pure syntax metadata to the candidate compiler.
// Describe and Child must not read storage or retain request state. The compiler
// owns cancellation, compiled storage and execution. A dialect can supply its
// existing immutable syntax nodes directly, avoiding another allocated tree.
type FulltextLiteralReader[T any] interface {
	Describe(context.Context, T) (FulltextLiteralNode, error)
	Child(context.Context, T, FulltextLiteralOperation, int) (T, bool, error)
}

// FulltextLiteralTree reads an explicitly constructed selector tree. For example,
// ReadFulltextCandidates(ctx, documents, selector, FulltextLiteralTree{}, visit).
// Dialects with their own syntax trees implement FulltextLiteralReader directly.
type FulltextLiteralTree struct{}

func (FulltextLiteralTree) Describe(ctx context.Context, node FulltextLiteralSelector) (FulltextLiteralNode, error) {
	return FulltextLiteralNode{Op: node.Op, Text: node.Text, Children: len(node.Children)}, ctx.Err()
}

func (FulltextLiteralTree) Child(ctx context.Context, node FulltextLiteralSelector, _ FulltextLiteralOperation, index int) (FulltextLiteralSelector, bool, error) {
	if err := ctx.Err(); err != nil {
		return FulltextLiteralSelector{}, false, err
	}
	return node.Children[index], true, nil
}

type fulltextLiteralPlan struct {
	op        FulltextLiteralOperation
	signature literalSignature
	children  []fulltextLiteralPlan
}

func compileFulltextLiteralPlan[T any](ctx context.Context, index *fulltextLiteralIndex, node T, reader FulltextLiteralReader[T], workspace *[]fulltextLiteralPlan) (fulltextLiteralPlan, error) {
	if err := ctx.Err(); err != nil {
		return fulltextLiteralPlan{}, err
	}
	selector, err := reader.Describe(ctx, node)
	if err != nil {
		return fulltextLiteralPlan{}, err
	}
	plan := fulltextLiteralPlan{op: selector.Op}
	switch selector.Op {
	case FulltextAllCandidates:
	case FulltextLiteral:
		if index == nil {
			return fulltextLiteralPlan{}, nil
		}
		if err := addLiteralText(ctx, &plan.signature, selector.Text, index.width, nil); err != nil {
			return fulltextLiteralPlan{}, err
		}
		restricted, err := index.mayRestrict(ctx, &plan.signature)
		if err != nil {
			return fulltextLiteralPlan{}, err
		}
		if !restricted {
			plan.op = FulltextAllCandidates
		}
	case FulltextAllOf, FulltextAnyOf:
		admitsAll := false
		start, count := -1, 0
		for i := 0; i < selector.Children; i++ {
			if err := ctx.Err(); err != nil {
				return fulltextLiteralPlan{}, err
			}
			child, included, err := reader.Child(ctx, node, selector.Op, i)
			if err != nil {
				return fulltextLiteralPlan{}, err
			}
			if !included {
				continue
			}
			next, err := compileFulltextLiteralPlan(ctx, index, child, reader, workspace)
			if err != nil {
				return fulltextLiteralPlan{}, err
			}
			if next.op == FulltextAllCandidates {
				if selector.Op == FulltextAnyOf {
					admitsAll = true
				}
				continue
			}
			if !admitsAll {
				if start < 0 {
					start = len(*workspace)
					*workspace = slices.Grow(*workspace, selector.Children)
					*workspace = (*workspace)[:start+selector.Children]
					clear((*workspace)[start:])
				}
				(*workspace)[start+count] = next
				count++
			}
		}
		if count > 0 {
			plan.children = (*workspace)[start : start+count]
		}
		if admitsAll || selector.Op == FulltextAllOf && count == 0 {
			plan.op = FulltextAllCandidates
			plan.children = nil
		}
	default:
		return fulltextLiteralPlan{}, fmt.Errorf("unknown fulltext literal operation %d", selector.Op)
	}
	return plan, ctx.Err()
}

func (index *fulltextLiteralIndex) selectBlock(ctx context.Context, block int, plan *fulltextLiteralPlan) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	switch plan.op {
	case FulltextAllCandidates:
		return index.live[block], nil
	case FulltextLiteral:
		return index.candidates(block, &plan.signature), nil
	case FulltextAllOf:
		result := index.live[block]
		for i := range plan.children {
			selected, err := index.selectBlock(ctx, block, &plan.children[i])
			if err != nil {
				return 0, err
			}
			result &= selected
			if result == 0 {
				break
			}
		}
		return result, nil
	case FulltextAnyOf:
		result := uint64(0)
		for i := range plan.children {
			selected, err := index.selectBlock(ctx, block, &plan.children[i])
			if err != nil {
				return 0, err
			}
			result |= selected
			if result == index.live[block] {
				break
			}
		}
		return result, nil
	}
	panic("uncompiled fulltext literal operation")
}

// ReadFulltextCandidates visits a conservative candidate superset. The visit must still
// run the dialect's exact matcher; collisions never establish matches or scores.
// Complete-corpus statistics are independent of this selected population.
func ReadFulltextCandidates[T any](ctx context.Context, d FulltextDocuments, node T, reader FulltextLiteralReader[T], visit func(string, *FulltextDocument) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	description, err := reader.Describe(ctx, node)
	if err != nil {
		return err
	}
	if description.Op == FulltextAllCandidates {
		return d.Each(ctx, visit)
	}
	index, err := d.view.literalIndex(ctx)
	if err != nil {
		return err
	}
	// Borrow bounded cleared compiler scratch under the existing projection
	// lease. Remove it while in use so a visitor's nested selection cannot
	// overwrite its caller's plan. Large query arenas remain request-owned.
	var workspace []fulltextLiteralPlan
	if index != nil {
		workspace, index.plans = index.plans, nil
		defer func() {
			clear(workspace)
			if cap(workspace) <= 16 {
				index.plans = workspace[:0]
			}
		}()
	}
	plan, err := compileFulltextLiteralPlan(ctx, index, node, reader, &workspace)
	if err != nil {
		return err
	}
	if plan.op == FulltextAnyOf && len(plan.children) == 0 {
		return ctx.Err()
	}
	if index == nil || plan.op == FulltextAllCandidates {
		return d.Each(ctx, visit)
	}
	for block := range index.live {
		if err := ctx.Err(); err != nil {
			return err
		}
		remaining, err := index.selectBlock(ctx, block, &plan)
		if err != nil {
			return err
		}
		for remaining != 0 {
			bit := bits.TrailingZeros64(remaining)
			remaining &= remaining - 1
			if err := ctx.Err(); err != nil {
				return err
			}
			id := index.ids[block*64+bit]
			if err := visit(id, d.view.documents[id]); err != nil {
				return err
			}
		}
	}
	return ctx.Err()
}
