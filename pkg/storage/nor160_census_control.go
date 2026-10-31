package storage

// NOR160ControlCache is a planted retention point for the census controls.
type NOR160ControlCache struct {
	nodes map[NodeID]*Node
	max   int
}

// The capture-callback shape (a cache-answered read, cachedAdjacency): a read
// view runs the read's callback, which fills the read's captured result. A
// callback that copies inside it may be reported as left undelivered; one that
// hands the retained value out through it must still be K0, whether it
// captures one value or collects a population.
type nor160ControlView struct{ cached func() bool }

func nor160ControlCapture(view nor160ControlView) bool { return view.cached != nil && view.cached() }

func (c *NOR160ControlCache) GetCapturedLent(id NodeID) *Node {
	var out *Node
	nor160ControlCapture(nor160ControlView{cached: func() bool { out = c.nodes[id]; return true }})
	return out
}

type nor160ControlLent struct{ node *Node }

func (c *NOR160ControlCache) GetCapturedLentField(id NodeID) *Node {
	var r nor160ControlLent
	nor160ControlCapture(nor160ControlView{cached: func() bool { r.node = c.nodes[id]; return true }})
	return r.node
}

func (c *NOR160ControlCache) GetCapturedLentAll(ids []NodeID) []*Node {
	var out []*Node
	nor160ControlCapture(nor160ControlView{cached: func() bool {
		for _, id := range ids {
			out = append(out, c.nodes[id])
		}
		return true
	}})
	return out
}

// Exits through deeper result shapes: an anonymous struct, and a named
// result holding a named result holding the node. Each drops a copy on one
// path (K1) or hands the retained node out (K0).
func (c *NOR160ControlCache) GetAnonSelected(id NodeID, hidden func(*Node) bool) struct{ N *Node } {
	n := copyNode(c.nodes[id])
	if hidden(n) {
		return struct{ N *Node }{}
	}
	return struct{ N *Node }{N: n}
}

type NOR160ControlInner struct{ Hit NOR160ControlNodeHolder }

type NOR160ControlOuter struct{ Inner NOR160ControlInner }

func (c *NOR160ControlCache) GetOuterSelected(id NodeID, hidden func(*Node) bool) NOR160ControlOuter {
	n := copyNode(c.nodes[id])
	if hidden(n) {
		return NOR160ControlOuter{}
	}
	return NOR160ControlOuter{Inner: NOR160ControlInner{Hit: NOR160ControlNodeHolder{Node: n}}}
}

func (c *NOR160ControlCache) GetOuterShared(id NodeID) NOR160ControlOuter {
	return NOR160ControlOuter{Inner: NOR160ControlInner{Hit: NOR160ControlNodeHolder{Node: c.nodes[id]}}}
}

// K0: a struct that directly wraps one retained node, handed out uncopied.
type NOR160ControlNodeHolder struct{ Node *Node }

func (c *NOR160ControlCache) GetWrapShared(id NodeID) NOR160ControlNodeHolder {
	return NOR160ControlNodeHolder{Node: c.nodes[id]}
}

// Clean for K0: a service handle that holds a node cache only transitively.
type NOR160ControlService struct{ cache *NOR160ControlCache }

type NOR160ControlRegistry struct{ services map[string]*NOR160ControlService }

func (r *NOR160ControlRegistry) GetService(name string) *NOR160ControlService {
	return r.services[name]
}

// K0: a retained node handed out without a copy.
func (c *NOR160ControlCache) GetShared(id NodeID) *Node { return c.nodes[id] }

// K1: copied, then rejected by a selection decision.
func (c *NOR160ControlCache) GetSelected(id NodeID, hidden func(*Node) bool) *Node {
	n := copyNode(c.nodes[id])
	if hidden(n) {
		return nil
	}
	return n
}

// K1: a producer that copies on a hit and decodes on a miss; its caller rejects.
func (c *NOR160ControlCache) nor160Load(id NodeID) *Node {
	if n, ok := c.nodes[id]; ok {
		return copyNode(n)
	}
	return &Node{ID: id}
}

func (c *NOR160ControlCache) GetLoaded(id NodeID, hidden func(*Node) bool) *Node {
	n := c.nor160Load(id)
	if hidden(n) {
		return nil
	}
	return n
}

// K5: a second population-copy loop. K1: a selection over its copied population.
func (c *NOR160ControlCache) nor160CopyAll() []*Node {
	out := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		out = append(out, copyNode(n))
	}
	return out
}

func (c *NOR160ControlCache) GetUnskipped(skip map[NodeID]bool) []*Node {
	var out []*Node
	for _, n := range c.nor160CopyAll() {
		if skip[n.ID] {
			continue
		}
		out = append(out, n)
	}
	return out
}

func nor160ControlSize(n *Node) int { return len(n.Properties) }

// K2: a copy made for cache admission, then abandoned by its budget check.
func (c *NOR160ControlCache) nor160Admit(n *Node) {
	cached := copyNode(n)
	if nor160ControlSize(cached) > c.max {
		return
	}
	c.nodes[n.ID] = cached
}

func (c *NOR160ControlCache) GetAdmitted(id NodeID) *Node {
	n := &Node{ID: id, Properties: map[string]interface{}{}}
	c.nor160Admit(n)
	return n
}

// K3: a pre-read copied population that a stopping callback leaves unvisited.
func (c *NOR160ControlCache) StreamCopied(ids []NodeID, visit func(*Node) error) error {
	buf := make([]*Node, 0, len(ids))
	for _, id := range ids {
		buf = append(buf, copyNode(c.nodes[id]))
	}
	for _, n := range buf {
		if err := visit(n); err != nil {
			return err
		}
	}
	return nil
}

// K4: ownership dispatchers. The plant hit type has two, so neither is its
// owner: a retained hit passed through one keeps its origin (K0), even on the
// copying branch (condition 1).
func nor160ControlDeliver(h *NOR160ControlHit, owned bool) *NOR160ControlHit {
	if owned {
		return h
	}
	return nor160ControlCloneHitComplete(h)
}

func nor160ControlDeliverAgain(h *NOR160ControlHit, owned bool) *NOR160ControlHit {
	if owned {
		return h
	}
	return nor160ControlCloneHitComplete(h)
}

func (s *NOR160ControlHits) GetDispatched(k string) *NOR160ControlHit {
	return nor160ControlDeliver(s.hits[k], false)
}

func (s *NOR160ControlHits) GetDispatchedAgain(k string) *NOR160ControlHit {
	return nor160ControlDeliverAgain(s.hits[k], false)
}

// K6: a copy of a node decoded for this read, and a copy of a copy.
func (c *NOR160ControlCache) GetDecodedCopy(id NodeID) *Node {
	return copyNode(&Node{ID: id, Properties: map[string]interface{}{"k": []interface{}{int64(1)}}})
}

func (c *NOR160ControlCache) GetCopyOfCopy(id NodeID) *Node { return copyNode(copyNode(c.nodes[id])) }

// Clean: select on the retained image, then copy once. Must not be reported.
func (c *NOR160ControlCache) GetClean(id NodeID, hidden func(*Node) bool) *Node {
	n := c.nodes[id]
	if n == nil || hidden(n) {
		return nil
	}
	return copyNode(n)
}

// K7: a partial copier of a family type; and a complete one (must not be reported).
type NOR160ControlHit struct {
	Node *Node
	Tags []string
}

// CallerCopy makes the planted hit a copyable entity.
func (h *NOR160ControlHit) CallerCopy() any { return nor160ControlCloneHitComplete(h) }

type NOR160ControlHits struct{ hits map[string]*NOR160ControlHit }

func nor160ControlCloneHit(h *NOR160ControlHit) *NOR160ControlHit {
	c := *h
	c.Tags = append([]string(nil), h.Tags...)
	return &c
}

func nor160ControlCloneHitComplete(h *NOR160ControlHit) *NOR160ControlHit {
	c := *h
	c.Tags = append([]string(nil), h.Tags...)
	c.Node = copyNode(h.Node)
	return &c
}

func (s *NOR160ControlHits) GetHit(k string) *NOR160ControlHit { return nor160ControlCloneHit(s.hits[k]) }

func (s *NOR160ControlHits) GetHitComplete(k string) *NOR160ControlHit {
	return nor160ControlCloneHitComplete(s.hits[k])
}

// Exclusion "written after construction": K7 ignores a shared pointer only to
// a struct type no function writes outside its own allocation. NOR160ControlMeta
// is written after construction (NOR160Bump), so sharing it must be reported;
// NOR160ControlFrozen is never written, so sharing it must not.
type NOR160ControlMeta struct{ Count int }

func (m *NOR160ControlMeta) NOR160Bump() { m.Count++ }

type NOR160ControlFrozen struct{ Name string }

type NOR160ControlRecHit struct {
	Node   *Node
	Meta   *NOR160ControlMeta
	Frozen *NOR160ControlFrozen
}

// CallerCopy makes the planted record hit a copyable entity.
func (h *NOR160ControlRecHit) CallerCopy() any {
	c := *h
	c.Node = copyNode(h.Node)
	c.Meta = &NOR160ControlMeta{Count: h.Meta.Count}
	return &c
}

type NOR160ControlRecHits struct{ hits map[string]*NOR160ControlRecHit }

// K7: the copy keeps the written record shared with the retained hit.
func (s *NOR160ControlRecHits) GetSharedMeta(k string) *NOR160ControlRecHit {
	h := s.hits[k]
	c := *h
	c.Node = copyNode(h.Node)
	return &c
}

// Clean: the copy shares only the never-written record.
func (s *NOR160ControlRecHits) GetFrozenShared(k string) *NOR160ControlRecHit {
	h := s.hits[k]
	c := *h
	c.Node = copyNode(h.Node)
	c.Meta = &NOR160ControlMeta{Count: h.Meta.Count}
	return &c
}

// Exclusion "received from another package's public read": a caller that drops
// a value an exported read of another package delivered is not a member. The
// same read called inside its own package is not a delivery, so a selection
// over its copy must still be reported (K1). NOR160ControlReceiver in
// pkg/cypher is the cross-package clean case.
func (c *NOR160ControlCache) GetCopied(id NodeID) *Node { return copyNode(c.nodes[id]) }

func (c *NOR160ControlCache) GetCopiedSelected(id NodeID, hidden func(*Node) bool) *Node {
	n := c.GetCopied(id)
	if hidden(n) {
		return nil
	}
	return n
}

// Captured-variable flow: a read whose results a closure sets from a bound
// method value, and a population built by a closure that appends to a
// captured variable. Selection after the copy must be reported (K1), through
// an array of populations and after a same-package read; building the whole
// population must not.
type nor160ControlReader struct{ c *NOR160ControlCache }

func (r *nor160ControlReader) deliver(first bool) []*Node { return r.c.nor160CopyAll() }

func nor160ControlReadBoth(c *NOR160ControlCache) (first, second []*Node) {
	r := nor160ControlReader{c: c}
	consume := func(read func(bool) []*Node) {
		first = read(true)
		second = read(false)
	}
	consume(r.deliver)
	return first, second
}

func (c *NOR160ControlCache) GetCapturedSelected(skip map[NodeID]bool) []*Node {
	var out []*Node
	include := func(n *Node) { out = append(out, n) }
	first, second := nor160ControlReadBoth(c)
	for _, part := range [...][]*Node{first, second} {
		for _, n := range part {
			if !skip[n.ID] {
				include(n)
			}
		}
	}
	return out
}

func (c *NOR160ControlCache) GetCapturedAll() []*Node {
	var out []*Node
	include := func(n *Node) { out = append(out, n) }
	first, second := nor160ControlReadBoth(c)
	for _, part := range [...][]*Node{first, second} {
		for _, n := range part {
			include(n)
		}
	}
	return out
}

func (c *NOR160ControlCache) GetCapturedLabelled(label string) []*Node {
	var out []*Node
	for _, n := range c.GetCapturedAll() {
		if len(n.Labels) == 0 || n.Labels[0] != label {
			continue
		}
		out = append(out, n)
	}
	return out
}

// Selection inside a callee: a copied population handed to a function that
// drops some of its elements must be reported at the caller (K1); handed to
// one that returns all of them, not.
func nor160ControlKeep(all []*Node, skip map[NodeID]bool) []*Node {
	var out []*Node
	for _, n := range all {
		if skip[n.ID] {
			continue
		}
		out = append(out, n)
	}
	return out
}

func nor160ControlKeepAll(all []*Node) []*Node {
	var out []*Node
	for _, n := range all {
		out = append(out, n)
	}
	return out
}

func (c *NOR160ControlCache) GetSelectedInCallee(skip map[NodeID]bool) []*Node {
	return nor160ControlKeep(c.nor160CopyAll(), skip)
}

func (c *NOR160ControlCache) GetKeptInCallee() []*Node {
	return nor160ControlKeepAll(c.nor160CopyAll())
}

// A copy accumulated in a local population that one path does not return
// must be reported (K1); one returned on every path, not.
func (c *NOR160ControlCache) GetFirstUnused(ids []NodeID, small bool) []*Node {
	var first []*Node
	for _, id := range ids {
		first = append(first, copyNode(c.nodes[id]))
	}
	if small {
		return c.nor160CopyAll()
	}
	return first
}

func (c *NOR160ControlCache) GetFirstUsed(ids []NodeID) []*Node {
	var first []*Node
	for _, id := range ids {
		first = append(first, copyNode(c.nodes[id]))
	}
	return first
}

// Copied fields discarded after the copy: a copy whose vectors a callee clears,
// directly or as the callback of a stream that hands it copies, must be
// reported (K1); clearing a node decoded for this read, or a callback that
// keeps the copy whole, must not.
func nor160ControlStrip(n *Node) *Node {
	n.ChunkEmbeddings, n.NamedEmbeddings = nil, nil
	return n
}

func (c *NOR160ControlCache) nor160StreamCopies(visit func(*Node) error) error {
	for _, n := range c.nodes {
		if err := visit(copyNode(n)); err != nil {
			return err
		}
	}
	return nil
}

func (c *NOR160ControlCache) GetStripped(id NodeID) *Node {
	return nor160ControlStrip(copyNode(c.nodes[id]))
}

func (c *NOR160ControlCache) GetDecodedStripped(id NodeID) *Node {
	return nor160ControlStrip(&Node{ID: id, ChunkEmbeddings: [][]float32{{1}}})
}

func (c *NOR160ControlCache) StreamStripped(visit func(*Node) error) error {
	return c.nor160StreamCopies(func(n *Node) error { return visit(nor160ControlStrip(n)) })
}

func (c *NOR160ControlCache) StreamWhole(visit func(*Node) error) error {
	return c.nor160StreamCopies(func(n *Node) error { return visit(n) })
}

// Populations delivered only on some paths. Must report (K1): an in-place
// filter over a copied population (x[:0]); a fast path that returns the copied
// population whole beside a filtering path; copies a callback writes into a
// captured variable that one path does not return. Must not: an in-place
// compaction that keeps every element; a reading loop before returning the
// population whole; a callback population returned on every path.
func (c *NOR160ControlCache) GetFilteredInPlace(skip map[NodeID]bool) []*Node {
	all := c.nor160CopyAll()
	out := all[:0]
	for _, n := range all {
		if skip[n.ID] {
			continue
		}
		out = append(out, n)
	}
	return out
}

func (c *NOR160ControlCache) GetCompactedInPlace() []*Node {
	all := c.nor160CopyAll()
	out := all[:0]
	for _, n := range all {
		if n != nil {
			out = append(out, n)
		}
	}
	return out
}

func (c *NOR160ControlCache) GetFastPathOrFiltered(skip map[NodeID]bool) []*Node {
	all := c.nor160CopyAll()
	if len(skip) == 0 {
		return all
	}
	var out []*Node
	for _, n := range all {
		if skip[n.ID] {
			continue
		}
		out = append(out, n)
	}
	return out
}

func (c *NOR160ControlCache) GetCountedWhole(seen map[NodeID]int) []*Node {
	all := c.nor160CopyAll()
	for _, n := range all {
		seen[n.ID]++
	}
	return all
}

func (c *NOR160ControlCache) nor160Each(visit func(*Node)) {
	for _, n := range c.nodes {
		visit(n)
	}
}

func (c *NOR160ControlCache) GetCallbackFirst(small bool) []*Node {
	var first []*Node
	c.nor160Each(func(n *Node) { first = append(first, copyNode(n)) })
	if small {
		return c.nor160CopyAll()
	}
	return first
}

// A population copied through a mapper before a loop whose callback can stop
// it must be reported (K3); visiting each copy as it is made, not.
func nor160ControlMap(xs []*Node, f func(*Node) *Node) []*Node {
	out := make([]*Node, len(xs))
	for i, x := range xs {
		out[i] = f(x)
	}
	return out
}

func (c *NOR160ControlCache) StreamMappedTail(tail []*Node, visit func(*Node) error) error {
	for _, n := range nor160ControlMap(tail, copyNode) {
		if err := visit(n); err != nil {
			return err
		}
	}
	return nil
}

func (c *NOR160ControlCache) StreamEachCopied(tail []*Node, visit func(*Node) error) error {
	for _, n := range tail {
		if err := visit(copyNode(n)); err != nil {
			return err
		}
	}
	return nil
}

func (c *NOR160ControlCache) GetCallbackAll() []*Node {
	var first []*Node
	c.nor160Each(func(n *Node) { first = append(first, copyNode(n)) })
	return first
}

// Real-shape plants: a generic mapper before a stoppable loop (K3); a
// population gathered through a closure that another closure captures, then
// filtered in place by a caller (K1); copies a callback writes into a captured
// variable behind a call that returns an error (K1). Clean twins: the generic
// mapper's result returned whole; the gathered population returned whole; the
// callback population returned on every path.
func nor160ControlMapG[T any](xs []T, f func(T) T) []T {
	if xs == nil {
		return nil
	}
	out := make([]T, len(xs))
	for i, x := range xs {
		out[i] = f(x)
	}
	return out
}

func (c *NOR160ControlCache) StreamGenericTail(tail []*Node, visit func(*Node) error) error {
	for _, n := range nor160ControlMapG(tail, copyNode) {
		if err := visit(n); err != nil {
			return err
		}
	}
	return nil
}

func (c *NOR160ControlCache) GetGenericCopies(tail []*Node) []*Node {
	return nor160ControlMapG(tail, copyNode)
}

func (c *NOR160ControlCache) nor160Gather(more bool) []*Node {
	var out []*Node
	include := func(n *Node) { out = append(out, n) }
	later := func() {
		for _, n := range c.nodes {
			include(n)
		}
	}
	for _, n := range c.nodes {
		include(copyNode(n))
	}
	if more {
		later()
	}
	return out
}

func (c *NOR160ControlCache) GetGatheredFiltered(skip map[NodeID]bool) []*Node {
	all := c.nor160Gather(false)
	out := all[:0]
	for _, n := range all {
		if skip[n.ID] {
			continue
		}
		out = append(out, n)
	}
	return out
}

func (c *NOR160ControlCache) GetGatheredAll() []*Node {
	return c.nor160Gather(false)
}

func (c *NOR160ControlCache) nor160EachErr(visit func(*Node)) error {
	for _, n := range c.nodes {
		visit(n)
	}
	return nil
}

func (c *NOR160ControlCache) GetCallbackFirstErr(small bool) ([]*Node, error) {
	var first []*Node
	err := c.nor160EachErr(func(n *Node) { first = append(first, copyNode(n)) })
	if err != nil {
		return nil, err
	}
	if small {
		return c.nor160CopyAll(), nil
	}
	return first, nil
}

// The same behind a defer, so every return goes through result slots.
func (c *NOR160ControlCache) GetCallbackFirstDeferred(small bool) ([]*Node, bool, error) {
	defer func() {}()
	var first []*Node
	err := c.nor160EachErr(func(n *Node) { first = append(first, copyNode(n)) })
	if err != nil {
		return nil, false, err
	}
	if !small {
		return first, true, nil
	}
	return c.nor160CopyAll(), true, nil
}

func (c *NOR160ControlCache) GetCallbackAllDeferred() ([]*Node, bool, error) {
	defer func() {}()
	var first []*Node
	err := c.nor160EachErr(func(n *Node) { first = append(first, copyNode(n)) })
	if err != nil {
		return nil, false, err
	}
	return first, true, nil
}

func (c *NOR160ControlCache) GetCallbackAllErr() ([]*Node, error) {
	var first []*Node
	err := c.nor160EachErr(func(n *Node) { first = append(first, copyNode(n)) })
	if err != nil {
		return nil, err
	}
	return first, nil
}

// The owner's decision (condition 2). NOR160ControlRecHit has one owner, gated
// on an image field; NOR160ControlTag has one, gated on a bool parameter. A
// retained value labelled or passed for the unchanged branch must be reported
// (K0); one labelled or passed for the copy, not.
type nor160ControlRecImage struct {
	hit  *NOR160ControlRecHit
	kept bool
}

func nor160ControlCopyRec(h *NOR160ControlRecHit) *NOR160ControlRecHit {
	c := *h
	c.Node = copyNode(h.Node)
	c.Meta = &NOR160ControlMeta{Count: h.Meta.Count}
	return &c
}

func nor160ControlDeliverRec(img nor160ControlRecImage) *NOR160ControlRecHit {
	if !img.kept {
		return img.hit
	}
	return nor160ControlCopyRec(img.hit)
}

func (s *NOR160ControlRecHits) GetRecMislabelled(k string) *NOR160ControlRecHit {
	return nor160ControlDeliverRec(nor160ControlRecImage{hit: s.hits[k]})
}

func (s *NOR160ControlRecHits) GetRecLabelled(k string) *NOR160ControlRecHit {
	return nor160ControlDeliverRec(nor160ControlRecImage{hit: s.hits[k], kept: true})
}

type NOR160ControlTag struct {
	Node *Node
	Tags []string
}

// CallerCopy makes the planted tag a copyable entity.
func (t *NOR160ControlTag) CallerCopy() any { return nor160ControlCopyTag(t) }

func nor160ControlCopyTag(t *NOR160ControlTag) *NOR160ControlTag {
	c := *t
	c.Node = copyNode(t.Node)
	c.Tags = append([]string(nil), t.Tags...)
	return &c
}

func nor160ControlDeliverTag(t *NOR160ControlTag, owned bool) *NOR160ControlTag {
	if owned {
		return t
	}
	return nor160ControlCopyTag(t)
}

type NOR160ControlTags struct{ tags map[string]*NOR160ControlTag }

func (s *NOR160ControlTags) GetTagAsOwned(k string) *NOR160ControlTag {
	return nor160ControlDeliverTag(s.tags[k], true)
}

func (s *NOR160ControlTags) GetTagCopied(k string) *NOR160ControlTag {
	return nor160ControlDeliverTag(s.tags[k], false)
}

// Result slots (condition 3): a function with a defer returns a retained node
// on one return only; it must be reported there, once. Copying on every
// return must not be reported.
func (c *NOR160ControlCache) GetSlotted(id NodeID, early bool) (n *Node, err error) {
	defer func() {}()
	if early {
		return nil, nil
	}
	return c.nodes[id], nil
}

func (c *NOR160ControlCache) GetSlottedCopied(id NodeID, early bool) (*Node, error) {
	defer func() {}()
	if early {
		return copyNode(c.nodes[id]), nil
	}
	return copyNode(c.nodes[id]), nil
}

// Read scope. A conditional copy whose result is only encoded (clean); the
// same shape returned from an exported read (K4, must report); write staging
// that drops its copy on an error (clean); a copy dropped inside a method that
// reaches a read exit only through an interface (K1, must report).
func nor160ControlPrepare(n *Node) *Node {
	if len(n.Labels) == 0 {
		return n
	}
	c := copyNode(n)
	c.Labels = nil
	return c
}

func nor160ControlEncode(n *Node) []byte { return []byte(nor160ControlPrepare(n).ID) }

func nor160ControlPrepareRead(n *Node) *Node {
	if len(n.Labels) == 0 {
		return n
	}
	c := copyNode(n)
	c.Labels = nil
	return c
}

func (c *NOR160ControlCache) GetPrepared(id NodeID) *Node { return nor160ControlPrepareRead(c.nodes[id]) }

func (c *NOR160ControlCache) nor160Stage(n *Node) error {
	staged := copyNode(n)
	if staged.ID == "" {
		return ErrInvalidData
	}
	c.nodes[n.ID] = staged
	return nil
}

func (c *NOR160ControlCache) PutStaged(n *Node) error { return c.nor160Stage(n) }

type nor160ControlGetter interface {
	nor160GetVia(id NodeID, hidden func(*Node) bool) *Node
}

func (c *NOR160ControlCache) nor160GetVia(id NodeID, hidden func(*Node) bool) *Node {
	n := copyNode(c.nodes[id])
	if hidden(n) {
		return nil
	}
	return n
}

func (c *NOR160ControlCache) GetViaInterface(id NodeID, hidden func(*Node) bool) *Node {
	var g nor160ControlGetter = c
	return g.nor160GetVia(id, hidden)
}

// Call-graph scope: a dropped copy inside a function value stored in a struct
// field, reached from an exported read only by calling that stored value. VTA
// must resolve it (K1, must report).
type NOR160ControlStoredFn struct {
	get func(NodeID, func(*Node) bool) *Node
}

func nor160ControlStoredGet(c *NOR160ControlCache) func(NodeID, func(*Node) bool) *Node {
	return func(id NodeID, hidden func(*Node) bool) *Node {
		n := copyNode(c.nodes[id])
		if hidden(n) {
			return nil
		}
		return n
	}
}

func NewNOR160ControlStoredFn(c *NOR160ControlCache) *NOR160ControlStoredFn {
	return &NOR160ControlStoredFn{get: nor160ControlStoredGet(c)}
}

func (s *NOR160ControlStoredFn) GetStored(id NodeID, hidden func(*Node) bool) *Node {
	return s.get(id, hidden)
}

// Narrowed exits: an exported getter whose result is a struct wrapping one
// entity directly is still a read exit; a copy it drops is a member (K1, must
// report).
type NOR160ControlWrap struct{ Node *Node }

func (c *NOR160ControlCache) GetWrappedSelected(id NodeID, hidden func(*Node) bool) NOR160ControlWrap {
	n := copyNode(c.nodes[id])
	if hidden(n) {
		return NOR160ControlWrap{}
	}
	return NOR160ControlWrap{Node: n}
}

// Callback receipt. A copy streamed to another package's callback that
// overwrites it is that package's handling (clean, see pkg/cypher
// NOR160ControlStripReceived); a shared cached node lent to such a callback
// stays a storage member (K0, must report).
func (c *NOR160ControlCache) StreamCopiesFor(visit func(*Node) error) error {
	for _, n := range c.nodes {
		if err := visit(copyNode(n)); err != nil {
			return err
		}
	}
	return nil
}

func (c *NOR160ControlCache) StreamLent(visit func(*Node) error) error {
	for _, n := range c.nodes {
		if err := visit(n); err != nil {
			return err
		}
	}
	return nil
}

// Origins through non-entity types. Retained nodes lent through a map range in
// a helper, a comma-ok lookup and an interface box must report K0 at the read
// exit; a map range over nodes decoded for this read must not.
func (c *NOR160ControlCache) nor160CollectLent() []*Node {
	var out []*Node
	for _, n := range c.nodes {
		out = append(out, n)
	}
	return out
}

func (c *NOR160ControlCache) GetAllLent() []*Node { return c.nor160CollectLent() }

func (c *NOR160ControlCache) GetSharedOk(id NodeID) *Node {
	n, ok := c.nodes[id]
	if !ok {
		return nil
	}
	return n
}

func (c *NOR160ControlCache) GetSharedViaAny(id NodeID) *Node {
	var x any = c.nodes[id]
	n, _ := x.(*Node)
	return n
}

func (c *NOR160ControlCache) GetDecodedRange(ids []NodeID) []*Node {
	decoded := make(map[NodeID]*Node, len(ids))
	for _, id := range ids {
		decoded[id] = &Node{ID: id}
	}
	var out []*Node
	for _, n := range decoded {
		out = append(out, n)
	}
	return out
}

// Scope ruling plants. An evaluator returning an interface that holds a node it
// copied, narrowed by its caller, is not a copy root (clean). A read exit
// whose entity-typed copy its caller drops still is (K1, must report).
// GetSharedViaAny (a lend through an interface) stays K0.
func nor160ControlEval(c *NOR160ControlCache, id NodeID) any { return copyNode(c.nodes[id]) }

func (c *NOR160ControlCache) GetEvalKeyed(id NodeID) map[string]*Node {
	out := map[string]*Node{}
	if s, ok := nor160ControlEval(c, id).(string); ok {
		out[s] = nil
	}
	return out
}

func (c *NOR160ControlCache) GetCopyFor(id NodeID) *Node { return copyNode(c.nodes[id]) }

func (c *NOR160ControlCache) GetCopyIfKept(id NodeID, keep bool) *Node {
	n := c.GetCopyFor(id)
	if !keep {
		return nil
	}
	return n
}

// Receipt with a pass-through. An exported read that returns either the
// caller's own node unchanged or a fresh copy from its cache: a cross-package
// caller that drops the result handles a received value (clean, see pkg/cypher
// NOR160ControlDropsFresh).
func (c *NOR160ControlCache) GetFreshOrSame(n *Node) *Node {
	if n == nil || n.ID == "" {
		return n
	}
	return copyNode(c.nodes[n.ID])
}

// Handoff controls: what each callee does with a copy it is handed must be
// reported per callee: one only reads it, one overwrites it, one retains it.
func nor160ControlHandoffReads(n *Node) bool { return n.ID == "" }

func nor160ControlHandoffOverwrites(n *Node) { n.Properties = nil }

func (c *NOR160ControlCache) nor160ControlHandoffKeeps(n *Node) { c.nodes[n.ID] = n }

func (c *NOR160ControlCache) GetHandoffMixed(id NodeID) *Node {
	n := copyNode(c.nodes[id])
	if nor160ControlHandoffReads(n) {
		return nil
	}
	nor160ControlHandoffOverwrites(n)
	return n
}

func nor160ControlHandoffReturns(n *Node) *Node { return n }

func nor160ControlHandoffSends(ch chan *Node, n *Node) { ch <- n }

func (c *NOR160ControlCache) GetHandoffReturned(id NodeID) *Node {
	n := copyNode(c.nodes[id])
	kept := nor160ControlHandoffReturns(n)
	nor160ControlHandoffOverwrites(kept)
	return kept
}

func (c *NOR160ControlCache) GetHandoffSent(id NodeID, ch chan *Node) *Node {
	n := copyNode(c.nodes[id])
	nor160ControlHandoffSends(ch, n)
	nor160ControlHandoffOverwrites(n)
	return n
}

func (c *NOR160ControlCache) GetHandoffCaptured(id NodeID) *Node {
	var out *Node
	keep := func(n *Node) { out = n }
	n := copyNode(c.nodes[id])
	keep(n)
	nor160ControlHandoffOverwrites(n)
	return out
}

// A store into the parameter's own field keeps nothing elsewhere.
func nor160ControlHandoffSelf(n *Node) { n.Properties = n.EmbedMeta }

func (c *NOR160ControlCache) GetHandoffSelf(id NodeID) *Node {
	n := copyNode(c.nodes[id])
	nor160ControlHandoffSelf(n)
	return n
}

// An unresolved call is attributed to its own site, through a caller.
type nor160ControlSink interface{ nor160Sink(*Node) }

func nor160ControlHandoffUnknownCall(s nor160ControlSink, n *Node) { s.nor160Sink(n) }

func nor160ControlHandoffUnknownVia(s nor160ControlSink, n *Node) {
	nor160ControlHandoffUnknownCall(s, n)
}

func (c *NOR160ControlCache) GetHandoffUnknown(id NodeID, s nor160ControlSink) *Node {
	n := copyNode(c.nodes[id])
	nor160ControlHandoffUnknownVia(s, n)
	nor160ControlHandoffOverwrites(n)
	return n
}

func (c *NOR160ControlCache) GetHandoffKept(id NodeID) *Node {
	n := copyNode(c.nodes[id])
	c.nor160ControlHandoffKeeps(n)
	nor160ControlHandoffOverwrites(n)
	return n
}
