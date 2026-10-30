// NOR-160 lane (Claude), 30 October 2026. Analysis-only client, not product code.
//
// Family census by value flow, over the maintained workcensus typed/SSA loader.
// Membership comes from the code: every value of a family type (storage Node and
// Edge, any module type that contains one, any module type that declares
// CallerCopy) carries an origin computed through calls, interface and closure
// callees (VTA), function-valued arguments, locals, slices and maps:
//
//	retained  read out of a container held by an object (a cache, buffer,
//	          pending map, snapshot population, global);
//	copy      a fresh value filled from another value of its own type;
//	decoded   a fresh value not filled from another family value;
//	param i   parameter i passed through;
//	funcres i the result of calling function-valued parameter i.
//
// Classes (no allowlist: owners are reported like everything else):
//
//	K0 retained escape     an exported function returns or visits a retained value.
//	K1 copy before select  a copy (or an element of a copied population) reaches a
//	                       function exit or its own next iteration undelivered.
//	                       Reading or measuring is not delivery; a bool callback is
//	                       a selection, not a delivery.
//	K1e                    the same, only on a path that returns a non-nil error.
//	K2 admission abandon   a copy stored into a retention point on one path and
//	                       dropped on another.
//	K3 partial consumption a copied population iterated by a loop that a call's
//	                       result can leave early (callback stop, error).
//	K4 ownership dispatch  one value merges an uncopied value and a copy of it.
//	K4b Copier dispatch    Copier assertions and CallerCopy calls; a second
//	                       CallerCopy reachable after a first one in a function.
//	K5 population copy     a loop stores copies of the elements it iterates.
//	                       Mappers (loops storing a function parameter's result)
//	                       are listed separately: the one slice owner is a mapper.
//	K6 copy of private     an unconditional copier applied to a value made for
//	                       this read (decoded or already copied).
//	K7 partial copier      a copy of a family type that keeps a reference field of
//	                       its source.
//
// Planted controls (virtual file in pkg/storage, compiles on every revision)
// must each be reported in their class; the clean controls must not be.
package main

import (
	"encoding/json"
	"fmt"
	"go/token"
	"go/types"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"golang.org/x/tools/go/ssa"
)

const module = "github.com/orneryd/nornicdb"
const storagePkg = module + "/pkg/storage"

const controlFile = `package storage

// NOR160ControlCache is a planted retention point for the census controls.
type NOR160ControlCache struct {
	nodes map[NodeID]*Node
	max   int
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
`

const cypherControlFile = `package cypher

import "github.com/orneryd/nornicdb/pkg/storage"

// NOR160ControlCrossCopy copies its own argument through another package's
// exported copier and drops the copy on one path: a copy it made, not a
// receipt (K1, must be reported).
func NOR160ControlCrossCopy(n *storage.Node, keep bool) *storage.Node {
	c := storage.CopyNode(n)
	if !keep {
		return nil
	}
	return c
}

// The boundary (received provenance). A helper that passes on a node a
// public read delivered, and a caller that drops it: the receiver's own
// handling (clean). A copy of a received node, dropped: clean. A cache this
// package owns, copying its own retained node and dropping the copy: in scope
// (K1, must be reported).
func nor160ControlCypherHelper(c *storage.NOR160ControlCache, id storage.NodeID) *storage.Node {
	return c.GetCopied(id)
}

func NOR160ControlCypherDrops(c *storage.NOR160ControlCache, id storage.NodeID, hidden func(*storage.Node) bool) *storage.Node {
	n := nor160ControlCypherHelper(c, id)
	if hidden(n) {
		return nil
	}
	return n
}

func NOR160ControlCypherCopyReceived(c *storage.NOR160ControlCache, id storage.NodeID, keep bool) *storage.Node {
	n := storage.CopyNode(c.GetCopied(id))
	if !keep {
		return nil
	}
	return n
}

type NOR160ControlCypherCache struct{ nodes map[string]*storage.Node }

func (c *NOR160ControlCypherCache) Get(k string, hidden func(*storage.Node) bool) *storage.Node {
	n := storage.CopyNode(c.nodes[k])
	if hidden(n) {
		return nil
	}
	return n
}

// NOR160ControlReceiver drops a node that storage's public read delivered to
// it. The census must prove the receipt and not report it.
func NOR160ControlReceiver(c *storage.NOR160ControlCache, id storage.NodeID, hidden func(*storage.Node) bool) *storage.Node {
	n := c.GetCopied(id)
	if hidden(n) {
		return nil
	}
	return n
}
`

// control: a planted function and the class that must report it.
type control struct {
	class, fn string
	clean     bool
}

var controls = []control{
	{"K0", "(*" + storagePkg + ".NOR160ControlCache).GetShared", false},
	{"K0", "(*" + storagePkg + ".NOR160ControlCache).GetWrapShared", false},
	{"K0", "(*" + storagePkg + ".NOR160ControlRegistry).GetService", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetSelected", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetLoaded", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetUnskipped", false},
	{"K2", "(*" + storagePkg + ".NOR160ControlCache).nor160Admit", false},
	{"K3", "(*" + storagePkg + ".NOR160ControlCache).StreamCopied", false},
	{"K4", storagePkg + ".nor160ControlDeliver", false},
	{"K4", storagePkg + ".nor160ControlDeliverAgain", false},
	{"K0", "(*" + storagePkg + ".NOR160ControlHits).GetDispatched", false},
	{"K0", "(*" + storagePkg + ".NOR160ControlRecHits).GetRecMislabelled", false},
	{"K0", "(*" + storagePkg + ".NOR160ControlRecHits).GetRecLabelled", true},
	{"K0", "(*" + storagePkg + ".NOR160ControlTags).GetTagAsOwned", false},
	{"K0", "(*" + storagePkg + ".NOR160ControlTags).GetTagCopied", true},
	{"K0", "(*" + storagePkg + ".NOR160ControlCache).GetSlotted", false},
	{"K0", "(*" + storagePkg + ".NOR160ControlCache).GetSlottedCopied", true},
	{"K1", module + "/pkg/cypher.NOR160ControlCrossCopy", false},
	{"K4", storagePkg + ".nor160ControlPrepare", true},
	{"K4", storagePkg + ".nor160ControlPrepareRead", false},
	{"K1e", "(*" + storagePkg + ".NOR160ControlCache).nor160Stage", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).nor160GetVia", false},
	{"K1", storagePkg + ".nor160ControlStoredGet$1", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetWrappedSelected", false},
	{"K1", module + "/pkg/cypher.NOR160ControlCypherDrops", true},
	{"K1", module + "/pkg/cypher.NOR160ControlCypherCopyReceived", true},
	{"K1", "(*" + module + "/pkg/cypher.NOR160ControlCypherCache).Get", false},
	{"K5", "(*" + storagePkg + ".NOR160ControlCache).nor160CopyAll", false},
	{"K6", "(*" + storagePkg + ".NOR160ControlCache).GetDecodedCopy", false},
	{"K6", "(*" + storagePkg + ".NOR160ControlCache).GetCopyOfCopy", false},
	{"K7", "(*" + storagePkg + ".NOR160ControlHits).GetHit", false},
	{"K0", "(*" + storagePkg + ".NOR160ControlCache).GetClean", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetClean", true},
	{"K6", "(*" + storagePkg + ".NOR160ControlCache).GetClean", true},
	{"K7", "(*" + storagePkg + ".NOR160ControlHits).GetHitComplete", true},
	{"K7", "(*" + storagePkg + ".NOR160ControlRecHits).GetSharedMeta", false},
	{"K7", "(*" + storagePkg + ".NOR160ControlRecHits).GetFrozenShared", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetCopiedSelected", false},
	{"K1", module + "/pkg/cypher.NOR160ControlReceiver", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetCapturedSelected", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetCapturedLabelled", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetCapturedAll", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetSelectedInCallee", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetKeptInCallee", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetFirstUnused", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetFirstUsed", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetStripped", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetDecodedStripped", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).StreamStripped", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).StreamWhole", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetFilteredInPlace", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetCompactedInPlace", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetFastPathOrFiltered", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetCountedWhole", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetCallbackFirst", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetCallbackAll", true},
	{"K3", "(*" + storagePkg + ".NOR160ControlCache).StreamMappedTail", false},
	{"K3", "(*" + storagePkg + ".NOR160ControlCache).StreamEachCopied", true},
	{"K3", "(*" + storagePkg + ".NOR160ControlCache).StreamGenericTail", false},
	{"K3", "(*" + storagePkg + ".NOR160ControlCache).GetGenericCopies", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetGatheredFiltered", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetGatheredAll", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetCallbackFirstErr", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetCallbackAllErr", true},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetCallbackFirstDeferred", false},
	{"K1", "(*" + storagePkg + ".NOR160ControlCache).GetCallbackAllDeferred", true},
}

var tree string

// ---------------------------------------------------------------- origins

type org struct {
	Copy, Retained, Decoded, Unknown bool
	Params, FuncRes                  uint64
	// Delivered: parameters handed back through the owner's decision. On
	// substitution a retained argument becomes the owner's copy.
	Delivered uint64
	// Received: delivered to this package by another package's public read.
	Received bool
}

func (o org) or(p org) org {
	return org{o.Copy || p.Copy, o.Retained || p.Retained, o.Decoded || p.Decoded, o.Unknown || p.Unknown, o.Params | p.Params, o.FuncRes | p.FuncRes, o.Delivered | p.Delivered, o.Received || p.Received}
}

// receivedOnly: nothing but a value received across the boundary.
func (o org) receivedOnly() bool { return o == org{Received: true} }

func (o org) empty() bool { return o == (org{}) }

// private: only values made for this read (decoded or copied here).
func (o org) private() bool {
	return !o.empty() && !o.Retained && !o.Unknown && !o.Received && o.Params == 0 && o.FuncRes == 0 && o.Delivered == 0
}

func (o org) overlaps(p org) bool {
	return o.Params&p.Params != 0 || (o.Retained && p.Retained) || (o.Decoded && p.Decoded) || (o.Unknown && p.Unknown) || o.FuncRes&p.FuncRes != 0 || o.Delivered&p.Delivered != 0
}

func bit(i int) uint64 {
	if i < 0 || i >= 64 {
		return 0
	}
	return 1 << uint(i)
}

type summary struct {
	Ret     []org
	Escapes []bool
	Copies  []bool
	Partial map[int][]string // parameter -> fields a copy of it keeps shared
	// FreeStores: what a closure stores into each captured variable, in terms
	// of its own parameters; the enclosing function's memory receives it at
	// every call of the closure.
	FreeStores []org
}

type memKey struct {
	base  ssa.Value
	field int
}

type fnState struct {
	o     map[ssa.Value]org
	mem   map[memKey]org
	src   map[ssa.Value]org  // copies of a known source: the origin of what each copies
	intro map[ssa.Value]bool // calls and allocations that introduce a copy here
	res   map[*ssa.Call][]org
	free  map[*ssa.FreeVar]org // captured variables: what the enclosing function holds there
}

func (s *fnState) val(v ssa.Value) org {
	switch x := v.(type) {
	case *ssa.Const, *ssa.Function, *ssa.Builtin, nil:
		return org{}
	case *ssa.Global:
		if family(x.Type()) {
			return org{Retained: true}
		}
		return org{}
	}
	return s.o[v]
}

type analysis struct {
	reach      map[*ssa.Function]map[ssa.Value]bool // values that reach a public read exit
	scope      *readScope
	candidates []string                // every function found deciding between a value and its copy
	owners     map[*ssa.Function]owner // the single owner of each entity type
	slots      map[*ssa.Function]map[*ssa.Alloc]bool
	cellRoot   bool // mayDrop is following callback-filled variables
	p          *workcensus.Program
	fns        []*ssa.Function
	sums       map[*ssa.Function]*summary
	states     map[*ssa.Function]*fnState
	callees    map[ssa.CallInstruction][]*ssa.Function
	callers    map[*ssa.Function][]*ssa.Function
}

// ---------------------------------------------------------------- types

var famMemo = map[types.Type]bool{}

func family(t types.Type) bool {
	t = types.Unalias(t)
	if v, ok := famMemo[t]; ok {
		return v
	}
	v := familyRec(t, map[types.Type]bool{})
	famMemo[t] = v
	return v
}

func familyRec(t types.Type, seen map[types.Type]bool) bool {
	t = types.Unalias(t)
	if seen[t] {
		return false
	}
	seen[t] = true
	switch x := t.(type) {
	case *types.Pointer:
		return familyRec(x.Elem(), seen)
	case *types.Slice:
		return familyRec(x.Elem(), seen)
	case *types.Array:
		return familyRec(x.Elem(), seen)
	case *types.Map:
		return familyRec(x.Elem(), seen)
	case *types.Chan:
		return familyRec(x.Elem(), seen)
	case *types.Tuple:
		for i := 0; i < x.Len(); i++ {
			if familyRec(x.At(i).Type(), seen) {
				return true
			}
		}
		return false
	case *types.Named:
		obj := x.Obj()
		if obj.Pkg() == nil || !inModulePath(obj.Pkg().Path()) {
			return false
		}
		if obj.Pkg().Path() == storagePkg && (obj.Name() == "Node" || obj.Name() == "Edge") {
			return true
		}
		if declaresCallerCopy(x) {
			return true
		}
		return familyRec(x.Underlying(), seen)
	case *types.Struct:
		for i := 0; i < x.NumFields(); i++ {
			if familyRec(x.Field(i).Type(), seen) {
				return true
			}
		}
	}
	return false
}

func declaresCallerCopy(n *types.Named) bool {
	for _, t := range []types.Type{n, types.NewPointer(n)} {
		ms := types.NewMethodSet(t)
		for i := 0; i < ms.Len(); i++ {
			if ms.At(i).Obj().Name() == "CallerCopy" {
				return true
			}
		}
	}
	return false
}

func inModulePath(path string) bool { return path == module || strings.HasPrefix(path, module+"/") }

// container: a map, slice or array (of family values).
func container(t types.Type) bool {
	switch types.Unalias(t).Underlying().(type) {
	case *types.Map, *types.Slice, *types.Array:
		return true
	}
	return false
}

func reference(t types.Type) bool {
	switch types.Unalias(t).Underlying().(type) {
	case *types.Pointer, *types.Map, *types.Slice, *types.Interface, *types.Chan:
		return true
	}
	return false
}

func structOf(t types.Type) (*types.Named, *types.Struct) {
	n, ok := types.Unalias(t).(*types.Named)
	if !ok {
		return nil, nil
	}
	s, ok := n.Underlying().(*types.Struct)
	if !ok {
		return nil, nil
	}
	return n, s
}

func pointee(t types.Type) types.Type {
	if p, ok := types.Unalias(t).Underlying().(*types.Pointer); ok {
		return p.Elem()
	}
	return nil
}

func resultTypes(sig *types.Signature) []types.Type {
	var out []types.Type
	for i := 0; i < sig.Results().Len(); i++ {
		out = append(out, sig.Results().At(i).Type())
	}
	return out
}

func isNilConst(v ssa.Value) bool {
	c, ok := v.(*ssa.Const)
	return ok && c.Value == nil
}

func isBoolResult(sig *types.Signature) bool {
	if sig.Results().Len() != 1 {
		return false
	}
	b, ok := sig.Results().At(0).Type().Underlying().(*types.Basic)
	return ok && b.Kind() == types.Bool
}

func isErrorType(t types.Type) bool {
	return types.Identical(t, types.Universe.Lookup("error").Type())
}

// ---------------------------------------------------------------- helpers

func inModule(fn *ssa.Function) bool {
	for fn != nil && fn.Pkg == nil {
		if o := fn.Origin(); o != nil && o != fn {
			fn = o // an instance of a generic function
			continue
		}
		fn = fn.Parent()
	}
	if fn == nil || fn.Pkg == nil {
		return false
	}
	return inModulePath(fn.Pkg.Pkg.Path())
}

func pos(fn *ssa.Function, p token.Pos) (string, int) {
	if !p.IsValid() {
		p = fn.Pos()
	}
	x := fn.Prog.Fset.Position(p)
	rel, err := filepath.Rel(tree, x.Filename)
	if err != nil {
		rel = x.Filename
	}
	return filepath.ToSlash(rel), x.Line
}

func isTestFile(fn *ssa.Function) bool {
	file, _ := pos(fn, fn.Pos())
	return strings.HasSuffix(file, "_test.go")
}

func originFn(fn *ssa.Function) *ssa.Function {
	if fn != nil && fn.Origin() != nil {
		return fn.Origin()
	}
	return fn
}

func fullName(fn *ssa.Function) string {
	fn = originFn(fn)
	if fn.Pkg == nil || fn.Signature.Recv() != nil || fn.Parent() != nil {
		return fn.String()
	}
	return fn.Pkg.Pkg.Path() + "." + fn.Name()
}

func top(fn *ssa.Function) *ssa.Function {
	for fn.Parent() != nil {
		fn = fn.Parent()
	}
	return fn
}

func exported(fn *ssa.Function) bool {
	fn = originFn(fn)
	if fn.Parent() != nil || fn.Pkg == nil || !token.IsExported(fn.Name()) || !inModule(fn) {
		return false
	}
	if recv := fn.Signature.Recv(); recv != nil {
		t := recv.Type()
		if p := pointee(t); p != nil {
			t = p
		}
		if n, ok := types.Unalias(t).(*types.Named); ok && !n.Obj().Exported() {
			return false
		}
	}
	return true
}

func paramIndex(fn *ssa.Function, v ssa.Value) int {
	for i, p := range fn.Params {
		if p == v {
			return i
		}
	}
	return -1
}

// local: memory this function allocated itself.
func local(v ssa.Value) bool {
	switch x := v.(type) {
	case *ssa.Alloc, *ssa.MakeSlice, *ssa.MakeMap:
		return true
	case *ssa.Slice:
		return local(x.X)
	}
	return false
}

func localBase(v ssa.Value) ssa.Value {
	switch x := v.(type) {
	case *ssa.Alloc, *ssa.MakeSlice, *ssa.MakeMap:
		return v
	case *ssa.Slice:
		return localBase(x.X)
	}
	return nil
}

func memKeyOf(addr ssa.Value) (memKey, bool) {
	switch a := addr.(type) {
	case *ssa.Alloc:
		return memKey{a, -1}, true
	case *ssa.FieldAddr:
		if b := localBase(a.X); b != nil {
			return memKey{b, a.Field}, true
		}
	case *ssa.IndexAddr:
		if b := localBase(a.X); b != nil {
			return memKey{b, -1}, true
		}
	}
	return memKey{}, false
}

func callArgs(c *ssa.CallCommon) []ssa.Value {
	if c.IsInvoke() {
		return append([]ssa.Value{c.Value}, c.Args...)
	}
	return c.Args
}

// receivedAcross: the call is a public read of another package; what it
// returns has been delivered to this function, its caller.
func (a *analysis) receivedAcross(fn *ssa.Function, call ssa.CallInstruction) bool {
	here := top(originFn(fn)).Pkg
	c := call.Common()
	if c.IsInvoke() {
		return c.Method.Exported() && (here == nil || c.Method.Pkg() == nil || c.Method.Pkg() != here.Pkg)
	}
	callees := a.calleesOf(call)
	if len(callees) == 0 {
		return false
	}
	args := callArgs(c)
	for _, g := range callees {
		if sg := a.sums[g]; sg != nil {
			// A copy of the caller's own argument is made for the caller,
			// not received from the callee's state.
			for j, cp := range sg.Copies {
				if cp && j < len(args) && family(args[j].Type()) {
					return false
				}
			}
			for _, r := range sg.Ret {
				if r.Params|r.FuncRes|r.Delivered != 0 {
					return false
				}
			}
		}
		g = originFn(g)
		if !exported(g) || g.Pkg == here {
			return false
		}
	}
	return true
}

func (a *analysis) calleesOf(call ssa.CallInstruction) []*ssa.Function {
	c := call.Common()
	if f := c.StaticCallee(); f != nil {
		return []*ssa.Function{f}
	}
	return a.callees[call]
}

// freeEnv: what the enclosing function holds in captured variable i of the
// closure fn, over every place it makes the closure. Before the enclosing
// function has a state the environment is empty; summaries grow to the
// fixpoint from there. A function with no enclosing function (a synthetic
// wrapper) has no known environment.
func (a *analysis) freeEnv(fn *ssa.Function, i int) (org, bool) {
	parent := fn.Parent()
	if parent == nil {
		return org{}, false
	}
	ps := a.states[parent]
	if ps == nil {
		return org{}, true
	}
	var env org
	for _, b := range parent.Blocks {
		for _, ins := range b.Instrs {
			mc, ok := ins.(*ssa.MakeClosure)
			if !ok || mc.Fn != fn || i >= len(mc.Bindings) {
				continue
			}
			if al, ok := mc.Bindings[i].(*ssa.Alloc); ok {
				env = env.or(a.loadOrigin(ps, al))
			} else {
				env = env.or(ps.val(mc.Bindings[i]))
			}
		}
	}
	return env, true
}

// closureWrites: the captured variables of fn that a call of a closure made
// in fn writes, and what it writes there.
func (a *analysis) closureWrites(fn *ssa.Function, s *fnState, call ssa.CallInstruction) map[memKey]org {
	c := call.Common()
	mc := closureOf(c.Value)
	if mc == nil {
		return nil
	}
	g, _ := mc.Fn.(*ssa.Function)
	sg := a.sums[g]
	if sg == nil {
		return nil
	}
	var out map[memKey]org
	for i, b := range mc.Bindings {
		al, ok := b.(*ssa.Alloc)
		if !ok || i >= len(sg.FreeStores) || sg.FreeStores[i].empty() {
			continue
		}
		if out == nil {
			out = map[memKey]org{}
		}
		k := memKey{al, -1}
		out[k] = out[k].or(a.subst(fn, s, sg.FreeStores[i], c.Args))
	}
	return out
}

// unwrapped: a synthetic wrapper (a bound method value, an interface thunk)
// stands for the method it calls.
func (a *analysis) unwrapped(g *ssa.Function) *ssa.Function {
	if a.sums[g] != nil || g.Synthetic == "" {
		return g
	}
	for _, b := range g.Blocks {
		for _, ins := range b.Instrs {
			if call, ok := ins.(*ssa.Call); ok {
				if f := call.Common().StaticCallee(); f != nil && a.sums[f] != nil {
					return f
				}
			}
		}
	}
	return g
}

// ---------------------------------------------------------------- per-function origins

func (a *analysis) analyzeFn(fn *ssa.Function) *fnState {
	s := &fnState{o: map[ssa.Value]org{}, mem: map[memKey]org{}, src: map[ssa.Value]org{}, intro: map[ssa.Value]bool{}, res: map[*ssa.Call][]org{}, free: map[*ssa.FreeVar]org{}}
	for i, p := range fn.Params {
		if _, isFunc := p.Type().Underlying().(*types.Signature); isFunc || family(p.Type()) {
			s.o[p] = org{Params: bit(i)}
		}
	}
	for i, fv := range fn.FreeVars {
		if family(fv.Type()) || pointee(fv.Type()) != nil && family(pointee(fv.Type())) {
			env, known := a.freeEnv(fn, i)
			if !known {
				env = org{Unknown: true}
			}
			s.o[fv] = env
			if pointee(fv.Type()) != nil {
				s.free[fv] = env
			}
		}
	}
	for iter := 0; iter < 16; iter++ {
		changed := false
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				switch x := ins.(type) {
				case *ssa.Store:
					if k, ok := memKeyOf(x.Addr); ok {
						n := s.mem[k].or(s.val(x.Val))
						if n != s.mem[k] {
							s.mem[k], changed = n, true
						}
					}
				case *ssa.MapUpdate:
					if base := localBase(x.Map); base != nil {
						k := memKey{base, -1}
						n := s.mem[k].or(s.val(x.Value))
						if n != s.mem[k] {
							s.mem[k], changed = n, true
						}
					}
				case ssa.CallInstruction:
					// A closure called here writes the variables it captured;
					// so does one passed here as a callback, with what it is
					// given unknown here.
					for k, n := range a.closureWrites(fn, s, x) {
						if m := s.mem[k].or(n); m != s.mem[k] {
							s.mem[k], changed = m, true
						}
					}
					for k, n := range a.callbackWrites(x) {
						if m := s.mem[k].or(n); m != s.mem[k] {
							s.mem[k], changed = m, true
						}
					}
				}
				switch x := ins.(type) {
				case *ssa.Call:
					// copy(dst, src) moves elements into dst.
					if bi, ok := x.Call.Value.(*ssa.Builtin); ok && bi.Name() == "copy" && len(x.Call.Args) == 2 {
						if base := localBase(x.Call.Args[0]); base != nil {
							k := memKey{base, -1}
							n := s.mem[k].or(s.val(x.Call.Args[1]))
							if n != s.mem[k] {
								s.mem[k], changed = n, true
							}
						}
					}
				}
				if v, ok := ins.(ssa.Value); ok {
					n := a.origin(fn, s, v)
					if n != s.o[v] {
						s.o[v], changed = n, true
					}
				}
			}
		}
		if !changed {
			break
		}
	}
	return s
}

func (a *analysis) origin(fn *ssa.Function, s *fnState, v ssa.Value) org {
	var o org
	switch x := v.(type) {
	case *ssa.Alloc:
		elem := pointee(x.Type())
		if src, ok := copySource(s, x); ok {
			if src.receivedOnly() {
				return org{Received: true} // the receiver's own handling
			}
			s.src[x] = src
			s.intro[x] = true
			return org{Copy: true}
		}
		all := s.mem[memKey{x, -1}]
		for k, m := range s.mem {
			if k.base == x && k.field >= 0 {
				all = all.or(m)
			}
		}
		if all.empty() && elem != nil && family(elem) {
			if _, st := structOf(elem); st != nil {
				return org{Decoded: true}
			}
		}
		return all
	case *ssa.UnOp:
		switch x.Op {
		case token.MUL:
			o = a.loadOrigin(s, x.X)
		case token.ARROW:
			o = s.val(x.X)
		}
	case *ssa.Field:
		o = s.val(x.X)
	case *ssa.FieldAddr:
		o = s.val(x.X)
	case *ssa.IndexAddr:
		o = s.val(x.X)
		if b := localBase(x.X); b != nil {
			o = o.or(s.mem[memKey{b, -1}])
		}
	case *ssa.Index:
		o = s.val(x.X)
	case *ssa.Lookup:
		o = s.val(x.X)
		if b := localBase(x.X); b != nil {
			o = o.or(s.mem[memKey{b, -1}])
		}
	case *ssa.Range:
		o = s.val(x.X)
		if b := localBase(x.X); b != nil {
			o = o.or(s.mem[memKey{b, -1}])
		}
	case *ssa.Next:
		o = s.val(x.Iter)
	case *ssa.Extract:
		switch t := x.Tuple.(type) {
		case *ssa.Call:
			if rs := s.res[t]; x.Index < len(rs) {
				o = rs[x.Index]
			}
		case *ssa.Next:
			if x.Index > 0 {
				o = s.val(t)
			}
		default:
			if x.Index == 0 {
				o = s.val(x.Tuple)
			}
		}
	case *ssa.Phi:
		for _, e := range x.Edges {
			o = o.or(s.val(e))
		}
	case *ssa.Slice:
		o = s.val(x.X)
		if b := localBase(x.X); b != nil {
			o = o.or(s.mem[memKey{b, -1}])
		}
	case *ssa.MakeSlice, *ssa.MakeMap:
		o = s.mem[memKey{v, -1}]
	case *ssa.MakeInterface:
		o = s.val(x.X)
	case *ssa.ChangeType:
		o = s.val(x.X)
	case *ssa.Convert:
		o = s.val(x.X)
	case *ssa.ChangeInterface:
		o = s.val(x.X)
	case *ssa.TypeAssert:
		o = s.val(x.X)
	case *ssa.SliceToArrayPointer:
		o = s.val(x.X)
	case *ssa.MultiConvert:
		o = s.val(x.X)
	case *ssa.Call:
		rs := a.callResults(fn, s, x)
		s.res[x] = rs
		for _, r := range rs {
			o = o.or(r)
		}
		return o
	default:
		return org{}
	}
	if !family(v.Type()) {
		return org{}
	}
	return o
}

// loadOrigin: the origin of what addr points to.
func (a *analysis) loadOrigin(s *fnState, addr ssa.Value) org {
	switch ad := addr.(type) {
	case *ssa.Alloc:
		all := s.mem[memKey{ad, -1}]
		for k, m := range s.mem {
			if k.base == ad && k.field >= 0 {
				all = all.or(m)
			}
		}
		return all
	case *ssa.FieldAddr:
		if b := localBase(ad.X); b != nil {
			return s.mem[memKey{b, ad.Field}].or(s.mem[memKey{b, -1}])
		}
		ft := pointee(ad.Type())
		if ft == nil || !family(ft) {
			return org{}
		}
		if container(ft) {
			return org{Retained: true} // a container held by an object
		}
		return s.val(ad.X)
	case *ssa.IndexAddr:
		o := s.val(ad.X)
		if b := localBase(ad.X); b != nil {
			o = o.or(s.mem[memKey{b, -1}])
		}
		return o
	case *ssa.Global:
		if family(pointee(ad.Type())) {
			return org{Retained: true}
		}
		return org{}
	case *ssa.FreeVar:
		if env, ok := s.free[ad]; ok {
			return env
		}
		if t := pointee(ad.Type()); t != nil && family(t) {
			return org{Unknown: true}
		}
		return org{}
	}
	return s.val(addr)
}

// copySource: an allocation of a family struct filled from another value of
// the same type (whole, or field by field) is a copy of it.
func copySource(s *fnState, alloc *ssa.Alloc) (org, bool) {
	elem := pointee(alloc.Type())
	if elem == nil || !family(elem) {
		return org{}, false
	}
	n, st := structOf(elem)
	if n == nil || !entityType(n) {
		return org{}, false // a wrapper passed by value is not a structural copy
	}
	refs := alloc.Referrers()
	if refs == nil {
		return org{}, false
	}
	var src org
	found := false
	sameSource := func(v ssa.Value) (ssa.Value, bool) {
		if !types.Identical(v.Type(), elem) {
			return nil, false
		}
		switch x := v.(type) {
		case *ssa.UnOp:
			if x.Op == token.MUL {
				if _, isLocal := x.X.(*ssa.Alloc); !isLocal {
					return x.X, true
				}
			}
		case *ssa.Parameter, *ssa.Field, *ssa.Index:
			return v, true
		case *ssa.Extract:
			if _, isCall := x.Tuple.(*ssa.Call); !isCall {
				return v, true
			}
		}
		return nil, false
	}
	fresh := false
	for _, r := range *refs {
		switch x := r.(type) {
		case *ssa.Store:
			if x.Addr == alloc {
				if from, ok := sameSource(x.Val); ok {
					src, found = src.or(s.val(from)), true
				}
			}
		case *ssa.FieldAddr:
			if fr := x.Referrers(); fr != nil {
				for _, fref := range *fr {
					sto, ok := fref.(*ssa.Store)
					if !ok || sto.Addr != x {
						continue
					}
					direct := false
					if ld, ok := sto.Val.(*ssa.UnOp); ok && ld.Op == token.MUL {
						if fa, ok := ld.X.(*ssa.FieldAddr); ok && fa.Field == x.Field && types.Identical(pointee(fa.X.Type()), elem) {
							src, found, direct = src.or(s.val(fa.X)), true, true
						}
					}
					if !direct && x.Field < st.NumFields() && reference(st.Field(x.Field).Type()) && !isNilConst(sto.Val) {
						fresh = true
					}
				}
			}
		}
	}
	return src, found && fresh
}

// entityType: a storage Node or Edge, or a module type that declares CallerCopy.
func entityType(n *types.Named) bool {
	obj := n.Obj()
	if obj.Pkg() == nil || !inModulePath(obj.Pkg().Path()) {
		return false
	}
	if obj.Pkg().Path() == storagePkg && (obj.Name() == "Node" || obj.Name() == "Edge") {
		return true
	}
	return declaresCallerCopy(n)
}

func (a *analysis) callResults(fn *ssa.Function, s *fnState, call *ssa.Call) []org {
	c := call.Common()
	rts := resultTypes(c.Signature())
	out := make([]org, len(rts))
	if bi, ok := c.Value.(*ssa.Builtin); ok {
		if bi.Name() == "append" && len(out) == 1 && len(c.Args) == 2 {
			out[0] = s.val(c.Args[0]).or(s.val(c.Args[1]))
			for _, arg := range c.Args {
				if b := localBase(arg); b != nil {
					out[0] = out[0].or(s.mem[memKey{b, -1}])
				}
			}
		}
		return out
	}
	if a.receivedAcross(fn, call) {
		for i, t := range rts {
			if family(t) {
				out[i] = org{Received: true}
			}
		}
		return out
	}
	args := callArgs(c)
	callees := a.calleesOf(call)
	if len(callees) == 0 {
		idx := -1
		if p, ok := c.Value.(*ssa.Parameter); ok {
			idx = paramIndex(fn, p)
		}
		for i, t := range rts {
			if family(t) {
				if idx >= 0 {
					out[i] = org{FuncRes: bit(idx)}
				} else {
					out[i] = org{Unknown: true}
				}
			}
		}
		return out
	}
	var src org
	introduces := false
	for _, g := range callees {
		sg := a.sums[g]
		if sg == nil {
			for i, t := range rts {
				if family(t) {
					out[i].Unknown = true
				}
			}
			continue
		}
		for i := 0; i < len(out) && i < len(sg.Ret); i++ {
			ret := sg.Ret[i]
			if own, ok := a.owners[originFn(g)]; ok && ret.Params&bit(own.value) != 0 {
				// The owner decides: what it hands back is delivered.
				ret.Params &^= bit(own.value)
				ret.Delivered |= bit(own.value)
			}
			out[i] = out[i].or(a.subst(fn, s, ret, args))
			if sg.Ret[i].Copy {
				s.intro[call] = true
			}
			// A mapper copies through the function it is given here
			// (callerowned.Each(xs, copyEdge)): the call introduces the copy.
			if fr := sg.Ret[i].FuncRes; fr != 0 && a.subst(fn, s, org{FuncRes: fr}, args).Copy {
				s.intro[call] = true
			}
		}
		for j, cp := range sg.Copies {
			if cp && j < len(args) {
				src, introduces = src.or(s.val(args[j])), true
			}
		}
	}
	if introduces && src.receivedOnly() && !a.copiesOwnState(callees) {
		// A copy of a received value: the receiver's own handling.
		delete(s.intro, call)
		for i := range out {
			if out[i].Copy {
				out[i].Copy = false
				out[i].Received = true
			}
		}
		return out
	}
	if introduces && len(out) > 0 && out[0].Copy {
		s.src[call] = src
	}
	return out
}

// copiesOwnState: some callee also returns a copy of state it holds itself
// (not only of its arguments).
func (a *analysis) copiesOwnState(callees []*ssa.Function) bool {
	for _, g := range callees {
		sg := a.sums[g]
		if sg == nil {
			return true
		}
		for _, r := range sg.Ret {
			if r.Retained || r.Decoded || r.Unknown {
				return true
			}
		}
	}
	return false
}

func (a *analysis) subst(fn *ssa.Function, s *fnState, o org, args []ssa.Value) org {
	r := org{Copy: o.Copy, Retained: o.Retained, Decoded: o.Decoded, Unknown: o.Unknown, Received: o.Received}
	for j := 0; j < 64; j++ {
		if o.Params&bit(j) != 0 {
			if j < len(args) {
				r = r.or(s.val(args[j]))
			} else {
				r.Unknown = true
			}
		}
		if o.FuncRes&bit(j) != 0 {
			if j < len(args) {
				r = r.or(a.funcValueResult(fn, s, args[j], args))
			} else {
				r.Unknown = true
			}
		}
		if o.Delivered&bit(j) != 0 {
			if j < len(args) {
				v := s.val(args[j])
				r = r.or(org{Copy: v.Copy || v.Retained, Decoded: v.Decoded, Unknown: v.Unknown || v.FuncRes != 0, Delivered: v.Params | v.Delivered, Received: v.Received})
			} else {
				r.Unknown = true
			}
		}
	}
	return r
}

// funcValueResult: what calling the function value v returns, its own
// parameters standing for the elements the callee passes it.
func (a *analysis) funcValueResult(fn *ssa.Function, s *fnState, v ssa.Value, args []ssa.Value) org {
	var g *ssa.Function
	switch x := v.(type) {
	case *ssa.Function:
		g = x
	case *ssa.MakeClosure:
		g, _ = x.Fn.(*ssa.Function)
	case *ssa.UnOp:
		if mc := closureOf(x); mc != nil {
			g, _ = mc.Fn.(*ssa.Function)
		}
	case *ssa.Parameter:
		if i := paramIndex(fn, x); i >= 0 {
			return org{FuncRes: bit(i)}
		}
	}
	if g == nil {
		return org{Unknown: true}
	}
	g = a.unwrapped(g)
	sg := a.sums[g]
	if sg == nil || len(sg.Ret) == 0 {
		return org{Unknown: true}
	}
	o := sg.Ret[0]
	r := org{Copy: o.Copy, Retained: o.Retained, Decoded: o.Decoded, Unknown: o.Unknown || o.FuncRes != 0}
	if o.Params != 0 {
		for _, arg := range args {
			if _, isFunc := arg.Type().Underlying().(*types.Signature); !isFunc {
				r = r.or(s.val(arg))
			}
		}
	}
	return r
}

// ---------------------------------------------------------------- summaries

func (a *analysis) summarize(fn *ssa.Function, s *fnState) *summary {
	sum := &summary{Ret: make([]org, fn.Signature.Results().Len()), Escapes: make([]bool, len(fn.Params)), Copies: make([]bool, len(fn.Params)), FreeStores: make([]org, len(fn.FreeVars))}
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			if st, ok := ins.(*ssa.Store); ok {
				for i, fv := range fn.FreeVars {
					if st.Addr == fv {
						sum.FreeStores[i] = sum.FreeStores[i].or(s.val(st.Val))
					}
				}
			}
		}
	}
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			if ret, ok := ins.(*ssa.Return); ok {
				for i, r := range ret.Results {
					if i < len(sum.Ret) {
						sum.Ret[i] = sum.Ret[i].or(s.val(r))
					}
				}
			}
		}
	}
	for v, src := range s.src {
		for j := range sum.Copies {
			if src.Params&bit(j) != 0 {
				sum.Copies[j] = true
			}
		}
		if al, ok := v.(*ssa.Alloc); ok {
			if _, st := structOf(pointee(al.Type())); st != nil {
				if fields := sharedFields(al, st); len(fields) > 0 {
					for j := range sum.Copies {
						if src.Params&bit(j) != 0 {
							if sum.Partial == nil {
								sum.Partial = map[int][]string{}
							}
							sum.Partial[j] = fields
						}
					}
				}
			}
		}
	}
	for j := range sum.Escapes {
		for i := range sum.Ret {
			if (sum.Ret[i].Params|sum.Ret[i].Delivered)&bit(j) != 0 {
				sum.Escapes[j] = true
			}
		}
	}
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			for j := range sum.Escapes {
				if sum.Escapes[j] {
					continue
				}
				has := func(v ssa.Value) bool { return s.val(v).Params&bit(j) != 0 }
				if a.deliversWith(fn, ins, has) {
					sum.Escapes[j] = true
				}
			}
		}
	}
	return sum
}

func (a *analysis) equalSummary(x, y *summary) bool {
	if x == nil || y == nil || len(x.Ret) != len(y.Ret) || len(x.Escapes) != len(y.Escapes) {
		return false
	}
	for i := range x.Ret {
		if x.Ret[i] != y.Ret[i] {
			return false
		}
	}
	for i := range x.Escapes {
		if x.Escapes[i] != y.Escapes[i] || x.Copies[i] != y.Copies[i] {
			return false
		}
	}
	if len(x.FreeStores) != len(y.FreeStores) {
		return false
	}
	for i := range x.FreeStores {
		if x.FreeStores[i] != y.FreeStores[i] {
			return false
		}
	}
	return true
}

// deliversWith: ins hands a value accepted by has to someone outside the
// function: a return, a store into memory it does not own, a send, a non-bool
// callback, or a callee that keeps or passes on that parameter.
func (a *analysis) deliversWith(fn *ssa.Function, ins ssa.Instruction, has func(ssa.Value) bool) bool {
	switch x := ins.(type) {
	case *ssa.Return:
		for _, r := range x.Results {
			if has(r) {
				return true
			}
		}
	case *ssa.Store:
		if al, ok := x.Addr.(*ssa.Alloc); ok && a.resultSlots(fn)[al] {
			return has(x.Val) // returned at this return
		}
		return has(x.Val) && !local(storeBase(x.Addr))
	case *ssa.MapUpdate:
		return has(x.Value) && localBase(x.Map) == nil
	case *ssa.Send:
		return has(x.X)
	case ssa.CallInstruction:
		c := x.Common()
		if _, ok := c.Value.(*ssa.Builtin); ok {
			return false
		}
		args := callArgs(c)
		callees := a.calleesOf(x)
		if len(callees) == 0 {
			if c.IsInvoke() {
				for _, arg := range args {
					if has(arg) {
						return true
					}
				}
				return false
			}
			if isBoolResult(c.Signature()) {
				return false // a predicate reads its argument
			}
			for _, arg := range c.Args {
				if has(arg) {
					return true
				}
			}
			return false
		}
		for _, g := range callees {
			sg := a.sums[g]
			for j, arg := range args {
				if !has(arg) {
					continue
				}
				if sg == nil || j >= len(sg.Escapes) || sg.Escapes[j] {
					return true
				}
			}
		}
	}
	return false
}

func storeBase(addr ssa.Value) ssa.Value {
	switch a := addr.(type) {
	case *ssa.FieldAddr:
		if b := localBase(a.X); b != nil {
			return b
		}
		return a.X
	case *ssa.IndexAddr:
		if b := localBase(a.X); b != nil {
			return b
		}
		return a.X
	}
	return addr
}

// ---------------------------------------------------------------- derived sets

// derived: the values that are, or hold, the root.
// With elements false it does not follow element reads: the population as a whole.
func (a *analysis) derived(fn *ssa.Function, s *fnState, roots []ssa.Value, elements bool) map[ssa.Value]bool {
	d := map[ssa.Value]bool{}
	for _, r := range roots {
		d[r] = true
	}
	in := func(v ssa.Value) bool { return d[v] }
	for iter := 0; iter < 12; iter++ {
		changed := false
		add := func(v ssa.Value) {
			if v != nil && !d[v] {
				d[v], changed = true, true
			}
		}
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				switch x := ins.(type) {
				case *ssa.Phi:
					for _, e := range x.Edges {
						if in(e) {
							add(x)
						}
					}
				case *ssa.ChangeType:
					if in(x.X) {
						add(x)
					}
				case *ssa.MakeInterface:
					if in(x.X) {
						add(x)
					}
				case *ssa.Convert:
					if in(x.X) {
						add(x)
					}
				case *ssa.ChangeInterface:
					if in(x.X) {
						add(x)
					}
				case *ssa.TypeAssert:
					if in(x.X) {
						add(x)
					}
				case *ssa.Slice:
					if in(x.X) && !emptyReslice(x) {
						add(x)
					}
				case *ssa.Extract:
					if in(x.Tuple) && family(x.Type()) {
						if _, isNext := x.Tuple.(*ssa.Next); !isNext || elements {
							add(x)
						}
					}
				case *ssa.Field:
					if in(x.X) && family(x.Type()) {
						add(x)
					}
				case *ssa.FieldAddr:
					if in(x.X) && family(x.Type()) {
						add(x)
					}
				case *ssa.IndexAddr:
					if in(x.X) && elements {
						add(x)
					}
				case *ssa.Index:
					if in(x.X) && family(x.Type()) && elements {
						add(x)
					}
				case *ssa.Lookup:
					if in(x.X) && family(x.Type()) && elements {
						add(x)
					}
				case *ssa.Range:
					if in(x.X) && elements {
						add(x)
					}
				case *ssa.Next:
					if in(x.Iter) && elements {
						add(x)
					}
				case *ssa.UnOp:
					if x.Op == token.MUL {
						if in(x.X) && family(x.Type()) {
							add(x)
						}
						if b := localBase(x.X); b != nil && in(b) {
							add(x)
						}
						if fa, ok := x.X.(*ssa.FieldAddr); ok {
							if b := localBase(fa.X); b != nil && in(b) {
								add(x)
							}
						}
					}
				case *ssa.Store:
					if al, ok := x.Addr.(*ssa.Alloc); ok && a.resultSlots(fn)[al] {
						continue // a result slot is the return, not a holder
					}
					if in(x.Val) {
						if b := localBase(storeBase(x.Addr)); b != nil {
							add(b)
						} else if al, ok := x.Addr.(*ssa.Alloc); ok {
							add(al)
						}
					}
				case *ssa.MapUpdate:
					if in(x.Value) {
						if b := localBase(x.Map); b != nil {
							add(b)
						}
					}
				case *ssa.Call:
					c := x.Common()
					if bi, ok := c.Value.(*ssa.Builtin); ok {
						switch bi.Name() {
						case "append":
							for _, arg := range c.Args {
								if in(arg) {
									add(x)
								}
							}
						case "copy":
							if len(c.Args) == 2 && in(c.Args[1]) {
								if b := localBase(c.Args[0]); b != nil {
									add(b)
								}
							}
						}
						continue
					}
					// A closure that stores its argument into a captured
					// variable puts the root into that variable.
					if mc := closureOf(c.Value); mc != nil {
						if g, _ := mc.Fn.(*ssa.Function); g != nil && a.sums[g] != nil {
							for i, b := range mc.Bindings {
								al, ok := b.(*ssa.Alloc)
								if !ok || i >= len(a.sums[g].FreeStores) {
									continue
								}
								for j, arg := range c.Args {
									if in(arg) && a.sums[g].FreeStores[i].Params&bit(j) != 0 {
										add(al)
									}
								}
							}
						}
					}
					args := callArgs(c)
					for _, g := range a.calleesOf(x) {
						sg := a.sums[g]
						if sg == nil {
							continue
						}
						for j, arg := range args {
							if !in(arg) {
								continue
							}
							for _, r := range sg.Ret {
								if r.Params&bit(j) != 0 {
									add(x)
								}
							}
						}
					}
				}
			}
		}
		if !changed {
			break
		}
	}
	return d
}

// ---------------------------------------------------------------- drops

type dropKind int

const (
	noDrop dropKind = iota
	dropPlain
	dropError
)

// presentBranch: if cond is decided by the root (nil checks of the root or of a
// sibling result of its call, or its ok), the successor index on which the root
// exists; -1 otherwise.
func presentBranch(cond ssa.Value, d map[ssa.Value]bool, rootCall *ssa.Call) int {
	sibling := func(v ssa.Value) (bool, bool) { // (from root call, is the root value itself)
		if d[v] {
			return true, true
		}
		if ex, ok := v.(*ssa.Extract); ok && rootCall != nil && ex.Tuple == rootCall {
			return true, false
		}
		return false, false
	}
	if ok, self := sibling(cond); ok && !self {
		return 0 // the ok result: present when true
	}
	bin, ok := cond.(*ssa.BinOp)
	if !ok || (bin.Op != token.EQL && bin.Op != token.NEQ) {
		return -1
	}
	for _, pair := range [][2]ssa.Value{{bin.X, bin.Y}, {bin.Y, bin.X}} {
		if !isNilConst(pair[1]) {
			continue
		}
		from, self := sibling(pair[0])
		if !from {
			continue
		}
		// root != nil: present on true; err != nil: present on false.
		presentWhenNonNil := self
		if bin.Op == token.NEQ {
			if presentWhenNonNil {
				return 0
			}
			return 1
		}
		if presentWhenNonNil {
			return 1
		}
		return 0
	}
	return -1
}

func errorReturn(ret *ssa.Return) bool {
	if len(ret.Results) == 0 {
		return false
	}
	last := ret.Results[len(ret.Results)-1]
	// Through a result slot, the error is what this return statement stored.
	if ld, ok := last.(*ssa.UnOp); ok && ld.Op == token.MUL {
		if al, ok := ld.X.(*ssa.Alloc); ok {
			for i := len(ret.Block().Instrs) - 1; i >= 0; i-- {
				if st, ok := ret.Block().Instrs[i].(*ssa.Store); ok && st.Addr == al {
					last = st.Val
					break
				}
			}
		}
	}
	return isErrorType(last.Type()) && !isNilConst(last)
}

// mayDrop: a path from the root's definition reaches a function exit, or the
// root's own block again, without delivering anything derived from it.
func (a *analysis) mayDrop(fn *ssa.Function, s *fnState, root ssa.Instruction, d map[ssa.Value]bool, whole ...map[ssa.Value]bool) (dropKind, bool) {
	var population map[ssa.Value]bool
	if len(whole) > 0 {
		population = whole[0]
	}
	rootCall, _ := root.(*ssa.Call)
	if a.cellRoot {
		rootCall = nil // the call's own results say nothing about the cells
	}
	has := func(v ssa.Value) bool { return d[v] }
	var start *ssa.BasicBlock
	idx := -1
	if root == nil {
		// A parameter: its obligation starts at the function's entry.
		if len(fn.Blocks) == 0 {
			return noDrop, false
		}
		start = fn.Blocks[0]
	} else {
		start = root.Block()
		for i, ins := range start.Instrs {
			if ins == root {
				idx = i
			}
		}
		if idx < 0 {
			return noDrop, false
		}
	}
	storedRetention := false
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			switch x := ins.(type) {
			case *ssa.Store:
				if has(x.Val) && !local(storeBase(x.Addr)) {
					storedRetention = true
				}
			case *ssa.MapUpdate:
				if has(x.Value) && localBase(x.Map) == nil {
					storedRetention = true
				}
			}
		}
	}
	// moves: the root goes into a local container. The obligation moves with
	// it: the container must then be delivered on every path, and coming
	// round to the root again after a move is not a drop.
	moves := func(ins ssa.Instruction) bool {
		switch x := ins.(type) {
		case *ssa.Call:
			if bi, ok := x.Call.Value.(*ssa.Builtin); ok && bi.Name() == "append" {
				for _, arg := range x.Call.Args {
					if has(arg) {
						return true
					}
				}
			}
			if len(a.closureWritesTo(x, has)) > 0 {
				return true
			}
		case *ssa.Store:
			return has(x.Val) && local(storeBase(x.Addr))
		case *ssa.MapUpdate:
			return has(x.Value) && localBase(x.Map) != nil
		}
		return false
	}
	// scan: whether the instructions deliver, and whether the root has moved.
	// Pending: the element came round its loop unmoved; only delivering the
	// whole population still covers it.
	scan := func(instrs []ssa.Instruction, moved, pending bool) (bool, bool) {
		for _, ins := range instrs {
			if pending {
				if a.deliversWith(fn, ins, func(v ssa.Value) bool { return population[v] }) {
					return true, moved
				}
				continue
			}
			if a.deliversWith(fn, ins, has) {
				return true, moved
			}
			if moves(ins) {
				moved = true
			}
		}
		return false, moved
	}
	type state struct {
		b       *ssa.BasicBlock
		moved   bool
		pending bool
	}
	var first []ssa.Instruction
	if idx+1 < len(start.Instrs) {
		first = start.Instrs[idx+1:]
	}
	done, moved0 := scan(first, false, false)
	if done {
		return noDrop, storedRetention
	}
	if ret, ok := start.Instrs[len(start.Instrs)-1].(*ssa.Return); ok {
		if errorReturn(ret) {
			return dropError, storedRetention
		}
		return dropPlain, storedRetention
	}
	seen := map[state]bool{}
	var stack []state
	push := func(b *ssa.BasicBlock, moved, pending bool) {
		if iff, ok := b.Instrs[len(b.Instrs)-1].(*ssa.If); ok && !moved && !pending {
			if br := presentBranch(iff.Cond, d, rootCall); br >= 0 {
				stack = append(stack, state{b.Succs[br], moved, pending})
				return
			}
		}
		for _, succ := range b.Succs {
			stack = append(stack, state{succ, moved, pending})
		}
	}
	push(start, moved0, false)
	result := noDrop
	for len(stack) > 0 {
		st := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		b := st.b
		if b == start && root != nil && !st.pending {
			// The next iteration redefines the root: unless it moved into a
			// container, this one is dropped, or for an element of a
			// population, covered only if the population is delivered whole.
			if !st.moved {
				if population == nil {
					if result == noDrop {
						result = dropPlain
					}
				} else {
					stack = append(stack, state{start, false, true})
				}
			}
			continue
		}
		if seen[st] {
			continue
		}
		seen[st] = true
		done, moved := scan(b.Instrs, st.moved, st.pending)
		if done {
			continue
		}
		last := b.Instrs[len(b.Instrs)-1]
		if ret, ok := last.(*ssa.Return); ok {
			if errorReturn(ret) {
				if result == noDrop {
					result = dropError
				}
			} else {
				result = dropPlain
			}
			continue
		}
		if _, ok := last.(*ssa.Panic); ok {
			continue
		}
		push(b, moved, st.pending)
	}
	return result, storedRetention
}

// closureWritesTo: captured variables a call of a closure made here writes
// with a value accepted by has.
func (a *analysis) closureWritesTo(call *ssa.Call, has func(ssa.Value) bool) []*ssa.Alloc {
	mc := closureOf(call.Call.Value)
	if mc == nil {
		return nil
	}
	g, _ := mc.Fn.(*ssa.Function)
	sg := a.sums[g]
	if sg == nil {
		return nil
	}
	var out []*ssa.Alloc
	for i, b := range mc.Bindings {
		al, ok := b.(*ssa.Alloc)
		if !ok || i >= len(sg.FreeStores) {
			continue
		}
		for j, arg := range call.Call.Args {
			if has(arg) && sg.FreeStores[i].Params&bit(j) != 0 {
				out = append(out, al)
			}
		}
	}
	return out
}

// paramDrops: for each function, the parameters whose value (an entity) or
// some element (a population) it can leave undelivered after handing some of
// it on: a selection made inside the callee. A function that never hands the
// value on only reads it, and its caller still holds it. Computed after the
// summaries converge, callees first, so a parameter passed on to a callee that
// drops it is dropped here too.
func (a *analysis) paramDrops() map[*ssa.Function][]bool {
	out := map[*ssa.Function][]bool{}
	for _, fn := range a.postOrder() {
		s := a.states[fn]
		if s == nil {
			continue
		}
		drops := make([]bool, len(fn.Params))
		for j, p := range fn.Params {
			if !family(p.Type()) {
				continue
			}
			if a.passesToDropper(fn, s, p, out) {
				drops[j] = true
				continue
			}
			if container(p.Type()) {
				d := a.derived(fn, s, []ssa.Value{p}, false)
				for _, e := range a.leafElements(fn, s, d) {
					ei, _ := e.(ssa.Instruction)
					if ei == nil {
						continue
					}
					ed := a.derived(fn, s, []ssa.Value{e}, true)
					if !a.receivesAny(fn, ed) {
						continue
					}
					if k, _ := a.mayDrop(fn, s, ei, ed, d); k == dropPlain {
						drops[j] = true
					}
				}
				continue
			}
			d := a.derived(fn, s, []ssa.Value{p}, true)
			if !a.receivesAny(fn, d) {
				continue
			}
			if k, _ := a.mayDrop(fn, s, nil, d); k == dropPlain {
				drops[j] = true
			}
		}
		out[fn] = drops
	}
	return out
}

// receivesAny: some instruction hands a value of d on.
func (a *analysis) receivesAny(fn *ssa.Function, d map[ssa.Value]bool) bool {
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			if a.deliversWith(fn, ins, func(x ssa.Value) bool { return d[x] }) {
				return true
			}
		}
	}
	return false
}

// passesToDropper: fn hands parameter p to a callee that drops it.
func (a *analysis) passesToDropper(fn *ssa.Function, s *fnState, p ssa.Value, drops map[*ssa.Function][]bool) bool {
	d := a.derived(fn, s, []ssa.Value{p}, false)
	_, _, ok := a.droppingCallIn(fn, d, drops)
	return ok
}

// droppingCallIn: some call in fn hands a value of d to a callee that drops it.
func (a *analysis) droppingCallIn(fn *ssa.Function, d map[ssa.Value]bool, drops map[*ssa.Function][]bool) (*ssa.Function, int, bool) {
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			call, ok := ins.(ssa.CallInstruction)
			if !ok {
				continue
			}
			args := callArgs(call.Common())
			for _, g := range a.calleesOf(call) {
				dg := drops[g]
				for j, arg := range args {
					if j < len(dg) && dg[j] && d[arg] {
						return g, j, true
					}
				}
			}
		}
	}
	return nil, 0, false
}

// ---------------------------------------------------------------- loops

func reach(from *ssa.BasicBlock) map[*ssa.BasicBlock]bool {
	seen := map[*ssa.BasicBlock]bool{}
	stack := append([]*ssa.BasicBlock(nil), from.Succs...)
	for len(stack) > 0 {
		b := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[b] {
			continue
		}
		seen[b] = true
		stack = append(stack, b.Succs...)
	}
	return seen
}

// loopOf: the blocks on a cycle through b (empty when b is on no loop).
func loopOf(fn *ssa.Function, b *ssa.BasicBlock) map[*ssa.BasicBlock]bool {
	fwd := reach(b)
	if !fwd[b] {
		return nil
	}
	out := map[*ssa.BasicBlock]bool{b: true}
	for _, x := range fn.Blocks {
		if fwd[x] && reach(x)[b] {
			out[x] = true
		}
	}
	return out
}

// callDecided: cond depends on the result of a non-builtin call.
func callDecided(v ssa.Value, depth int) bool {
	if depth > 6 || v == nil {
		return false
	}
	switch x := v.(type) {
	case *ssa.Call:
		_, builtin := x.Call.Value.(*ssa.Builtin)
		return !builtin
	case *ssa.Extract:
		if _, ok := x.Tuple.(*ssa.Next); ok {
			return false
		}
		return callDecided(x.Tuple, depth+1)
	case *ssa.BinOp:
		return callDecided(x.X, depth+1) || callDecided(x.Y, depth+1)
	case *ssa.UnOp:
		if x.Op == token.NOT {
			return callDecided(x.X, depth+1)
		}
	case *ssa.Phi:
		for _, e := range x.Edges {
			if callDecided(e, depth+1) {
				return true
			}
		}
	}
	return false
}

// elementsOf: element reads of any value in d (index, lookup, range).
func elementsOf(fn *ssa.Function, d map[ssa.Value]bool) []ssa.Value {
	var out []ssa.Value
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			switch x := ins.(type) {
			case *ssa.UnOp:
				if ia, ok := x.X.(*ssa.IndexAddr); ok && x.Op == token.MUL && d[ia.X] && family(x.Type()) {
					out = append(out, x)
				}
			case *ssa.Index:
				if d[x.X] && family(x.Type()) {
					out = append(out, x)
				}
			case *ssa.Lookup:
				if d[x.X] && family(x.Type()) && !x.CommaOk {
					out = append(out, x)
				}
			case *ssa.Extract:
				if nx, ok := x.Tuple.(*ssa.Next); ok && x.Index == 2 {
					if rg, ok := nx.Iter.(*ssa.Range); ok && d[rg.X] && family(x.Type()) {
						out = append(out, x)
					}
				}
			}
		}
	}
	return out
}

// leafElements: the entity elements of a population, through populations of
// populations (an array of slices ranged in turn).
func (a *analysis) leafElements(fn *ssa.Function, s *fnState, d map[ssa.Value]bool) []ssa.Value {
	var out []ssa.Value
	seen := map[ssa.Value]bool{}
	var walk func(d map[ssa.Value]bool, depth int)
	walk = func(d map[ssa.Value]bool, depth int) {
		for _, e := range elementsOf(fn, d) {
			if seen[e] {
				continue
			}
			seen[e] = true
			if container(e.Type()) && depth < 4 {
				walk(a.derived(fn, s, []ssa.Value{e}, false), depth+1)
				continue
			}
			out = append(out, e)
		}
	}
	walk(d, 0)
	return out
}

// overwrites: for each function, the entity parameters whose reference fields
// (labels, properties, vectors, metadata) it overwrites, itself or through a
// callee. Overwriting a field of a copy discards what the copy copied there.
func (a *analysis) overwrites() map[*ssa.Function][]bool {
	out := map[*ssa.Function][]bool{}
	for _, fn := range a.postOrder() {
		s := a.states[fn]
		if s == nil {
			continue
		}
		w := make([]bool, len(fn.Params))
		for j, p := range fn.Params {
			n, _ := structOf(pointee(p.Type()))
			if n == nil || !entityType(n) {
				continue
			}
			d := a.derived(fn, s, []ssa.Value{p}, false)
			for _, b := range fn.Blocks {
				for _, ins := range b.Instrs {
					switch x := ins.(type) {
					case *ssa.Store:
						fa, ok := x.Addr.(*ssa.FieldAddr)
						if !ok || !d[fa.X] {
							continue
						}
						_, st := structOf(pointee(fa.X.Type()))
						// Replacing a pointer to a record no function writes
						// after construction (the shared runtime record)
						// discards nothing the copy copied.
						if st == nil {
							continue
						}
						if ft := st.Field(fa.Field).Type(); !reference(ft) || immutableReferent(ft) {
							continue
						}
						w[j] = true
					case ssa.CallInstruction:
						args := callArgs(x.Common())
						for _, g := range a.calleesOf(x) {
							for m, arg := range args {
								if m < len(out[g]) && out[g][m] && d[arg] {
									w[j] = true
								}
							}
						}
					}
				}
			}
		}
		out[fn] = w
	}
	return out
}

// copiesToCallback: for each function, the function-valued parameters it
// calls with a copy, itself or by handing the parameter to a callee that does.
func (a *analysis) copiesToCallback() map[*ssa.Function][]bool {
	out := map[*ssa.Function][]bool{}
	for _, fn := range a.postOrder() {
		s := a.states[fn]
		if s == nil {
			continue
		}
		cb := make([]bool, len(fn.Params))
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				call, ok := ins.(ssa.CallInstruction)
				if !ok {
					continue
				}
				c := call.Common()
				if pv, ok := c.Value.(*ssa.Parameter); ok {
					if k := paramIndex(fn, pv); k >= 0 {
						for _, arg := range c.Args {
							if family(arg.Type()) && s.val(arg).Copy {
								cb[k] = true
							}
						}
					}
				}
				args := callArgs(c)
				for _, g := range a.calleesOf(call) {
					for m, arg := range args {
						if m >= len(out[g]) || !out[g][m] {
							continue
						}
						if k := paramIndex(fn, arg); k >= 0 {
							cb[k] = true
						}
					}
				}
			}
		}
		out[fn] = cb
	}
	return out
}

// funcOf: the function a function value denotes, when it is known here.
func (a *analysis) funcOf(v ssa.Value) *ssa.Function {
	switch x := v.(type) {
	case *ssa.Function:
		return a.unwrapped(x)
	case *ssa.MakeClosure:
		if g, ok := x.Fn.(*ssa.Function); ok {
			return a.unwrapped(g)
		}
	case *ssa.ChangeType:
		return a.funcOf(x.X)
	case *ssa.UnOp:
		if mc := closureOf(x); mc != nil {
			if g, ok := mc.Fn.(*ssa.Function); ok {
				return a.unwrapped(g)
			}
		}
	}
	return nil
}

func anyTrue(xs []bool) bool {
	for _, x := range xs {
		if x {
			return true
		}
	}
	return false
}

// emptyReslice: x[:0], an empty buffer over x's storage.
func emptyReslice(x *ssa.Slice) bool {
	c, ok := x.High.(*ssa.Const)
	return ok && c.Value != nil && c.Value.String() == "0"
}

// callbackWrites: captured variables written by closures passed as arguments
// to call (callbacks the callee invokes), with what the closure itself puts
// there; what the callee hands the closure is not known here.
func (a *analysis) callbackWrites(call ssa.CallInstruction) map[memKey]org {
	var out map[memKey]org
	for _, arg := range callArgs(call.Common()) {
		mc := closureOf(arg)
		if mc == nil {
			continue
		}
		g, _ := mc.Fn.(*ssa.Function)
		sg := a.sums[g]
		if sg == nil {
			continue
		}
		for i, b := range mc.Bindings {
			al, ok := b.(*ssa.Alloc)
			if !ok || i >= len(sg.FreeStores) {
				continue
			}
			o := sg.FreeStores[i]
			o.Params, o.FuncRes = 0, 0
			if o.empty() {
				continue
			}
			if out == nil {
				out = map[memKey]org{}
			}
			k := memKey{al, -1}
			out[k] = out[k].or(o)
		}
	}
	return out
}

// callbackCopyCells: the captured variables a callback passed to call fills
// with copies it makes.
func (a *analysis) callbackCopyCells(call ssa.CallInstruction) []ssa.Value {
	var out []ssa.Value
	for k, o := range a.callbackWrites(call) {
		if o.Copy {
			out = append(out, k.base)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Pos() < out[j].Pos() })
	return out
}

// closureOf: the closure a function value is, directly or loaded from a local
// variable cell that only ever holds that closure.
func closureOf(v ssa.Value) *ssa.MakeClosure {
	switch x := v.(type) {
	case *ssa.MakeClosure:
		return x
	case *ssa.UnOp:
		al, ok := x.X.(*ssa.Alloc)
		if !ok || x.Op != token.MUL || al.Referrers() == nil {
			return nil
		}
		var only *ssa.MakeClosure
		for _, r := range *al.Referrers() {
			st, ok := r.(*ssa.Store)
			if !ok || st.Addr != al {
				continue
			}
			mc, ok := st.Val.(*ssa.MakeClosure)
			if !ok || (only != nil && only != mc) {
				return nil
			}
			only = mc
		}
		return only
	}
	return nil
}

// resultSlots: the allocations a function returns through (named results, or
// the results of a function with a defer): stored at each return statement and
// loaded only by its returns.
func (a *analysis) resultSlots(fn *ssa.Function) map[*ssa.Alloc]bool {
	if m, ok := a.slots[fn]; ok {
		return m
	}
	m := map[*ssa.Alloc]bool{}
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			al, ok := ins.(*ssa.Alloc)
			if !ok || al.Heap || al.Referrers() == nil {
				continue
			}
			slot, returned := true, false
			for _, r := range *al.Referrers() {
				switch x := r.(type) {
				case *ssa.Store:
					if x.Addr != al {
						slot = false
					}
				case *ssa.UnOp:
					if x.Op != token.MUL || x.Referrers() == nil {
						slot = false
						continue
					}
					for _, u := range *x.Referrers() {
						if _, ok := u.(*ssa.Return); ok {
							returned = true
						} else {
							slot = false
						}
					}
				default:
					slot = false
				}
			}
			if slot && returned {
				m[al] = true
			}
		}
	}
	if a.slots == nil {
		a.slots = map[*ssa.Function]map[*ssa.Alloc]bool{}
	}
	a.slots[fn] = m
	return m
}

// returnPos: the position of the return statement a result-slot store belongs
// to (the return that ends its block), else the store's own.
func returnPos(st *ssa.Store) token.Pos {
	if last, ok := st.Block().Instrs[len(st.Block().Instrs)-1].(*ssa.Return); ok && last.Pos().IsValid() {
		return last.Pos()
	}
	if st.Pos().IsValid() {
		return st.Pos()
	}
	return st.Val.Pos()
}

// returnInputs: what each result of fn can be, looking through result slots.
func (a *analysis) returnInputs(fn *ssa.Function) [][]ssa.Value {
	slots := a.resultSlots(fn)
	var rets [][]ssa.Value
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			ret, ok := ins.(*ssa.Return)
			if !ok {
				continue
			}
			for i, res := range ret.Results {
				for len(rets) <= i {
					rets = append(rets, nil)
				}
				if ld, ok := res.(*ssa.UnOp); ok && ld.Op == token.MUL {
					if al, ok := ld.X.(*ssa.Alloc); ok && slots[al] {
						for _, r := range *al.Referrers() {
							if st, ok := r.(*ssa.Store); ok {
								rets[i] = append(rets[i], st.Val)
							}
						}
						continue
					}
				}
				rets[i] = append(rets[i], res)
			}
		}
	}
	return rets
}

// owner: the single function that decides, for one entity type, whether a
// value is handed back unchanged or copied; value is the parameter it decides
// on, gate how it decides.
type owner struct {
	entity *types.Named
	value  int
	gate   gate
}

// gate: the condition under which the owner hands its value back unchanged:
// a bool parameter (field < 0) or a bool field of a struct parameter, equal to
// unchanged.
type gate struct {
	param, field int
	unchanged    bool
	known        bool
}

// gateSpec: a struct type whose field gateField decides the owner's branch for
// the value in field valueField, unchanged when equal to unchanged.
type gateSpec struct {
	strct                 *types.Named
	gateField, valueField int
	unchanged             bool
	owner                 *ssa.Function
}

// mergeOf: the parameter whose value fn returns either unchanged or copied,
// and the entity type returned; ok when fn is such a function.
func (a *analysis) mergeOf(fn *ssa.Function, s *fnState) (int, *types.Named, ssa.Value, ssa.Value, bool) {
	for _, in := range a.returnInputs(fn) {
		for _, x := range in {
			ox := s.val(x)
			if ox.empty() || ox.Copy || !family(x.Type()) || ox.Params == 0 {
				continue
			}
			for _, y := range in {
				src, ok := s.src[y]
				if !ok || !src.overlaps(ox) {
					continue
				}
				n, _ := structOf(pointee(x.Type()))
				if n == nil || !entityType(n) {
					continue
				}
				for j := range fn.Params {
					if ox.Params&bit(j) != 0 {
						return j, n, x, y, true
					}
				}
			}
		}
	}
	return 0, nil, nil, nil, false
}

// paramOf: the parameter a value is, directly or through the local the
// function spilled it into.
func paramOf(fn *ssa.Function, v ssa.Value) int {
	if i := paramIndex(fn, v); i >= 0 {
		return i
	}
	if ld, ok := v.(*ssa.UnOp); ok && ld.Op == token.MUL {
		v = ld.X
	}
	al, ok := v.(*ssa.Alloc)
	if !ok || al.Referrers() == nil {
		return -1
	}
	for _, r := range *al.Referrers() {
		if st, ok := r.(*ssa.Store); ok && st.Addr == al {
			if i := paramIndex(fn, st.Val); i >= 0 {
				return i
			}
		}
	}
	return -1
}

// gateOf: the parameter (and field) a condition reads, and whether it is negated.
func gateOf(fn *ssa.Function, v ssa.Value, neg bool, depth int) (gate, bool) {
	if depth > 4 {
		return gate{}, false
	}
	switch x := v.(type) {
	case *ssa.Parameter:
		if i := paramIndex(fn, x); i >= 0 {
			return gate{param: i, field: -1, unchanged: !neg, known: true}, true
		}
	case *ssa.UnOp:
		switch x.Op {
		case token.NOT:
			return gateOf(fn, x.X, !neg, depth+1)
		case token.MUL:
			if fa, ok := x.X.(*ssa.FieldAddr); ok {
				if i := paramOf(fn, fa.X); i >= 0 {
					return gate{param: i, field: fa.Field, unchanged: !neg, known: true}, true
				}
			}
		}
	case *ssa.Field:
		if i := paramOf(fn, x.X); i >= 0 {
			return gate{param: i, field: x.Field, unchanged: !neg, known: true}, true
		}
	}
	return gate{}, false
}

// ownerGate: the branch in the owner that leads only to the unchanged return
// on one side and only to the copy on the other.
func (a *analysis) ownerGate(fn *ssa.Function, unchanged, copied ssa.Value) gate {
	returnsOf := func(b *ssa.BasicBlock) (bool, bool) {
		seen := reach(b)
		seen[b] = true
		u, c := false, false
		for bb := range seen {
			ret, ok := bb.Instrs[len(bb.Instrs)-1].(*ssa.Return)
			if !ok {
				continue
			}
			for _, r := range ret.Results {
				if r == unchanged {
					u = true
				}
				if r == copied {
					c = true
				}
			}
		}
		return u, c
	}
	for _, b := range fn.Blocks {
		iff, ok := b.Instrs[len(b.Instrs)-1].(*ssa.If)
		if !ok {
			continue
		}
		u0, c0 := returnsOf(b.Succs[0])
		u1, c1 := returnsOf(b.Succs[1])
		var trueUnchanged bool
		switch {
		case u0 && !c0 && c1 && !u1:
			trueUnchanged = true
		case u1 && !c1 && c0 && !u0:
			trueUnchanged = false
		default:
			continue
		}
		if g, ok := gateOf(fn, iff.Cond, !trueUnchanged, 0); ok {
			// gateOf reports the value equal to which the gate reads true;
			// unchanged when that value is read on the unchanged side.
			return g
		}
	}
	return gate{}
}

// findOwners: per entity type, the one function that returns a value of it or
// a copy of that value. A type with two such functions has no owner.
func (a *analysis) findOwners() map[*ssa.Function]owner {
	byType := map[*types.Named][]owner{}
	fnOf := map[*types.Named][]*ssa.Function{}
	for _, fn := range a.fns {
		s := a.states[fn]
		if s == nil {
			continue
		}
		j, n, x, y, ok := a.mergeOf(fn, s)
		if !ok {
			continue
		}
		o := owner{entity: n, value: j, gate: a.ownerGate(fn, x, y)}
		fn = originFn(fn)
		dup := false
		for _, f := range fnOf[n] {
			if f == fn {
				dup = true
			}
		}
		if !dup {
			byType[n] = append(byType[n], o)
			fnOf[n] = append(fnOf[n], fn)
		}
	}
	out := map[*ssa.Function]owner{}
	for n, os := range byType {
		for i, o := range os {
			a.candidates = append(a.candidates, fmt.Sprintf("%s: %s, parameter %d, gate known %v, field %d, %d candidates for the type", n.Obj().Name(), strings.TrimPrefix(fullName(fnOf[n][i]), module+"/"), o.value, o.gate.known, o.gate.field, len(os)))
		}
		if len(os) == 1 && os[0].gate.known {
			out[fnOf[n][0]] = os[0]
		}
	}
	sort.Strings(a.candidates)
	return out
}

// constBool: the value of a boolean constant.
func constBool(v ssa.Value) (bool, bool) {
	c, ok := v.(*ssa.Const)
	if !ok || c.Value == nil {
		return false, false
	}
	switch c.Value.String() {
	case "true":
		return true, true
	case "false":
		return false, true
	}
	return false, false
}

// ownerChecks: condition 2. A retained value reaches the owner's unchanged
// branch: passed with a constant gate set to unchanged, or built into a gate
// struct whose gate field says unchanged (directly, or through a helper that
// labels its parameter so). Returns the reports as (function, position, detail).
func (a *analysis) ownerChecks() []ownerReport {
	var out []ownerReport
	var specs []gateSpec
	// The owner's own field gate, and field gates its callers pass to a bool gate.
	for ofn, o := range a.owners {
		if o.gate.field >= 0 {
			if n, _ := structOf(derefType(ofn.Params[o.gate.param].Type())); n != nil {
				specs = append(specs, gateSpec{strct: n, gateField: o.gate.field, valueField: a.valueField(ofn, o), unchanged: o.gate.unchanged, owner: ofn})
			}
		}
	}
	for _, fn := range a.fns {
		s := a.states[fn]
		if s == nil {
			continue
		}
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				call, ok := ins.(ssa.CallInstruction)
				if !ok {
					continue
				}
				args := callArgs(call.Common())
				for _, g := range a.calleesOf(call) {
					o, ok := a.owners[originFn(g)]
					if !ok || o.gate.field >= 0 || o.gate.param >= len(args) || o.value >= len(args) {
						continue
					}
					if v, ok := constBool(args[o.gate.param]); ok {
						if v == o.gate.unchanged && s.val(args[o.value]).Retained {
							out = append(out, ownerReport{fn, call.Pos(), "a retained value reaches the unchanged branch of " + strings.TrimPrefix(fullName(g), module+"/")})
						}
						continue
					}
					// A gate read from a struct field, with the value from the same struct.
					if gs, ok := structGate(args[o.gate.param], args[o.value], o.gate.unchanged); ok {
						gs.owner = g
						specs = append(specs, gs)
					}
				}
			}
		}
	}
	if len(specs) == 0 {
		return out
	}
	// Labels: built structs whose gate field says unchanged around a value.
	marks := map[*ssa.Function]map[int]bool{}
	marksIf := map[*ssa.Function][][3]int{} // value param, gate param, unchanged (1/0)
	for _, fn := range a.postOrder() {
		s := a.states[fn]
		if s == nil {
			continue
		}
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				switch x := ins.(type) {
				case *ssa.Alloc:
					n, _ := structOf(pointee(x.Type()))
					for _, sp := range specs {
						if n == nil || n != sp.strct {
							continue
						}
						gateVal, valueOrg, gateParam := false, org{}, -1
						gateKnown := true
						if x.Referrers() != nil {
							for _, r := range *x.Referrers() {
								fa, ok := r.(*ssa.FieldAddr)
								if !ok || fa.Referrers() == nil {
									continue
								}
								for _, u := range *fa.Referrers() {
									st, ok := u.(*ssa.Store)
									if !ok || st.Addr != fa {
										continue
									}
									switch fa.Field {
									case sp.gateField:
										if v, ok := constBool(st.Val); ok {
											gateVal = v
										} else if k := paramIndex(fn, st.Val); k >= 0 {
											gateParam = k
										} else {
											gateKnown = false
										}
									case sp.valueField:
										valueOrg = valueOrg.or(s.val(st.Val))
									}
								}
							}
						}
						if !gateKnown {
							continue
						}
						if gateParam >= 0 {
							for j := range fn.Params {
								if valueOrg.Params&bit(j) != 0 {
									u := 0
									if sp.unchanged {
										u = 1
									}
									marksIf[fn] = append(marksIf[fn], [3]int{j, gateParam, u})
								}
							}
							continue
						}
						if gateVal != sp.unchanged {
							continue
						}
						if valueOrg.Retained {
							out = append(out, ownerReport{fn, x.Pos(), "a retained value is labelled for the unchanged branch of " + strings.TrimPrefix(fullName(sp.owner), module+"/")})
						}
						for j := range fn.Params {
							if valueOrg.Params&bit(j) != 0 {
								if marks[fn] == nil {
									marks[fn] = map[int]bool{}
								}
								marks[fn][j] = true
							}
						}
					}
				case ssa.CallInstruction:
					args := callArgs(x.Common())
					for _, g := range a.calleesOf(x) {
						label := func(j int) {
							if j >= len(args) {
								return
							}
							v := s.val(args[j])
							if v.Retained {
								out = append(out, ownerReport{fn, x.Pos(), "a retained value is labelled for an owner's unchanged branch by " + strings.TrimPrefix(fullName(g), module+"/")})
							}
							for k := range fn.Params {
								if v.Params&bit(k) != 0 {
									if marks[fn] == nil {
										marks[fn] = map[int]bool{}
									}
									marks[fn][k] = true
								}
							}
						}
						for j := range marks[g] {
							label(j)
						}
						for _, m := range marksIf[g] {
							if m[1] >= len(args) {
								continue
							}
							if v, ok := constBool(args[m[1]]); ok {
								if v == (m[2] == 1) {
									label(m[0])
								}
							} else if k := paramIndex(fn, args[m[1]]); k >= 0 && m[0] < len(args) {
								for j := range fn.Params {
									if s.val(args[m[0]]).Params&bit(j) != 0 {
										marksIf[fn] = append(marksIf[fn], [3]int{j, k, m[2]})
									}
								}
							}
						}
					}
				}
			}
		}
	}
	return out
}

type ownerReport struct {
	fn     *ssa.Function
	pos    token.Pos
	detail string
}

// valueField: the struct field whose value the owner hands back unchanged.
func (a *analysis) valueField(fn *ssa.Function, o owner) int {
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			switch x := ins.(type) {
			case *ssa.Field:
				if paramOf(fn, x.X) == o.value && family(x.Type()) {
					return x.Field
				}
			case *ssa.FieldAddr:
				if paramOf(fn, x.X) == o.value && family(pointee(x.Type())) {
					return x.Field
				}
			}
		}
	}
	return -1
}

// structGate: a gate argument read from a struct value's field, with the value
// argument read from another field of the same struct.
func structGate(gateArg, valueArg ssa.Value, unchanged bool) (gateSpec, bool) {
	neg := false
	for {
		u, ok := gateArg.(*ssa.UnOp)
		if !ok || u.Op != token.NOT {
			break
		}
		neg, gateArg = !neg, u.X
	}
	field := func(v ssa.Value) (ssa.Value, int, bool) {
		switch x := v.(type) {
		case *ssa.Field:
			return x.X, x.Field, true
		case *ssa.UnOp:
			if fa, ok := x.X.(*ssa.FieldAddr); ok && x.Op == token.MUL {
				return fa.X, fa.Field, true
			}
		}
		return nil, 0, false
	}
	gx, gf, ok1 := field(gateArg)
	vx, vf, ok2 := field(valueArg)
	if !ok1 || !ok2 || gx != vx {
		return gateSpec{}, false
	}
	n, _ := structOf(derefType(gx.Type()))
	if n == nil {
		return gateSpec{}, false
	}
	return gateSpec{strct: n, gateField: gf, valueField: vf, unchanged: unchanged != neg}, true
}

func derefType(t types.Type) types.Type {
	if p := pointee(t); p != nil {
		return p
	}
	return t
}

// ---------------------------------------------------------------- read scope

type fieldKey struct {
	strct *types.Named
	field int
}

type readScope struct {
	ret, param, cb, cell map[*ssa.Function][]bool
	field                map[fieldKey]bool
	under                map[*ssa.Function]bool // runs under a read exit
	exitOf               map[*ssa.Function]*ssa.Function
}

func flags(m map[*ssa.Function][]bool, fn *ssa.Function, n int) []bool {
	if m[fn] == nil {
		m[fn] = make([]bool, n)
	}
	return m[fn]
}

// entityValue: an entity (or pointer to one), a slice, array, map or channel
// of entity values, an iterator yielding them, or a struct holding one of
// those directly in a field. A struct that holds entities only transitively (a
// service, a database handle) is not one.
func entityValue(t types.Type, depth int) bool {
	if depth > 3 {
		return false
	}
	t = types.Unalias(t)
	if p := pointee(t); p != nil {
		t = p
	}
	switch u := t.(type) {
	case *types.Named:
		if entityType(u) {
			return true
		}
		if st, ok := u.Underlying().(*types.Struct); ok && depth == 0 {
			for i := 0; i < st.NumFields(); i++ {
				if entityValue(st.Field(i).Type(), depth+1) {
					return true
				}
			}
			return false
		}
		return entityValue(u.Underlying(), depth)
	case *types.Slice:
		return entityValue(u.Elem(), depth+1)
	case *types.Array:
		return entityValue(u.Elem(), depth+1)
	case *types.Map:
		return entityValue(u.Elem(), depth+1)
	case *types.Chan:
		return entityValue(u.Elem(), depth+1)
	case *types.Signature:
		// An iterator: func(yield func(..entity..) bool).
		for i := 0; i < u.Params().Len(); i++ {
			if y, ok := u.Params().At(i).Type().Underlying().(*types.Signature); ok {
				for k := 0; k < y.Params().Len(); k++ {
					if entityValue(y.Params().At(k).Type(), depth+1) {
						return true
					}
				}
			}
		}
	}
	return false
}

// exitResult and exitCallback: a public read exit hands entity values out
// through its results or its entity callbacks.
func exitResult(fn *ssa.Function, i int) bool {
	rs := fn.Signature.Results()
	return exported(fn) && i < rs.Len() && entityValue(rs.At(i).Type(), 0)
}

func exitCallback(fn *ssa.Function, k int) bool {
	if !exported(fn) || k >= len(fn.Params) {
		return false
	}
	sig, ok := fn.Params[k].Type().Underlying().(*types.Signature)
	if !ok {
		return false
	}
	for i := 0; i < sig.Params().Len(); i++ {
		if entityValue(sig.Params().At(i).Type(), 0) {
			return true
		}
	}
	return false
}

func isExit(fn *ssa.Function) bool {
	for i := 0; i < fn.Signature.Results().Len(); i++ {
		if exitResult(fn, i) {
			return true
		}
	}
	for k := range fn.Params {
		if exitCallback(fn, k) {
			return true
		}
	}
	return false
}

// fieldOf: the struct field an address names, when its base is not local.
func fieldOf(addr ssa.Value) (fieldKey, bool) {
	fa, ok := addr.(*ssa.FieldAddr)
	if !ok || localBase(fa.X) != nil {
		return fieldKey{}, false
	}
	n, _ := structOf(pointee(fa.X.Type()))
	if n == nil {
		return fieldKey{}, false
	}
	return fieldKey{n, fa.Field}, true
}

// containerField: the retention field a non-local container value was loaded from.
func containerField(v ssa.Value) (fieldKey, bool) {
	if ld, ok := v.(*ssa.UnOp); ok && ld.Op == token.MUL {
		return fieldOf(ld.X)
	}
	return fieldKey{}, false
}

// reachSet: the values of fn that can reach a public read exit, backwards
// from its sinks.
func (a *analysis) reachSet(fn *ssa.Function, rs *readScope) map[ssa.Value]bool {
	set := map[ssa.Value]bool{}
	add := func(v ssa.Value) bool {
		if v == nil || set[v] {
			return false
		}
		if _, ok := v.(*ssa.Const); ok {
			return false
		}
		set[v] = true
		return true
	}
	slots := a.resultSlots(fn)
	free := map[*ssa.FreeVar]int{}
	for i, fv := range fn.FreeVars {
		free[fv] = i
	}
	sinkCall := func(c *ssa.CallCommon, call ssa.CallInstruction) {
		args := callArgs(c)
		if _, builtin := c.Value.(*ssa.Builtin); !builtin {
			if _, isParam := c.Value.(*ssa.Parameter); !isParam {
				// An unresolved or synthetic callee: not followed, so kept.
				opaque := len(a.calleesOf(call)) == 0
				for _, g := range a.calleesOf(call) {
					if a.states[g] == nil {
						opaque = true
					}
				}
				if opaque {
					for _, arg := range args {
						if family(arg.Type()) || pointee(arg.Type()) != nil {
							add(arg)
						}
					}
				}
			}
		}
		if pv, ok := c.Value.(*ssa.Parameter); ok {
			if k := paramIndex(fn, pv); k >= 0 && (exitCallback(fn, k) || flags(rs.cb, fn, len(fn.Params))[k]) {
				for _, arg := range c.Args {
					add(arg)
				}
			}
		}
		for _, g := range a.calleesOf(call) {
			pg := rs.param[g]
			for j, arg := range args {
				if j < len(pg) && pg[j] {
					add(arg)
				}
			}
		}
	}
	// Sinks.
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			switch x := ins.(type) {
			case *ssa.Return:
				for i, r := range x.Results {
					if exitResult(fn, i) || flags(rs.ret, fn, len(x.Results))[i] {
						add(r)
					}
				}
			case *ssa.Store:
				if fv, ok := x.Addr.(*ssa.FreeVar); ok {
					if i, ok := free[fv]; ok && flags(rs.cell, fn, len(fn.FreeVars))[i] {
						add(x.Val)
					}
				}
				if k, ok := fieldOf(storeBase(x.Addr)); ok && rs.under[fn] && rs.field[k] {
					add(x.Val)
				}
				if fa, ok := x.Addr.(*ssa.FieldAddr); ok {
					if k, ok := fieldOf(fa); ok && rs.under[fn] && rs.field[k] {
						add(x.Val)
					}
				}
			case *ssa.MapUpdate:
				if k, ok := containerField(x.Map); ok && rs.under[fn] && rs.field[k] {
					add(x.Value)
				}
			case ssa.CallInstruction:
				sinkCall(x.Common(), x)
			case *ssa.MakeClosure:
				// A bound method value, or a closure that escapes into memory
				// (stored, not only called or passed): its variables are not
				// followed, so they are kept.
				g, _ := x.Fn.(*ssa.Function)
				escapes := g == nil || g.Synthetic != ""
				if x.Referrers() != nil {
					for _, u := range *x.Referrers() {
						switch u.(type) {
						case *ssa.Store, *ssa.MapUpdate, *ssa.MakeInterface:
							escapes = true
						}
					}
				}
				if escapes {
					for _, bnd := range x.Bindings {
						add(bnd)
					}
				}
			}
		}
	}
	// Backwards to a fixpoint.
	for iter := 0; iter < 24; iter++ {
		changed := false
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				switch x := ins.(type) {
				case *ssa.Store:
					if al, ok := x.Addr.(*ssa.Alloc); ok && slots[al] {
						// A result slot: the return statement's value.
						for _, r := range *al.Referrers() {
							if ld, ok := r.(*ssa.UnOp); ok && set[ld] {
								changed = add(x.Val) || changed
							}
						}
						continue
					}
					if base := localBase(storeBase(x.Addr)); base != nil && set[base] {
						changed = add(x.Val) || changed
					}
					if al, ok := x.Addr.(*ssa.Alloc); ok && set[al] {
						changed = add(x.Val) || changed
					}
				case *ssa.MapUpdate:
					if base := localBase(x.Map); base != nil && set[base] {
						changed = add(x.Value) || changed
					}
				}
				v, ok := ins.(ssa.Value)
				if !ok || !set[v] {
					continue
				}
				switch x := v.(type) {
				case *ssa.Phi:
					for _, e := range x.Edges {
						changed = add(e) || changed
					}
				case *ssa.ChangeType:
					changed = add(x.X) || changed
				case *ssa.MakeInterface:
					changed = add(x.X) || changed
				case *ssa.Convert:
					changed = add(x.X) || changed
				case *ssa.ChangeInterface:
					changed = add(x.X) || changed
				case *ssa.TypeAssert:
					changed = add(x.X) || changed
				case *ssa.Slice:
					changed = add(x.X) || changed
				case *ssa.Extract:
					changed = add(x.Tuple) || changed
				case *ssa.Field:
					changed = add(x.X) || changed
				case *ssa.FieldAddr:
					changed = add(x.X) || changed
				case *ssa.IndexAddr:
					changed = add(x.X) || changed
				case *ssa.Index:
					changed = add(x.X) || changed
				case *ssa.Lookup:
					changed = add(x.X) || changed
				case *ssa.Range:
					changed = add(x.X) || changed
				case *ssa.Next:
					changed = add(x.Iter) || changed
				case *ssa.UnOp:
					if x.Op == token.MUL {
						changed = add(x.X) || changed
						if b := localBase(storeBase(x.X)); b != nil {
							changed = add(b) || changed
						}
					}
				case *ssa.Call:
					c := x.Common()
					if bi, ok := c.Value.(*ssa.Builtin); ok {
						if bi.Name() == "append" {
							for _, arg := range c.Args {
								changed = add(arg) || changed
							}
						}
						continue
					}
					args := callArgs(c)
					for _, g := range a.calleesOf(x) {
						sg := a.sums[g]
						if sg == nil {
							continue
						}
						for _, r := range sg.Ret {
							for j, arg := range args {
								if (r.Params|r.Delivered)&bit(j) != 0 {
									changed = add(arg) || changed
								}
							}
						}
					}
				}
			}
		}
		if !changed {
			break
		}
	}
	return set
}

// scopeByFlow selects the replaced value-flow scope (method history only).
const scopeByFlow = false

// computeReadScope: the flow facts to a fixpoint, then each function's reach set.
func (a *analysis) computeReadScope() {
	rs := &readScope{ret: map[*ssa.Function][]bool{}, param: map[*ssa.Function][]bool{}, cb: map[*ssa.Function][]bool{}, cell: map[*ssa.Function][]bool{}, field: map[fieldKey]bool{}, under: map[*ssa.Function]bool{}, exitOf: map[*ssa.Function]*ssa.Function{}}
	// Functions that run under a read exit.
	var queue []*ssa.Function
	for _, fn := range a.fns {
		if isExit(fn) {
			rs.under[fn] = true
			rs.exitOf[fn] = fn
			queue = append(queue, fn)
		}
	}
	for len(queue) > 0 {
		fn := queue[0]
		queue = queue[1:]
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				if call, ok := ins.(ssa.CallInstruction); ok {
					for _, g := range a.calleesOf(call) {
						if !rs.under[g] {
							rs.under[g] = true
							rs.exitOf[g] = rs.exitOf[fn]
							queue = append(queue, g)
						}
					}
				}
				if mc, ok := ins.(*ssa.MakeClosure); ok {
					if g, ok := mc.Fn.(*ssa.Function); ok && !rs.under[g] {
						rs.under[g] = true
						rs.exitOf[g] = rs.exitOf[fn]
						queue = append(queue, g)
					}
				}
			}
		}
	}
	a.scope = rs
	if !scopeByFlow {
		return // call-graph scope: the flow fixpoint below is not needed
	}
	set := func(m map[*ssa.Function][]bool, fn *ssa.Function, n, i int) bool {
		f := flags(m, fn, n)
		if i >= len(f) || f[i] {
			return false
		}
		f[i] = true
		return true
	}
	sets := map[*ssa.Function]map[ssa.Value]bool{}
	for round := 0; round < 40; round++ {
		changed := false
		for _, fn := range a.fns {
			r := a.reachSet(fn, rs)
			sets[fn] = r
			for j, p := range fn.Params {
				if sg := a.sums[fn]; sg != nil && j < len(sg.Copies) && sg.Copies[j] {
					continue // a copier hands out its copy, never its source
				}
				if r[p] {
					changed = set(rs.param, fn, len(fn.Params), j) || changed
				}
			}
			for _, b := range fn.Blocks {
				for _, ins := range b.Instrs {
					switch x := ins.(type) {
					case *ssa.MakeClosure:
						g, ok := x.Fn.(*ssa.Function)
						if !ok {
							continue
						}
						for i, bnd := range x.Bindings {
							if r[bnd] {
								changed = set(rs.cell, g, len(g.FreeVars), i) || changed
							}
						}
						if x.Referrers() != nil {
							for _, u := range *x.Referrers() {
								switch u.(type) {
								case *ssa.Store, *ssa.MapUpdate, *ssa.MakeInterface:
									// Escaping closures are not followed: kept.
									for i := 0; i < g.Signature.Results().Len(); i++ {
										changed = set(rs.ret, g, g.Signature.Results().Len(), i) || changed
									}
									for k := range g.Params {
										changed = set(rs.param, g, len(g.Params), k) || changed
									}
								}
							}
						}
					case *ssa.UnOp:
						if x.Op == token.MUL && r[x] {
							if k, ok := fieldOf(x.X); ok && !rs.field[k] {
								rs.field[k], changed = true, true
							}
						}
					}
					call, ok := ins.(ssa.CallInstruction)
					if !ok {
						continue
					}
					args := callArgs(call.Common())
					for _, g := range a.calleesOf(call) {
						nres := g.Signature.Results().Len()
						if w := a.unwrapped(g); w != g {
							g = w // a wrapper's results are its method's
						}
						if v, ok := call.(*ssa.Call); ok {
							if nres == 1 && r[v] {
								changed = set(rs.ret, g, nres, 0) || changed
							}
							if v.Referrers() != nil {
								for _, u := range *v.Referrers() {
									if ex, ok := u.(*ssa.Extract); ok && r[ex] {
										changed = set(rs.ret, g, nres, ex.Index) || changed
									}
								}
							}
						}
						for k, arg := range args {
							if k >= len(g.Params) {
								continue
							}
							if _, isFunc := arg.Type().Underlying().(*types.Signature); !isFunc {
								continue
							}
							reaches := false
							if pk := paramIndex(fn, arg); pk >= 0 && (exitCallback(fn, pk) || flags(rs.cb, fn, len(fn.Params))[pk]) {
								reaches = true
							}
							if h := a.funcOf(arg); h != nil {
								for _, pr := range rs.param[h] {
									reaches = reaches || pr
								}
							}
							if reaches {
								changed = set(rs.cb, g, len(g.Params), k) || changed
							}
						}
					}
				}
			}
		}
		if !changed {
			break
		}
	}
	a.reach = sets
	a.scope = rs
}

// inReadScope: a member at pos in fn concerns a value that reaches a reader.
// With no instruction at pos (a function-level member), it counts.
func (a *analysis) inReadScope(fn *ssa.Function, pos token.Pos) bool {
	// Scope by the call graph: a member counts when its function runs under a
	// public read exit (reachable from one). No value flow is modelled, so no
	// modelling gap can drop a member.
	if a.scope != nil {
		return a.scope.under[originFn(fn)] || a.scope.under[fn]
	}
	return true
}

// inReadScopeByFlow is the value-flow scope it replaces (kept for reference
// in the method history; not used).
func (a *analysis) inReadScopeByFlow(fn *ssa.Function, pos token.Pos) bool {
	r := a.reach[fn]
	s := a.states[fn]
	found := false
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			if ins.Pos() != pos {
				continue
			}
			found = true
			var roots []ssa.Value
			switch x := ins.(type) {
			case *ssa.Return:
				roots = append(roots, x.Results...)
			case *ssa.Store:
				roots = append(roots, x.Val)
			case *ssa.MapUpdate:
				roots = append(roots, x.Value)
			case ssa.Value:
				roots = append(roots, x)
			case ssa.CallInstruction:
				roots = append(roots, callArgs(x.Common())...)
			}
			// A callback passed here: it reaches a reader through its own
			// parameters, or through the variables it fills.
			if call, ok := ins.(ssa.CallInstruction); ok && a.scope != nil {
				for _, arg := range callArgs(call.Common()) {
					if h := a.funcOf(arg); h != nil {
						for _, pr := range a.scope.param[h] {
							if pr {
								return true
							}
						}
					}
					if mc := closureOf(arg); mc != nil {
						for _, bnd := range mc.Bindings {
							if r[bnd] {
								return true
							}
						}
					}
				}
			}
			if len(roots) == 0 {
				return true
			}
			if s == nil {
				return true
			}
			for v := range a.derived(fn, s, roots, true) {
				if r[v] {
					return true
				}
			}
			// The population a reached element belongs to, and a reached
			// container holding the value.
			for _, root := range roots {
				if r[root] {
					return true
				}
			}
		}
	}
	return !found
}

// ---------------------------------------------------------------- report

type hit struct {
	Class    string   `json:"class"`
	Function string   `json:"function"`
	File     string   `json:"file"`
	Line     int      `json:"line"`
	Detail   string   `json:"detail"`
	Entries  int      `json:"public_entries_reaching"`
	Examples []string `json:"entry_examples,omitempty"`
	ReadExit string   `json:"read_exit,omitempty"`
}

type report struct {
	Author          string            `json:"author"`
	Target          string            `json:"target"`
	Loader          string            `json:"loader"`
	Functions       int               `json:"functions_loaded"`
	Analyzed        int               `json:"functions_analyzed"`
	SummaryRounds   int               `json:"summary_rounds"`
	Converged       bool              `json:"summaries_converged"`
	Owners          []string          `json:"owners"`
	OutOfScope      map[string]int    `json:"out_of_read_scope"`
	OwnerCandidates []string          `json:"owner_candidates"`
	Counts          map[string]int    `json:"counts"`
	Hits            []hit             `json:"hits"`
	Mappers         []string          `json:"population_mappers"`
	Controls        map[string]string `json:"controls"`
	AllControlsOK   bool              `json:"all_controls_detected"`
}

func (a *analysis) postOrder() []*ssa.Function {
	inSet := map[*ssa.Function]bool{}
	for _, fn := range a.fns {
		inSet[fn] = true
	}
	seen := map[*ssa.Function]bool{}
	var out []*ssa.Function
	var visit func(fn *ssa.Function)
	visit = func(fn *ssa.Function) {
		if seen[fn] {
			return
		}
		seen[fn] = true
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				if call, ok := ins.(ssa.CallInstruction); ok {
					for _, g := range a.calleesOf(call) {
						if inSet[g] {
							visit(g)
						}
					}
				}
			}
		}
		out = append(out, fn)
	}
	for _, fn := range a.fns {
		visit(fn)
	}
	return out
}

func (a *analysis) entriesReaching(fn *ssa.Function) (int, []string) {
	start := top(originFn(fn))
	seen := map[*ssa.Function]bool{start: true}
	queue := []*ssa.Function{start}
	var found []string
	count := 0
	for len(queue) > 0 && len(seen) < 4000 {
		f := queue[0]
		queue = queue[1:]
		if exported(f) {
			count++
			if len(found) < 3 {
				found = append(found, fullName(f))
			}
		}
		for _, c := range a.callers[f] {
			c = top(originFn(c))
			if !seen[c] {
				seen[c] = true
				queue = append(queue, c)
			}
		}
	}
	return count, found
}

func allFunctions(p *workcensus.Program) []*ssa.Function {
	seen := map[*ssa.Function]bool{}
	var out []*ssa.Function
	var add func(fn *ssa.Function)
	add = func(fn *ssa.Function) {
		if fn == nil || seen[fn] {
			return
		}
		seen[fn] = true
		if inModule(fn) && fn.Blocks != nil && !isTestFile(fn) {
			out = append(out, fn)
		}
		for _, anon := range fn.AnonFuncs {
			add(anon)
		}
	}
	for _, fn := range p.Functions {
		add(fn)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

func targetTree(spec string) (string, func(), error) {
	if !strings.HasPrefix(spec, "git:") {
		return spec, func() {}, nil
	}
	sha := strings.TrimPrefix(spec, "git:")
	if len(sha) != 40 {
		return "", nil, fmt.Errorf("target must use a full commit SHA")
	}
	for _, c := range sha {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return "", nil, fmt.Errorf("invalid target SHA")
		}
	}
	dir, err := os.MkdirTemp("", "nor160-consumers-"+sha[:8]+"-")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { _ = os.RemoveAll(dir) } // exactly the directory MkdirTemp created
	archive := filepath.Join(dir, "source.tar")
	for _, argv := range [][]string{{"git", "archive", "--format=tar", "--output", archive, sha}, {"tar", "-xf", archive, "-C", dir}} {
		command := exec.Command(argv[0], argv[1:]...)
		command.Stderr = os.Stderr
		if err := command.Run(); err != nil {
			cleanup()
			return "", nil, err
		}
	}
	if err := os.Remove(archive); err != nil {
		cleanup()
		return "", nil, err
	}
	return dir, cleanup, nil
}

func main() {
	if len(os.Args) != 3 {
		panic("usage: nor160-consumers <tree|git:fullSHA> <output|->")
	}
	spec, output := os.Args[1], os.Args[2]
	var cleanup func()
	var err error
	tree, cleanup, err = targetTree(spec)
	if err != nil {
		panic(err)
	}
	defer cleanup()
	config := workcensus.Config{Dir: tree, Patterns: []string{"./..."}, BuildFlags: []string{"-tags=noui,nolocalllm"}, Env: append(os.Environ(), "CGO_ENABLED=0", "GOWORK=off")}
	config.Overlay = map[string][]byte{
		filepath.Join(tree, "pkg/storage/nor160_census_control.go"): []byte(controlFile),
		filepath.Join(tree, "pkg/cypher/nor160_census_control.go"):  []byte(cypherControlFile),
	}
	p, err := workcensus.LoadProgram(config)
	if err != nil {
		panic(err)
	}
	a := &analysis{p: p, fns: allFunctions(p), sums: map[*ssa.Function]*summary{}, states: map[*ssa.Function]*fnState{}, callees: map[ssa.CallInstruction][]*ssa.Function{}, callers: map[*ssa.Function][]*ssa.Function{}}
	for fn, node := range p.CallGraph.Nodes {
		if fn == nil || node == nil {
			continue
		}
		for _, e := range node.Out {
			if e.Callee == nil || e.Callee.Func == nil {
				continue
			}
			g := e.Callee.Func
			if e.Site != nil && inModule(g) && g.Blocks != nil {
				a.callees[e.Site] = append(a.callees[e.Site], g)
			}
			a.callers[g] = append(a.callers[g], fn)
		}
	}
	r := report{Author: "NOR-160 lane (Claude), 30 October 2026", Target: spec, Loader: "maintained internal/workcensus.LoadProgram", Functions: len(p.Functions), Analyzed: len(a.fns), Counts: map[string]int{}, Controls: map[string]string{}}

	// Summaries to a fixpoint, callees before callers. Then, with the single
	// owner of each entity type found, again with its decisions accepted.
	collectMutatedTypes(a.fns)
	order := a.postOrder()
	fixpoint := func() (int, bool) {
		for round := 1; round <= 40; round++ {
			changed := false
			for _, fn := range order {
				s := a.analyzeFn(fn)
				a.states[fn] = s
				sum := a.summarize(fn, s)
				if !a.equalSummary(a.sums[fn], sum) {
					a.sums[fn] = sum
					changed = true
				}
			}
			if !changed {
				return round, true
			}
		}
		return 40, false
	}
	rounds1, converged1 := fixpoint()
	a.owners = a.findOwners()
	for ofn, o := range a.owners {
		r.Owners = append(r.Owners, fmt.Sprintf("%s owns %s (parameter %d)", strings.TrimPrefix(fullName(ofn), module+"/"), o.entity.Obj().Name(), o.value))
	}
	sort.Strings(r.Owners)
	r.OwnerCandidates = a.candidates
	rounds2, converged2 := 0, true
	if len(a.owners) > 0 {
		a.sums = map[*ssa.Function]*summary{}
		a.states = map[*ssa.Function]*fnState{}
		rounds2, converged2 = fixpoint()
	}
	r.SummaryRounds = rounds1 + rounds2
	r.Converged = converged1 && converged2

	drops := a.paramDrops()
	ownerReports := a.ownerChecks()
	overwrite := a.overwrites()
	callbackCopy := a.copiesToCallback()
	a.computeReadScope()
	seenHit := map[string]bool{}
	outOfScope := map[string]int{}
	add := func(class string, fn *ssa.Function, p token.Pos, detail string) {
		if !strings.HasPrefix(detail, "a retained value") && !a.inReadScope(fn, p) {
			outOfScope[class]++
			return
		}
		file, line := pos(fn, p)
		key := class + "|" + fullName(fn) + "|" + file + fmt.Sprint(line) + "|" + detail
		if seenHit[key] {
			return
		}
		seenHit[key] = true
		n, ex := a.entriesReaching(fn)
		exit := ""
		if a.scope != nil {
			e := a.scope.exitOf[fn]
			if e == nil {
				e = a.scope.exitOf[originFn(fn)]
			}
			if e != nil {
				exit = strings.TrimPrefix(fullName(e), module+"/")
			}
		}
		r.Hits = append(r.Hits, hit{Class: class, Function: fullName(fn), File: file, Line: line, Detail: detail, Entries: n, Examples: ex, ReadExit: exit})
	}
	mappers := map[string]*ssa.Function{}
	for _, or := range ownerReports {
		add("K0", or.fn, or.pos, or.detail)
	}

	for _, fn := range a.fns {
		s := a.states[fn]

		// K0: exported functions returning or visiting retained entity values
		// (the read-exit definition: an entity, a pointer, slice, map, channel
		// or iterator of entities, or a struct holding one directly). A handle
		// that reaches entities only transitively is not one. A result slot
		// counts each stored value once, at its own return.
		if exported(fn) {
			slots := a.resultSlots(fn)
			for _, b := range fn.Blocks {
				for _, ins := range b.Instrs {
					switch x := ins.(type) {
					case *ssa.Store:
						if al, ok := x.Addr.(*ssa.Alloc); ok && slots[al] && entityValue(x.Val.Type(), 0) && s.val(x.Val).Retained {
							add("K0", fn, returnPos(x), "returns a retained "+x.Val.Type().String())
						}
					case *ssa.Return:
						for _, v := range x.Results {
							if ld, ok := v.(*ssa.UnOp); ok && ld.Op == token.MUL {
								if al, ok := ld.X.(*ssa.Alloc); ok && slots[al] {
									continue
								}
							}
							if entityValue(v.Type(), 0) && s.val(v).Retained {
								add("K0", fn, x.Pos(), "returns a retained "+v.Type().String())
							}
						}
					case *ssa.Call:
						c := x.Common()
						if pv, ok := c.Value.(*ssa.Parameter); ok && paramIndex(fn, pv) >= 0 && !isBoolResult(c.Signature()) {
							for _, arg := range c.Args {
								if entityValue(arg.Type(), 0) && s.val(arg).Retained {
									add("K0", fn, x.Pos(), "visits a retained "+arg.Type().String())
								}
							}
						}
					}
				}
			}
		}

		// K1: copies a callback writes into captured variables, left undelivered.
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				call, ok := ins.(ssa.CallInstruction)
				if !ok {
					continue
				}
				cells := a.callbackCopyCells(call)
				if len(cells) == 0 {
					continue
				}
				d := a.derived(fn, s, cells, true)
				a.cellRoot = true
				k, _ := a.mayDrop(fn, s, call, d)
				a.cellRoot = false
				if k == dropPlain {
					add("K1", fn, call.Pos(), "copies a callback writes into captured variables can be left undelivered")
				}
			}
		}

		// K1: a callback that overwrites fields, given to a function that hands it copies.
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				call, ok := ins.(ssa.CallInstruction)
				if !ok {
					continue
				}
				args := callArgs(call.Common())
				for _, t := range a.calleesOf(call) {
					for k, arg := range args {
						if k >= len(callbackCopy[t]) || !callbackCopy[t][k] {
							continue
						}
						if g := a.funcOf(arg); g != nil && anyTrue(overwrite[g]) {
							add("K1", fn, call.Pos(), strings.TrimPrefix(fullName(t), module+"/")+" hands copies to "+strings.TrimPrefix(fullName(g), module+"/")+", which overwrites their fields")
						}
					}
				}
			}
		}

		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				v, isValue := ins.(ssa.Value)
				if !isValue || !family(v.Type()) {
					// tuples: the call value carries the tuple
					if call, ok := ins.(*ssa.Call); ok {
						v, isValue = call, true
					} else {
						continue
					}
				}
				if !s.intro[v] {
					continue
				}
				if call, ok := v.(*ssa.Call); ok && a.receivedAcross(fn, call) {
					continue // the caller of a public read received it
				}
				d := a.derived(fn, s, []ssa.Value{v}, true)

				// K1 on a copy handed to a callee that selects it out.
				if g, j, ok := a.droppingCallIn(fn, d, drops); ok {
					add("K1", fn, ins.Pos(), "copy "+copyLabel(v)+" is selected out inside "+strings.TrimPrefix(fullName(g), module+"/")+" (parameter "+fmt.Sprint(j)+")")
				}

				// K1 on a copy whose fields a callee overwrites.
				for _, bb := range fn.Blocks {
					for _, ii := range bb.Instrs {
						call, ok := ii.(ssa.CallInstruction)
						if !ok {
							continue
						}
						args := callArgs(call.Common())
						for _, g := range a.calleesOf(call) {
							for m, arg := range args {
								if m < len(overwrite[g]) && overwrite[g][m] && d[arg] {
									add("K1", fn, ins.Pos(), "fields of copy "+copyLabel(v)+" are overwritten inside "+strings.TrimPrefix(fullName(g), module+"/"))
								}
							}
						}
					}
				}

				// K1 / K1e / K2 on the copy itself.
				kind, stored := a.mayDrop(fn, s, ins, d)
				if kind != noDrop {
					class := "K1"
					if kind == dropError {
						class = "K1e"
					}
					if stored && kind == dropPlain {
						class = "K2"
					}
					add(class, fn, ins.Pos(), "copy "+copyLabel(v)+" can be left undelivered")
				}

				// Elements of a copied population.
				if t := v.Type(); container(t) || isTupleWithContainer(t) {
					d := a.derived(fn, s, []ssa.Value{v}, false)
					for _, e := range a.leafElements(fn, s, d) {
						ei, _ := e.(ssa.Instruction)
						if ei == nil {
							continue
						}
						ed := a.derived(fn, s, []ssa.Value{e}, true)
						if a.receivesAny(fn, ed) {
							if k, _ := a.mayDrop(fn, s, ei, ed, d); k == dropPlain {
								add("K1", fn, ei.Pos(), "element of copied population "+copyLabel(v)+" is selected out after the copy")
							}
						}
						// K3: the loop over the population can be left early by a call's result.
						if loop := loopOf(fn, ei.Block()); loop != nil {
							for lb := range loop {
								iff, ok := lb.Instrs[len(lb.Instrs)-1].(*ssa.If)
								if !ok {
									continue
								}
								for _, succ := range lb.Succs {
									if !loop[succ] && callDecided(iff.Cond, 0) {
										add("K3", fn, iff.Pos(), "loop over copied population "+copyLabel(v)+" can stop early")
									}
								}
							}
						}
					}
				}
			}
		}

		// K3 for local buffers filled with copies (pre-read populations).
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				ms, ok := ins.(ssa.Value)
				if !ok || !container(ms.Type()) || !family(ms.Type()) || !s.val(ms).Copy {
					continue
				}
				switch x := ms.(type) {
				case *ssa.Phi, *ssa.MakeSlice:
				case *ssa.Call:
					if bi, isB := x.Call.Value.(*ssa.Builtin); !isB || bi.Name() != "append" {
						continue
					}
				default:
					continue
				}
				d := a.derived(fn, s, []ssa.Value{ms}, false)
				for _, e := range elementsOf(fn, d) {
					ei, _ := e.(ssa.Instruction)
					if ei == nil {
						continue
					}
					if loop := loopOf(fn, ei.Block()); loop != nil {
						for lb := range loop {
							iff, ok := lb.Instrs[len(lb.Instrs)-1].(*ssa.If)
							if !ok {
								continue
							}
							for _, succ := range lb.Succs {
								if !loop[succ] && callDecided(iff.Cond, 0) {
									add("K3", fn, iff.Pos(), "loop over a buffer of copies can stop early")
								}
							}
						}
					}
				}
			}
		}

		// K4: one value merging an uncopied value and a copy of it.
		checkMerge := func(p token.Pos, inputs []ssa.Value) {
			for _, x := range inputs {
				ox := s.val(x)
				if ox.empty() || ox.Copy || !family(x.Type()) {
					continue
				}
				for _, y := range inputs {
					src, ok := s.src[y]
					if !ok {
						continue
					}
					if src.overlaps(ox) {
						add("K4", fn, p, "chooses between a value and a copy of it")
						return
					}
				}
			}
		}
		var retPos token.Pos
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				switch x := ins.(type) {
				case *ssa.Phi:
					checkMerge(x.Pos(), x.Edges)
				case *ssa.Return:
					if !retPos.IsValid() {
						retPos = x.Pos()
					}
				}
			}
		}
		for _, in := range a.returnInputs(fn) {
			checkMerge(retPos, in)
		}

		// K4b: Copier dispatch sites, and a second CallerCopy after a first.
		var callerCopies []*ssa.Call
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				switch x := ins.(type) {
				case *ssa.TypeAssert:
					if it, ok := x.AssertedType.Underlying().(*types.Interface); ok {
						for i := 0; i < it.NumMethods(); i++ {
							if it.Method(i).Name() == "CallerCopy" {
								add("K4b", fn, x.Pos(), "asserts a Copier")
							}
						}
					}
				case *ssa.Call:
					c := x.Common()
					if c.IsInvoke() && c.Method.Name() == "CallerCopy" {
						callerCopies = append(callerCopies, x)
					} else if f := c.StaticCallee(); f != nil && originFn(f).Name() == "CallerCopy" {
						callerCopies = append(callerCopies, x)
					}
				}
			}
		}
		for _, first := range callerCopies {
			for _, second := range callerCopies {
				if first == second {
					continue
				}
				later := first.Block() == second.Block() && blockIndex(first) < blockIndex(second) || reach(first.Block())[second.Block()]
				if later {
					add("K4b", fn, second.Pos(), "a second CallerCopy can follow a first")
				}
			}
		}

		// K5: loops storing copies of the elements they iterate; mappers.
		for _, b := range fn.Blocks {
			if loopOf(fn, b) == nil {
				continue
			}
			for _, ins := range b.Instrs {
				call, ok := ins.(*ssa.Call)
				if !ok {
					continue
				}
				if _, isParam := call.Common().Value.(*ssa.Parameter); isParam && storedInSlice(call) && anyElementArg(call.Common().Args) {
					mappers[fullName(fn)] = originFn(fn)
					continue
				}
				if _, intro := s.src[call]; intro && storedInSlice(call) && anyElementArg(callArgs(call.Common())) {
					add("K5", fn, call.Pos(), "loop stores copies of the elements it iterates ("+copyLabel(call)+")")
				}
			}
		}

		// K6: an unconditional copier applied to a value made for this read.
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				call, ok := ins.(*ssa.Call)
				if !ok {
					continue
				}
				args := callArgs(call.Common())
				for _, g := range a.calleesOf(call) {
					sg := a.sums[g]
					if sg == nil {
						continue
					}
					for j, cp := range sg.Copies {
						if !cp || j >= len(args) {
							continue
						}
						passes, returnsCopy := false, false
						for _, rr := range sg.Ret {
							if rr.Params&bit(j) != 0 {
								passes = true
							}
							returnsCopy = returnsCopy || rr.Copy
						}
						if passes || !returnsCopy {
							continue // a dispatcher decides, or the copy is kept by a retention point
						}
						if s.val(args[j]).private() {
							add("K6", fn, call.Pos(), "copies a value made for this read ("+copyLabel(call)+")")
						}
					}
				}
			}
		}

		// K7: a copy of a family struct keeping a reference field of its source.
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				al, ok := ins.(*ssa.Alloc)
				if !ok {
					continue
				}
				if _, intro := s.src[al]; !intro {
					continue
				}
				n, st := structOf(pointee(al.Type()))
				if st == nil {
					continue
				}
				if !s.src[al].Retained && !s.src[al].Unknown {
					continue // shared with a value the caller owns; callers passing retained values are judged below
				}
				for _, f := range sharedFields(al, st) {
					add("K7", fn, al.Pos(), n.Obj().Name()+"."+f+" is shared with a retained source")
				}
			}
		}
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				call, ok := ins.(*ssa.Call)
				if !ok {
					continue
				}
				args := callArgs(call.Common())
				for _, g := range a.calleesOf(call) {
					sg := a.sums[g]
					if sg == nil {
						continue
					}
					for j, fields := range sg.Partial {
						if j < len(args) && s.val(args[j]).Retained {
							for _, f := range fields {
								add("K7", fn, call.Pos(), strings.TrimPrefix(fullName(g), module+"/")+" keeps "+f+" of a retained value shared")
							}
						}
					}
				}
			}
		}
	}

	// K5m: every population mapper (a loop storing a function parameter's
	// results) and every call to one, with the function it applies. Counted
	// with the other classes; the one slice owner must be among them.
	for m, mfn := range mappers {
		r.Mappers = append(r.Mappers, m)
		add("K5m", mfn, mfn.Pos(), "population mapper: a loop stores a function parameter's results")
	}
	sort.Strings(r.Mappers)
	for _, fn := range a.fns {
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				call, ok := ins.(*ssa.Call)
				if !ok {
					continue
				}
				f := call.Common().StaticCallee()
				if f == nil {
					continue
				}
				name := fullName(f)
				if _, isMapper := mappers[name]; !isMapper {
					continue
				}
				applied := "a function value"
				for _, arg := range call.Common().Args {
					switch x := arg.(type) {
					case *ssa.Function:
						applied = strings.TrimPrefix(fullName(x), module+"/")
					case *ssa.MakeClosure:
						if g, ok := x.Fn.(*ssa.Function); ok {
							applied = "closure " + strings.TrimPrefix(fullName(g), module+"/")
						}
					}
				}
				add("K5m", fn, call.Pos(), "calls population mapper "+strings.TrimPrefix(name, module+"/")+" with "+applied)
			}
		}
	}
	sort.Slice(r.Hits, func(i, j int) bool {
		if r.Hits[i].Class != r.Hits[j].Class {
			return r.Hits[i].Class < r.Hits[j].Class
		}
		if r.Hits[i].File != r.Hits[j].File {
			return r.Hits[i].File < r.Hits[j].File
		}
		return r.Hits[i].Line < r.Hits[j].Line
	})
	for _, h := range r.Hits {
		r.Counts[h.Class]++
	}
	r.AllControlsOK = true
	for _, c := range controls {
		found := false
		for _, h := range r.Hits {
			if h.Class == c.class && h.Function == c.fn {
				found = true
			}
		}
		state := "detected"
		ok := found
		if c.clean {
			ok = !found
			state = "clean"
			if found {
				state = "FALSELY REPORTED"
			}
		} else if !found {
			state = "MISSED"
		}
		if !ok {
			r.AllControlsOK = false
		}
		r.Controls[c.class+" "+c.fn] = state
	}
	r.OutOfScope = outOfScope
	data, err := json.MarshalIndent(r, "", " ")
	if err != nil {
		panic(err)
	}
	if output == "-" {
		fmt.Println(string(data))
		return
	}
	if err := os.WriteFile(output, data, 0o644); err != nil {
		panic(err)
	}
}

// mutatedTypes: struct types whose fields some function writes in a value it
// did not allocate itself. A pointer to any other struct type is an immutable
// record: sharing it shares nothing a caller can change.
var mutatedTypes = map[*types.Named]bool{}

func immutableReferent(t types.Type) bool {
	p := pointee(t)
	if p == nil {
		return false
	}
	n, st := structOf(p)
	if st == nil {
		return false
	}
	return !mutatedTypes[n]
}

func collectMutatedTypes(fns []*ssa.Function) {
	for _, fn := range fns {
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				st, ok := ins.(*ssa.Store)
				if !ok {
					continue
				}
				fa, ok := st.Addr.(*ssa.FieldAddr)
				if !ok {
					continue
				}
				if _, own := fa.X.(*ssa.Alloc); own {
					continue
				}
				if n, _ := structOf(pointee(fa.X.Type())); n != nil {
					mutatedTypes[n] = true
				}
			}
		}
	}
}

func isTupleWithContainer(t types.Type) bool {
	tu, ok := t.(*types.Tuple)
	if !ok {
		return false
	}
	for i := 0; i < tu.Len(); i++ {
		if container(tu.At(i).Type()) && family(tu.At(i).Type()) {
			return true
		}
	}
	return false
}

func blockIndex(ins ssa.Instruction) int {
	for i, x := range ins.Block().Instrs {
		if x == ins {
			return i
		}
	}
	return -1
}

func copyLabel(v ssa.Value) string {
	switch x := v.(type) {
	case *ssa.Call:
		c := x.Common()
		if c.IsInvoke() {
			return c.Value.Type().String() + "." + c.Method.Name()
		}
		if f := c.StaticCallee(); f != nil {
			return strings.TrimPrefix(fullName(f), module+"/")
		}
		return "dynamic call"
	case *ssa.Alloc:
		return "new " + strings.TrimPrefix(pointee(x.Type()).String(), module+"/")
	}
	return v.Name()
}

// storedInSlice: the call's result is appended to a slice or stored at an index.
func storedInSlice(call *ssa.Call) bool {
	refs := call.Referrers()
	if refs == nil {
		return false
	}
	for _, r := range *refs {
		switch x := r.(type) {
		case *ssa.Store:
			if _, ok := x.Addr.(*ssa.IndexAddr); ok && x.Val == call {
				return true
			}
		case *ssa.Call:
			if bi, ok := x.Call.Value.(*ssa.Builtin); ok && bi.Name() == "append" {
				return true
			}
		case *ssa.Extract:
			if x.Index == 0 && x.Referrers() != nil {
				for _, rr := range *x.Referrers() {
					if st, ok := rr.(*ssa.Store); ok {
						if _, ok := st.Addr.(*ssa.IndexAddr); ok {
							return true
						}
					}
					if c, ok := rr.(*ssa.Call); ok {
						if bi, ok := c.Call.Value.(*ssa.Builtin); ok && bi.Name() == "append" {
							return true
						}
					}
				}
			}
		}
	}
	return false
}

// anyElementArg: an argument read out of a slice, array or map being iterated.
func anyElementArg(args []ssa.Value) bool {
	for _, arg := range args {
		switch x := arg.(type) {
		case *ssa.UnOp:
			if _, ok := x.X.(*ssa.IndexAddr); ok && x.Op == token.MUL {
				return true
			}
		case *ssa.Index, *ssa.Lookup:
			return true
		case *ssa.Extract:
			if _, ok := x.Tuple.(*ssa.Next); ok {
				return true
			}
		}
	}
	return false
}

// sharedFields: reference fields of a copied struct that keep the source's
// value: a whole copy never overwrites them, or they are filled straight
// from the same field of a value of the same type.
func sharedFields(al *ssa.Alloc, st *types.Struct) []string {
	elem := pointee(al.Type())
	refs := al.Referrers()
	if refs == nil {
		return nil
	}
	whole := false
	overwritten := map[int]bool{}
	direct := map[int]bool{}
	for _, r := range *refs {
		switch x := r.(type) {
		case *ssa.Store:
			if x.Addr == al && types.Identical(x.Val.Type(), elem) {
				if _, isCall := x.Val.(*ssa.Call); !isCall {
					whole = true
				}
			}
		case *ssa.FieldAddr:
			if fr := x.Referrers(); fr != nil {
				for _, fref := range *fr {
					sto, ok := fref.(*ssa.Store)
					if !ok || sto.Addr != x {
						continue
					}
					overwritten[x.Field] = true
					if ld, ok := sto.Val.(*ssa.UnOp); ok && ld.Op == token.MUL {
						if fa, ok := ld.X.(*ssa.FieldAddr); ok && fa.Field == x.Field && types.Identical(pointee(fa.X.Type()), elem) {
							direct[x.Field] = true
						}
					}
				}
			}
		}
	}
	var out []string
	for i := 0; i < st.NumFields(); i++ {
		f := st.Field(i)
		if !reference(f.Type()) || immutableReferent(f.Type()) {
			continue
		}
		if (whole && !overwritten[i]) || direct[i] {
			out = append(out, f.Name())
		}
	}
	return out
}
