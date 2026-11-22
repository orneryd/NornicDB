package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// s1Keys are the property names of the model nodes; s1Absent is never stored.
var s1Keys = []string{"a", "b", "c", "bytes", "list", "nested", "vec", "when", "body", "weird key ü"}

const s1Absent = "never_stored"

func s1Value(key string, n int64) any {
	switch key {
	case "a":
		return n
	case "b":
		return fmt.Sprintf("b-%d", n)
	case "c":
		return float64(n) + 0.5
	case "bytes":
		return []byte{byte(n), byte(n >> 8), 0xff}
	case "list":
		return []any{n, fmt.Sprintf("l-%d", n), n%2 == 0}
	case "nested":
		return map[string]any{"n": n, "inner": []any{fmt.Sprintf("i-%d", n), n}}
	case "vec":
		return []float32{float32(n), 0.5}
	case "when":
		return time.Unix(1700000000+n, 0).UTC()
	case "body":
		return strings.Repeat(fmt.Sprintf("body-%d;", n), int(n%300)+1)
	}
	return fmt.Sprintf("%s-%d", key, n)
}

// s1Norm makes a decoded value comparable with a written one.
func s1Norm(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case time.Time:
		return "time:" + x.UTC().Format(time.RFC3339Nano)
	case []byte:
		return fmt.Sprintf("bytes:%x", x)
	case []any:
		out := make([]any, len(x))
		for i := range x {
			out[i] = s1Norm(x[i])
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = s1Norm(e)
		}
		return out
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice:
		out := make([]any, rv.Len())
		for i := range out {
			out[i] = s1Norm(rv.Index(i).Interface())
		}
		return out
	case reflect.Float32, reflect.Float64:
		return rv.Float()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int()
	}
	return v
}

func s1NormProps(p map[string]any) map[string]any {
	out := make(map[string]any, len(p))
	for k, v := range p {
		out[k] = s1Norm(v)
	}
	return out
}

// s1Scribble changes every container a caller received, in place.
func s1Scribble(p map[string]any) {
	names := make([]string, 0, len(p))
	for k := range p {
		names = append(names, k)
	}
	for _, k := range names {
		switch x := p[k].(type) {
		case []byte:
			for i := range x {
				x[i] ^= 0xa5
			}
		case []any:
			for i := range x {
				x[i] = "scribbled"
			}
		case []float32:
			for i := range x {
				x[i] = -77
			}
		case []string:
			for i := range x {
				x[i] = "scribbled"
			}
		case map[string]any:
			if inner, ok := x["inner"].([]any); ok {
				for i := range inner {
					inner[i] = "scribbled"
				}
			}
			x["injected"] = true
		}
		p[k+"-injected"] = "scribbled"
	}
}

type s1ModelNode struct {
	labels string
	props  map[string]any
}

type s1Model map[NodeID]s1ModelNode

func (m s1Model) clone() s1Model {
	out := make(s1Model, len(m))
	for k, v := range m {
		props := make(map[string]any, len(v.props))
		for pk, pv := range v.props {
			props[pk] = pv
		}
		out[k] = s1ModelNode{v.labels, props}
	}
	return out
}

func s1Pick(props map[string]any, names []string) map[string]any {
	out := map[string]any{}
	for _, n := range names {
		if v, ok := props[n]; ok {
			out[n] = v
		}
	}
	return out
}

func s1RandomProjection(r *rand.Rand) []string {
	out := []string{}
	for _, k := range s1Keys {
		if r.Intn(3) == 0 {
			out = append(out, k)
		}
	}
	if r.Intn(3) == 0 {
		out = append(out, s1Absent)
	}
	if r.Intn(6) == 0 && len(out) > 0 {
		out = append(out, out[0]) // a repeated name
	}
	r.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// TestPR1826S1ModelSequences drives random narrow, wide, full, scan and
// snapshot reads between writes, relabels, deletes, re-creates, sidecar writes,
// prunes and rollbacks, and compares every read with an independent model. The
// cache budgets vary from generous to almost nothing.
func TestPR1826S1ModelSequences(t *testing.T) {
	type budget struct {
		name        string
		entries     int
		bodyBytes   int64
		writeBehind bool
	}
	for _, bud := range []budget{{"default", -1, -1, false}, {"oneEntry", 1, 1 << 20, false}, {"tinyBytes", 64, 700, false}, {"mid", 4, 6000, false}, {"writeBehind", -1, -1, true}} {
		for seed := int64(1); seed <= 4; seed++ {
			t.Run(fmt.Sprintf("%s/seed%d", bud.name, seed), func(t *testing.T) {
				var b *BadgerEngine
				var err error
				if bud.writeBehind {
					b, err = fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, WriteBehind: true, WriteBehindInterval: 2 * time.Millisecond}))
				} else {
					b, err = fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
				}
				require.NoError(t, err)
				t.Cleanup(func() { _ = b.Close() })
				if bud.entries >= 0 {
					b.nodeCacheMaxEntries = bud.entries
					b.nodeBodyCacheMaxBytes = bud.bodyBytes
				}
				// With write-behind on, a transaction opened earlier sees later
				// acknowledged writes (both builds: S1.md), so that budget
				// leaves the snapshot operations out.
				s1RunModel(t, b, rand.New(rand.NewSource(seed)), 2500, !bud.writeBehind)
			})
		}
	}
}

func s1RunModel(t *testing.T, b *BadgerEngine, r *rand.Rand, steps int, snapshots bool) {
	ctx := t.Context()
	model := s1Model{}
	type snap struct {
		tx    *BadgerTransaction
		model s1Model
	}
	var snaps []snap
	defer func() {
		for _, s := range snaps {
			_ = s.tx.Rollback()
		}
	}()
	var counter int64
	trace := make([]string, 0, 64)
	note := func(format string, args ...any) {
		if len(trace) == 60 {
			trace = trace[1:]
		}
		trace = append(trace, fmt.Sprintf(format, args...))
	}
	fail := func(format string, args ...any) {
		t.Fatalf("%s\ntrace:\n%s", fmt.Sprintf(format, args...), strings.Join(trace, "\n"))
	}
	id := func() NodeID { return NodeID(fmt.Sprintf("m:n%d", r.Intn(12))) }
	randomProps := func() map[string]any {
		p := map[string]any{}
		for _, k := range s1Keys {
			if r.Intn(2) == 0 {
				counter++
				p[k] = s1Value(k, counter)
			}
		}
		return p
	}
	labels := func() []string { return []string{"M", []string{"X", "Y"}[r.Intn(2)]} }
	toModel := func(l []string, p map[string]any) s1ModelNode {
		return s1ModelNode{strings.Join(l, ","), s1NormProps(p)}
	}
	scribble := true
	check := func(what string, want s1ModelNode, exists bool, names []string, node *Node, err error) {
		if !exists {
			if !errors.Is(err, ErrNotFound) {
				fail("%s: want ErrNotFound, got node=%v err=%v", what, node, err)
			}
			return
		}
		if err != nil {
			fail("%s: unexpected error %v", what, err)
		}
		expect := want.props
		if names != nil {
			expect = s1Pick(want.props, names)
		}
		got := s1NormProps(node.Properties)
		if !(len(expect) == 0 && len(got) == 0) && !reflect.DeepEqual(expect, got) {
			fail("%s: projection %q\nwant %v\ngot  %v", what, names, expect, got)
		}
		if strings.Join(node.Labels, ",") != want.labels {
			fail("%s: labels want %s got %v", what, want.labels, node.Labels)
		}
		if scribble {
			s1Scribble(node.Properties)
		}
	}
	for step := 0; step < steps; step++ {
		n := id()
		cur, exists := model[n]
		switch op := r.Intn(26); op {
		case 0, 1, 2, 3, 4, 5, 6, 7:
			names := s1RandomProjection(r)
			note("%d projected %s %q", step, n, names)
			node, err := b.GetNodeProjected(ctx, n, names)
			check("projected "+string(n), cur, exists, names, node, err)
		case 8:
			note("%d full %s", step, n)
			node, err := b.GetNode(ctx, n)
			check("full "+string(n), cur, exists, nil, node, err)
		case 9:
			note("%d query %s", step, n)
			node, err := b.GetQueryNode(ctx, n)
			check("query "+string(n), cur, exists, nil, node, err)
		case 10:
			note("%d projected-nil %s", step, n)
			node, err := b.GetNodeProjected(ctx, n, nil)
			check("projected-nil "+string(n), cur, exists, nil, node, err)
		case 11, 12:
			names := s1RandomProjection(r)
			label := []string{"M", "X", "Y"}[r.Intn(3)]
			note("%d labelscan %s %q", step, label, names)
			seen := map[NodeID]bool{}
			err := b.StreamNodesByLabelProjected(ctx, label, names, func(node *Node) error {
				seen[node.ID] = true
				want, ok := model[node.ID]
				check("labelscan "+string(node.ID), want, ok, names, node, nil)
				return nil
			})
			if err != nil {
				fail("labelscan error %v", err)
			}
			for mid, mn := range model {
				has := false
				for _, l := range strings.Split(mn.labels, ",") {
					has = has || l == label
				}
				if has != seen[mid] {
					fail("labelscan %s: node %s want listed=%v got %v", label, mid, has, seen[mid])
				}
			}
		case 13, 14:
			p, l := randomProps(), labels()
			if !exists {
				note("%d create %s", step, n)
				if _, err := b.CreateNode(ctx, &Node{ID: n, Labels: l, Properties: p}); err != nil {
					fail("create: %v", err)
				}
			} else {
				note("%d update %s", step, n)
				if err := b.UpdateNode(ctx, NodeUpdateRequest{After: &Node{ID: n, Labels: l, Properties: p}}); err != nil {
					fail("update: %v", err)
				}
			}
			model[n] = toModel(l, p)
			s1Scribble(p) // the caller's own input may not reach stored state
		case 15:
			if exists {
				note("%d delete %s", step, n)
				if err := b.DeleteNode(ctx, n); err != nil {
					fail("delete: %v", err)
				}
				delete(model, n)
			}
		case 16:
			if !snapshots {
				continue
			}
			if len(snaps) < 3 {
				note("%d snapshot open", step)
				tx, err := b.BeginTransaction(ctx, OrdinaryBegin)
				if err != nil {
					fail("begin: %v", err)
				}
				// pin the snapshot with a first read
				_, _ = tx.GetNodeProjected(ctx, n, []string{"a"})
				snaps = append(snaps, snap{tx, model.clone()})
			} else {
				i := r.Intn(len(snaps))
				note("%d snapshot close %d", step, i)
				_ = snaps[i].tx.Rollback()
				snaps = append(snaps[:i], snaps[i+1:]...)
			}
		case 17, 18, 19:
			if len(snaps) > 0 {
				i := r.Intn(len(snaps))
				s := snaps[i]
				want, ok := s.model[n]
				names := s1RandomProjection(r)
				note("%d snapshot %d projected %s %q", step, i, n, names)
				node, err := s.tx.GetNodeProjected(ctx, n, names)
				check(fmt.Sprintf("snapshot %d projected %s", i, n), want, ok, names, node, err)
			}
		case 20:
			if len(snaps) > 0 {
				i := r.Intn(len(snaps))
				s := snaps[i]
				names := s1RandomProjection(r)
				note("%d snapshot %d labelscan %q", step, i, names)
				seen := 0
				// A transaction retains its completed label streams as shared
				// immutable images (badger_transaction.go:206), so a visitor
				// only reads them.
				scribble = false
				err := s.tx.StreamNodesByLabelProjected(ctx, "M", names, func(node *Node) error {
					want, ok := s.model[node.ID]
					seen++
					check(fmt.Sprintf("snapshot %d labelscan %s", i, node.ID), want, ok, names, node, nil)
					return nil
				})
				scribble = true
				if err != nil || seen != len(s.model) {
					fail("snapshot labelscan: err=%v seen=%d want %d", err, seen, len(s.model))
				}
			}
		case 21:
			// a transaction with pending writes: read-your-writes, then commit or roll back
			note("%d tx on %s", step, n)
			tx, err := b.BeginTransaction(ctx, OrdinaryBegin)
			if err != nil {
				fail("begin: %v", err)
			}
			p, l := randomProps(), labels()
			pending, pendingExists := s1ModelNode{}, true
			switch {
			case !exists:
				_, err = tx.CreateNode(ctx, &Node{ID: n, Labels: l, Properties: p})
				pending = toModel(l, p)
			case r.Intn(3) == 0:
				err = tx.DeleteGraphNode(ctx, n, CascadeEdges)
				pendingExists = false
			default:
				err = tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: &Node{ID: n, Labels: l, Properties: p}, Policy: ClientNodeEdits})
				pending = toModel(l, p)
			}
			if err != nil {
				fail("tx write: %v", err)
			}
			for k := 0; k < 3; k++ {
				names := s1RandomProjection(r)
				node, readErr := tx.GetNodeProjected(ctx, n, names)
				check("own pending "+string(n), pending, pendingExists, names, node, readErr)
				node, readErr = b.GetNodeProjected(ctx, n, names)
				check("outside pending "+string(n), cur, exists, names, node, readErr)
			}
			if r.Intn(2) == 0 {
				note("%d tx rollback", step)
				_ = tx.Rollback()
			} else {
				note("%d tx commit exists=%v", step, pendingExists)
				if err := tx.Commit(); err != nil {
					fail("commit: %v", err)
				}
				if pendingExists {
					model[n] = pending
				} else {
					delete(model, n)
				}
			}
		case 22:
			if exists {
				note("%d sidecar %s", step, n)
				stored, err := b.GetNode(ctx, n)
				if err != nil {
					fail("sidecar read: %v", err)
				}
				payload := CopyNode(stored)
				payload.ChunkEmbeddings = [][]float32{{float32(step), 1, 2}}
				payload.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": 1}
				if err := b.UpdateNodeEmbeddingSidecar(ctx, payload); err != nil {
					note("sidecar refused: %v", err)
				}
			}
		case 23:
			if exists {
				names := append([]string{hasEmbeddingProperty}, s1RandomProjection(r)...)
				note("%d projected+has_embedding %s", step, n)
				full, err := b.GetNode(ctx, n)
				if err != nil {
					fail("full: %v", err)
				}
				node, err := b.GetNodeProjected(ctx, n, names)
				if err != nil {
					fail("projected has_embedding: %v", err)
				}
				if fmt.Sprint(full.EmbedMeta["has_embedding"]) != fmt.Sprint(node.EmbedMeta["has_embedding"]) {
					fail("has_embedding want %v got %v", full.EmbedMeta, node.EmbedMeta)
				}
				check("has_embedding "+string(n), cur, exists, names, node, nil)
			}
		case 24:
			note("%d prune", step)
			if _, err := b.PruneMVCCVersions(ctx, MVCCPruneOptions{MaxVersionsPerKey: 1}); err != nil {
				fail("prune: %v", err)
			}
		case 25:
			var nodes []*Node
			staged := s1Model{}
			for i := 0; i < 3; i++ {
				nid := id()
				if _, ok := model[nid]; ok {
					continue
				}
				if _, ok := staged[nid]; ok {
					continue
				}
				p, l := randomProps(), labels()
				staged[nid] = toModel(l, p)
				nodes = append(nodes, &Node{ID: nid, Labels: l, Properties: p})
			}
			note("%d bulkcreate %d then bulkdelete %s (existed %v)", step, len(nodes), n, exists)
			if len(nodes) > 0 {
				if err := b.BulkCreateNodes(ctx, nodes); err != nil {
					fail("bulk create: %v", err)
				}
				for k, v := range staged {
					model[k] = v
				}
			}
			if exists {
				if err := b.BulkDeleteNodes(ctx, []NodeID{n}); err != nil {
					fail("bulk delete: %v", err)
				}
				delete(model, n)
			}
		}
	}
}

// TestPR1826S1RestartAndRestore: a populated projection cache never outlives
// the store it was read from.
func TestPR1826S1RestartAndRestore(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	open := func() *BadgerEngine {
		b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
		require.NoError(t, err)
		t.Cleanup(func() { _ = b.Close() })
		return b
	}
	b := open()
	id := NodeID("r:one")
	_, err := b.CreateNode(ctx, &Node{ID: id, Labels: []string{"R"}, Properties: map[string]any{"a": int64(1), "body": strings.Repeat("v1", 5000), "bytes": []byte{1}}})
	require.NoError(t, err)
	read := func(b *BadgerEngine, names ...string) map[string]any {
		node, err := b.GetNodeProjected(ctx, id, names)
		require.NoError(t, err)
		return node.Properties
	}
	require.Equal(t, map[string]any{"a": int64(1)}, read(b, "a", "absent"))
	backup := filepath.Join(t.TempDir(), "backup.bin")
	require.NoError(t, b.Backup(backup))
	require.NoError(t, b.UpdateNode(ctx, NodeUpdateRequest{After: &Node{ID: id, Labels: []string{"R"}, Properties: map[string]any{"a": int64(2), "absent": "now", "body": strings.Repeat("v2", 5000)}}}))
	require.Equal(t, map[string]any{"a": int64(2), "absent": "now"}, read(b, "a", "absent"))
	require.Equal(t, map[string]any{"a": int64(2), "body": strings.Repeat("v2", 5000)}, read(b, "a", "body", "bytes"))
	require.NoError(t, b.Close())

	b = open()
	require.Equal(t, map[string]any{"a": int64(2), "absent": "now"}, read(b, "a", "absent"))
	require.Equal(t, map[string]any{"a": int64(2), "body": strings.Repeat("v2", 5000)}, read(b, "a", "body", "bytes"))
	// restore the older image underneath a warm cache
	require.NoError(t, b.Restore(backup))
	require.Equal(t, map[string]any{"a": int64(1)}, read(b, "a", "absent"))
	require.Equal(t, map[string]any{"a": int64(1), "body": strings.Repeat("v1", 5000), "bytes": []byte{1}}, read(b, "a", "body", "bytes"))
	require.NoError(t, b.Close())
	b = open()
	require.Equal(t, map[string]any{"a": int64(1)}, read(b, "a", "absent"))
	require.NoError(t, b.Close())
}

// TestPR1826S1DropAndRecreateNamespace: dropping a database's data revokes its
// projected images, and re-created ids serve the new values.
func TestPR1826S1DropAndRecreateNamespace(t *testing.T) {
	ctx := t.Context()
	b := createTestBadgerEngine(t)
	for round := int64(1); round <= 5; round++ {
		for i := 0; i < 20; i++ {
			for _, ns := range []string{"one", "two"} {
				if ns == "two" && round > 1 {
					continue
				}
				_, err := b.CreateNode(ctx, &Node{ID: NodeID(fmt.Sprintf("%s:n%d", ns, i)), Labels: []string{"D"}, Properties: map[string]any{"owner": ns, "round": round, "body": strings.Repeat("x", 2000)}})
				require.NoError(t, err)
			}
		}
		for i := 0; i < 20; i++ {
			for pass := 0; pass < 2; pass++ {
				one, err := b.GetNodeProjected(ctx, NodeID(fmt.Sprintf("one:n%d", i)), []string{"owner", "round"})
				require.NoError(t, err)
				require.Equal(t, map[string]any{"owner": "one", "round": round}, one.Properties)
				two, err := b.GetNodeProjected(ctx, NodeID(fmt.Sprintf("two:n%d", i)), []string{"owner", "round"})
				require.NoError(t, err)
				require.Equal(t, map[string]any{"owner": "two", "round": int64(1)}, two.Properties)
			}
		}
		_, _, err := b.DeleteByPrefix("one:")
		require.NoError(t, err)
		for i := 0; i < 20; i++ {
			_, err := b.GetNodeProjected(ctx, NodeID(fmt.Sprintf("one:n%d", i)), []string{"owner"})
			require.ErrorIs(t, err, ErrNotFound)
		}
	}
}

// TestPR1826S1LargeValues: values around the inline limit and well above it.
func TestPR1826S1LargeValues(t *testing.T) {
	ctx := t.Context()
	b, dir := createTestBadgerEngineOnDisk(t)
	sizes := []int{0, 1, 31, 32, 255, 256, 65535, 65536, maxNodeSize - 1, maxNodeSize, maxNodeSize + 1, 1 << 20, 4 << 20}
	props := func(size int) map[string]any {
		return map[string]any{"key": int64(size), "s": strings.Repeat("s", size), "b": bytes.Repeat([]byte{7}, size), "tail": "tail"}
	}
	for _, size := range sizes {
		_, err := b.CreateNode(ctx, &Node{ID: NodeID(fmt.Sprintf("l:%d", size)), Labels: []string{"L"}, Properties: props(size), ChunkEmbeddings: [][]float32{make([]float32, 3000), make([]float32, 3000), make([]float32, 3000), make([]float32, 3000)}})
		require.NoError(t, err, size)
	}
	verify := func(b *BadgerEngine) {
		for _, size := range sizes {
			id := NodeID(fmt.Sprintf("l:%d", size))
			for _, names := range [][]string{{"key"}, {"tail"}, {"key", "tail", "nope"}, {"s"}, {"b", "key"}, {"s", "b", "tail", "key"}, {}} {
				node, err := b.GetNodeProjected(ctx, id, names)
				require.NoError(t, err)
				want := s1NormProps(s1Pick(props(size), names))
				require.True(t, reflect.DeepEqual(want, s1NormProps(node.Properties)), "size %d names %v", size, names)
			}
			full, err := b.GetNode(ctx, id)
			require.NoError(t, err)
			require.Len(t, full.ChunkEmbeddings, 4, "size %d", size)
			require.Len(t, full.Properties, 4)
		}
	}
	verify(b)
	verify(b)
	require.NoError(t, b.Close())
	reopened, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	verify(reopened)
}

// TestPR1826S1ConcurrentMix runs narrow, wide, full, label-scan and snapshot
// readers against writers, deletes, re-creates, pruning and a small cache
// budget. Every image must belong to one committed version, never go backwards
// for a sequential reader, and snapshots must stay put.
func TestPR1826S1ConcurrentMix(t *testing.T) {
	for _, small := range []bool{false, true} {
		t.Run(fmt.Sprintf("smallBudget=%v", small), func(t *testing.T) {
			b := createTestBadgerEngine(t)
			if small {
				b.nodeCacheMaxEntries, b.nodeBodyCacheMaxBytes = 2, 3000
			}
			ctx, cancel := context.WithTimeout(t.Context(), 180*time.Second)
			defer cancel()
			image := func(n int64) map[string]any {
				p := map[string]any{"seq": n, "s": fmt.Sprintf("s-%d", n), "bytes": []byte{byte(n)}, "body": strings.Repeat(fmt.Sprintf("%d|", n), 40)}
				if n%3 == 0 {
					p["third"] = n
				}
				return p
			}
			verify := func(node *Node, names []string) error {
				seq, ok := node.Properties["seq"].(int64)
				if !ok {
					return fmt.Errorf("no seq in %v", node.Properties)
				}
				want := s1NormProps(s1Pick(image(seq), names))
				if got := s1NormProps(node.Properties); !reflect.DeepEqual(want, got) {
					return fmt.Errorf("torn image names=%v want %v got %v", names, want, got)
				}
				s1Scribble(node.Properties)
				return nil
			}
			const nodes = 6
			id := func(i int) NodeID { return NodeID(fmt.Sprintf("c:n%d", i)) }
			for i := 0; i < nodes; i++ {
				_, err := b.CreateNode(ctx, &Node{ID: id(i), Labels: []string{"C"}, Properties: image(0)})
				require.NoError(t, err)
			}
			var stop atomic.Bool
			errs := make(chan error, 64)
			report := func(err error) {
				select {
				case errs <- err:
				default:
				}
				stop.Store(true)
			}
			sets := [][]string{{"seq"}, {"seq", "s"}, {"seq", "bytes", "third"}, {"seq", "body", "s", "bytes", "third", "absent"}, {"seq", "absent"}}
			var readers, writers sync.WaitGroup
			for w := 0; w < 6; w++ {
				readers.Go(func() {
					r := rand.New(rand.NewSource(int64(w)))
					last := map[NodeID]int64{}
					for !stop.Load() {
						n := id(r.Intn(nodes))
						names := sets[r.Intn(len(sets))]
						node, err := b.GetNodeProjected(ctx, n, names)
						if errors.Is(err, ErrNotFound) && n == id(nodes-1) {
							continue // the last node is deleted and re-created
						}
						if err != nil {
							report(fmt.Errorf("projected: %w", err))
							return
						}
						seq, _ := node.Properties["seq"].(int64)
						if seq < last[n] {
							report(fmt.Errorf("%s went back %d -> %d", n, last[n], seq))
							return
						}
						last[n] = seq
						if err := verify(node, names); err != nil {
							report(err)
							return
						}
					}
				})
			}
			readers.Go(func() {
				for !stop.Load() {
					names := sets[3]
					err := b.StreamNodesByLabelProjected(ctx, "C", names, func(node *Node) error { return verify(node, names) })
					if err != nil {
						report(fmt.Errorf("label scan: %w", err))
						return
					}
				}
			})
			readers.Go(func() {
				for !stop.Load() {
					tx, err := b.BeginTransaction(ctx, OrdinaryBegin)
					if err != nil {
						report(err)
						return
					}
					first := map[NodeID]int64{}
					for round := 0; round < 20; round++ {
						for i := 0; i < nodes-1; i++ {
							names := sets[round%len(sets)]
							node, err := tx.GetNodeProjected(ctx, id(i), names)
							if err != nil {
								report(fmt.Errorf("snapshot read: %w", err))
								_ = tx.Rollback()
								return
							}
							seq, _ := node.Properties["seq"].(int64)
							if prior, ok := first[id(i)]; ok && prior != seq {
								report(fmt.Errorf("snapshot moved %s %d -> %d", id(i), prior, seq))
							}
							first[id(i)] = seq
							if err := verify(node, names); err != nil {
								report(fmt.Errorf("snapshot: %w", err))
							}
						}
					}
					_ = tx.Rollback()
				}
			})
			readers.Go(func() {
				for !stop.Load() {
					if _, err := b.PruneMVCCVersions(ctx, MVCCPruneOptions{MaxVersionsPerKey: 1}); err != nil {
						report(fmt.Errorf("prune: %w", err))
						return
					}
					_, _ = b.GetNode(ctx, id(0))
					_, _ = b.GetQueryNode(ctx, id(1))
					time.Sleep(time.Millisecond)
				}
			})
			for i := 0; i < nodes; i++ {
				writers.Go(func() {
					for n := int64(1); n <= 250 && !stop.Load(); n++ {
						if i == nodes-1 && n%10 == 0 {
							if err := b.DeleteNode(ctx, id(i)); err != nil {
								report(fmt.Errorf("delete: %w", err))
								return
							}
							if _, err := b.GetNodeProjected(ctx, id(i), []string{"seq"}); !errors.Is(err, ErrNotFound) {
								report(fmt.Errorf("deleted node readable: %v", err))
								return
							}
							if _, err := b.CreateNode(ctx, &Node{ID: id(i), Labels: []string{"C"}, Properties: image(n)}); err != nil {
								report(fmt.Errorf("recreate: %w", err))
								return
							}
						} else if err := b.UpdateNode(ctx, NodeUpdateRequest{After: &Node{ID: id(i), Labels: []string{"C"}, Properties: image(n)}}); err != nil {
							report(fmt.Errorf("update: %w", err))
							return
						}
						node, err := b.GetNodeProjected(ctx, id(i), []string{"seq", "third"})
						if err != nil || node.Properties["seq"] != n {
							report(fmt.Errorf("own write not visible: want %d node=%v err=%v", n, node, err))
							return
						}
					}
				})
			}
			writers.Wait()
			stop.Store(true)
			readers.Wait()
			close(errs)
			for err := range errs {
				t.Error(err)
			}
		})
	}
}

// s1StoredBody returns the stored bytes of a node.
func s1StoredBody(t testing.TB, b *BadgerEngine, id NodeID) []byte {
	var valid []byte
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(id))
		if err != nil {
			return err
		}
		valid, err = item.ValueCopy(nil)
		return err
	}))
	return valid
}

// s1DecoderVerdict compares the full decode of body with every projected
// decode of it. A panic is reported as a verdict.
func s1DecoderVerdict(b *BadgerEngine, id NodeID, body []byte, names []string) (verdict string) {
	defer func() {
		if p := recover(); p != nil {
			verdict = fmt.Sprintf("PANIC %v", p)
		}
	}()
	full, fullErr := b.decodeNodeProjected(nil, namespaceForNodeID(id), body, nil)
	point, pointErr := b.decodeNodeProjected(nil, namespaceForNodeID(id), body, propertyProjectionSet(names))
	scan, scanErr := newProjectedNodeDecoder(b, nil, StreamNodesOptions{Projection: names}).decode(nodeKey(id), body)
	if fullErr == nil {
		if pointErr != nil || scanErr != nil {
			return fmt.Sprintf("FULL-OK-PROJECTED-ERR point=%v scan=%v", pointErr, scanErr)
		}
		want := selectProperties(full.Properties, names)
		if !reflect.DeepEqual(want, point.Properties) || !reflect.DeepEqual(want, scan.Properties) {
			// NaN compares unequal to itself; compare rendered forms as well
			if fmt.Sprintf("%#v", want) != fmt.Sprintf("%#v", point.Properties) || fmt.Sprintf("%#v", want) != fmt.Sprintf("%#v", scan.Properties) {
				return fmt.Sprintf("DIFFER want=%#v point=%#v scan=%#v", want, point.Properties, scan.Properties)
			}
		}
		if !reflect.DeepEqual(full.Labels, point.Labels) || full.ID != point.ID {
			return "METADATA-DIFFERS"
		}
		return "ok"
	}
	if (pointErr == nil) != (scanErr == nil) {
		// A token the dictionary does not name is an error for the point decoder
		// and skipped by the scan decoder; both revisions do this.
		if pointErr != nil && strings.Contains(pointErr.Error(), "not in dictionary") {
			return "full-err-unknown-token"
		}
		return fmt.Sprintf("DECODERS-DISAGREE point=%v scan=%v full=%v", pointErr, scanErr, fullErr)
	}
	if pointErr == nil {
		return "full-err-projected-ok"
	}
	return "both-err"
}

// s1Ext is an opaque MessagePack extension with a variable-length payload.
type s1Ext struct{ Data []byte }

func init() {
	msgpack.RegisterExtEncoder(101, s1Ext{}, func(_ *msgpack.Encoder, v reflect.Value) ([]byte, error) {
		return v.Interface().(s1Ext).Data, nil
	})
	msgpack.RegisterExtDecoder(101, s1Ext{}, func(d *msgpack.Decoder, v reflect.Value, n int) error {
		data := make([]byte, n)
		if err := d.ReadFull(data); err != nil {
			return err
		}
		v.Set(reflect.ValueOf(s1Ext{Data: data}))
		return nil
	})
}

func s1FuzzFixture(t testing.TB, b *BadgerEngine) ([]NodeID, [][]string) {
	specs := []map[string]any{
		{"keep": "value", "bytes": []byte{0, 1, 2, 255}, "skip": strings.Repeat("x", 1024), "list": []any{int64(1), "two", false}, "nested": map[string]any{"n": int64(7), "l": []any{"a"}}},
		{"keep": "", "skip": strings.Repeat("y", 70000), "bytes": bytes.Repeat([]byte{1}, 70000)},
		{"keep": int64(-5), "when": time.Unix(1700000000, 5).UTC(), "float": 1.5, "flag": true, "vec": []float32{1, 2, 3}, "strs": []string{"a", ""}},
		{"keep": strings.Repeat("k", 31), "skip": strings.Repeat("k", 32), "bytes": bytes.Repeat([]byte{2}, 255)},
	}
	var ids []NodeID
	for i, p := range specs {
		id := NodeID(fmt.Sprintf("fz:%d", i))
		_, err := b.CreateNode(context.Background(), &Node{ID: id, Labels: []string{"Fuzz"}, Properties: p})
		require.NoError(t, err)
		ids = append(ids, id)
	}
	// Opaque extension values of several widths. The client write contract may
	// refuse the type, so a refusal is logged and the fixture goes on without.
	id := NodeID("fz:ext")
	if _, err := b.CreateNode(context.Background(), &Node{ID: id, Labels: []string{"Fuzz"}, Properties: map[string]any{
		"keep": "k", "ext": s1Ext{Data: bytes.Repeat([]byte{9}, 3)}, "ext2": s1Ext{Data: bytes.Repeat([]byte{9}, 300)}, "ext16": s1Ext{Data: bytes.Repeat([]byte{9}, 16)}}}); err != nil {
		t.Logf("extension value refused by the write path: %v", err)
	} else {
		ids = append(ids, id)
	}
	return ids, [][]string{{"keep"}, {"bytes", "nested"}, {}, {"missing", "list"}, {"skip"}, {"when", "ext"}, {"vec", "float", "strs"}}
}

// TestPR1826S1MutatedBodies mutates stored bodies systematically (every
// truncation, every byte set to boundary values, random splices) and prints the
// verdict counts: the counts must be the same on both revisions.
func TestPR1826S1MutatedBodies(t *testing.T) {
	b, _ := createTestBadgerEngineOnDisk(t)
	ids, sets := s1FuzzFixture(t, b)
	counts := map[string]int{}
	bad := 0
	judge := func(id NodeID, body []byte, what string) {
		for _, names := range sets {
			v := s1DecoderVerdict(b, id, body, names)
			key := v
			if i := strings.IndexByte(v, ' '); i > 0 {
				key = v[:i]
			}
			counts[key]++
			if key != "ok" && key != "both-err" && key != "full-err-projected-ok" && key != "full-err-unknown-token" {
				bad++
				if bad <= 15 {
					t.Errorf("%s id=%s names=%v: %.400s", what, id, names, v)
				}
			}
		}
	}
	r := rand.New(rand.NewSource(1826))
	for _, id := range ids {
		body := s1StoredBody(t, b, id)
		judge(id, body, "valid")
		step := 1
		if len(body) > 4000 {
			step = len(body) / 1000
		}
		for i := 0; i < len(body); i += step {
			judge(id, body[:i], fmt.Sprintf("truncate %d", i))
			for _, x := range []byte{0x00, 0x7f, 0x80, 0xa0, 0xbf, 0xc0, 0xc1, 0xc4, 0xc5, 0xc6, 0xc7, 0xc8, 0xc9, 0xd4, 0xd8, 0xd9, 0xda, 0xdb, 0xdc, 0xdd, 0xde, 0xdf, 0xff} {
				m := append([]byte(nil), body...)
				m[i] = x
				judge(id, m, fmt.Sprintf("byte %d=%02x", i, x))
			}
		}
		for k := 0; k < 4000; k++ {
			m := append([]byte(nil), body...)
			if len(m) > 4000 {
				m = m[:200+r.Intn(3000)]
			}
			edits := 1 + r.Intn(4)
			for e := 0; e < edits && len(m) > 0; e++ {
				p := r.Intn(len(m))
				switch r.Intn(3) {
				case 0:
					m[p] = byte(r.Intn(256))
				case 1:
					m = append(m[:p], m[min(len(m), p+1+r.Intn(5)):]...)
				case 2:
					ins := make([]byte, 1+r.Intn(6))
					for i := range ins {
						ins[i] = byte(r.Intn(256))
					}
					m = append(m[:p:p], append(ins, m[p:]...)...)
				}
			}
			judge(id, m, "random")
		}
	}
	t.Logf("S1COUNT mutated bodies: %v", counts)
}

// FuzzPR1826S1FullAndProjectedAgree: whatever the full decoder accepts, both
// projected decoders accept with the same selected values; neither panics.
func FuzzPR1826S1FullAndProjectedAgree(f *testing.F) {
	b, err := NewBadgerEngine(f.TempDir())
	require.NoError(f, err)
	f.Cleanup(func() { _ = b.Close() })
	ids, sets := s1FuzzFixture(f, b)
	for i, id := range ids {
		body := s1StoredBody(f, b, id)
		if len(body) > 4096 {
			body = body[:4096]
		}
		f.Add(body, uint8(i))
		f.Add(body[:len(body)/2], uint8(i))
	}
	f.Fuzz(func(t *testing.T, body []byte, selector uint8) {
		names := sets[int(selector)%len(sets)]
		v := s1DecoderVerdict(b, ids[0], body, names)
		if v != "ok" && v != "both-err" && v != "full-err-projected-ok" && v != "full-err-unknown-token" {
			t.Fatalf("names=%v verdict %s body=%x", names, v, body)
		}
	})
}
