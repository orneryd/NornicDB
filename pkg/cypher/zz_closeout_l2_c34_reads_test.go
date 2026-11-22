//go:build closeout_l2_c34

package cypher

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// c34ReadSpy records allocation bytes and a stack at each point-read API call.
// The returned node's decoded body is excluded from any synthetic byte count;
// allocated bytes are the direct per-call TotalAlloc delta.
type c34ReadSpy struct {
	*storage.NamespacedEngine
	t *testing.T
	active bool
}

func (s *c34ReadSpy) record(kind string, id storage.NodeID, read func() (*storage.Node, error)) (*storage.Node, error) {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	n, err := read()
	runtime.ReadMemStats(&after)
	if !s.active { return n, err }
	pcs := make([]uintptr, 16)
	nf := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:nf])
	var stack []string
	for len(stack) < 8 {
		f, more := frames.Next()
		if f.Function == "" {
			break
		}
		if strings.Contains(f.Function, "nornicdb") {
			stack = append(stack, fmt.Sprintf("%s:%d", f.Function, f.Line))
		}
		if !more {
			break
		}
	}
	s.t.Logf("C34READ type=%s id=%s bytes_allocated=%d stack=%s", kind, id, after.TotalAlloc-before.TotalAlloc, strings.Join(stack, " <- "))
	return n, err
}

func (s *c34ReadSpy) GetQueryNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	return s.record("GetQueryNode", id, func() (*storage.Node, error) { return s.NamespacedEngine.GetQueryNode(ctx, id) })
}
func (s *c34ReadSpy) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	return s.record("GetNode", id, func() (*storage.Node, error) { return s.NamespacedEngine.GetNode(ctx, id) })
}
func (s *c34ReadSpy) GetNodeProjected(ctx context.Context, id storage.NodeID, fields []string) (*storage.Node, error) {
	label := "GetNodeProjected"
	if fields == nil {
		label += "(nil)"
	} else {
		label += fmt.Sprintf("(%d fields)", len(fields))
	}
	return s.record(label, id, func() (*storage.Node, error) { return s.NamespacedEngine.GetNodeProjected(ctx, id, fields) })
}

func (s *c34ReadSpy) recordOther(kind string, read func() error) error {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err := read()
	runtime.ReadMemStats(&after)
	if !s.active { return err }
	pcs := make([]uintptr, 16)
	nf := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:nf])
	var stack []string
	for len(stack) < 8 {
		f, more := frames.Next()
		if f.Function == "" { break }
		if strings.Contains(f.Function, "nornicdb") { stack = append(stack, fmt.Sprintf("%s:%d", f.Function, f.Line)) }
		if !more { break }
	}
	s.t.Logf("C34READ type=%s bytes_allocated=%d stack=%s", kind, after.TotalAlloc-before.TotalAlloc, strings.Join(stack, " <- "))
	return err
}

func (s *c34ReadSpy) GetNodesByLabel(ctx context.Context, label string) (nodes []*storage.Node, err error) {
	err = s.recordOther("GetNodesByLabel", func() error { nodes, err = s.NamespacedEngine.GetNodesByLabel(ctx, label); return err })
	return
}

func (s *c34ReadSpy) GetFirstNodeByLabel(ctx context.Context, label string) (node *storage.Node, err error) {
	err = s.recordOther("GetFirstNodeByLabel", func() error { node, err = s.NamespacedEngine.GetFirstNodeByLabel(ctx, label); return err })
	return
}

func (s *c34ReadSpy) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	return s.recordOther("StreamNodesWithOptions", func() error { return s.NamespacedEngine.StreamNodesWithOptions(ctx, opts, visit) })
}

func (s *c34ReadSpy) StreamNodesByLabelProjected(ctx context.Context, label string, properties []string, visit func(*storage.Node) error) error {
	kind := fmt.Sprintf("StreamNodesByLabelProjected(%d fields)", len(properties))
	if properties == nil { kind = "StreamNodesByLabelProjected(nil)" }
	return s.recordOther(kind, func() error { return s.NamespacedEngine.StreamNodesByLabelProjected(ctx, label, properties, visit) })
}

func TestC34_ReadsWhileEvaluating(t *testing.T) {
	base, err := testfixture.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	if err != nil { t.Fatal(err) }
	ns := storage.NewNamespacedEngine(base, "nornic")
	big := make([]interface{}, 2048)
	for i := range big { big[i] = int64(i) }
	if _, err := ns.CreateNode(context.Background(), &storage.Node{ID:"EIBig-1", Labels:[]string{"EIBig"}, Properties:map[string]interface{}{"small":int64(1),"big":big}, NamedEmbeddings:map[string][]float32{"default":make([]float32,4096)}, ChunkEmbeddings:[][]float32{make([]float32,2048),make([]float32,2048)}}); err != nil { t.Fatal(err) }
	spy := &c34ReadSpy{NamespacedEngine:ns, t:t}
	exec := NewStorageExecutor(spy, QueryCachePolicy{MaxEntries:0, TTL:0})
	spy.active = true
	for _, expr := range []string{"1", "labels(n)", "size(keys(n))", "coalesce(n, 1) IS NULL", "CASE WHEN n IS NULL THEN 0 ELSE 1 END"} {
		t.Run(expr, func(t *testing.T) {
			q := fmt.Sprintf("MATCH (n:EIBig) RETURN %s AS v", expr)
			if _, err := exec.Execute(context.Background(), q, nil); err != nil { t.Fatalf("%s: %v", q, err) }
		})
	}
}
