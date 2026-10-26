package storagecensus

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

// Author: Lane #1644 (Codex). The real shared loader supplies the call graph.
// Indirect callbacks and structural fact results must not erase a stored-read
// dependency; a same-named operation returning a different type grants none.
func TestStoredDecisionDependencyThroughDispatch(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "pkg", "storage"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module github.com/orneryd/nornicdb\ngo 1.26.0\n"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pkg", "storage", "fixture.go"), []byte(`package storage
 type Node struct{}
 type StoredEntityReader interface{Enumerate(func(*Node)error)error;GetStoredNode()(*Node,error)}
 type Engine interface{GetNode()(*Node,error)}
 type StoredNodeFact struct{Exists bool}
 type StoredMetadata struct{Nodes []StoredNodeFact}
 type provider struct{}
 type stopped struct{}
 func(stopped)Error()string{return "stopped"}
 var stop error=stopped{}
 func(provider)Enumerate(visit func(*Node)error)error{return visit(&Node{})}
 func(provider)GetStoredNode()(*Node,error){return &Node{},nil}
 func(p provider)GetNode()(*Node,error){return p.GetStoredNode()}
 func(provider)ReadStored()StoredMetadata{return StoredMetadata{[]StoredNodeFact{{true}}}}
 type storedStream interface{Enumerate(func(*Node)error)error}
 func VisitWithCapability[T any](reader T,work func(T)error)error{return work(reader)}
 func empty(reader storedStream)bool{
  invoke:=VisitWithCapability[storedStream]
  err:=invoke(reader,func(selected storedStream)error{return selected.Enumerate(func(*Node)error{return stop})})
  if err==stop{return false};return true
 }
 func projected(read func()StoredMetadata)bool{return read().Nodes[0].Exists}
 var state int
 func mutate(){state++}
 func callbackDecision(){if empty(provider{}){mutate()}}
 func factDecision(){if projected(provider{}.ReadStored){mutate()}}
 func visibleDecision(){node,_:=(provider{}).GetNode();if node!=nil{mutate()}}
 type ordinary struct{}
 type OtherMetadata struct{Exists bool}
 func(ordinary)ReadStored()OtherMetadata{return OtherMetadata{true}}
 func(ordinary)VisitWithCapability(reader storedStream,work func(storedStream)error)error{return nil}
 func unrelatedDecision(){if (ordinary{}).ReadStored().Exists{mutate()}}
 func unrelatedRegistration(){if (ordinary{}).VisitWithCapability(provider{},nil)==nil{mutate()}}
 `), 0644))
	p, err := load(dir, nil)
	require.NoError(t, err)
	a := &analysis{p: p, bodyMarks: map[ssa.Instruction]bool{}, bodyChains: map[ssa.Instruction]map[string][]string{}}
	for _, fn := range p.funcs {
		if fn.Name() != "mutate" {
			continue
		}
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				if _, ok := ins.(*ssa.Store); ok {
					a.bodyMarks[ins] = true
					a.bodyChains[ins] = map[string][]string{"fixture-body": {p.funcName(fn) + " fixture-body"}}
				}
			}
		}
	}
	require.NotEmpty(t, a.bodyMarks)
	a.markBodyWriters()
	found := map[string]bool{}
	for _, decision := range a.decisions() {
		found[decision.Func] = true
	}
	require.True(t, found["pkg/storage.callbackDecision"], "the branch depends on a stored callback result through a function value")
	require.True(t, found["pkg/storage.factDecision"], "a payload-free fact is a stored-state source")
	require.False(t, found["pkg/storage.unrelatedDecision"], "a coincidental method name and field do not identify stored facts")
	require.False(t, found["pkg/storage.visibleDecision"], "an Engine visible outcome does not disclose the underlying stored state")
	require.False(t, found["pkg/storage.unrelatedRegistration"], "passing a reader to a same-named unrelated method does not execute a read")
}
