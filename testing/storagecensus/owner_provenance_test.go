package storagecensus

import (
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCensusRejectsSuppressionEvaluatorBypass(t *testing.T) {
	abs, err := filepath.Abs(root)
	require.NoError(t, err)
	path := filepath.Join(abs, "pkg/storage/badger_deindex_enqueue.go")
	source, err := os.ReadFile(path)
	require.NoError(t, err)
	text := strings.Replace(string(source), "b.evaluateEdgeSuppressionInTxn(txn, EdgeID(entityID))", "b.censusBypassEdgeSuppressionInTxn(txn, EdgeID(entityID))", 1)
	require.NotEqual(t, string(source), text)
	text += `
func(b *BadgerEngine)censusBypassEdgeSuppressionInTxn(txn *badger.Txn,id EdgeID)(bool,*Edge,error){
 edge,err:=b.GetStoredEdge(id)
 if err==nil { data,encodeErr:=b.encodeEdgeInTxn(txn,namespaceForEdgeID(id),edge);if encodeErr!=nil{return false,nil,encodeErr};return true,edge,txn.Set(edgeKey(id),data) }
 return false,nil,err
}
`
	report, err := Census(root, map[string][]byte{path: []byte(text)})
	require.NoError(t, err)
	base, err := Census(root, nil)
	require.NoError(t, err)
	require.Empty(t, base.Violations)
	require.Contains(t, strings.Join(report.Violations, "\n"), "hidden decision outside the disposition owner: pkg/cypher.StorageExecutor.executeCreateConstraint")
	require.Contains(t, strings.Join(report.Violations, "\n"), "stale: decision owner pkg/storage.BadgerEngine.evaluateEdgeSuppressionInTxn")
}

// A legitimate effect cannot mask a second direct body write, and a method
// with a similar name cannot acquire the evaluator's ownership.
func TestBodyOwnershipRequiresEveryEffectPath(t *testing.T) {
	p, pkg := coverageProgram(t, `package storage
 type BadgerEngine struct{}
 func(b *BadgerEngine)evaluateEdgeSuppressionInTxn(){}
 func(b *BadgerEngine)evaluateEdgeSuppressionInTxnLookalike(){}
 func(b *BadgerEngine)bypass(){}
 func owned(b *BadgerEngine){b.evaluateEdgeSuppressionInTxn()}
 func mixed(b *BadgerEngine){b.evaluateEdgeSuppressionInTxn();b.bypass()}
 func lookalike(b *BadgerEngine){b.evaluateEdgeSuppressionInTxnLookalike()}
 `)
	a := &analysis{p: p, bodyMarks: map[ssa.Instruction]bool{}, bodyChains: map[ssa.Instruction]map[string][]string{}}
	for fn := range ssautil.AllFunctions(pkg.Prog) {
		if fn.Pkg != pkg {
			continue
		}
		p.funcs = append(p.funcs, fn)
		if fn.Signature.Recv() != nil {
			ins := fn.Blocks[0].Instrs[0]
			a.bodyMarks[ins] = true
			a.bodyChains[ins] = map[string][]string{"body": {p.funcName(fn) + " body"}}
		}
	}
	a.markBodyWriters()
	proofs := a.bodyOwnerProofs()
	require.False(t, proofs[pkg.Func("owned")].unowned)
	require.True(t, proofs[pkg.Func("owned")].owners["pkg/storage.BadgerEngine.evaluateEdgeSuppressionInTxn"])
	require.True(t, proofs[pkg.Func("mixed")].unowned)
	require.True(t, proofs[pkg.Func("lookalike")].unowned)
}

func TestPublicationTimestampHasExactOwner(t *testing.T) {
	p, pkg := coverageProgram(t, `package storage
 type commitPublication struct{ts uint64}
 type commitPublicationLookalike struct{ts uint64}
 type prefixDrops struct{pendingCount uint64; pending map[string]*commitPublication}
 type BadgerEngine struct{prefixDrops prefixDrops}
 func(b *BadgerEngine)transitionPending(){b.prefixDrops.pendingCount=1;b.prefixDrops.pending["held"]=&commitPublication{}}
 func rogueDrop(d *prefixDrops){d.pendingCount=2;alias:=d.pending;delete(alias,"held")}
 type publicationStage struct{}
 func(s *publicationStage)register(ts uint64,pub *commitPublication){pub.ts=ts}
 func owner(s *publicationStage,pub *commitPublication){s.register(3,pub)}
 func rogue(pub *commitPublication){pub.ts=7}
 func lookalike(pub *commitPublicationLookalike){pub.ts=7}
 `)
	for fn := range ssautil.AllFunctions(pkg.Prog) {
		if fn.Pkg == pkg {
			p.funcs = append(p.funcs, fn)
		}
	}
	a := &analysis{p: p}
	r := &Report{}
	a.checkFields(r)
	text := strings.Join(r.Violations, "\n")
	require.Contains(t, text, "unowned publication-order change: pkg/storage.commitPublication.ts")
	require.Contains(t, text, "in pkg/storage.rogue")
	require.NotContains(t, text, "in pkg/storage.publicationStage.register")
	for _, field := range []string{"pendingCount", "pending"} {
		require.Contains(t, text, "unowned publication-order change: pkg/storage.prefixDrops."+field)
	}
	require.Contains(t, text, "in pkg/storage.rogueDrop")
	require.NotContains(t, text, "in pkg/storage.BadgerEngine.transitionPending")
	_, classified := classOf("pkg/storage.commitPublicationLookalike.ts")
	require.False(t, classified)
}

func TestReviewCensusBranchEveryGatedInstruction(t *testing.T) {
	p, pkg := coverageProgram(t, `package storage
 type BadgerEngine struct{}
 func(b *BadgerEngine)evaluateEdgeSuppressionInTxn(){}
 func(b *BadgerEngine)bypass(){}
 func(b *BadgerEngine)hidesNode()bool{return true}
 func deciding(b *BadgerEngine){if b.hidesNode(){b.evaluateEdgeSuppressionInTxn();b.bypass()}}
 func mixed(b *BadgerEngine,hidden bool){if hidden {b.evaluateEdgeSuppressionInTxn();b.bypass()}}
 `)
	a := &analysis{p: p, bodyMarks: map[ssa.Instruction]bool{}, bodyChains: map[ssa.Instruction]map[string][]string{}}
	for fn := range ssautil.AllFunctions(pkg.Prog) {
		if fn.Pkg != pkg {
			continue
		}
		p.funcs = append(p.funcs, fn)
		if fn.Name() == "evaluateEdgeSuppressionInTxn" || fn.Name() == "bypass" {
			ins := fn.Blocks[0].Instrs[0]
			a.bodyMarks[ins] = true
			a.bodyChains[ins] = map[string][]string{"body": {p.funcName(fn) + " body"}}
		}
	}
	a.markBodyWriters()
	found := false
	for _, decision := range a.decisions() {
		if decision.Func == "pkg/storage.deciding" {
			found = true
			require.True(t, decision.unowned, "the real decision collector must reject the bypass")
		}
	}
	require.True(t, found, "the hidden decision must be collected")
	proofs := a.bodyOwnerProofs()
	fn := pkg.Func("mixed")
	require.True(t, proofs[fn].unowned, "aggregate sees the bypass")
	d := &decider{a: a}
	for _, block := range fn.Blocks {
		if _, ok := block.Instrs[len(block.Instrs)-1].(*ssa.If); ok {
			gates := d.gatedWrites(fn, block)
			require.Len(t, gates, 2)
			proof := d.branchOwnerProof(fn, block, proofs)
			require.True(t, proof.unowned, "branch ownership must account for the later bypass instruction, not only its first owned instruction")
		}
	}
}
