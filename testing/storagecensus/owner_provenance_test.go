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
	text := strings.ReplaceAll(string(source), "b.evaluateSuppressionInTxn(", "b.censusBypassSuppressionInTxn(")
	require.NotEqual(t, string(source), text)
	// Retire the declaration as well as its callers: the shared transition
	// still has a real ownership decision even when no entry point calls it.
	retired := strings.Replace(text, "func (b *BadgerEngine) evaluateSuppressionInTxn(", "func (b *BadgerEngine) retiredSuppressionInTxn(", 1)
	require.NotEqual(t, text, retired)
	text = retired
	text += `
func(b *BadgerEngine)censusBypassSuppressionInTxn(txn *badger.Txn,input suppressionEvaluation)(suppressionMutation,error){
 id:=input.id;if input.node!=nil{id=string(input.node.ID)}
 edge,err:=b.GetStoredEdge(EdgeID(id))
 if err==nil { data,encodeErr:=b.encodeEdgeInTxn(txn,namespaceForEdgeID(EdgeID(id)),edge);if encodeErr!=nil{return suppressionMutation{},encodeErr};return suppressionMutation{edge:edge},txn.Set(edgeKey(EdgeID(id)),data) }
 return suppressionMutation{},err
}
`
	labelPath := filepath.Join(abs, "pkg/storage/badger_nodes.go")
	labels, err := os.ReadFile(labelPath)
	require.NoError(t, err)
	labelText := strings.ReplaceAll(string(labels), "b.evaluateSuppressionInTxn(", "b.censusBypassSuppressionInTxn(")
	require.NotEqual(t, string(labels), labelText)
	report, err := Census(root, map[string][]byte{path: []byte(text), labelPath: []byte(labelText)})
	require.NoError(t, err)
	base, err := Census(root, nil)
	require.NoError(t, err)
	require.Empty(t, base.Violations)
	require.Contains(t, strings.Join(report.Violations, "\n"), "hidden decision outside the disposition owner: pkg/cypher.StorageExecutor.executeCreateConstraint")
	require.Contains(t, strings.Join(report.Violations, "\n"), "stale: decision owner pkg/storage.BadgerEngine.evaluateSuppressionInTxn")
}

// A legitimate effect cannot mask a second direct body write, and a method
// with a similar name cannot acquire the evaluator's ownership.
func TestBodyOwnershipRequiresEveryEffectPath(t *testing.T) {
	p, pkg := coverageProgram(t, `package storage
 type BadgerEngine struct{}
 func(b *BadgerEngine)evaluateSuppressionInTxn(){}
 func(b *BadgerEngine)evaluateSuppressionInTxnLookalike(){}
 func(b *BadgerEngine)bypass(){}
 func owned(b *BadgerEngine){b.evaluateSuppressionInTxn()}
 func mixed(b *BadgerEngine){b.evaluateSuppressionInTxn();b.bypass()}
 func lookalike(b *BadgerEngine){b.evaluateSuppressionInTxnLookalike()}
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
	require.True(t, proofs[pkg.Func("owned")].owners["pkg/storage.BadgerEngine.evaluateSuppressionInTxn"])
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
 func(b *BadgerEngine)evaluateSuppressionInTxn(){}
 func(b *BadgerEngine)bypass(){}
 func(b *BadgerEngine)filterEntityByDecay()bool{return true}
 func deciding(b *BadgerEngine){if b.filterEntityByDecay(){b.evaluateSuppressionInTxn();b.bypass()}}
 func mixed(b *BadgerEngine,hidden bool){if hidden {b.evaluateSuppressionInTxn();b.bypass()}}
 `)
	a := &analysis{p: p, bodyMarks: map[ssa.Instruction]bool{}, bodyChains: map[ssa.Instruction]map[string][]string{}}
	for fn := range ssautil.AllFunctions(pkg.Prog) {
		if fn.Pkg != pkg {
			continue
		}
		p.funcs = append(p.funcs, fn)
		if fn.Name() == "evaluateSuppressionInTxn" || fn.Name() == "bypass" {
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
