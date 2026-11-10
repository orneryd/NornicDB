package storagecensus

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

// orderGateFixture is the write-behind order gate the census reads: a commit
// decides once whether the buffer takes it, the direct-write gate acquires the
// order only for the commit the buffer does not take, and the direct writes
// follow the branch that returns a buffered commit. Every probe (sink) is one
// write boundary, the acquisition is the census's own order owner, and the
// release is that owner's own release.
const orderGateFixture = `package storage
 type BadgerEngine struct{ writeBehind *int }
 func (b *BadgerEngine) writeBehindEnabled() bool { return b.writeBehind != nil }
 type writtenEntities struct{}
 type orderHold struct{}
 type bufferedOrderResources struct{}
 type writeRelease[T any] struct{}
 func (writeRelease[T]) release() {}
 func (b *BadgerEngine) orderAfterBufferedCommits(o *orderHold, e writtenEntities) (int, error) { return 0, nil }
 func (o *orderHold) release() {}
 func noop() {}
 func sink() {}
 func repeated(b *BadgerEngine, implicit bool) error {
  buffered := b.writeBehindEnabled() && implicit
  if !buffered && b.writeBehindEnabled() {
   var order orderHold
   if _, err := b.orderAfterBufferedCommits(&order, writtenEntities{}); err != nil {
    return err
   }
   defer order.release()
  }
  if buffered {
   return nil
  }
  sink()
  return nil
 }
 func inverted(b *BadgerEngine, implicit bool) error {
  buffered := b.writeBehindEnabled() && implicit
  if buffered == false && b.writeBehindEnabled() {
   var order orderHold
   if _, err := b.orderAfterBufferedCommits(&order, writtenEntities{}); err != nil {
    return err
   }
   defer order.release()
  }
  if !buffered {
   sink()
  }
  return nil
 }
 func missing(b *BadgerEngine, implicit bool) error {
  buffered := b.writeBehindEnabled() && implicit
  if !buffered && b.writeBehindEnabled() {
   noop()
  }
  if buffered {
   return nil
  }
  sink()
  return nil
 }
 func released(b *BadgerEngine, implicit bool) error {
  buffered := b.writeBehindEnabled() && implicit
  if !buffered && b.writeBehindEnabled() {
   var order orderHold
   if _, err := b.orderAfterBufferedCommits(&order, writtenEntities{}); err != nil {
    return err
   }
   writeRelease[bufferedOrderResources]{}.release()
  }
  if buffered {
   return nil
  }
  sink()
  return nil
 }
 func changed(b *BadgerEngine, implicit bool) error {
  buffered := b.writeBehindEnabled() && implicit
  if !buffered && b.writeBehindEnabled() {
   var order orderHold
   if _, err := b.orderAfterBufferedCommits(&order, writtenEntities{}); err != nil {
    return err
   }
   defer order.release()
  }
  again := b.writeBehindEnabled() && implicit
  if again {
   return nil
  }
  sink()
  return nil
 }
 func reloaded(b *BadgerEngine, implicit bool) error {
  buffered := b.writeBehindEnabled() && implicit
  if !buffered && b.writeBehindEnabled() {
   var order orderHold
   if _, err := b.orderAfterBufferedCommits(&order, writtenEntities{}); err != nil {
    return err
   }
   defer order.release()
  }
  if b.writeBehindEnabled() && implicit {
   return nil
  }
  sink()
  return nil
 }
 func independent(b *BadgerEngine, implicit, unrelated bool) error {
  buffered := b.writeBehindEnabled() && implicit
  if !buffered && b.writeBehindEnabled() {
   var order orderHold
   if _, err := b.orderAfterBufferedCommits(&order, writtenEntities{}); err != nil {
    return err
   }
   defer order.release()
  }
  if unrelated {
   return nil
  }
  sink()
  return nil
 }
 func unknown(b *BadgerEngine, implicit bool) error {
  buffered := b.writeBehindEnabled() && implicit
  if !buffered && b.writeBehindEnabled() {
   var order orderHold
   if _, err := b.orderAfterBufferedCommits(&order, writtenEntities{}); err != nil {
    return err
   }
   defer order.release()
  }
  if buffered {
   noop()
  }
  sink()
  return nil
 }
 func failed(b *BadgerEngine, implicit bool) error {
  buffered := b.writeBehindEnabled() && implicit
  if !buffered && b.writeBehindEnabled() {
   var order orderHold
   if _, err := b.orderAfterBufferedCommits(&order, writtenEntities{}); err != nil {
    sink()
    return err
   }
   defer order.release()
  }
  if buffered {
   return nil
  }
  return nil
 }
 func unguarded(b *BadgerEngine, implicit bool) error {
  buffered := b.writeBehindEnabled() && implicit
  if !buffered && b.writeBehindEnabled() {
   var order orderHold
   if _, err := b.orderAfterBufferedCommits(&order, writtenEntities{}); err != nil {
    return err
   }
   defer order.release()
  }
  if buffered {
   sink()
  }
  return nil
 }
 func recomputed(b *BadgerEngine, pending []bool) error {
  for _, buffered := range pending {
   if !buffered && b.writeBehindEnabled() {
    var order orderHold
    if _, err := b.orderAfterBufferedCommits(&order, writtenEntities{}); err != nil {
     return err
    }
    defer order.release()
   }
   if buffered {
    return nil
   }
   sink()
  }
  return nil
 }
`

// TestWriteOrderGateUsesOnlyAnImmutableCommittedBoolean proves the census
// reads the order gate's own boolean: a branch that fixes one immutable SSA
// boolean for the queried instruction decides which earlier edges can reach
// it, so a repeated (or inverted) decision proves the acquisition on every
// path that reaches the write. Every other shape stays unproven: a missing,
// failed or released acquisition, a predicate read or computed a second time,
// an unrelated guard, a join the guard does not fix, a write on the buffered
// path, and a predicate a loop computes again.
func TestWriteOrderGateUsesOnlyAnImmutableCommittedBoolean(t *testing.T) {
	p, pkg := ssaFixture(t, orderGateFixture, "pkg/storage", ssa.SanityCheckFunctions, standaloneFixture)
	q := &acquirer{p: p, cache: map[flowKey]*flow{}}
	for _, tc := range []struct {
		name    string
		ordered bool
	}{
		{"repeated", true},
		{"inverted", true},
		{"missing", false},
		{"released", false},
		{"changed", false},
		{"reloaded", false},
		{"independent", false},
		{"unknown", false},
		{"failed", false},
		{"unguarded", false},
		{"recomputed", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.ordered, q.covers(pkg.Func(tc.name), callTo(t, pkg.Func(tc.name), "sink"))&orderKind != 0)
		})
	}
	// The release control is genuine only if the census recognizes its call as
	// the order owner's release: with the same gate and the same fact, that
	// hold-ending call is the one difference between repeated and released.
	released := pkg.Func("released")
	require.True(t, q.flow(released, 0, flowAssumption{}).kills[callTo(t, released, "release")],
		"the fixture release ends the order the gate acquired")
}

// booleanTestFixture holds the three condition forms the census reads about
// one boolean: the value, that value's negation, and a comparison with a
// literal.
const booleanTestFixture = `package storage
 func probe(flag bool) bool {
  inverse := !flag
  if inverse {
   return true
  }
  if flag == false {
   return false
  }
  if flag != false {
   return true
  }
  return false
 }
`

// TestBooleanTestNormalizesNotAndBooleanLiteral is the fixture's own check:
// a negation and a comparison with a literal normalize to the one value they
// are about, with the truth the tested condition takes when that value is
// true. Neither the negation nor the comparison becomes a value of its own.
func TestBooleanTestNormalizesNotAndBooleanLiteral(t *testing.T) {
	_, pkg := ssaFixture(t, booleanTestFixture, "pkg/storage", ssa.SanityCheckFunctions, standaloneFixture)
	fn := pkg.Func("probe")
	flag := fn.Params[0]
	var positives []bool
	for _, block := range fn.Blocks {
		condition, ok := block.Instrs[len(block.Instrs)-1].(*ssa.If)
		if !ok {
			continue
		}
		value, positive, ok := booleanTest(condition.Cond)
		require.True(t, ok, "every probe condition is a boolean")
		require.Equal(t, flag, value, "each condition names the one boolean it is about")
		positives = append(positives, positive)
	}
	require.Equal(t, []bool{false, false, true}, positives,
		"a negated boolean, an equality with false, and the boolean itself")
}
