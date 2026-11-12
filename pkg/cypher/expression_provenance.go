package cypher

import (
	"context"
	"maps"
	"sync"
)

// PROFILE-scoped source provenance.
//
// A statement's PROFILE collector reports the programs the statement ran once
// per written occurrence, so a program invoked from an expression must say
// which occurrence of that expression started it: two equal scalar subqueries
// in one projection are two operators, one correlated occurrence is one.
//
// The occurrence travels in two pieces, both relative to the text they belong
// to. The row carries the expression's bound source origin
// (expression_fragment.go): how far into the clause's text the expression
// starts. The subquery reports the origin it was evaluated at, and the
// collector keys the records of the program started there by that occurrence.
// A statement that binds and reports none keeps exactly the records it kept
// before.
//
// A compiled program is keyed by expression text and shared between
// statements, so a program that reports occurrences may carry only positions
// relative to its own text. The statement's collector therefore owns a second,
// source-bearing compilation of the same text: the canonical compiler
// (rowValueProgramFor), compiled with origins, kept in the collector and
// discarded with it. Positions live in one PROFILE-only wrapper
// (rowSourceValueNode) that a source-bearing compilation attaches where it
// reports one, so a normal program carries neither a position nor a wrapper,
// and no statement identity enters a text-keyed cache.
//
// A predicate is read through its own compiler (row_predicate_program.go), so
// it has its own source-bearing compilation: the collector compiles the
// predicate with the canonical predicate compiler, passing the predicate's own
// text as the position source, and installs the row's origin as the program's
// base (profileSourcePredicateProgramNode). Every part that compiler splits is
// an exact byte window of that text, so a part's position is read from the
// bytes it borrows (profilePredicateSourceFragment) rather than searched for,
// and a text the predicate delegates back to the canonical predicate evaluator
// carries where it starts (profileSourcePredicateNode). The row a PROFILE
// statement evaluates a predicate against is that evaluation's own
// (profileSourcePredicateValues), so the origin it binds never writes into a
// scope its caller may share.

// profileRuntimeSubqueryOccurrenceKey is the context slot holding the source
// occurrence of the subquery body currently running, when one was reported.
type profileRuntimeSubqueryOccurrenceKey struct{}

// withProfileRuntimeSubqueryOccurrence returns ctx carrying occurrence as the
// source occurrence of the subquery body about to run. An occurrence is known
// even when it is zero - the first position of a text that reports source
// positions - so it always replaces the occurrence ctx already carried: a body
// read inside a predicate reports where the predicate wrote it, never the
// position of the expression enclosing the predicate. Only a caller holding a
// bound origin reports one (rowSubqueryValue), so an ordinary statement still
// allocates nothing here.
func withProfileRuntimeSubqueryOccurrence(ctx context.Context, occurrence int) context.Context {
	return context.WithValue(ctx, profileRuntimeSubqueryOccurrenceKey{}, occurrence)
}

// profileRuntimeSubqueryOccurrence returns the source occurrence of the
// subquery body ctx runs, or 0 when it is not inside one.
func profileRuntimeSubqueryOccurrence(ctx context.Context) int {
	occurrence, _ := ctx.Value(profileRuntimeSubqueryOccurrenceKey{}).(int)
	return occurrence
}

// profileSourceBaseKey is the context slot holding the base origin a
// source-bearing program was entered with: where that program's own text starts
// in the statement's source. Its root installs the base once, for the program's
// whole synchronous evaluation, so every position inside the program is added to
// the same base.
type profileSourceBaseKey struct{}

// profileSourceProgramNode is a collector's source-bearing program root. It
// reads the origin bound for the row, which is where the program's text starts,
// and installs it as the program's base. A program entered from inside another
// source-bearing program reads the position that program's wrapper bound, so a
// nested program inherits its own base instead of adding an outer position to
// its text again.
type profileSourceProgramNode struct{ root rowValueNode }

func (n *profileSourceProgramNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	base := rowExpressionOrigin(values)
	if !base.had {
		return n.root.eval(ctx, e, values, frames)
	}
	return n.root.eval(context.WithValue(ctx, profileSourceBaseKey{}, base.previous), e, values, frames)
}

// rowSourceValueNode is a form that reports where it starts in the text of the
// source-bearing program it was compiled in: the one PROFILE-only wrapper a
// source-bearing compilation attaches, and the only place a compiled program
// keeps a position. It binds that position, with the program's base, for the
// form's evaluation and restores the row's origin afterwards, so the form after
// it reports its own position. A form no source-bearing program evaluates keeps
// the origin it was given.
type rowSourceValueNode struct {
	start int
	inner rowValueNode
}

func (n *rowSourceValueNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	base, inProgram := ctx.Value(profileSourceBaseKey{}).(int)
	if !inProgram {
		return n.inner.eval(ctx, e, values, frames)
	}
	saved, bound := bindRowExpressionOrigin(values, base+n.start)
	if !bound {
		return n.inner.eval(ctx, e, values, frames)
	}
	value, resolved, err := n.inner.eval(ctx, e, values, frames)
	restoreRowExpressionOrigin(values, saved)
	return value, resolved, err
}

// profileProgramCache is the statement-owned publication owner for source-bearing
// programs. Both value and predicate compilers use the same cancellation and
// concurrent-publication rules; their compiled program types remain distinct.
// Compilation runs outside the mutex and only a completed read is published.
type profileProgramCache[T any] struct {
	mu       sync.Mutex
	programs map[string]T
}

func (c *profileProgramCache[T]) program(ctx context.Context, expression string, compile func(context.Context, string) T) T {
	var zero T
	if ctx.Err() != nil {
		return zero
	}
	c.mu.Lock()
	program, cached := c.programs[expression]
	c.mu.Unlock()
	if cached {
		return program
	}
	program = compile(ctx, expression)
	if ctx.Err() != nil {
		return zero
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, cached := c.programs[expression]; cached {
		return existing
	}
	if c.programs == nil {
		c.programs = make(map[string]T)
	}
	c.programs[expression] = program
	return program
}

func compileProfileRowValue(ctx context.Context, expression string) *rowValueProgram {
	compiler := rowValueCompiler{origins: true}
	root, _ := compiler.compile(ctx, expressionPart(expression))
	return &rowValueProgram{root: &profileSourceProgramNode{root: root}}
}

// profilePredicateSourceKey is the compile-only context slot carrying the text
// of the predicate a source-bearing predicate program is compiled from. It is a
// position source, not a row: the predicate compiler reads its parts from that
// text, so a part's position is the byte window it occupies there. Only a
// PROFILE statement's collector installs it, and only while compiling - never
// while evaluating - so a compiled program keeps positions relative to its own
// text and nothing statement-specific enters a text-keyed cache.
type profilePredicateSourceKey struct{}

// withProfilePredicateSource returns ctx carrying root as the predicate text
// being compiled with source occurrences.
func withProfilePredicateSource(ctx context.Context, root string) context.Context {
	if root == "" {
		return ctx
	}
	return context.WithValue(ctx, profilePredicateSourceKey{}, root)
}

// profilePredicateSource returns the root predicate text a compilation reports
// occurrences against, or "" for a compilation that reports none.
func profilePredicateSource(ctx context.Context) string {
	root, _ := ctx.Value(profilePredicateSourceKey{}).(string)
	return root
}

// profilePredicateSourceFragment returns part as a fragment of the root
// predicate text: the text itself and where it starts in root. The position is
// read from the bytes part borrows (borrowedExpressionOffset), never by
// searching root for that text, so two written occurrences of one part keep
// their own positions. A part that is not an exact byte window of root starts
// where root starts: a position is never guessed.
func profilePredicateSourceFragment(root, part string) expressionFragment {
	if start, owned := borrowedExpressionOffset(root, part); owned {
		return expressionFragment{text: part, start: start}
	}
	return expressionPart(part)
}

// profileSourcePredicateProgramNode is a collector's source-bearing predicate
// program root. It reads the origin bound for the row - where the predicate
// starts - and installs it as the program's base for the program's whole
// synchronous evaluation, exactly as profileSourceProgramNode does for a
// source-bearing value program. A statement that bound no origin evaluates the
// program's root as it is.
type profileSourcePredicateProgramNode struct{ root rowPredicateNode }

func (n *profileSourcePredicateProgramNode) holds(e *StorageExecutor, ctx context.Context, values map[string]interface{}) bool {
	base := rowExpressionOrigin(values)
	if !base.had {
		return n.root.holds(e, ctx, values)
	}
	return n.root.holds(e, context.WithValue(ctx, profileSourceBaseKey{}, base.previous), values)
}

// profileSourcePredicateNode is a predicate's text delegation in a
// source-bearing predicate program: the one PROFILE-only wrapper a
// source-bearing predicate compilation attaches, and the only place a compiled
// predicate keeps a position. It binds where that text starts, with the
// program's base, for the delegation's evaluation and restores the row's origin
// afterwards, so a subquery the text evaluator reaches reports the occurrence it
// was written at - including the position zero of the predicate's own text. A
// predicate no source-bearing program evaluates keeps the origin it was given.
type profileSourcePredicateNode struct {
	start int
	inner rowPredicateNode
}

func (n *profileSourcePredicateNode) holds(e *StorageExecutor, ctx context.Context, values map[string]interface{}) bool {
	base, inProgram := ctx.Value(profileSourceBaseKey{}).(int)
	if !inProgram {
		return n.inner.holds(e, ctx, values)
	}
	saved, bound := bindRowExpressionOrigin(values, base+n.start)
	if !bound {
		return n.inner.holds(e, ctx, values)
	}
	held := n.inner.holds(e, ctx, values)
	restoreRowExpressionOrigin(values, saved)
	return held
}

// profileSourcePredicateValues returns the row a PROFILE statement's predicate
// is evaluated against. A row that already carries an origin is the row to use:
// the predicate is being evaluated inside an expression that reports its own
// origin, and the predicate's positions compose with it. Any other row is
// copied once, so binding the predicate's root origin never writes into a scope
// its caller may share - a row selection that reuses one row for every
// candidate, for instance. An ordinary statement evaluates the row it was
// given, untouched.
func profileSourcePredicateValues(values map[string]interface{}) map[string]interface{} {
	if rowExpressionOrigin(values).had {
		return values
	}
	owned := maps.Clone(values)
	if owned == nil {
		owned = make(map[string]interface{}, 1)
	}
	bindRowExpressionOrigin(owned, 0)
	return owned
}

// compileProfileRowPredicate uses the canonical predicate compiler with the
// predicate's own immutable source. The compiler attaches the source base once;
// no compilation context or statement identity is retained in the program.
func compileProfileRowPredicate(ctx context.Context, expression string) *rowPredicateProgram {
	return compileRowPredicateProgram(withProfilePredicateSource(ctx, expression), expression)
}
