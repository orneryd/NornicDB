package cypher

import (
	"runtime"
	"strings"
	"unicode"
	"unsafe"
)

// Source provenance for expression parts.
//
// The row evaluator and the row value compiler both read an expression by
// splitting it: at an operator, at a comma, at a delimiter, at a function's
// argument list. Every split produces a part of the parent expression, and a
// part's text alone does not say where it was written:
// [COUNT { … }, COUNT { … }] holds the same part twice. PROFILE reports the
// programs a statement ran once per written occurrence - two equal scalar
// subqueries in one projection are two operators - so a part carries where it
// starts in the expression it came from.
//
// An expressionFragment is that part. Origins compose: a part split from a part
// starts where its parent starts plus where it starts in it, and a splitter
// adds those two instead of resetting the second. A part's position is never
// guessed: it is read from the same split that produced its text, either by the
// scan that reports it (the comma kernel) or as the distance between the
// parent's text and the part's trimmed text, which is exactly where the part
// starts.
//
// The running total lives in the row (rowExpressionOriginKey), never in a
// cache: programs and plans are keyed by text and shared between statements, so
// only positions relative to that text may live in them.
//
// The projection owners bind a statement projection item's retained expression
// origin, and that binding is theirs alone: it is written only while the
// statement's own PROFILE collector observes the rows
// (profileRuntimeFromContext). Every later descent asks the row whether an
// origin is bound, so a statement nobody profiles writes nothing, allocates
// nothing and reads its expressions exactly as before.

// leadingExpressionSpace is how much whitespace a part's text is trimmed of
// before it is evaluated: the start of the part's first token.
func leadingExpressionSpace(text string) int {
	return len(text) - len(strings.TrimLeftFunc(text, unicode.IsSpace))
}

// expressionFragment is one part of an expression an evaluator or compiler
// split: its text, and where that text starts in the expression it came from.
type expressionFragment struct {
	text  string
	start int
}

// expressionPart is a whole expression: a fragment that starts where its
// parent started.
func expressionPart(text string) expressionFragment {
	return expressionFragment{text: text}
}

// rowExpressionOriginKey binds the source origin of the expression a row is
// being evaluated against: where that expression's text starts in the text the
// running program was keyed by (a clause's text, for a projected expression).
// It is the evaluator's own key, as rowStatementContextKey is, and never a
// variable.
const rowExpressionOriginKey = "\x00expression_origin"

// rowExpressionOriginBinding is what a caller saves to restore the origin an
// evaluation replaced.
type rowExpressionOriginBinding struct {
	previous int
	had      bool
}

// rowExpressionOrigin returns the origin bound for the expression being
// evaluated, and whether one is bound. A row an ordinary statement evaluates
// binds none, so every descent there is one failed lookup and no allocation.
func rowExpressionOrigin(values map[string]interface{}) rowExpressionOriginBinding {
	origin, bound := values[rowExpressionOriginKey].(int)
	return rowExpressionOriginBinding{previous: origin, had: bound}
}

// bindRowExpressionOrigin binds origin as the source origin of the expression
// about to be evaluated in values. A statement's projection owner writes it,
// and only while that statement's own PROFILE collector observes the rows.
func bindRowExpressionOrigin(values map[string]interface{}, origin int) (rowExpressionOriginBinding, bool) {
	if values == nil || origin < 0 {
		return rowExpressionOriginBinding{}, false
	}
	previous := rowExpressionOrigin(values)
	values[rowExpressionOriginKey] = origin
	return previous, true
}

// advanceRowExpressionOrigin moves the bound origin to a part that starts at
// start in the expression being evaluated. A row without a bound origin, and a
// part that starts where its parent started, are left as they are.
func advanceRowExpressionOrigin(values map[string]interface{}, start int, parent rowExpressionOriginBinding) (rowExpressionOriginBinding, bool) {
	if start == 0 || !parent.had {
		return rowExpressionOriginBinding{}, false
	}
	values[rowExpressionOriginKey] = parent.previous + start
	return parent, true
}

// borrowedExpressionOffset returns where child starts in text when child is an
// actual byte window of it: the two strings share their bytes, so the distance
// between their data pointers is exactly the part's position. A copy or a
// synthesized expression has no source extent. No pointer is dereferenced or
// reconstructed, and both strings remain alive throughout the comparison.
func borrowedExpressionOffset(text, child string) (int, bool) {
	if len(child) == 0 || len(child) > len(text) {
		return 0, false
	}
	base := uintptr(unsafe.Pointer(unsafe.StringData(text)))
	part := uintptr(unsafe.Pointer(unsafe.StringData(child)))
	inside := part >= base && part-base <= uintptr(len(text)-len(child))
	runtime.KeepAlive(text)
	runtime.KeepAlive(child)
	if !inside {
		return 0, false
	}
	return int(part - base), true
}

// bindBorrowedExpressionOrigin keeps the extent already owned by a splitter
// when its callback accepts a string. Equal text is insufficient: two written
// arguments can contain identical bytes. A child must be an actual byte window
// of this immutable parent, not a copy or a synthesized expression, which is
// what borrowedExpressionOffset reads. Ordinary evaluation never performs that
// comparison: a row bound no origin advances nothing.
func bindBorrowedExpressionOrigin(values map[string]interface{}, text, child string, parent rowExpressionOriginBinding) (rowExpressionOriginBinding, bool) {
	if !parent.had {
		return rowExpressionOriginBinding{}, false
	}
	start, owned := borrowedExpressionOffset(text, child)
	if !owned {
		return rowExpressionOriginBinding{}, false
	}
	return advanceRowExpressionOrigin(values, start, parent)
}

// restoreRowExpressionOrigin puts back the origin an evaluation replaced.
func restoreRowExpressionOrigin(values map[string]interface{}, saved rowExpressionOriginBinding) {
	if saved.had {
		values[rowExpressionOriginKey] = saved.previous
		return
	}
	delete(values, rowExpressionOriginKey)
}

// rowExpressionOccurrence returns the source occurrence a subquery body starts
// at: the expression's bound origin plus where the subquery starts in it. Two
// written occurrences of one body keep two occurrences; one occurrence keeps
// one, however many rows it runs for.
func rowExpressionOccurrence(values map[string]interface{}, start int) int {
	origin := rowExpressionOrigin(values)
	return origin.previous + start
}

// offsetFragments adds base to every part's start: the parts of a part start
// where that part starts plus where they start in it.
func offsetFragments(parts []expressionFragment, base int) []expressionFragment {
	if base == 0 {
		return parts
	}
	for index := range parts {
		parts[index].start += base
	}
	return parts
}
