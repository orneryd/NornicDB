// Copyright (c) 2012-2022 The ANTLR Project. All rights reserved.
// Use of this file is governed by the BSD 3-clause license that
// can be found in the LICENSE.txt file in the project root.

package antlr

import (
	"fmt"
	"strconv"
)

// SemanticContext is a tree structure used to record the semantic context in which
//
//	an ATN configuration is valid.  It's either a single predicate,
//	a conjunction p1 && p2, or a sum of products p1 || p2.
//
//	I have scoped the AND, OR, and Predicate subclasses of
//	[SemanticContext] within the scope of this outer ``class''
type SemanticContext interface {
	Equals(other Collectable[SemanticContext]) bool
	Hash() int

	evaluate(parser Recognizer, outerContext RuleContext) bool
	evalPrecedence(parser Recognizer, outerContext RuleContext) SemanticContext

	String() string
}

func SemanticContextandContext(a, b SemanticContext) SemanticContext {
	return SemanticContextandContextWithWork(a, b, nil)
}
func SemanticContextandContextWithWork(a, b SemanticContext, check func()) SemanticContext {
	chargeWork(check)
	if a == nil || a == SemanticContextNone {
		return b
	}
	if b == nil || b == SemanticContextNone {
		return a
	}
	result := NewANDWithWork(a, b, check)
	if result.opnds.Len() == 1 {
		return result.opnds.At(0, nil)
	}

	return result
}

func SemanticContextorContext(a, b SemanticContext) SemanticContext {
	return SemanticContextorContextWithWork(a, b, nil)
}
func SemanticContextorContextWithWork(a, b SemanticContext, check func()) SemanticContext {
	chargeWork(check)
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if a == SemanticContextNone || b == SemanticContextNone {
		return SemanticContextNone
	}
	result := NewORWithWork(a, b, check)
	if result.opnds.Len() == 1 {
		return result.opnds.At(0, nil)
	}

	return result
}

type Predicate struct {
	ruleIndex      int
	predIndex      int
	isCtxDependent bool
}

func NewPredicate(ruleIndex, predIndex int, isCtxDependent bool) *Predicate {
	p := new(Predicate)

	p.ruleIndex = ruleIndex
	p.predIndex = predIndex
	p.isCtxDependent = isCtxDependent // e.g., $i ref in pred
	return p
}

//The default {@link SemanticContext}, which is semantically equivalent to
//a predicate of the form {@code {true}?}.

var SemanticContextNone = NewPredicate(-1, -1, false)

func (p *Predicate) evalPrecedence(_ Recognizer, _ RuleContext) SemanticContext {
	return p
}

func (p *Predicate) evaluate(parser Recognizer, outerContext RuleContext) bool {

	var localctx RuleContext

	if p.isCtxDependent {
		localctx = outerContext
	}

	return parser.Sempred(localctx, p.ruleIndex, p.predIndex)
}

func (p *Predicate) Equals(other Collectable[SemanticContext]) bool {
	if p == other {
		return true
	} else if _, ok := other.(*Predicate); !ok {
		return false
	} else {
		return p.ruleIndex == other.(*Predicate).ruleIndex &&
			p.predIndex == other.(*Predicate).predIndex &&
			p.isCtxDependent == other.(*Predicate).isCtxDependent
	}
}

func (p *Predicate) Hash() int {
	h := murmurInit(0)
	h = murmurUpdate(h, p.ruleIndex)
	h = murmurUpdate(h, p.predIndex)
	if p.isCtxDependent {
		h = murmurUpdate(h, 1)
	} else {
		h = murmurUpdate(h, 0)
	}
	return murmurFinish(h, 3)
}

func (p *Predicate) String() string {
	return "{" + strconv.Itoa(p.ruleIndex) + ":" + strconv.Itoa(p.predIndex) + "}?"
}

type PrecedencePredicate struct {
	precedence int
}

func NewPrecedencePredicate(precedence int) *PrecedencePredicate {

	p := new(PrecedencePredicate)
	p.precedence = precedence

	return p
}

func (p *PrecedencePredicate) evaluate(parser Recognizer, outerContext RuleContext) bool {
	return parser.Precpred(outerContext, p.precedence)
}

func (p *PrecedencePredicate) evalPrecedence(parser Recognizer, outerContext RuleContext) SemanticContext {
	if parser.Precpred(outerContext, p.precedence) {
		return SemanticContextNone
	}

	return nil
}

func (p *PrecedencePredicate) compareTo(other *PrecedencePredicate) int {
	return p.precedence - other.precedence
}

func (p *PrecedencePredicate) Equals(other Collectable[SemanticContext]) bool {

	var op *PrecedencePredicate
	var ok bool
	if op, ok = other.(*PrecedencePredicate); !ok {
		return false
	}

	if p == op {
		return true
	}

	return p.precedence == other.(*PrecedencePredicate).precedence
}

func (p *PrecedencePredicate) Hash() int {
	h := uint32(1)
	h = 31*h + uint32(p.precedence)
	return int(h)
}

func (p *PrecedencePredicate) String() string {
	return "{" + strconv.Itoa(p.precedence) + ">=prec}?"
}

func PrecedencePredicatefilterPrecedencePredicates(set *JStore[SemanticContext, Comparator[SemanticContext]]) []*PrecedencePredicate {
	return precedencePredicatesWithWork(set, nil).materialize()
}
func precedencePredicatesWithWork(set *JStore[SemanticContext, Comparator[SemanticContext]], check func()) *workSequence[*PrecedencePredicate] {
	result := &workSequence[*PrecedencePredicate]{}

	set.eachWithWork(check, func(v SemanticContext) bool {
		if c2, ok := v.(*PrecedencePredicate); ok {
			result.Append(c2, check)
		}
		return true
	})

	return result
}

// A semantic context which is true whenever none of the contained contexts
// is false.`

type AND struct {
	opnds workSequence[SemanticContext]
}

func NewAND(a, b SemanticContext) *AND { return NewANDWithWork(a, b, nil) }
func NewANDWithWork(a, b SemanticContext, check func()) *AND {
	chargeWork(check)

	operands := NewJStore[SemanticContext, Comparator[SemanticContext]](semctxEqInst, SemanticContextCollection, "NewAND() operands")
	if aa, ok := a.(*AND); ok {
		for operandIndex := 0; operandIndex < aa.opnds.Len(); operandIndex++ {
			o := aa.opnds.At(operandIndex, nil)
			chargeWork(check)
			operands.PutWithOperation(o, semanticOperation(check))
		}
	} else {
		operands.PutWithOperation(a, semanticOperation(check))
	}

	if ba, ok := b.(*AND); ok {
		for operandIndex := 0; operandIndex < ba.opnds.Len(); operandIndex++ {
			o := ba.opnds.At(operandIndex, nil)
			chargeWork(check)
			operands.PutWithOperation(o, semanticOperation(check))
		}
	} else {
		operands.PutWithOperation(b, semanticOperation(check))
	}
	precedencePredicates := precedencePredicatesWithWork(operands, check)
	if precedencePredicates.Len() > 0 {
		// interested in the transition with the lowest precedence
		var reduced *PrecedencePredicate

		for i := 0; i < precedencePredicates.Len(); i++ {
			p := precedencePredicates.At(i, check)
			chargeWork(check)
			if reduced == nil || p.precedence < reduced.precedence {
				reduced = p
			}
		}

		operands.PutWithOperation(reduced, semanticOperation(check))
	}

	opnds := workSequence[SemanticContext]{}
	operands.eachWithWork(check, func(operand SemanticContext) bool { opnds.Append(operand, check); return true })

	and := new(AND)
	and.opnds = opnds

	return and
}

func (a *AND) Equals(other Collectable[SemanticContext]) bool {
	value, ok := other.(SemanticContext)
	return ok && semanticEqualWithWork(a, value, nil)
}

// {@inheritDoc}
//
// <p>
// The evaluation of predicates by a context is short-circuiting, but
// unordered.</p>
func (a *AND) evaluate(parser Recognizer, outerContext RuleContext) bool {
	for i := 0; i < a.opnds.Len(); i++ {
		chargeWork(recognizerWork(parser))
		if !a.opnds.At(i, nil).evaluate(parser, outerContext) {
			return false
		}
	}
	return true
}

func (a *AND) evalPrecedence(parser Recognizer, outerContext RuleContext) SemanticContext {
	differs := false
	operands := workSequence[SemanticContext]{}

	for i := 0; i < a.opnds.Len(); i++ {
		chargeWork(recognizerWork(parser))
		context := a.opnds.At(i, nil)
		evaluated := context.evalPrecedence(parser, outerContext)
		differs = differs || (evaluated != context)
		if evaluated == nil {
			// The AND context is false if any element is false
			return nil
		} else if evaluated != SemanticContextNone {
			// Reduce the result by Skipping true elements
			operands.Append(evaluated, recognizerWork(parser))
		}
	}
	if !differs {
		return a
	}

	if operands.Len() == 0 {
		// all elements were true, so the AND context is true
		return SemanticContextNone
	}

	var result SemanticContext

	for operandIndex := 0; operandIndex < operands.Len(); operandIndex++ {
		o := operands.At(operandIndex, recognizerWork(parser))
		chargeWork(recognizerWork(parser))
		if result == nil {
			result = o
		} else {
			result = SemanticContextandContextWithWork(result, o, recognizerWork(parser))
		}
	}

	return result
}

func (a *AND) Hash() int { return semanticHashWithWork(a, nil) }
func (o *OR) Hash() int  { return semanticHashWithWork(o, nil) }
func (a *AND) String() string {
	s := ""

	for operandIndex := 0; operandIndex < a.opnds.Len(); operandIndex++ {
		o := a.opnds.At(operandIndex, nil)
		s += "&& " + fmt.Sprint(o)
	}

	if len(s) > 3 {
		return s[0:3]
	}

	return s
}

//
// A semantic context which is true whenever at least one of the contained
// contexts is true.
//

type OR struct {
	opnds workSequence[SemanticContext]
}

func NewOR(a, b SemanticContext) *OR { return NewORWithWork(a, b, nil) }
func NewORWithWork(a, b SemanticContext, check func()) *OR {
	chargeWork(check)

	operands := NewJStore[SemanticContext, Comparator[SemanticContext]](semctxEqInst, SemanticContextCollection, "NewOR() operands")
	if aa, ok := a.(*OR); ok {
		for operandIndex := 0; operandIndex < aa.opnds.Len(); operandIndex++ {
			o := aa.opnds.At(operandIndex, nil)
			chargeWork(check)
			operands.PutWithOperation(o, semanticOperation(check))
		}
	} else {
		operands.PutWithOperation(a, semanticOperation(check))
	}

	if ba, ok := b.(*OR); ok {
		for operandIndex := 0; operandIndex < ba.opnds.Len(); operandIndex++ {
			o := ba.opnds.At(operandIndex, nil)
			chargeWork(check)
			operands.PutWithOperation(o, semanticOperation(check))
		}
	} else {
		operands.PutWithOperation(b, semanticOperation(check))
	}
	precedencePredicates := precedencePredicatesWithWork(operands, check)
	if precedencePredicates.Len() > 0 {
		// interested in the transition with the lowest precedence
		var reduced *PrecedencePredicate

		for i := 0; i < precedencePredicates.Len(); i++ {
			p := precedencePredicates.At(i, check)
			chargeWork(check)
			if reduced == nil || p.precedence > reduced.precedence {
				reduced = p
			}
		}

		operands.PutWithOperation(reduced, semanticOperation(check))
	}

	opnds := workSequence[SemanticContext]{}
	operands.eachWithWork(check, func(operand SemanticContext) bool { opnds.Append(operand, check); return true })

	o := new(OR)
	o.opnds = opnds

	return o
}

func (o *OR) Equals(other Collectable[SemanticContext]) bool {
	value, ok := other.(SemanticContext)
	return ok && semanticEqualWithWork(o, value, nil)
}

// <p>
// The evaluation of predicates by o context is short-circuiting, but
// unordered.</p>
func (o *OR) evaluate(parser Recognizer, outerContext RuleContext) bool {
	for i := 0; i < o.opnds.Len(); i++ {
		chargeWork(recognizerWork(parser))
		if o.opnds.At(i, nil).evaluate(parser, outerContext) {
			return true
		}
	}
	return false
}

func (o *OR) evalPrecedence(parser Recognizer, outerContext RuleContext) SemanticContext {
	differs := false
	operands := workSequence[SemanticContext]{}
	for i := 0; i < o.opnds.Len(); i++ {
		chargeWork(recognizerWork(parser))
		context := o.opnds.At(i, nil)
		evaluated := context.evalPrecedence(parser, outerContext)
		differs = differs || (evaluated != context)
		if evaluated == SemanticContextNone {
			// The OR context is true if any element is true
			return SemanticContextNone
		} else if evaluated != nil {
			// Reduce the result by Skipping false elements
			operands.Append(evaluated, recognizerWork(parser))
		}
	}
	if !differs {
		return o
	}
	if operands.Len() == 0 {
		// all elements were false, so the OR context is false
		return nil
	}
	var result SemanticContext

	for operandIndex := 0; operandIndex < operands.Len(); operandIndex++ {
		o := operands.At(operandIndex, recognizerWork(parser))
		chargeWork(recognizerWork(parser))
		if result == nil {
			result = o
		} else {
			result = SemanticContextorContextWithWork(result, o, recognizerWork(parser))
		}
	}

	return result
}

func (o *OR) String() string {
	s := ""

	for operandIndex := 0; operandIndex < o.opnds.Len(); operandIndex++ {
		o := o.opnds.At(operandIndex, nil)
		s += "|| " + fmt.Sprint(o)
	}

	if len(s) > 3 {
		return s[0:3]
	}

	return s
}
