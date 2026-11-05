package capturecensus

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInlineValueStructContextInputs(t *testing.T) {
	c, p := fixture(t, `package fixture
import("context";"errors")
type Engine struct{}
type Pair struct{Alpha,Beta context.Context}
type Nested struct{End Pair}
type Single struct{Item context.Context}
func(e *Engine) validateSingle(s Single)error{c:=s.Item;if c==nil{return errors.New("nil")};return c.Err()}
func(e *Engine) HiddenGuardedFresh(request context.Context)error{_=request;s:=Single{context.Background()};if s.Item==nil{return nil};return e.validateSingle(s)}
func(e *Engine) normalized(ctx context.Context)error{if ctx==nil{ctx=context.Background()};return ctx.Err()}
func(e *Engine) validate(v Pair)error{c,d:=v.Alpha,v.Beta;if c==nil||d==nil{return errors.New("nil terminal context")};if err:=c.Err();err!=nil{return err};return d.Err()}
func(e *Engine) nested(v Nested)error{return e.validate(v.End)}
func(e *Engine) LiteralNil()error{return e.normalized(nil)}
func(e *Engine) MissingAlpha()error{return e.validate(Pair{Beta:context.Background()})}
func(e *Engine) MissingBeta()error{return e.validate(Pair{Alpha:context.Background()})}
func(e *Engine) BothNonNil()error{return e.validate(Pair{context.Background(),context.Background()})}
func(e *Engine) NestedNil()error{return e.nested(Nested{Pair{Beta:context.Background()}})}
type Alias = Pair
type Named Pair
var stored Pair
func(e *Engine) UnknownGlobal()error{return e.validate(stored)}
func(e *Engine) NamedGood()error{return e.validate(Pair(Named{context.Background(),context.Background()}))}
type Two struct{Left,Right Pair}
func(e *Engine) twins(v Two)error{if err:=e.validate(v.Left);err!=nil{return err};return e.validate(v.Right)}
func(e *Engine) ExplicitNil()error{return e.validate(Pair{nil,context.Background()})}
func(e *Engine) Zero()error{return e.validate(Pair{})}
func(e *Engine) AliasGood()error{return e.validate(Alias{context.Background(),context.Background()})}
func(e *Engine) NestedGood()error{return e.nested(Nested{Pair{context.Background(),context.Background()}})}
func(e *Engine) SiblingNil()error{return e.twins(Two{Left:Pair{context.Background(),context.Background()},Right:Pair{context.Background(),nil}})}
func(e *Engine) SiblingsGood()error{return e.twins(Two{Pair{context.Background(),context.Background()},Pair{context.Background(),context.Background()}})}
func(e *Engine) CopyGood()error{p:=Pair{context.Background(),context.Background()};q:=p;p.Alpha=nil;return e.validate(q)}
func(e *Engine) CopyBad()error{p:=Pair{context.Background(),nil};q:=p;p.Beta=context.Background();return e.validate(q)}
func(e *Engine) Clobber()error{p:=Pair{context.Background(),context.Background()};p=Pair{};return e.validate(p)}
func(e *Engine) HiddenUnknownFresh(request context.Context)error{_=request;p:=Pair{context.Background(),context.Background()};alter(&p);return e.validate(p)}
func(e *Engine) Branch(ok bool)error{p:=Pair{context.Background(),context.Background()};if ok{p.Alpha=nil};return e.validate(p)}
var factory func()Pair
func(e *Engine) UnknownFactory()error{return e.validate(factory())}
func(e *Engine) Dynamic()error{f:=e.validate;return f(Pair{Beta:context.Background()})}
func makeGood()Pair{return Pair{context.Background(),context.Background()}}
func makeBad()Pair{return Pair{context.Background(),nil}}
func(e *Engine) FactoryGood()error{return e.validate(makeGood())}
func(e *Engine) FactoryBad()error{return e.validate(makeBad())}
func(e *Engine) Guarded(request context.Context)error{p:=Pair{request,request};if request==nil{return nil};return e.validate(p)}
func(e *Engine) HiddenCopyFresh(request context.Context)error{_=request;p:=Pair{context.Background(),context.Background()};q:=p;p=Pair{};return e.validate(q)}
func(e *Engine) HiddenNestedFresh(request context.Context)error{_=request;return e.nested(Nested{Pair{context.Background(),context.Background()}})}
func(e *Engine) HiddenFresh(request context.Context)error{_=request;return e.validate(Pair{context.Background(),context.Background()})}
var alter func(*Pair)
func(e *Engine) UnknownAlias()error{p:=Pair{context.Background(),context.Background()};alter(&p);return e.validate(p)}
`)
	p.Capture = []string{"(*capture.fixture.Engine).validate", "(*capture.fixture.Engine).nested", "(*capture.fixture.Engine).normalized", "(*capture.fixture.Engine).twins", "(*capture.fixture.Engine).validateSingle"}
	p.ContextOwners = append([]string(nil), p.Capture...)
	f, err := Extract(c, p)
	require.NoError(t, err)
	all := strings.Join(nilContextFlows(f), "\n")
	t.Log(all)
	require.Contains(t, all, ".LiteralNil ->")
	for _, n := range []string{"MissingAlpha", "MissingBeta", "NestedNil", "UnknownAlias", "ExplicitNil", "Zero", "SiblingNil", "CopyBad", "Clobber", "Branch", "UnknownFactory", "Dynamic", "FactoryBad", "UnknownGlobal"} {
		assert.Contains(t, all, "."+n+" ->", "by-value context fields cannot hide nil boundary input")
	}
	for _, n := range []string{"BothNonNil", "AliasGood", "NestedGood", "SiblingsGood", "CopyGood", "Guarded", "FactoryGood", "NamedGood"} {
		assert.NotContains(t, all, "."+n+" ->")
	}
	for _, n := range []string{"HiddenFresh", "HiddenCopyFresh", "HiddenNestedFresh", "HiddenUnknownFresh", "HiddenGuardedFresh"} {
		assert.Contains(t, strings.Join(f.Violations, "\n"), "."+n+" creates context.Background: a request context is in scope")
	}
	require.NotContains(t, all, "nil-context use key is not unique")
	// Both fields and repeated nested fields remain separate uses.
	count := 0
	for _, finding := range nilContextFlows(f) {
		if strings.Contains(finding, ".Zero ->") {
			count++
		}
	}
	require.Equal(t, 2, count)
	var siblings []string
	for _, finding := range nilContextFlows(f) {
		if strings.Contains(finding, ".SiblingNil ->") {
			siblings = append(siblings, finding)
		}
	}
	require.Len(t, siblings, 1)
	require.Contains(t, siblings[0], "field-[1 1](Right.Beta)")
}
