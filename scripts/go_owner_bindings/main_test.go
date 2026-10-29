package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// runRequest analyzes a synthetic module rooted at a temporary directory.
func runRequest(t *testing.T, files map[string]string) (output, string) {
	t.Helper()
	root := t.TempDir()
	for name, content := range files {
		full := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", name, err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	reqFiles := make([]string, 0, len(files))
	for name := range files {
		if strings.HasSuffix(name, ".go") {
			reqFiles = append(reqFiles, name)
		}
	}
	sort.Strings(reqFiles)
	req := request{Root: root, Files: reqFiles}
	payload, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	var out bytes.Buffer
	if err := run(bytes.NewReader(payload), &out); err != nil {
		t.Fatalf("run: %v", err)
	}
	var res output
	if err := json.Unmarshal(out.Bytes(), &res); err != nil {
		t.Fatalf("decode output: %v\n%s", err, out.String())
	}
	return res, out.String()
}

// pkgNamed returns the single package with the given declared name.
func pkgNamed(t *testing.T, res output, name string) *packageInfo {
	t.Helper()
	var found *packageInfo
	for _, p := range res.Packages {
		if p.Name == name {
			if found != nil {
				t.Fatalf("package %q found twice", name)
			}
			found = p
		}
	}
	if found == nil {
		t.Fatalf("package %q not found in %+v", name, res.Packages)
	}
	return found
}

// funcNamed returns the function declaration with the given name.
func funcNamed(t *testing.T, p *packageInfo, name string) funcInfo {
	t.Helper()
	for _, f := range p.Files {
		for _, fn := range f.Funcs {
			if fn.Name == name {
				return fn
			}
		}
	}
	t.Fatalf("func %q not found in package %q", name, p.Name)
	return funcInfo{}
}

// targetOfCall returns the bound target of a call, failing when unbound.
func targetOfCall(t *testing.T, c callInfo) target {
	t.Helper()
	if c.Target == nil {
		t.Fatalf("call on line %d has no bound target (kind %s)", c.Line, c.Kind)
	}
	return *c.Target
}

func TestSamePackageCrossFileDirectFunction(t *testing.T) {
	res, raw := runRequest(t, map[string]string{
		"go.mod": "module example.com/same\n",
		"pkg/a.go": `package pkg

func Inner(v int) int { return v + 1 }
`,
		"pkg/b.go": `package pkg

func Outer(v int) int { return Inner(v) }
`,
	})
	if !res.OK {
		t.Fatalf("expected proof to succeed, got %s", raw)
	}
	p := pkgNamed(t, res, "pkg")
	outer := funcNamed(t, p, "Outer")
	if len(outer.Calls) != 1 {
		t.Fatalf("expected one call, got %+v", outer.Calls)
	}
	c := outer.Calls[0]
	if c.Kind != kindFree {
		t.Fatalf("expected free call, got %q", c.Kind)
	}
	tgt := targetOfCall(t, c)
	if tgt.Name != "Inner" || tgt.File != "pkg/a.go" || tgt.Receiver != "" {
		t.Fatalf("unexpected target %+v", tgt)
	}
	if tgt.Package != "example.com/same/pkg" {
		t.Fatalf("unexpected package path %q", tgt.Package)
	}
	inner := funcNamed(t, p, "Inner")
	if tgt.Line != inner.Decl.Line {
		t.Fatalf("target line %d does not match declaration line %d", tgt.Line, inner.Decl.Line)
	}
	// The argument v must resolve to Outer's parameter object.
	if len(c.Args) != 1 {
		t.Fatalf("expected one argument object, got %+v", c.Args)
	}
	if c.Args[0].File != "pkg/b.go" || c.Args[0].Pos <= outer.Decl.Start || c.Args[0].Pos >= outer.Decl.End {
		t.Fatalf("argument identity %+v is not Outer's parameter", c.Args[0])
	}
	if p.TypeCheckFailed {
		t.Fatalf("unexpected typecheck failure: %+v", p.Diagnostics)
	}
}

func TestGenericFreeFunctionUnwrapsToObject(t *testing.T) {
	res, raw := runRequest(t, map[string]string{
		"go.mod": "module example.com/gen\n",
		"pkg/a.go": `package pkg

func Identity[T any](v T) T { return v }

func Use() int { return Identity[int](7) }
`,
	})
	if !res.OK {
		t.Fatalf("expected proof to succeed, got %s", raw)
	}
	p := pkgNamed(t, res, "pkg")
	use := funcNamed(t, p, "Use")
	if len(use.Calls) != 1 {
		t.Fatalf("expected one call, got %+v", use.Calls)
	}
	c := use.Calls[0]
	tgt := targetOfCall(t, c)
	if c.Kind != kindFree || tgt.Name != "Identity" || tgt.File != "pkg/a.go" {
		t.Fatalf("generic instantiation did not unwrap: %+v", c)
	}
	identity := funcNamed(t, p, "Identity")
	if tgt.Line != identity.Decl.Line {
		t.Fatalf("target line %d != declaration line %d", tgt.Line, identity.Decl.Line)
	}
	if c.ArgCount != 1 {
		t.Fatalf("expected one argument, got %d", c.ArgCount)
	}
}

func TestLocalAndParamShadowingKeepsDistinctObjects(t *testing.T) {
	res, raw := runRequest(t, map[string]string{
		"go.mod": "module example.com/shadow\n",
		"pkg/a.go": `package pkg

func Target(v int) int { return v }

type S struct{}

func (S) Target(v int) int { return v }

func Caller(v int) int {
	Target := func(x int) int { return x }
	return Target(v)
}

func Caller2(v int) int {
	s := S{}
	return s.Target(v)
}
`,
	})
	if !res.OK {
		t.Fatalf("expected proof to succeed, got %s", raw)
	}
	p := pkgNamed(t, res, "pkg")

	caller := funcNamed(t, p, "Caller")
	if len(caller.Calls) != 1 {
		t.Fatalf("expected one call, got %+v", caller.Calls)
	}
	if caller.Calls[0].Kind != kindUnknown || caller.Calls[0].Target != nil {
		t.Fatalf("shadowed free function must not bind: %+v", caller.Calls[0])
	}

	caller2 := funcNamed(t, p, "Caller2")
	if len(caller2.Calls) != 1 {
		t.Fatalf("expected one call, got %+v", caller2.Calls)
	}
	c := caller2.Calls[0]
	if c.Kind != kindMethod {
		t.Fatalf("expected method call, got %q", c.Kind)
	}
	tgt := targetOfCall(t, c)
	if tgt.Receiver != "S" || tgt.Name != "Target" {
		t.Fatalf("unexpected method target %+v", tgt)
	}

	// The package-level function and the method are distinct declarations.
	free := funcNamed(t, p, "Target")
	if free.IsMethod {
		t.Fatalf("package-level Target must not be a method")
	}
	methods := 0
	for _, f := range p.Files {
		for _, fn := range f.Funcs {
			if fn.Name == "Target" && fn.IsMethod {
				methods++
			}
		}
	}
	if methods != 1 {
		t.Fatalf("expected one method Target, got %d", methods)
	}
	if free.Receiver != "" {
		t.Fatalf("free function must have no receiver, got %q", free.Receiver)
	}
}

func TestAliasImportFrameworkVsShadowedReceiver(t *testing.T) {
	res, raw := runRequest(t, map[string]string{
		"go.mod": "module example.com/alias\n",
		"pkg/a.go": `package pkg

import tst "testing"

func TestReal(tst *tst.T) {
	tst.Helper()
}

type fake struct{}

func (fake) Helper() {}

func UseShadow() {
	fake := fake{}
	fake.Helper()
}
`,
	})
	if !res.OK {
		t.Fatalf("expected proof to succeed, got %s", raw)
	}
	p := pkgNamed(t, res, "pkg")

	real := funcNamed(t, p, "TestReal")
	if real.TestKind != "test" || !real.TestSigOK {
		t.Fatalf("aliased *tst.T parameter must be recognized: %+v", real)
	}
	if len(real.Calls) != 1 {
		t.Fatalf("expected one call, got %+v", real.Calls)
	}
	c := real.Calls[0]
	if c.Kind != kindMethod {
		t.Fatalf("expected method call, got %q", c.Kind)
	}
	tgt := targetOfCall(t, c)
	if tgt.Name != "Helper" || tgt.Package != "testing" {
		t.Fatalf("helper must bind to testing.Helper: %+v", tgt)
	}
	if tgt.Receiver != "T" {
		t.Fatalf("expected call-site receiver T, got %q", tgt.Receiver)
	}

	shadow := funcNamed(t, p, "UseShadow")
	if len(shadow.Calls) != 1 {
		t.Fatalf("expected one call, got %+v", shadow.Calls)
	}
	sc := shadow.Calls[0]
	if sc.Kind != kindMethod || sc.ImportPath != "" {
		t.Fatalf("shadowed receiver must bind locally, not to a package: %+v", sc)
	}
	if sc.Target == nil || sc.Target.Package != "example.com/alias/pkg" {
		t.Fatalf("shadowed receiver bound to wrong package: %+v", sc.Target)
	}
	if sc.Target.Receiver != "fake" {
		t.Fatalf("shadowed receiver must bind the local method: %+v", sc.Target)
	}
}

func TestPackageQualifiedUnresolvedSymbolIsNotGuessed(t *testing.T) {
	res, raw := runRequest(t, map[string]string{
		"go.mod": "module example.com/unres\n",
		"pkg/a.go": `package pkg

import "example.com/doesnotexist"

func Use() {
	doesnotexist.Missing()
}
`,
	})
	// The imported module cannot be resolved, so the call cannot be bound.
	if res.OK {
		t.Fatalf("expected proof failure for an unresolved import, got %s", raw)
	}
	p := pkgNamed(t, res, "pkg")
	use := funcNamed(t, p, "Use")
	if len(use.Calls) != 1 {
		t.Fatalf("expected one call, got %+v", use.Calls)
	}
	c := use.Calls[0]
	if c.Target != nil {
		t.Fatalf("unresolved symbol must not be guessed: %+v", c.Target)
	}
	if c.Kind != kindUnknown && c.Kind != kindPkgUnres {
		t.Fatalf("unexpected kind %q for an unresolved module call", c.Kind)
	}
	if len(p.Diagnostics) == 0 {
		t.Fatalf("expected diagnostics for the unresolved import")
	}
}

func TestMethodsVersusFreeFunctions(t *testing.T) {
	res, raw := runRequest(t, map[string]string{
		"go.mod": "module example.com/method\n",
		"pkg/a.go": `package pkg

type Counter struct{}

func (c *Counter) Inc(v int) {}

func Inc(v int) {}

func Use(c *Counter) {
	c.Inc(1)
	Inc(1)
	f := c.Inc
	f(1)
}
`,
	})
	if !res.OK {
		t.Fatalf("expected proof to succeed, got %s", raw)
	}
	p := pkgNamed(t, res, "pkg")
	use := funcNamed(t, p, "Use")
	if len(use.Calls) != 3 {
		t.Fatalf("expected three calls, got %+v", use.Calls)
	}

	method := use.Calls[0]
	if method.Kind != kindMethod {
		t.Fatalf("expected method call, got %q", method.Kind)
	}
	mt := targetOfCall(t, method)
	if mt.Receiver != "Counter" || mt.Name != "Inc" {
		t.Fatalf("unexpected method target %+v", mt)
	}

	free := use.Calls[1]
	if free.Kind != kindFree {
		t.Fatalf("expected free call, got %q", free.Kind)
	}
	ft := targetOfCall(t, free)
	if ft.Receiver != "" || ft.Name != "Inc" {
		t.Fatalf("unexpected free target %+v", ft)
	}
	if ft.Line == mt.Line {
		t.Fatalf("free function and method must resolve to different declarations")
	}

	// Calling a method value through a variable is never a free-call binding.
	viaVar := use.Calls[2]
	if viaVar.Kind != kindUnknown || viaVar.Target != nil {
		t.Fatalf("method value call must not bind a free target: %+v", viaVar)
	}
}

func TestTestAndBenchmarkSignatureRealTypes(t *testing.T) {
	res, raw := runRequest(t, map[string]string{
		"go.mod": "module example.com/testsig\n",
		"pkg/a.go": `package pkg

func Helper() {}
`,
		"pkg/a_test.go": `package pkg

import "testing"

func TestGood(t *testing.T) { Helper() }

func BenchmarkGood(b *testing.B) { Helper() }

func TestWrongParam(t *testing.T, extra int) {}

func TestTwoParams(t *testing.T, b *testing.B) {}

type T struct{}

func TestFakeType(t *T) {}

func helperFunc(t *testing.T) {}

func Test1(t *testing.T) {}

func BenchmarkAsTest(t *testing.T) {}

func BenchNotAPrefixHelper(t *testing.T) {}
`,
		"pkg/ext_test.go": `package pkg_test

import (
	"testing"

	"example.com/testsig/pkg"
)

func TestExternal(t *testing.T) { pkg.Helper() }
`,
	})
	if !res.OK {
		t.Fatalf("expected proof to succeed, got %s", raw)
	}
	p := pkgNamed(t, res, "pkg")
	good := funcNamed(t, p, "TestGood")
	if good.TestKind != "test" || !good.TestSigOK {
		t.Fatalf("TestGood must be a test with a real *testing.T: %+v", good)
	}
	if len(good.Calls) != 1 || good.Calls[0].Kind != kindFree {
		t.Fatalf("TestGood must bind Helper across files: %+v", good.Calls)
	}
	if tgt := targetOfCall(t, good.Calls[0]); tgt.Name != "Helper" || tgt.File != "pkg/a.go" {
		t.Fatalf("cross-file test call bound wrongly: %+v", tgt)
	}

	bench := funcNamed(t, p, "BenchmarkGood")
	if bench.TestKind != "benchmark" || !bench.TestSigOK {
		t.Fatalf("BenchmarkGood must be a benchmark: %+v", bench)
	}

	// A benchmark name with the test parameter type is not a valid entry point.
	if fn := funcNamed(t, p, "BenchmarkAsTest"); fn.TestKind != "" || fn.TestSigOK {
		t.Fatalf("invalid benchmark signature accepted: %+v", fn)
	}
	if fn := funcNamed(t, p, "Test1"); fn.TestKind != "test" || !fn.TestSigOK {
		t.Fatalf("go test permits a non-lowercase suffix, including a digit: %+v", fn)
	}
	if fn := funcNamed(t, p, "BenchNotAPrefixHelper"); fn.TestKind != "" {
		t.Fatalf("a name not starting with Test/Benchmark is not a test: %+v", fn)
	}

	for _, name := range []string{"TestWrongParam", "TestTwoParams", "TestFakeType", "helperFunc"} {
		fn := funcNamed(t, p, name)
		if fn.TestKind != "" || fn.TestSigOK {
			t.Fatalf("%s must not be classified as a test: %+v", name, fn)
		}
	}

	// External _test packages are a separate package proof.
	ext := pkgNamed(t, res, "pkg_test")
	if !ext.IsTest {
		t.Fatalf("external test package must be marked as test: %+v", ext)
	}
	external := funcNamed(t, ext, "TestExternal")
	if external.TestKind != "test" {
		t.Fatalf("external test must be classified: %+v", external)
	}
	if len(external.Calls) != 1 {
		t.Fatalf("expected one call in external test, got %+v", external.Calls)
	}
	ec := external.Calls[0]
	if ec.Kind != kindFree || ec.ImportPath != "example.com/testsig/pkg" {
		t.Fatalf("external call must bind through the package name: %+v", ec)
	}
	if tgt := targetOfCall(t, ec); tgt.Name != "Helper" {
		t.Fatalf("unexpected external target %+v", tgt)
	}
}

func TestFakeTestingPackageIsNotATestBinding(t *testing.T) {
	res, raw := runRequest(t, map[string]string{
		"go.mod": "module example.com/faketesting\n",
		"pkg/a.go": `package pkg

type T struct{}
`,
		"pkg/a_test.go": `package pkg

func TestLocalFake(t *T) {}
`,
	})
	if !res.OK {
		t.Fatalf("expected proof to succeed, got %s", raw)
	}
	p := pkgNamed(t, res, "pkg")
	fn := funcNamed(t, p, "TestLocalFake")
	if fn.TestKind != "" || fn.TestSigOK {
		t.Fatalf("a local *T is not *testing.T: %+v", fn)
	}
}

func TestBodylessTestDeclarationIsRejected(t *testing.T) {
	res, raw := runRequest(t, map[string]string{
		"go.mod": "module example.com/nobody\n",
		"pkg/a_test.go": `package pkg

import "testing"

func TestNoBody(t *testing.T)
`,
	})
	if res.OK {
		t.Fatalf("expected a missing function body to reject the proof: %s", raw)
	}
	p := pkgNamed(t, res, "pkg")
	if !p.TypeCheckFailed {
		t.Fatalf("package must be marked as typecheck_failed")
	}
	fn := funcNamed(t, p, "TestNoBody")
	if fn.TestSigOK || fn.TestKind != "" {
		t.Fatalf("bodyless declaration must not be classified: %+v", fn)
	}
	if fn.Body != nil {
		t.Fatalf("bodyless declaration must have no body span")
	}
}

func TestAssignmentToArgumentFlowWithShadowing(t *testing.T) {
	res, raw := runRequest(t, map[string]string{
		"go.mod": "module example.com/flow\n",
		"pkg/a.go": `package pkg

func Named(v int) int { return v }

func Consume(a, b int) int { return a + b }

func Flow(v int) int {
	left, right := Named(v), 2
	{
		right := Consume(left, left)
		_ = right
	}
	return Consume(left, right)
}
`,
	})
	if !res.OK {
		t.Fatalf("expected proof to succeed, got %s", raw)
	}
	p := pkgNamed(t, res, "pkg")
	flow := funcNamed(t, p, "Flow")
	if len(flow.Calls) != 3 {
		t.Fatalf("expected three calls, got %+v", flow.Calls)
	}

	// left, right := Named(v), 2 assigns only left from the call.
	named := flow.Calls[0]
	if tgt := targetOfCall(t, named); tgt.Name != "Named" {
		t.Fatalf("unexpected first target %+v", tgt)
	}
	if len(named.Assigned) != 1 {
		t.Fatalf("expected one assignment target, got %+v", named.Assigned)
	}
	leftObj := named.Assigned[0]
	if len(named.Args) != 1 {
		t.Fatalf("expected the v argument identity, got %+v", named.Args)
	}

	// The shadowing block introduces a second `right` object.
	inner := flow.Calls[1]
	if tgt := targetOfCall(t, inner); tgt.Name != "Consume" {
		t.Fatalf("unexpected inner target %+v", tgt)
	}
	if len(inner.Assigned) != 1 {
		t.Fatalf("expected the shadowed assignment target, got %+v", inner.Assigned)
	}
	if len(inner.Args) != 2 || inner.Args[0] != inner.Args[1] {
		t.Fatalf("both arguments refer to the same variable left: %+v", inner.Args)
	}
	if inner.Args[0] != leftObj {
		t.Fatalf("inner left identity %+v != %+v", inner.Args[0], leftObj)
	}

	final := flow.Calls[2]
	if tgt := targetOfCall(t, final); tgt.Name != "Consume" {
		t.Fatalf("unexpected final target %+v", tgt)
	}
	if len(final.Args) != 2 || final.Args[0] == final.Args[1] {
		t.Fatalf("expected left and right to be distinct objects, got %+v", final.Args)
	}
	// `left` is the same object in both calls, despite the inner block.
	if final.Args[0] != leftObj {
		t.Fatalf("outer left identity %+v != %+v", final.Args[0], leftObj)
	}
}

func TestTupleAssignmentBindsBothDistinctResults(t *testing.T) {
	res, raw := runRequest(t, map[string]string{
		"go.mod": "module example.com/tuple\n",
		"pkg/a.go": `package pkg

func Pair() (int, int) { return 1, 2 }

func Flow() int {
	a, b := Pair()
	return a + b
}
`,
	})
	if !res.OK {
		t.Fatalf("expected proof to succeed, got %s", raw)
	}
	p := pkgNamed(t, res, "pkg")
	call := funcNamed(t, p, "Flow").Calls[0]
	if tgt := targetOfCall(t, call); tgt.Name != "Pair" {
		t.Fatalf("unexpected target %+v", tgt)
	}
	// The type checker proves both distinct result variables; neither is guessed.
	if len(call.Assigned) != 2 || call.Assigned[0] == call.Assigned[1] {
		t.Fatalf("tuple result identities missing or conflated: %+v", call.Assigned)
	}
}

func TestGroupAssignmentTargets(t *testing.T) {
	res, raw := runRequest(t, map[string]string{
		"go.mod": "module example.com/group\n",
		"pkg/a.go": `package pkg

func One() int { return 1 }

func Two(a, b int) {}

func Flow() {
	a, b := One(), 2
	Two(a, b)
	c := One()
	Two(c, c)
}
`,
	})
	if !res.OK {
		t.Fatalf("expected proof to succeed, got %s", raw)
	}
	p := pkgNamed(t, res, "pkg")
	flow := funcNamed(t, p, "Flow")
	if len(flow.Calls) != 4 {
		t.Fatalf("expected four calls, got %+v", flow.Calls)
	}
	// One() on the first line assigns only a.
	if len(flow.Calls[0].Assigned) != 1 {
		t.Fatalf("expected one assignment target, got %+v", flow.Calls[0].Assigned)
	}
	// Two(a, b) has exactly two distinct argument identities.
	args := flow.Calls[1].Args
	if len(args) != 2 || args[0] == args[1] {
		t.Fatalf("expected two distinct argument identities, got %+v", args)
	}
	if args[0] != flow.Calls[0].Assigned[0] {
		t.Fatalf("argument identity %+v != assigned identity %+v", args[0], flow.Calls[0].Assigned[0])
	}
	// c := One() assigns the single left-hand object.
	single := flow.Calls[2]
	if len(single.Assigned) != 1 {
		t.Fatalf("expected a single assignment target, got %+v", single.Assigned)
	}
	if last := flow.Calls[3]; len(last.Args) != 2 || last.Args[0] != last.Args[1] {
		t.Fatalf("repeated variable must yield equal argument identities: %+v", last.Args)
	}
	if flow.Calls[3].Args[0] != single.Assigned[0] {
		t.Fatalf("argument identity %+v != assigned identity %+v", flow.Calls[3].Args[0], single.Assigned[0])
	}
}

func TestFunctionLiteralParentContext(t *testing.T) {
	res, raw := runRequest(t, map[string]string{
		"go.mod": "module example.com/lit\n",
		"pkg/a.go": `package pkg

func Inner() {}

func Apply(f func()) {}

func Later() {
	Apply(func() { Inner() })
}
`,
	})
	if !res.OK {
		t.Fatalf("expected proof to succeed, got %s", raw)
	}
	p := pkgNamed(t, res, "pkg")
	later := funcNamed(t, p, "Later")
	if len(later.Calls) != 2 {
		t.Fatalf("expected two calls, got %+v", later.Calls)
	}

	// Apply(...) is the outer call that owns the literal.
	apply := later.Calls[0]
	if tgt := targetOfCall(t, apply); tgt.Name != "Apply" {
		t.Fatalf("unexpected outer target %+v", tgt)
	}
	if apply.ArgCount != 1 || apply.OuterCallArgIdx != nil {
		t.Fatalf("outer call must not claim an outer argument: %+v", apply)
	}
	if !apply.FuncLiteralArg {
		t.Fatalf("outer call must report a function-literal argument: %+v", apply)
	}

	inner := later.Calls[1]
	if !inner.InFuncLiteral {
		t.Fatalf("call inside literal must be flagged: %+v", inner)
	}
	if tgt := targetOfCall(t, inner); tgt.Name != "Inner" {
		t.Fatalf("unexpected inner target %+v", tgt)
	}
	if inner.OuterCallArgIdx == nil || *inner.OuterCallArgIdx != 0 {
		t.Fatalf("expected outer argument index 0, got %+v", inner.OuterCallArgIdx)
	}
	if inner.OuterCallLine != apply.Line {
		t.Fatalf("outer call line %d != %d", inner.OuterCallLine, apply.Line)
	}
	if inner.FuncLiteralArg {
		t.Fatalf("Inner() has no literal argument: %+v", inner)
	}
}

func TestGoStatementLiteralIsNotACallArgument(t *testing.T) {
	// Standard library code contains `go func() { ... }()`: the literal is the
	// callee of its own call, not an argument of an outer call.
	res, raw := runRequest(t, map[string]string{
		"go.mod": "module example.com/golit\n",
		"pkg/a.go": `package pkg

func Inner() {}

func Later() {
	go func() { Inner() }()
}
`,
	})
	if !res.OK {
		t.Fatalf("expected proof to succeed, got %s", raw)
	}
	p := pkgNamed(t, res, "pkg")
	later := funcNamed(t, p, "Later")
	var inner *callInfo
	for i := range later.Calls {
		if later.Calls[i].InFuncLiteral {
			inner = &later.Calls[i]
		}
	}
	if inner == nil {
		t.Fatalf("expected a call inside the literal, got %+v", later.Calls)
	}
	if tgt := targetOfCall(t, *inner); tgt.Name != "Inner" {
		t.Fatalf("unexpected inner target %+v", tgt)
	}
	if inner.OuterCallArgIdx != nil || inner.OuterCallLine != 0 {
		t.Fatalf("a go statement provides no outer call argument: %+v", inner)
	}
}

func TestParseErrorFailsClosed(t *testing.T) {
	res, raw := runRequest(t, map[string]string{
		"go.mod": "module example.com/bad\n",
		"pkg/a.go": `package pkg

func Broken( {
`,
	})
	if res.OK {
		t.Fatalf("expected proof failure on a parse error, got %s", raw)
	}
	if len(res.Errors) == 0 {
		t.Fatalf("expected parse diagnostics")
	}
	if res.Errors[0].Kind != "parse" {
		t.Fatalf("expected parse kind, got %+v", res.Errors[0])
	}
	for _, p := range res.Packages {
		for _, f := range p.Files {
			if len(f.Funcs) != 0 {
				t.Fatalf("unparsable file must contribute no functions: %+v", f.Funcs)
			}
		}
	}
}

func TestDuplicateDeclarationFailsClosed(t *testing.T) {
	res, raw := runRequest(t, map[string]string{
		"go.mod": "module example.com/dup\n",
		"pkg/a.go": `package pkg

func Twice() {}
`,
		"pkg/b.go": `package pkg

func Twice() {}
`,
	})
	if res.OK {
		t.Fatalf("expected proof failure on a redeclaration, got %s", raw)
	}
	p := pkgNamed(t, res, "pkg")
	if !p.TypeCheckFailed {
		t.Fatalf("package must be marked as typecheck_failed")
	}
	found := false
	for _, d := range p.Diagnostics {
		if d.Kind == "duplicate_declaration" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected an explicit duplicate_declaration diagnostic: %+v", p.Diagnostics)
	}
}

func TestDeterministicOutputAndBuildConstraints(t *testing.T) {
	files := map[string]string{
		"go.mod": "module example.com/det\n",
		"pkg/a.go": `package pkg

func A() { B() }
`,
		"pkg/b.go": `package pkg

func B() {}
`,
		"pkg/ignored.go": `//go:build ignore_this_tag_only

package pkg

func Ignored() {}
`,
	}
	first, rawFirst := runRequest(t, files)
	second, rawSecond := runRequest(t, files)
	if !first.OK || !second.OK {
		t.Fatalf("expected proofs to succeed: %s / %s", rawFirst, rawSecond)
	}
	if maskRoot(rawFirst, first.Root) != maskRoot(rawSecond, second.Root) {
		t.Fatalf("output is not deterministic")
	}
	p := pkgNamed(t, first, "pkg")
	for _, f := range p.Files {
		if strings.HasSuffix(f.Path, "ignored.go") {
			t.Fatalf("constraint-excluded file must not be analyzed: %s", f.Path)
		}
	}
}

// maskRoot removes the temporary root so two runs can be compared.
func maskRoot(s, root string) string {
	return strings.ReplaceAll(s, root, "<root>")
}

func TestEmptyRequestIsRejected(t *testing.T) {
	var out bytes.Buffer
	if err := run(strings.NewReader("   "), &out); err == nil {
		t.Fatalf("expected an empty request to be rejected")
	}
	if err := run(strings.NewReader(`{"root":".","bogus":1}`), &out); err == nil {
		t.Fatalf("expected unknown request fields to be rejected")
	}
}

func TestMultipleInitDeclarationsAreValidGo(t *testing.T) {
	result, raw := runRequest(t, map[string]string{
		"go.mod":   "module example.com/initializers\n",
		"pkg/a.go": "package pkg\nfunc init() {}\nfunc Owner() int { return 1 }\n",
		"pkg/b.go": "package pkg\nfunc init() {}\nfunc Caller() int { return Owner()+1 }\n",
	})
	if !result.OK {
		t.Fatalf("valid init declarations rejected: %s", raw)
	}
	caller := funcNamed(t, pkgNamed(t, result, "pkg"), "Caller")
	if len(caller.Calls) != 1 || caller.Calls[0].Target == nil || caller.Calls[0].Target.Name != "Owner" {
		t.Fatalf("owner binding missing: %+v", caller)
	}
}
