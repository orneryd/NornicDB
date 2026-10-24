package testfixture_test

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	fixture "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type lifetimeCase struct {
	name string
	leak bool
}

var lifetimeCases = []lifetimeCase{
	{"mvcc_constructor", true}, {"dot_import", true}, {"multi_assignment_captures_wrong_engine", true},
	{"var_with_direct_defer", false}, {"explicit_generic_owner", false}, {"ordinary_unowned", true},
	{"ordinary_owned", false}, {"closure_defer", false}, {"closure_return_unowned", true},
	{"closure_return_owned", false}, {"closure_cleanup_called", false}, {"cleanup_dormant_closure", true},
	{"cleanup_conditional_close", true}, {"field_direct_defer", false}, {"field_owner", false},
	{"field_unowned", true}, {"field_cleanup_capture", false}, {"field_container_reassignment", true},
	{"cleanup_shadowed_field", true}, {"cleanup_named_wrapper", false}, {"defer_closure_capture", false},
	{"returned_binding_owned", false}, {"grouped_var_owned", false}, {"parenthesized_binding", false},
	{"two_resources_first_only", true}, {"two_resources_both_deferred", false},
}

func TestFixtureLifetimeExecutableContract(t *testing.T) {
	for _, tc := range lifetimeCases {
		t.Run(tc.name, func(t *testing.T) {
			command := exec.Command(os.Args[0], "-test.run=^TestFixtureLifetimeExecutionChild$", "-test.count=1")
			command.Env = append(os.Environ(), "NORNIC_FIXTURE_LIFETIME_CASE="+tc.name)
			output, err := command.CombinedOutput()
			if tc.leak {
				if err == nil {
					t.Fatalf("unowned resource passed its test cleanup: %s", output)
				}
				for _, want := range []string{"--- FAIL: TestFixtureLifetimeExecutionChild/" + tc.name, "test fixture still open at cleanup", "runtime_execution_test.go:"} {
					if !strings.Contains(string(output), want) {
						t.Errorf("missing owner-boundary evidence %q: %s", want, output)
					}
				}
			} else if err != nil {
				t.Fatalf("closed resource failed: %v\n%s", err, output)
			}
		})
	}
}

func TestFixtureLifetimeExecutionChild(t *testing.T) {
	name := os.Getenv("NORNIC_FIXTURE_LIFETIME_CASE")
	if name == "" {
		t.Skip("executed by the parent subprocess contract")
	}
	var created []*storage.MemoryEngine
	makeEngine := func(owner *testing.T) *storage.MemoryEngine {
		var engine *storage.MemoryEngine
		if name == "mvcc_constructor" {
			engine = storage.NewMemoryEngineWithMVCCHistory()
		} else if name == "dot_import" {
			engine = newDotLifetimeEngine()
		} else {
			engine = storage.NewMemoryEngine()
		}
		created = append(created, engine)
		return fixture.Track(owner, engine, storage.ErrStorageClosed)
	}
	found := false
	for _, tc := range lifetimeCases {
		if tc.name == name {
			found = true
		}
	}
	if !found {
		t.Fatal("unknown executable case", name)
	}
	t.Run(name, func(t *testing.T) { executeLifetimeCase(t, name, makeEngine) })
	// The child deliberately contains leaks. Release them only AFTER the tested
	// test's cleanup boundary, keeping subsequent cases and process checks clean.
	for _, engine := range created {
		if err := engine.Close(); err != nil {
			t.Errorf("child disposal: %v", err)
		}
	}
}

func executeLifetimeCase(t *testing.T, name string, makeEngine func(*testing.T) *storage.MemoryEngine) {
	type holder struct{ e *storage.MemoryEngine }
	makeCleanup := func(e *storage.MemoryEngine) func() { return func() { _ = e.Close() } }
	switch name {
	case "mvcc_constructor", "dot_import", "ordinary_unowned":
		_ = makeEngine(t)
	case "multi_assignment_captures_wrong_engine":
		previous := makeEngine(t)
		defer previous.Close()
		owned, leaked := previous, makeEngine(t)
		defer owned.Close()
		_ = leaked
	case "var_with_direct_defer":
		var e = makeEngine(t)
		defer e.Close()
	case "explicit_generic_owner":
		_ = fixture.Own[*storage.MemoryEngine](t, makeEngine(t))
	case "ordinary_owned":
		_ = fixture.Own(t, makeEngine(t))
	case "closure_defer":
		func() { e := makeEngine(t); defer e.Close() }()
	case "closure_return_unowned":
		makeIt := func() *storage.MemoryEngine { return makeEngine(t) }
		_ = makeIt()
	case "closure_return_owned":
		makeIt := func() *storage.MemoryEngine { return fixture.Own(t, makeEngine(t)) }
		_ = makeIt()
	case "closure_cleanup_called":
		e := makeEngine(t)
		t.Cleanup(func() { func() { _ = e.Close() }() })
	case "cleanup_dormant_closure":
		e := makeEngine(t)
		t.Cleanup(func() { closeLater := func() { _ = e.Close() }; _ = closeLater })
	case "cleanup_conditional_close":
		e := makeEngine(t)
		t.Cleanup(func() {
			if false {
				_ = e.Close()
			}
		})
	case "field_direct_defer":
		var h holder
		h.e = makeEngine(t)
		defer h.e.Close()
	case "field_owner":
		h := holder{e: fixture.Own(t, makeEngine(t))}
		_ = h
	case "field_unowned":
		h := holder{e: makeEngine(t)}
		_ = h
	case "field_cleanup_capture":
		var h holder
		h.e = makeEngine(t)
		t.Cleanup(func() { _ = h.e.Close() })
	case "field_container_reassignment":
		var h holder
		h.e = makeEngine(t)
		other := holder{e: makeEngine(t)}
		defer other.e.Close()
		t.Cleanup(func() { _ = h.e.Close() })
		h = other
	case "cleanup_shadowed_field":
		var h holder
		h.e = makeEngine(t)
		other := holder{e: makeEngine(t)}
		defer other.e.Close()
		t.Cleanup(func() { h := other; _ = h.e.Close() })
	case "cleanup_named_wrapper":
		e := makeEngine(t)
		t.Cleanup(makeCleanup(e))
	case "defer_closure_capture":
		e := makeEngine(t)
		defer func() { _ = e.Close() }()
	case "returned_binding_owned":
		makeIt := func() (e *storage.MemoryEngine) { e = makeEngine(t); fixture.Own(t, e); return }
		_ = makeIt()
	case "grouped_var_owned":
		var (
			a = makeEngine(t)
			b = makeEngine(t)
		)
		fixture.Own(t, a)
		fixture.Own(t, b)
	case "parenthesized_binding":
		e := makeEngine(t)
		defer (e).Close()
	case "two_resources_first_only":
		a, b := makeEngine(t), makeEngine(t)
		defer a.Close()
		_ = b
	case "two_resources_both_deferred":
		a, b := makeEngine(t), makeEngine(t)
		defer a.Close()
		defer b.Close()
	default:
		t.Fatal(fmt.Sprintf("unhandled executable case %s", name))
	}
}
