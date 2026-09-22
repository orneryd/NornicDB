package fn

import (
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type idGetter struct{ id string }

func (g idGetter) GetID() string { return g.id }

func TestEvaluateFunctionRegistryAndValidation(t *testing.T) {
	t.Run("register panics for invalid inputs", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatalf("expected panic for empty name")
			}
		}()
		Register(" ", func(Context, []string) (interface{}, error) { return nil, nil })
	})

	t.Run("register panics for nil function", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatalf("expected panic for nil fn")
			}
		}()
		Register("custom_nil", nil)
	})

	Register("custom_add", func(ctx Context, args []string) (interface{}, error) {
		if len(args) != 2 {
			return nil, errors.New("need two args")
		}
		a, _ := ctx.Eval(args[0])
		b, _ := ctx.Eval(args[1])
		return a.(int) + b.(int), nil
	})

	ctx := Context{Eval: func(expr string) (interface{}, error) {
		switch expr {
		case "a":
			return 2, nil
		case "b":
			return 3, nil
		default:
			return nil, nil
		}
	}}

	v, found, err := EvaluateFunction("CUSTOM_ADD", []string{"a", "b"}, ctx)
	if err != nil || !found || v != 5 {
		t.Fatalf("expected custom add=5 found=true err=nil, got v=%v found=%v err=%v", v, found, err)
	}

	if _, found, err := EvaluateFunction("does_not_exist", nil, ctx); found || err != nil {
		t.Fatalf("unknown function should return found=false err=nil")
	}

	if _, found, err := EvaluateFunction("custom_add", []string{"a", "b"}, Context{}); !found || err == nil {
		t.Fatalf("expected missing Eval to return found=true with error")
	}

	calledNow := false
	Register("custom_now", func(ctx Context, _ []string) (interface{}, error) {
		calledNow = true
		return ctx.Now().Unix(), nil
	})
	if _, found, err := EvaluateFunction("custom_now", nil, ctx); !found || err != nil || !calledNow {
		t.Fatalf("expected default Now to be injected")
	}
}

func TestBuiltinsCore(t *testing.T) {
	n := &storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Person", "Engineer"}, Properties: map[string]any{"name": "Alice", "age": 30}}
	r := &storage.Edge{ID: storage.EdgeID("r1"), Type: "KNOWS", Properties: map[string]any{"since": 2020}}

	ctx := Context{
		Nodes:    map[string]*storage.Node{"n": n},
		Rels:     map[string]*storage.Edge{"r": r},
		Database: "test",
		Eval: func(expr string) (interface{}, error) {
			switch expr {
			case "node_expr":
				return n, nil
			case "rel_map":
				return map[string]interface{}{"type": "LIKES"}, nil
			case "string_expr":
				return "Hello", nil
			case "list_expr":
				return []string{"a", "b"}, nil
			case "iface_list":
				return []interface{}{1, 2, 3}, nil
			case "nil_expr":
				return nil, nil
			case "getter":
				return idGetter{id: "custom-id"}, nil
			default:
				return nil, nil
			}
		},
		Now: func() time.Time { return time.Unix(1, 0) },
	}

	cases := []struct {
		name string
		fn   string
		args []string
		want interface{}
	}{
		{name: "id from ctx nodes", fn: "id", args: []string{"n"}, want: "n1"},
		{name: "id from eval getter", fn: "id", args: []string{"getter"}, want: "custom-id"},
		{name: "elementId node", fn: "elementid", args: []string{"n"}, want: "4:test:n1"},
		{name: "elementId rel", fn: "elementid", args: []string{"r"}, want: "5:test:r1"},
		{name: "type from rel", fn: "type", args: []string{"r"}, want: "KNOWS"},
		{name: "type from map", fn: "type", args: []string{"rel_map"}, want: "LIKES"},
		{name: "size string", fn: "size", args: []string{"string_expr"}, want: int64(5)},
		{name: "size []string", fn: "size", args: []string{"list_expr"}, want: int64(2)},
		{name: "size []interface{}", fn: "size", args: []string{"iface_list"}, want: int64(3)},
		{name: "tolower", fn: "tolower", args: []string{"string_expr"}, want: "hello"},
		{name: "toupper", fn: "toupper", args: []string{"string_expr"}, want: "HELLO"},
		{name: "coalesce", fn: "coalesce", args: []string{"nil_expr", "string_expr"}, want: "Hello"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, found, err := EvaluateFunction(tc.fn, tc.args, ctx)
			if err != nil || !found {
				t.Fatalf("expected found builtin without error, got found=%v err=%v", found, err)
			}
			if got != tc.want {
				t.Fatalf("unexpected result for %s: got=%v want=%v", tc.fn, got, tc.want)
			}
		})
	}

	labels, _, _ := EvaluateFunction("labels", []string{"n"}, ctx)
	if got := labels.([]interface{}); len(got) != 2 || got[0] != "Person" {
		t.Fatalf("unexpected labels result: %#v", got)
	}

	keysNode, _, _ := EvaluateFunction("keys", []string{"n"}, ctx)
	if len(keysNode.([]interface{})) != 2 {
		t.Fatalf("expected 2 node keys")
	}
	keysRel, _, _ := EvaluateFunction("keys", []string{"r"}, ctx)
	if len(keysRel.([]interface{})) != 1 {
		t.Fatalf("expected 1 rel key")
	}

	propsNode, _, _ := EvaluateFunction("properties", []string{"n"}, ctx)
	if propsNode.(map[string]any)["name"] != "Alice" {
		t.Fatalf("unexpected node properties result")
	}

	if got, _, _ := EvaluateFunction("id", []string{"missing"}, ctx); got != nil {
		t.Fatalf("expected nil for missing id arg, got %v", got)
	}
	if got, _, _ := EvaluateFunction("coalesce", []string{"nil_expr"}, ctx); got != nil {
		t.Fatalf("expected nil coalesce when only nil values")
	}
	if got, _, _ := EvaluateFunction("size", nil, ctx); got != int64(0) {
		t.Fatalf("size with invalid args should be zero")
	}
	if got, _, _ := EvaluateFunction("tolower", []string{"nil_expr"}, ctx); got != nil {
		t.Fatalf("tolower(nil) should be nil")
	}
}
