package json

import "testing"

func TestJSONBasicOps(t *testing.T) {
	input := `{"user":{"name":"Alice"},"arr":[1,2,3],"n":5}`
	if !Validate(input) || Validate("{bad") {
		t.Fatalf("validate failed")
	}
	if got := Path(input, "$.user.name"); got != "Alice" {
		t.Fatalf("path extraction mismatch: %#v", got)
	}
	if got := Parse("{bad"); got != nil {
		t.Fatalf("invalid parse should return nil")
	}
	if got := Stringify(map[string]interface{}{"a": 1}); got == "" {
		t.Fatalf("stringify failed")
	}
	if got := Pretty(input); len(got) <= len(input) {
		t.Fatalf("pretty should expand json")
	}
	if got := Compact("{\n  \"a\": 1\n}"); got != `{"a":1}` {
		t.Fatalf("compact mismatch: %s", got)
	}
	if got := Type(input); got != "object" {
		t.Fatalf("type mismatch: %s", got)
	}
	if got := Size(input); got != 3 {
		t.Fatalf("size mismatch: %d", got)
	}
}

func TestJSONCollectionOps(t *testing.T) {
	keys := Keys(`{"a":1,"b":2}`)
	if len(keys) != 2 {
		t.Fatalf("keys length mismatch: %#v", keys)
	}
	vals := Values(`{"a":1,"b":2}`)
	if len(vals) != 2 {
		t.Fatalf("values length mismatch: %#v", vals)
	}

	merged := Merge(`{"a":1}`, `{"b":2}`, "bad")
	if !Validate(merged) || Size(merged) != 2 {
		t.Fatalf("merge mismatch: %s", merged)
	}

	set := Set(`{"user":{}}`, "$.user.name", "Alice")
	if Path(set, "$.user.name") != "Alice" {
		t.Fatalf("set path failed: %s", set)
	}

	del := Delete(`{"name":"Alice","age":30}`, "$.age")
	if Path(del, "$.age") != nil {
		t.Fatalf("delete path failed: %s", del)
	}
}

func TestJSONFlattenUnflattenAndArrayTransforms(t *testing.T) {
	flat := Flatten(`{"user":{"name":"Alice"}}`)
	if Path(flat, "$.user.name") != nil {
		t.Fatalf("flatten should use dotted keys: %s", flat)
	}
	unflat := Unflatten(flat)
	if Path(unflat, "$.user.name") != "Alice" {
		t.Fatalf("unflatten mismatch: %s", unflat)
	}

	filtered := Filter(`[1,2,3,4,5]`, func(v interface{}) bool {
		n, ok := v.(float64)
		return ok && n > 3
	})
	if Size(filtered) != 2 {
		t.Fatalf("filter mismatch: %s", filtered)
	}

	mapped := Map(`[1,2,3]`, func(v interface{}) interface{} {
		return int(v.(float64)) * 2
	})
	if Path(mapped, "$.0") != nil && !Validate(mapped) {
		t.Fatalf("mapped should be valid json")
	}

	sum := Reduce(`[1,2,3,4]`, 0.0, func(acc interface{}, v interface{}) interface{} {
		return acc.(float64) + v.(float64)
	})
	if sum.(float64) != 10 {
		t.Fatalf("reduce sum mismatch: %#v", sum)
	}
}
