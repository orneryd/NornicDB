package diff

import "testing"

func TestNodesRelationshipsAndMapsDiff(t *testing.T) {
	n1 := &Node{Properties: map[string]interface{}{"a": 1, "b": 2}}
	n2 := &Node{Properties: map[string]interface{}{"a": 3, "c": 4}}
	d := Nodes(n1, n2)
	if d.Added["c"] != 4 || d.Removed["b"] != 2 {
		t.Fatalf("node added/removed mismatch: %#v", d)
	}
	if ch, ok := d.Changed["a"].(map[string]interface{}); !ok || ch["old"] != 1 || ch["new"] != 3 {
		t.Fatalf("node changed mismatch: %#v", d.Changed["a"])
	}

	r1 := &Relationship{Properties: map[string]interface{}{"x": "old"}}
	r2 := &Relationship{Properties: map[string]interface{}{"x": "new", "y": true}}
	rd := Relationships(r1, r2)
	if rd.Added["y"] != true {
		t.Fatalf("rel added mismatch")
	}

	md := Maps(map[string]interface{}{"k": 1}, map[string]interface{}{"k": 1, "z": 9})
	if md.Added["z"] != 9 || md.Unchanged["k"] != 1 {
		t.Fatalf("map diff mismatch: %#v", md)
	}
}

func TestListStringDeepPatchMergeSummary(t *testing.T) {
	l := Lists([]interface{}{1, 2, 3}, []interface{}{2, 3, 4})
	if len(l["added"].([]interface{})) != 1 || len(l["removed"].([]interface{})) != 1 || len(l["common"].([]interface{})) != 2 {
		t.Fatalf("list diff mismatch: %#v", l)
	}

	s := Strings("hello", "hallo")
	if len(s) == 0 || s[0]["position"] != 1 {
		t.Fatalf("string diff mismatch: %#v", s)
	}

	if !Deep(map[string]int{"a": 1}, map[string]int{"a": 1}) || Deep(1, 2) {
		t.Fatalf("deep equality failed")
	}

	d := &DiffResult{
		Added:   map[string]interface{}{"c": 3},
		Removed: map[string]interface{}{"b": 2},
		Changed: map[string]interface{}{"a": map[string]interface{}{"old": 1, "new": 10}},
	}
	patched := Patch(map[string]interface{}{"a": 1, "b": 2}, d)
	if patched["a"] != 10 || patched["b"] != nil || patched["c"] != 3 {
		t.Fatalf("patch mismatch: %#v", patched)
	}

	m1 := map[string]interface{}{"x": 1.0, "s": "a", "arr": []interface{}{1}}
	m2 := map[string]interface{}{"x": 2.0, "s": "b", "arr": []interface{}{2}, "z": 9}
	if got := Merge(m1, m2, "prefer_new"); got["x"] != 2.0 {
		t.Fatalf("merge prefer_new failed")
	}
	if got := Merge(m1, m2, "prefer_old"); got["x"] != 1.0 {
		t.Fatalf("merge prefer_old failed")
	}
	if got := Merge(m1, m2, "combine"); got["x"] != 3.0 || got["s"] != "ab" {
		t.Fatalf("merge combine failed: %#v", got)
	}

	summary := Summary(d)
	if summary["added"] != 1 || summary["removed"] != 1 || summary["changed"] != 1 {
		t.Fatalf("summary mismatch: %#v", summary)
	}
}
