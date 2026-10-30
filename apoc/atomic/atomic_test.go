package atomic

import "testing"

func TestAdd(t *testing.T) {
	node := &Node{
		ID:         1,
		Properties: map[string]interface{}{"counter": int64(10)},
	}

	result := Add(node, "counter", int64(5))

	if val, ok := result.Properties["counter"].(int64); !ok || val != 15 {
		t.Errorf("Add result = %v; want 15", result.Properties["counter"])
	}
}

func TestSubtract(t *testing.T) {
	node := &Node{
		ID:         1,
		Properties: map[string]interface{}{"counter": int64(20)},
	}

	result := Subtract(node, "counter", int64(7))

	if val, ok := result.Properties["counter"].(int64); !ok || val != 13 {
		t.Errorf("Subtract result = %v; want 13", result.Properties["counter"])
	}
}

func TestIncrement(t *testing.T) {
	node := &Node{
		ID:         1,
		Properties: map[string]interface{}{"count": int64(5)},
	}

	result := Increment(node, "count")

	if val, ok := result.Properties["count"].(int64); !ok || val != 6 {
		t.Errorf("Increment result = %v; want 6", result.Properties["count"])
	}
}

func TestDecrement(t *testing.T) {
	node := &Node{
		ID:         1,
		Properties: map[string]interface{}{"count": int64(10)},
	}

	result := Decrement(node, "count")

	if val, ok := result.Properties["count"].(int64); !ok || val != 9 {
		t.Errorf("Decrement result = %v; want 9", result.Properties["count"])
	}
}

func TestConcat(t *testing.T) {
	node := &Node{
		ID:         1,
		Properties: map[string]interface{}{"name": "Hello"},
	}

	result := Concat(node, "name", " World")

	if val, ok := result.Properties["name"].(string); !ok || val != "Hello World" {
		t.Errorf("Concat result = %v; want 'Hello World'", result.Properties["name"])
	}
}

func TestCompareAndSwap(t *testing.T) {
	node := &Node{
		ID:         1,
		Properties: map[string]interface{}{"value": int64(10)},
	}

	success := CompareAndSwap(node, "value", int64(10), int64(20))
	if !success {
		t.Error("CompareAndSwap should succeed when old value matches")
	}

	if val, ok := node.Properties["value"].(int64); !ok || val != 20 {
		t.Errorf("After CAS: got %v, want 20", node.Properties["value"])
	}

	success = CompareAndSwap(node, "value", int64(10), int64(30))
	if success {
		t.Error("CompareAndSwap should fail when old value doesn't match")
	}
}
