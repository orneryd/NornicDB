// Package atomic provides APOC atomic operations.
//
// This package implements all apoc.atomic.* functions for atomic
// updates and operations on nodes and relationships.
package atomic

import (
	"fmt"
	"sync"
)

// Node represents a graph node.
type Node struct {
	ID         int64
	Labels     []string
	Properties map[string]interface{}
}

// Relationship represents a graph relationship.
type Relationship struct {
	ID         int64
	Type       string
	StartNode  int64
	EndNode    int64
	Properties map[string]interface{}
}

var mu sync.Mutex

// Add atomically adds a value to a numeric property.
//
// Example:
//
//	apoc.atomic.add(node, 'counter', 5) => updated node
func Add(node *Node, property string, value interface{}) *Node {
	mu.Lock()
	defer mu.Unlock()

	current := node.Properties[property]
	switch v := value.(type) {
	case int:
		if curr, ok := current.(int); ok {
			node.Properties[property] = curr + v
		} else {
			node.Properties[property] = v
		}
	case int64:
		if curr, ok := current.(int64); ok {
			node.Properties[property] = curr + v
		} else {
			node.Properties[property] = v
		}
	case float64:
		if curr, ok := current.(float64); ok {
			node.Properties[property] = curr + v
		} else {
			node.Properties[property] = v
		}
	}

	return node
}

// Subtract atomically subtracts a value from a numeric property.
//
// Example:
//
//	apoc.atomic.subtract(node, 'counter', 3) => updated node
func Subtract(node *Node, property string, value interface{}) *Node {
	mu.Lock()
	defer mu.Unlock()

	current := node.Properties[property]
	switch v := value.(type) {
	case int:
		if curr, ok := current.(int); ok {
			node.Properties[property] = curr - v
		} else {
			node.Properties[property] = -v
		}
	case int64:
		if curr, ok := current.(int64); ok {
			node.Properties[property] = curr - v
		} else {
			node.Properties[property] = -v
		}
	case float64:
		if curr, ok := current.(float64); ok {
			node.Properties[property] = curr - v
		} else {
			node.Properties[property] = -v
		}
	}

	return node
}

// Concat atomically concatenates a value to a string property.
//
// Example:
//
//	apoc.atomic.concat(node, 'name', ' Jr.') => updated node
func Concat(node *Node, property string, value string) *Node {
	mu.Lock()
	defer mu.Unlock()

	if current, ok := node.Properties[property].(string); ok {
		node.Properties[property] = current + value
	} else {
		node.Properties[property] = value
	}

	return node
}

// Insert atomically inserts a value into a list property.
//
// Example:
//
//	apoc.atomic.insert(node, 'tags', 2, 'new-tag') => updated node
func Insert(node *Node, property string, position int, value interface{}) *Node {
	mu.Lock()
	defer mu.Unlock()

	if current, ok := node.Properties[property].([]interface{}); ok {
		if position < 0 {
			position = 0
		}
		if position > len(current) {
			position = len(current)
		}

		newList := make([]interface{}, 0, len(current)+1)
		newList = append(newList, current[:position]...)
		newList = append(newList, value)
		newList = append(newList, current[position:]...)
		node.Properties[property] = newList
	} else {
		node.Properties[property] = []interface{}{value}
	}

	return node
}

// Remove atomically removes a value from a list property.
//
// Example:
//
//	apoc.atomic.remove(node, 'tags', 1) => updated node
func Remove(node *Node, property string, position int) *Node {
	mu.Lock()
	defer mu.Unlock()

	if current, ok := node.Properties[property].([]interface{}); ok {
		if position >= 0 && position < len(current) {
			newList := make([]interface{}, 0, len(current)-1)
			newList = append(newList, current[:position]...)
			newList = append(newList, current[position+1:]...)
			node.Properties[property] = newList
		}
	}

	return node
}

// Update atomically updates a property with a new value.
//
// Example:
//
//	apoc.atomic.update(node, 'status', 'active') => updated node
func Update(node *Node, property string, value interface{}) *Node {
	mu.Lock()
	defer mu.Unlock()

	node.Properties[property] = value
	return node
}

// Increment atomically increments a numeric property by 1.
//
// Example:
//
//	apoc.atomic.increment(node, 'counter') => updated node
func Increment(node *Node, property string) *Node {
	return Add(node, property, 1)
}

// Decrement atomically decrements a numeric property by 1.
//
// Example:
//
//	apoc.atomic.decrement(node, 'counter') => updated node
func Decrement(node *Node, property string) *Node {
	return Subtract(node, property, 1)
}

// CompareAndSwap atomically compares and swaps a property value.
//
// Example:
//
//	apoc.atomic.compareAndSwap(node, 'status', 'pending', 'active') => success
func CompareAndSwap(node *Node, property string, expected, newValue interface{}) bool {
	mu.Lock()
	defer mu.Unlock()

	current := node.Properties[property]
	if fmt.Sprintf("%v", current) == fmt.Sprintf("%v", expected) {
		node.Properties[property] = newValue
		return true
	}

	return false
}
