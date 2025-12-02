// Package registry provides a plugin system for APOC functions.
//
// This package allows APOC functions to be registered and called dynamically
// at runtime, enabling a flexible plugin architecture for extending Cypher
// query capabilities.
package registry

import (
	"fmt"
	"reflect"
	"sync"
)

// FunctionRegistry manages all registered APOC functions.
type FunctionRegistry struct {
	functions map[string]*FunctionDescriptor
	mu        sync.RWMutex
}

// FunctionDescriptor describes a registered function.
type FunctionDescriptor struct {
	Name        string                                 // Full name (e.g., "apoc.coll.sum")
	Category    string                                 // Category (e.g., "coll", "text", "math")
	Function    interface{}                            // The actual function
	Description string                                 // Human-readable description
	Examples    []string                               // Usage examples
	Handler     func(args []interface{}) interface{}   // Wrapper for type-safe execution
}

// Global registry instance
var globalRegistry = NewFunctionRegistry()

// NewFunctionRegistry creates a new function registry.
func NewFunctionRegistry() *FunctionRegistry {
	return &FunctionRegistry{
		functions: make(map[string]*FunctionDescriptor),
	}
}

// Register registers a function with the global registry.
//
// Example:
//   Register("apoc.coll.sum", "coll", coll.Sum, "Sum numeric values", []string{"apoc.coll.sum([1,2,3]) => 6"})
func Register(name, category string, fn interface{}, description string, examples []string) error {
	return globalRegistry.RegisterFunction(name, category, fn, description, examples)
}

// RegisterFunction registers a function in the registry.
func (r *FunctionRegistry) RegisterFunction(name, category string, fn interface{}, description string, examples []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	if _, exists := r.functions[name]; exists {
		return fmt.Errorf("function %s already registered", name)
	}
	
	// Create type-safe handler
	handler, err := createHandler(fn)
	if err != nil {
		return fmt.Errorf("failed to create handler for %s: %w", name, err)
	}
	
	r.functions[name] = &FunctionDescriptor{
		Name:        name,
		Category:    category,
		Function:    fn,
		Description: description,
		Examples:    examples,
		Handler:     handler,
	}
	
	return nil
}

// Call calls a registered function by name with the given arguments.
//
// Example:
//   result, err := Call("apoc.coll.sum", []interface{}{[]interface{}{1, 2, 3}})
func Call(name string, args []interface{}) (interface{}, error) {
	return globalRegistry.CallFunction(name, args)
}

// CallFunction calls a registered function.
func (r *FunctionRegistry) CallFunction(name string, args []interface{}) (interface{}, error) {
	r.mu.RLock()
	descriptor, exists := r.functions[name]
	r.mu.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("function %s not found", name)
	}
	
	// Call the handler
	result := descriptor.Handler(args)
	return result, nil
}

// Get retrieves a function descriptor by name.
func (r *FunctionRegistry) Get(name string) (*FunctionDescriptor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	descriptor, exists := r.functions[name]
	return descriptor, exists
}

// List returns all registered function names.
func (r *FunctionRegistry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	names := make([]string, 0, len(r.functions))
	for name := range r.functions {
		names = append(names, name)
	}
	return names
}

// ListByCategory returns all function names in a category.
func (r *FunctionRegistry) ListByCategory(category string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	names := make([]string, 0)
	for name, descriptor := range r.functions {
		if descriptor.Category == category {
			names = append(names, name)
		}
	}
	return names
}

// Categories returns all registered categories.
func (r *FunctionRegistry) Categories() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	categories := make(map[string]bool)
	for _, descriptor := range r.functions {
		categories[descriptor.Category] = true
	}
	
	result := make([]string, 0, len(categories))
	for category := range categories {
		result = append(result, category)
	}
	return result
}

// Unregister removes a function from the registry.
func (r *FunctionRegistry) Unregister(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	delete(r.functions, name)
}

// Clear removes all functions from the registry.
func (r *FunctionRegistry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	r.functions = make(map[string]*FunctionDescriptor)
}

// createHandler creates a type-safe handler for a function.
func createHandler(fn interface{}) (func([]interface{}) interface{}, error) {
	fnValue := reflect.ValueOf(fn)
	fnType := fnValue.Type()
	
	if fnType.Kind() != reflect.Func {
		return nil, fmt.Errorf("not a function")
	}
	
	return func(args []interface{}) interface{} {
		// Convert args to reflect.Value
		in := make([]reflect.Value, len(args))
		for i, arg := range args {
			in[i] = reflect.ValueOf(arg)
		}
		
		// Call function
		out := fnValue.Call(in)
		
		// Return first result (or nil if no results)
		if len(out) == 0 {
			return nil
		}
		return out[0].Interface()
	}, nil
}

// GetGlobalRegistry returns the global registry instance.
func GetGlobalRegistry() *FunctionRegistry {
	return globalRegistry
}
