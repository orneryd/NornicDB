// Package plugins provides concrete plugin implementations for APOC functions.
package plugins

import (
	"github.com/orneryd/nornicdb/apoc/coll"
	"github.com/orneryd/nornicdb/apoc/plugin"
	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/orneryd/nornicdb/apoc/storage"
)

// CollPlugin provides collection manipulation functions.
type CollPlugin struct {
	store storage.Storage
}

// NewCollPlugin creates a new collection plugin.
func NewCollPlugin() plugin.Plugin {
	return &CollPlugin{}
}

func (p *CollPlugin) Name() string {
	return "coll"
}

func (p *CollPlugin) Version() string {
	return "1.0.0"
}

func (p *CollPlugin) Description() string {
	return "Collection manipulation functions for lists and arrays"
}

func (p *CollPlugin) Initialize(store storage.Storage) error {
	p.store = store
	return nil
}

func (p *CollPlugin) Cleanup() error {
	return nil
}

func (p *CollPlugin) Register(reg *registry.FunctionRegistry) error {
	// Register all collection functions
	functions := []struct {
		name        string
		fn          interface{}
		description string
		examples    []string
	}{
		{
			"apoc.coll.sum",
			coll.Sum,
			"Sum all numeric values in a list",
			[]string{"apoc.coll.sum([1,2,3,4,5]) => 15"},
		},
		{
			"apoc.coll.avg",
			coll.Avg,
			"Average of all numeric values in a list",
			[]string{"apoc.coll.avg([1,2,3,4,5]) => 3.0"},
		},
		{
			"apoc.coll.min",
			coll.Min,
			"Minimum value in a list",
			[]string{"apoc.coll.min([5,2,8,1,9]) => 1"},
		},
		{
			"apoc.coll.max",
			coll.Max,
			"Maximum value in a list",
			[]string{"apoc.coll.max([5,2,8,1,9]) => 9"},
		},
		{
			"apoc.coll.sort",
			coll.Sort,
			"Sort a list in ascending order",
			[]string{"apoc.coll.sort([3,1,4,1,5,9,2,6]) => [1,1,2,3,4,5,6,9]"},
		},
		{
			"apoc.coll.reverse",
			coll.Reverse,
			"Reverse a list",
			[]string{"apoc.coll.reverse([1,2,3,4,5]) => [5,4,3,2,1]"},
		},
		{
			"apoc.coll.contains",
			coll.Contains,
			"Check if a list contains a value",
			[]string{"apoc.coll.contains([1,2,3,4,5], 3) => true"},
		},
		{
			"apoc.coll.containsAll",
			coll.ContainsAll,
			"Check if a list contains all values from another list",
			[]string{"apoc.coll.containsAll([1,2,3,4,5], [2,4]) => true"},
		},
		{
			"apoc.coll.union",
			coll.Union,
			"Union of multiple lists (unique values)",
			[]string{"apoc.coll.union([1,2,3], [3,4,5]) => [1,2,3,4,5]"},
		},
		{
			"apoc.coll.intersection",
			coll.Intersection,
			"Intersection of multiple lists",
			[]string{"apoc.coll.intersection([1,2,3,4], [2,3,4,5]) => [2,3,4]"},
		},
		{
			"apoc.coll.subtract",
			coll.Subtract,
			"Elements in first list not in second",
			[]string{"apoc.coll.subtract([1,2,3,4,5], [2,4,6]) => [1,3,5]"},
		},
		{
			"apoc.coll.flatten",
			coll.Flatten,
			"Flatten nested lists",
			[]string{"apoc.coll.flatten([[1,2],[3,4],[5]], true) => [1,2,3,4,5]"},
		},
		{
			"apoc.coll.zip",
			coll.Zip,
			"Combine multiple lists into list of lists",
			[]string{"apoc.coll.zip([1,2,3], ['a','b','c']) => [[1,'a'],[2,'b'],[3,'c']]"},
		},
		{
			"apoc.coll.toSet",
			coll.ToSet,
			"Remove duplicates from a list",
			[]string{"apoc.coll.toSet([1,2,1,3,2,4]) => [1,2,3,4]"},
		},
		{
			"apoc.coll.frequencies",
			coll.Frequencies,
			"Count occurrences of each value",
			[]string{"apoc.coll.frequencies([1,2,2,3,3,3]) => {1:1, 2:2, 3:3}"},
		},
	}
	
	for _, f := range functions {
		if err := reg.RegisterFunction(f.name, p.Name(), f.fn, f.description, f.examples); err != nil {
			return err
		}
	}
	
	return nil
}
