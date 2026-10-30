// Package trigger provides APOC trigger functions.
//
// This package implements all apoc.trigger.* functions for managing
// database triggers and event handlers.
package trigger

import (
	"fmt"
	"sync"
)

// Trigger represents a database trigger.
type Trigger struct {
	Name      string
	Statement string
	Selector  map[string]interface{}
	Params    map[string]interface{}
	Enabled   bool
	Phase     string // before, after, afterAsync
}

var (
	triggers = make(map[string]*Trigger)
	mu       sync.RWMutex
)

// Add adds a new trigger.
//
// Example:
//
//	apoc.trigger.add('onPersonCreate', 'MATCH (n:Person) SET n.created = timestamp()', {})
func Add(name, statement string, selector map[string]interface{}) error {
	mu.Lock()
	defer mu.Unlock()

	triggers[name] = &Trigger{
		Name:      name,
		Statement: statement,
		Selector:  selector,
		Enabled:   true,
		Phase:     "after",
	}

	fmt.Printf("Trigger added: %s\n", name)
	return nil
}

// Remove removes a trigger.
//
// Example:
//
//	apoc.trigger.remove('onPersonCreate')
func Remove(name string) error {
	mu.Lock()
	defer mu.Unlock()

	if _, exists := triggers[name]; !exists {
		return fmt.Errorf("trigger not found: %s", name)
	}

	delete(triggers, name)
	fmt.Printf("Trigger removed: %s\n", name)
	return nil
}

// RemoveAll removes all triggers.
//
// Example:
//
//	apoc.trigger.removeAll()
func RemoveAll() error {
	mu.Lock()
	defer mu.Unlock()

	triggers = make(map[string]*Trigger)
	fmt.Println("All triggers removed")
	return nil
}

// List lists all triggers.
//
// Example:
//
//	apoc.trigger.list() => [{name: 'trigger1', ...}]
func List() []*Trigger {
	mu.RLock()
	defer mu.RUnlock()

	result := make([]*Trigger, 0, len(triggers))
	for _, trigger := range triggers {
		result = append(result, trigger)
	}

	return result
}

// Show shows a specific trigger.
//
// Example:
//
//	apoc.trigger.show('onPersonCreate') => trigger details
func Show(name string) (*Trigger, error) {
	mu.RLock()
	defer mu.RUnlock()

	trigger, exists := triggers[name]
	if !exists {
		return nil, fmt.Errorf("trigger not found: %s", name)
	}

	return trigger, nil
}

// Pause pauses a trigger.
//
// Example:
//
//	apoc.trigger.pause('onPersonCreate')
func Pause(name string) error {
	mu.Lock()
	defer mu.Unlock()

	trigger, exists := triggers[name]
	if !exists {
		return fmt.Errorf("trigger not found: %s", name)
	}

	trigger.Enabled = false
	fmt.Printf("Trigger paused: %s\n", name)
	return nil
}

// Resume resumes a trigger.
//
// Example:
//
//	apoc.trigger.resume('onPersonCreate')
func Resume(name string) error {
	mu.Lock()
	defer mu.Unlock()

	trigger, exists := triggers[name]
	if !exists {
		return fmt.Errorf("trigger not found: %s", name)
	}

	trigger.Enabled = true
	fmt.Printf("Trigger resumed: %s\n", name)
	return nil
}

// Install installs a trigger from definition.
//
// Example:
//
//	apoc.trigger.install('db', 'trigger1', statement, selector)
func Install(database, name, statement string, selector map[string]interface{}) error {
	return Add(name, statement, selector)
}

// Drop drops a trigger.
//
// Example:
//
//	apoc.trigger.drop('trigger1')
func Drop(name string) error {
	return Remove(name)
}

// NodeByLabel creates a trigger for nodes with specific label.
//
// Example:
//
//	apoc.trigger.nodeByLabel('Person', 'SET n.processed = true')
func NodeByLabel(label, statement string) error {
	name := fmt.Sprintf("nodeByLabel_%s", label)
	selector := map[string]interface{}{
		"label": label,
	}
	return Add(name, statement, selector)
}

// RelationshipByType creates a trigger for relationships of specific type.
//
// Example:
//
//	apoc.trigger.relationshipByType('KNOWS', 'SET r.created = timestamp()')
func RelationshipByType(relType, statement string) error {
	name := fmt.Sprintf("relByType_%s", relType)
	selector := map[string]interface{}{
		"type": relType,
	}
	return Add(name, statement, selector)
}

// OnCreate creates a trigger for create events.
//
// Example:
//
//	apoc.trigger.onCreate('Person', statement)
func OnCreate(label, statement string) error {
	name := fmt.Sprintf("onCreate_%s", label)
	selector := map[string]interface{}{
		"label": label,
		"event": "create",
	}
	return Add(name, statement, selector)
}

// OnUpdate creates a trigger for update events.
//
// Example:
//
//	apoc.trigger.onUpdate('Person', statement)
func OnUpdate(label, statement string) error {
	name := fmt.Sprintf("onUpdate_%s", label)
	selector := map[string]interface{}{
		"label": label,
		"event": "update",
	}
	return Add(name, statement, selector)
}

// OnDelete creates a trigger for delete events.
//
// Example:
//
//	apoc.trigger.onDelete('Person', statement)
func OnDelete(label, statement string) error {
	name := fmt.Sprintf("onDelete_%s", label)
	selector := map[string]interface{}{
		"label": label,
		"event": "delete",
	}
	return Add(name, statement, selector)
}

// Before creates a before trigger.
//
// Example:
//
//	apoc.trigger.before('validation', statement)
func Before(name, statement string) error {
	mu.Lock()
	defer mu.Unlock()

	triggers[name] = &Trigger{
		Name:      name,
		Statement: statement,
		Enabled:   true,
		Phase:     "before",
	}

	return nil
}

// After creates an after trigger.
//
// Example:
//
//	apoc.trigger.after('audit', statement)
func After(name, statement string) error {
	mu.Lock()
	defer mu.Unlock()

	triggers[name] = &Trigger{
		Name:      name,
		Statement: statement,
		Enabled:   true,
		Phase:     "after",
	}

	return nil
}

// AfterAsync creates an asynchronous after trigger.
//
// Example:
//
//	apoc.trigger.afterAsync('notification', statement)
func AfterAsync(name, statement string) error {
	mu.Lock()
	defer mu.Unlock()

	triggers[name] = &Trigger{
		Name:      name,
		Statement: statement,
		Enabled:   true,
		Phase:     "afterAsync",
	}

	return nil
}

// Enable enables a trigger.
//
// Example:
//
//	apoc.trigger.enable('trigger1')
func Enable(name string) error {
	return Resume(name)
}

// Disable disables a trigger.
//
// Example:
//
//	apoc.trigger.disable('trigger1')
func Disable(name string) error {
	return Pause(name)
}

// IsEnabled checks if a trigger is enabled.
//
// Example:
//
//	apoc.trigger.isEnabled('trigger1') => true/false
func IsEnabled(name string) bool {
	mu.RLock()
	defer mu.RUnlock()

	trigger, exists := triggers[name]
	return exists && trigger.Enabled
}

// Count returns the number of triggers.
//
// Example:
//
//	apoc.trigger.count() => 5
func Count() int {
	mu.RLock()
	defer mu.RUnlock()

	return len(triggers)
}

// Stats returns trigger statistics.
//
// Example:
//
//	apoc.trigger.stats() => {total: 5, enabled: 3, disabled: 2}
func Stats() map[string]int {
	mu.RLock()
	defer mu.RUnlock()

	enabled := 0
	disabled := 0

	for _, trigger := range triggers {
		if trigger.Enabled {
			enabled++
		} else {
			disabled++
		}
	}

	return map[string]int{
		"total":    len(triggers),
		"enabled":  enabled,
		"disabled": disabled,
	}
}

// Export exports trigger definitions.
//
// Example:
//
//	apoc.trigger.export() => trigger definitions
func Export() []*Trigger {
	return List()
}

// Import imports trigger definitions.
//
// Example:
//
//	apoc.trigger.import(triggers) => imported
func Import(triggerDefs []*Trigger) error {
	mu.Lock()
	defer mu.Unlock()

	for _, trigger := range triggerDefs {
		triggers[trigger.Name] = trigger
	}

	return nil
}
