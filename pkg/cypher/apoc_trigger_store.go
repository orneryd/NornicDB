package cypher

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// APOC triggers are kept in the system database, one node per trigger, as
// APOC keeps its ApocTrigger nodes there (TriggerHandlerNewProcedures):
// node ID apoc_trigger:<database>:<name>, labels _ApocTrigger and _System,
// with the trigger's database, name, statement, selector (JSON), params
// (JSON) and paused. String IDs in the system namespace: no key prefix, no
// storage version. A node counts as a trigger only when its ID is the one
// the store gives its database and name, so a node made some other way
// can't pass for one.
const (
	apocTriggerLabel    = "_ApocTrigger"
	apocTriggerIDPrefix = "apoc_trigger:"
)

// apocTrigger is an installed trigger.
type apocTrigger struct {
	Database  string
	Name      string
	Statement string
	Selector  map[string]any
	Params    map[string]any
	Paused    bool
	// phase is the selector's phase (apocTriggerPhaseOf).
	phase apocTriggerPhase
}

func apocTriggerNodeID(database, name string) storage.NodeID {
	return storage.NodeID(apocTriggerIDPrefix + database + ":" + name)
}

// apocTriggerStore reads and writes the triggers in the system database.
type apocTriggerStore struct {
	system storage.Engine
}

// load returns every database's triggers, by database and name.
func (s apocTriggerStore) load(ctx context.Context) (map[string]map[string]*apocTrigger, error) {
	triggers := make(map[string]map[string]*apocTrigger)
	err := storage.StreamNodesWithFallback(ctx, s.system, 1000, func(node *storage.Node) error {
		trigger, ok := apocTriggerFromNode(node)
		if !ok {
			return nil
		}
		if triggers[trigger.Database] == nil {
			triggers[trigger.Database] = make(map[string]*apocTrigger)
		}
		triggers[trigger.Database][trigger.Name] = trigger
		return nil
	})
	return triggers, err
}

func apocTriggerFromNode(node *storage.Node) (*apocTrigger, bool) {
	if node == nil || !containsString(node.Labels, apocTriggerLabel) {
		return nil, false
	}
	database, _ := node.Properties["database"].(string)
	name, _ := node.Properties["name"].(string)
	if database == "" || node.ID != apocTriggerNodeID(database, name) {
		return nil, false
	}
	statement, _ := node.Properties["statement"].(string)
	paused, _ := node.Properties["paused"].(bool)
	trigger := &apocTrigger{Database: database, Name: name, Statement: statement, Paused: paused}
	trigger.Selector = apocTriggerJSONMap(node.Properties["selector"])
	trigger.Params = apocTriggerJSONMap(node.Properties["params"])
	phase, err := apocTriggerPhaseOf(trigger.Selector)
	if err != nil {
		return nil, false
	}
	trigger.phase = phase
	return trigger, true
}

func apocTriggerJSONMap(value any) map[string]any {
	text, _ := value.(string)
	if text == "" || text == "null" {
		return nil
	}
	decoded, err := apocTriggerDecodeJSON([]byte(text))
	if err != nil {
		return nil
	}
	return decoded
}

// save installs or replaces a trigger.
func (s apocTriggerStore) save(trigger *apocTrigger) error {
	selector, err := json.Marshal(trigger.Selector)
	if err != nil {
		return err
	}
	params, err := json.Marshal(trigger.Params)
	if err != nil {
		return err
	}
	node := &storage.Node{
		ID:     apocTriggerNodeID(trigger.Database, trigger.Name),
		Labels: []string{apocTriggerLabel, "_System"},
		Properties: map[string]any{
			"database":  trigger.Database,
			"name":      trigger.Name,
			"statement": trigger.Statement,
			"selector":  string(selector),
			"params":    string(params),
			"paused":    trigger.Paused,
		},
	}
	if _, err := s.system.GetNode(node.ID); errors.Is(err, storage.ErrNotFound) {
		_, err = s.system.CreateNode(node)
		return err
	} else if err != nil {
		return err
	}
	return s.system.UpdateNode(node)
}

// remove deletes a trigger; one that isn't stored is no error.
func (s apocTriggerStore) remove(database, name string) error {
	err := s.system.DeleteNode(apocTriggerNodeID(database, name))
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	return err
}

// apocTriggersByName returns a database's triggers sorted by name: the
// order they run and are listed in.
func apocTriggersByName(triggers map[string]*apocTrigger) []*apocTrigger {
	sorted := make([]*apocTrigger, 0, len(triggers))
	for _, trigger := range triggers {
		sorted = append(sorted, trigger)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	return sorted
}
