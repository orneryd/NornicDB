package fabric

import (
	"fmt"
	"strings"
)

// RegisterComposite registers a local logical coordinator. Its location is not
// a physical participant: native entities keep their own source database.
func (c *Catalog) RegisterComposite(name string, location *LocationLocal) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.registerGraphLocked(name, location, true)
}

func (c *Catalog) registerGraphLocked(name string, location Location, composite bool) {
	name = strings.ToLower(name)
	if c.graphs == nil {
		c.graphs = make(map[string]Location)
	}
	c.graphs[name] = location
	if composite {
		if c.compositeScopes == nil {
			c.compositeScopes = make(map[string]bool)
		}
		c.compositeScopes[name] = true
	} else {
		delete(c.compositeScopes, name)
	}
}

// BindTransactionScopes admits catalog coordinators before any fragment opens
// a participant. Alias locations use the existing physical-location key owner.
func (c *Catalog) BindTransactionScopes(transaction *FabricTransaction) error {
	if transaction == nil {
		return nil
	}
	c.mu.RLock()
	keys := make(map[string]bool, len(c.compositeScopes))
	for name := range c.compositeScopes {
		if location, exists := c.graphs[name]; exists {
			keys[participantKeyFromLocation(location)] = true
		}
	}
	c.mu.RUnlock()
	for key := range keys {
		if err := transaction.RegisterCoordinatorScope(key); err != nil {
			return err
		}
	}
	return nil
}

// RegisterCoordinatorScope distinguishes a logical scope from physical
// participants without changing their commit or rollback owner.
func (t *FabricTransaction) RegisterCoordinatorScope(name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != "open" {
		return fmt.Errorf("transaction is %s", t.state)
	}
	if _, physical := t.subTxns[name]; physical {
		return fmt.Errorf("participant %q is already physical", name)
	}
	if t.coordinatorScopes == nil {
		t.coordinatorScopes = make(map[string]bool)
	}
	t.coordinatorScopes[name] = true
	return nil
}

// IsCoordinatorScope reports a catalog-admitted logical scope. Such a scope
// may dispatch expressions but cannot own a storage transaction or writer vote.
func (t *FabricTransaction) IsCoordinatorScope(name string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.coordinatorScopes[name]
}

// GetOrOpenLocation joins a physical source through the same key owner as
// fragment dispatch; local aliases and remote locations never invent a key.
func (t *FabricTransaction) GetOrOpenLocation(location Location, write bool) (*SubTransaction, error) {
	return t.GetOrOpen(participantKeyFromLocation(location), write)
}
