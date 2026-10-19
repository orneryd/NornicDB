package storage

// constituentForElementID resolves a qualified identity before a local read.
// A qualified ID never falls back to another database with the same local ID.
// qualified is true even when its database is unavailable or the kind differs.
func (c *CompositeEngine) constituentForElementID(kind, value string) (engine Engine, local string, qualified bool) {
	actual, database, local, qualified := ParseElementID(value)
	if !qualified || actual != kind {
		return nil, local, qualified
	}
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		c.mu.RLock()
		name := c.constituentNames[alias]
		c.mu.RUnlock()
		if name == "" {
			name = DatabaseOf(engine)
		}
		if name == "" {
			name = alias
		}
		if name == database {
			return engine, local, true
		}
	}
	return nil, local, true
}

func (c *CompositeEngine) constituentNodeElementID(alias string, engine Engine, id NodeID) NodeID {
	database := DatabaseOf(engine)
	if database == "" {
		c.mu.RLock()
		database = c.constituentNames[alias]
		c.mu.RUnlock()
	}
	if database == "" {
		database = alias
	}
	return NodeID(SourceNodeElementID(database, id))
}
