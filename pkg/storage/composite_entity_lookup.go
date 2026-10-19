package storage

// constituentForElementID resolves a qualified identity before a local read.
// A qualified ID never falls back to another database with the same local ID.
// qualified is true even when its database is unavailable or the kind differs.
func (c *CompositeEngine) constituentForElementID(kind, value string) (engine Engine, local string, qualified bool) {
	aliases,local,qualified := c.constituentsForEntity(kind,value)
	if !qualified || len(aliases) == 0 { return nil,local,qualified }
	engine,_ = c.getConstituent(aliases[0])
	return engine,local,true
}

// constituentsForEntity is the shared source selector for ordinary and MVCC
// identity reads. A qualified reference cannot fall through to another source.
func (c *CompositeEngine) constituentsForEntity(kind,value string) ([]string,string,bool) {
	actual, database, local, qualified := ParseElementID(value)
	aliases := c.getConstituentsForRead()
	if !qualified { return aliases,value,false }
	if actual != kind { return nil,local,true }
	for _, alias := range aliases {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if c.constituentDatabase(alias,engine) == database {
			return []string{alias}, local, true
		}
	}
	return nil, local, true
}

func (c *CompositeEngine) constituentDatabase(alias string,engine Engine) string {
	if database := DatabaseOf(engine); database != "" { return database }
	c.mu.RLock()
	database := c.constituentNames[alias]
	c.mu.RUnlock()
	if database == "" { database = alias }
	return database
}

// constituentForNodePair scopes both endpoints to the same selected source.
// Unqualified endpoint IDs stay local within that source; two different
// qualified sources can never name a stored relationship.
func (c *CompositeEngine) constituentForNodePair(start, end NodeID) (Engine,NodeID,NodeID,bool) {
	startKind,startDB,startLocal,startQualified := ParseElementID(string(start))
	endKind,endDB,endLocal,endQualified := ParseElementID(string(end))
	if !startQualified && !endQualified { return nil,start,end,false }
	if startQualified && startKind != "4" || endQualified && endKind != "4" || startQualified && endQualified && startDB != endDB {
		return nil,"","",true
	}
	selected := string(start)
	if !startQualified { selected = string(end); startLocal = string(start) }
	if !endQualified { endLocal = string(end) }
	engine,_,_ := c.constituentForElementID("4",selected)
	return engine,NodeID(startLocal),NodeID(endLocal),true
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
