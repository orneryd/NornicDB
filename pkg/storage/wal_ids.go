package storage

import "strings"

// ParseDatabasePrefix splits an ID formatted as "<db>:<id>" into database and unprefixed ID.
//
// Returns ok=false if there is no valid prefix (no ':', empty db, or empty id).
func ParseDatabasePrefix(id string) (db string, unprefixed string, ok bool) {
	idx := strings.IndexByte(id, ':')
	if idx <= 0 || idx >= len(id)-1 {
		return "", "", false
	}
	return id[:idx], id[idx+1:], true
}

// StripDatabasePrefix removes "<db>:" from id only if it matches dbName.
// If dbName is empty, or id does not have the matching prefix, id is returned unchanged.
func StripDatabasePrefix(dbName, id string) string {
	if dbName == "" || id == "" {
		return id
	}
	prefix := dbName + ":"
	if strings.HasPrefix(id, prefix) {
		return id[len(prefix):]
	}
	return id
}

// EnsureDatabasePrefix adds "<dbName>:" to id if id has no existing valid prefix.
//
// If id already has a prefix (even a different database), id is returned unchanged to
// avoid accidentally rewriting cross-database IDs.
func EnsureDatabasePrefix(dbName, id string) string {
	if dbName == "" || id == "" {
		return id
	}
	if _, _, ok := ParseDatabasePrefix(id); ok {
		return id
	}
	return dbName + ":" + id
}

// EnsureNodeIDDatabasePrefixForEngine adds the active engine namespace to an
// unprefixed node ID. Schema backfills use this so cache entries match the
// storage IDs later seen by transaction commit validation.
func EnsureNodeIDDatabasePrefixForEngine(engine Engine, id NodeID) NodeID {
	namespace := namespaceForEngine(engine)
	if namespace == "" {
		return id
	}
	return NodeID(EnsureDatabasePrefix(namespace, string(id)))
}

func namespaceForEngine(engine Engine) string {
	visited := make(map[Engine]bool)
	for engine != nil && !visited[engine] {
		visited[engine] = true

		if provider, ok := engine.(interface{ Namespace() string }); ok {
			if namespace := provider.Namespace(); namespace != "" {
				return namespace
			}
		}

		switch wrapper := engine.(type) {
		case interface{ GetEngine() Engine }:
			engine = wrapper.GetEngine()
		case interface{ GetInnerEngine() Engine }:
			engine = wrapper.GetInnerEngine()
		default:
			engine = nil
		}
	}
	return ""
}
