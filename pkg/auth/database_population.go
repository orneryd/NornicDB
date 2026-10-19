package auth

// DatabasePopulationAccess evaluates an unpartitioned operation's complete
// database population through its existing live permission predicate. Catalog
// providers supply every canonical name, including system, empty databases and
// composites. No database is exempt from admission. Call again at delivery time
// for long-lived consumers: neither the population nor its verdict is cached.
//
// Any permits a filtered projection; all permits an unpartitioned result. Once
// both answers are determined, further permission checks cannot change them.
func DatabasePopulationAccess(databases []string, allows func(string) bool) (any, all bool) {
	all = true
	for _, database := range databases {
		allowed := allows(database)
		any = any || allowed
		all = all && allowed
		if any && !all {
			break
		}
	}
	return any, all
}
