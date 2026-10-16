package cypher

// commandPrivilege is a dispatch shape and its explicit operation contract.
// This is the sole policy table for administration and schema commands.
type commandPrivilege struct {
	first, rest string
	class       PrivilegeClass
}

var administrationCommandClasses = []commandPrivilege{
	{"GRANT", "", PrivilegeDBMS},
	{"DENY", "", PrivilegeDBMS},
	{"REVOKE", "", PrivilegeDBMS},
	{"CREATE", "OR REPLACE DATABASE", PrivilegeDBMS},
	{"CREATE", "OR REPLACE PROCEDURE", PrivilegeDBMS},
	{"CREATE", "PROCEDURE", PrivilegeDBMS},
	{"DROP", "PROCEDURE", PrivilegeDBMS},
	{"DROP", "ROLE", PrivilegeDBMS},
	{"CREATE", "DATABASE", PrivilegeDBMS},
	{"DROP", "DATABASE", PrivilegeDBMS},
	{"ALTER", "DATABASE", PrivilegeDBMS},
	{"CREATE", "COMPOSITE DATABASE", PrivilegeDBMS},
	{"DROP", "COMPOSITE DATABASE", PrivilegeDBMS},
	{"ALTER", "COMPOSITE DATABASE", PrivilegeDBMS},
	{"CREATE", "ALIAS", PrivilegeDBMS},
	{"DROP", "ALIAS", PrivilegeDBMS},
	{"CREATE", "DECAY PROFILE", PrivilegeDBMS},
	{"ALTER", "DECAY PROFILE", PrivilegeDBMS},
	{"DROP", "DECAY PROFILE", PrivilegeDBMS},
	{"CREATE", "PROMOTION PROFILE", PrivilegeDBMS},
	{"ALTER", "PROMOTION PROFILE", PrivilegeDBMS},
	{"DROP", "PROMOTION PROFILE", PrivilegeDBMS},
	{"CREATE", "PROMOTION POLICY", PrivilegeDBMS},
	{"ALTER", "PROMOTION POLICY", PrivilegeDBMS},
	{"DROP", "PROMOTION POLICY", PrivilegeDBMS},
	{"CREATE", "INDEX", PrivilegeSchema},
	{"CREATE", "RANGE INDEX", PrivilegeSchema},
	{"CREATE", "FULLTEXT INDEX", PrivilegeSchema},
	{"CREATE", "VECTOR INDEX", PrivilegeSchema},
	{"CREATE", "TEXT INDEX", PrivilegeSchema},
	{"CREATE", "POINT INDEX", PrivilegeSchema},
	{"CREATE", "LOOKUP INDEX", PrivilegeSchema},
	{"CREATE", "BTREE INDEX", PrivilegeSchema},
	{"DROP", "INDEX", PrivilegeSchema},
	{"CREATE", "CONSTRAINT", PrivilegeSchema},
	{"DROP", "CONSTRAINT", PrivilegeSchema},
	{"TERMINATE", "TRANSACTIONS", PrivilegeNone},
	{"TERMINATE", "TRANSACTION", PrivilegeNone},
	{"SHOW", "CURRENT USER", PrivilegeNone},
	{"SHOW", "USER PRIVILEGES", PrivilegeNone},
	{"SHOW", "USERS", PrivilegeAdmin},
	{"SHOW", "USER", PrivilegeAdmin},
	{"SHOW", "ROLES", PrivilegeAdmin},
	{"SHOW", "ROLE", PrivilegeAdmin},
	{"SHOW", "PRIVILEGES", PrivilegeAdmin},
	{"SHOW", "TRANSACTIONS", PrivilegeAdmin},
	{"SHOW", "TRANSACTION", PrivilegeAdmin},
	{"SHOW", "SERVERS", PrivilegeAdmin},
	{"SHOW", "SERVER", PrivilegeAdmin},
	{"SHOW", "PROCEDURES", PrivilegeNone},
	{"SHOW", "FUNCTIONS", PrivilegeNone},
	{"SHOW", "BUILT IN FUNCTIONS", PrivilegeNone},
	{"SHOW", "USER DEFINED FUNCTIONS", PrivilegeNone},
	{"SHOW", "DATABASES", PrivilegeRead},
	{"SHOW", "DATABASE", PrivilegeRead},
	{"SHOW", "DEFAULT DATABASE", PrivilegeRead},
	{"SHOW", "HOME DATABASE", PrivilegeRead},
	{"SHOW", "COMPOSITE DATABASES", PrivilegeRead},
	{"SHOW", "CONSTITUENTS", PrivilegeRead},
	{"SHOW", "ALIASES", PrivilegeRead},
	{"SHOW", "SETTINGS", PrivilegeRead},
	{"SHOW", "SETTING", PrivilegeRead},
	{"SHOW", "LIMITS", PrivilegeRead},
	{"SHOW", "CONSTRAINT CONTRACTS", PrivilegeRead},
	{"SHOW", "CONSTRAINTS", PrivilegeRead},
	{"SHOW", "CONSTRAINT", PrivilegeRead},
	{"SHOW", "DECAY PROFILES", PrivilegeRead},
	{"SHOW", "PROMOTION PROFILES", PrivilegeRead},
	{"SHOW", "PROMOTION POLICIES", PrivilegeRead},
	{"SHOW", "FULLTEXT INDEXES", PrivilegeRead},
	{"SHOW", "FULLTEXT INDEX", PrivilegeRead},
	{"SHOW", "RANGE INDEXES", PrivilegeRead},
	{"SHOW", "RANGE INDEX", PrivilegeRead},
	{"SHOW", "VECTOR INDEXES", PrivilegeRead},
	{"SHOW", "VECTOR INDEX", PrivilegeRead},
	{"SHOW", "LOOKUP INDEXES", PrivilegeRead},
	{"SHOW", "LOOKUP INDEX", PrivilegeRead},
	{"SHOW", "INDEXES", PrivilegeRead},
	{"SHOW", "INDEX", PrivilegeRead},
}

func administrationCommandClass(query string) (PrivilegeClass, bool) {
	query = classificationText(query)
	query = query[firstExecutableCypherOffset(query):]
	bestLength := -1
	var selected PrivilegeClass
	for _, command := range administrationCommandClasses {
		matches := false
		if command.rest == "" {
			matches = startsWithKeywordFold(query, command.first)
		} else if startsWithKeywords(query, command.first, command.rest) {
			matches = true
		}
		if length := len(command.first) + len(command.rest); matches && length > bestLength {
			bestLength, selected = length, command.class
		}
	}
	return selected, bestLength >= 0
}
