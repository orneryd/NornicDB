package auth

import "slices"

// FineGrainedAdministrationActions is the operation inventory for database
// schema, name tokens, security administration and database administration.
func FineGrainedAdministrationActions() []FineGrainedAction {
	var actions []FineGrainedAction
	for _, id := range []string{
		"database.start", "database.stop", "database.cdc.read", "database.index.create", "database.index.drop", "database.index.show",
		"database.constraint.create", "database.constraint.drop", "database.constraint.show",
		"database.token.label.create", "database.token.relationship_type.create", "database.token.property.create",
	} {
		actions = append(actions, FineGrainedAction{ID: id, ResourceKind: FineGrainedDatabase})
	}
	for _, id := range []string{"database.transaction.show", "database.transaction.terminate"} {
		actions = append(actions, FineGrainedAction{ID: id, ResourceKind: FineGrainedDatabase, Property: true})
	}

	for _, id := range []string{
		"dbms.role.create", "dbms.role.rename", "dbms.role.drop", "dbms.role.assign", "dbms.role.remove", "dbms.role.show",
		"dbms.user.create", "dbms.user.rename", "dbms.user.alter", "dbms.user.drop", "dbms.user.show",
		"dbms.user.password.set", "dbms.user.auth.set", "dbms.user.home.set", "dbms.user.status.set",
		"dbms.database.create", "dbms.database.drop", "dbms.database.replace", "dbms.database.alter", "dbms.database.access.set",
		"dbms.database.composite.create", "dbms.database.composite.drop",
		"dbms.alias.create", "dbms.alias.drop", "dbms.alias.alter", "dbms.alias.show",
		"dbms.server.manage", "dbms.server.show", "dbms.settings.show",
		"dbms.security.privilege.show", "dbms.security.privilege.assign", "dbms.security.privilege.remove",
		"dbms.security.policy.manage", "dbms.user.metadata.show", "dbms.user.metadata.set",
	} {
		actions = append(actions, FineGrainedAction{ID: id, ResourceKind: FineGrainedDBMS})
	}
	for _, id := range []string{
		"dbms.user.impersonate", "dbms.procedure.execute", "dbms.procedure.execute_boosted",
		"dbms.function.execute", "dbms.function.execute_boosted",
	} {
		actions = append(actions, FineGrainedAction{ID: id, ResourceKind: FineGrainedDBMS, Property: true})
	}
	return actions
}

// FineGrainedCoreActions returns the storage and administration contracts.
func FineGrainedCoreActions() []FineGrainedAction {
	return slices.Concat(GraphFineGrainedActions(), FineGrainedAdministrationActions())
}
