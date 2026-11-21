package cypher

import (
	"context"
	"strings"
)

func parseFineGrainedExecutablePrivilege(ctx context.Context, text string) (fineGrainedPrivilegeCommand, bool, error) {
	var command fineGrainedPrivilegeCommand
	cursor := fineGrainedPrivilegeCursor{ctx: ctx, text: text}
	cursor.skipGap()
	verb, immutable, found := cursor.readVerb()
	if !found || !cursor.takeKeyword("EXECUTE") {
		return command, false, nil
	}
	command.Verb, command.Immutable, command.Kind = verb, immutable, fineGrainedDBMSKind
	command.Privilege = "EXECUTE"
	if cursor.takeKeyword("BOOSTED") {
		command.Privilege += " BOOSTED"
	}
	switch {
	case cursor.takeKeyword("PROCEDURE"), cursor.takeKeyword("PROCEDURES"):
		command.Privilege += " PROCEDURE"
	case cursor.takeKeyword("FUNCTION"), cursor.takeKeyword("FUNCTIONS"):
		command.Privilege += " FUNCTION"
	default:
		return command, true, procedurePrivilegeSyntaxError("expected PROCEDURE or FUNCTION")
	}
	on := findKeywordIndex(ctx, text[cursor.at:], "ON")
	if on < 0 {
		return command, true, procedurePrivilegeSyntaxError("expected ON DBMS")
	}
	for _, pattern := range splitTopLevelComma(text[cursor.at : cursor.at+on]) {
		pattern = unquoteProcedureGlob(strings.TrimSpace(pattern))
		if pattern == "" {
			return command, true, procedurePrivilegeSyntaxError("expected a name or wildcard")
		}
		command.Properties = append(command.Properties, pattern)
	}
	cursor.at += on
	if !cursor.takeKeyword("ON") || !cursor.takeKeyword("DBMS") || !cursor.takeKeyword(fineGrainedPrivilegeRolesKeyword(verb)) {
		return command, true, procedurePrivilegeSyntaxError("expected ON DBMS and a role")
	}
	roles, err := cursor.readRoleList()
	if err == nil && cursor.at != len(text) {
		err = procedurePrivilegeSyntaxError("unexpected input after the role list")
	}
	command.Roles = roles
	return command, true, err
}
