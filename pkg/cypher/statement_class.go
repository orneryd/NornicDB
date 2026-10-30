package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/fabric"
)

// isNornicExtensionStatement reports whether cypher is a statement outside
// the shared grammar, which the native validator checks instead on either
// parser (#957): NornicDB's own knowledge-policy DDL,
// procedure DDL (CREATE [OR REPLACE] PROCEDURE, DROP PROCEDURE) and database
// limits (ALTER DATABASE … SET LIMIT, SHOW LIMITS), CREATE [OR REPLACE]
// DATABASE … OPTIONS (#937), ALTER DATABASE … SET / REMOVE OPTION, and schema
// commands (isSchemaCommandStatement), whose
// owner reads forms the grammar does not: unquoted dotted option keys such as
// vector.dimensions and IF EXISTS before a dropped name.
func isNornicExtensionStatement(cypher string) bool {
	return classifyStatement(cypher).extension
}

// statementClass is a statement's kind and the permission it needs, read
// from its body: the statement without its execution-mode and leading USE
// prefixes. classifyStatement is its one decider; every consumer (admission,
// the execution contract, permissions, routing, Fabric, the semantic
// validators and the USE adapters) reads these answers instead of naming
// statement families itself.
type statementClass struct {
	// extension: outside the shared grammar (isNornicExtensionStatement).
	extension bool
	// commandOwned: its command owner reads and diagnoses its syntax, so
	// statement admission does not ask the grammar (isCommandOwnedBody).
	commandOwned bool
	// graphQuery: a graph query, whose expressions take the operand checks.
	graphQuery bool
	// show: a SHOW command.
	show bool
	// schema: a schema command (CREATE / DROP CONSTRAINT, INDEX, …), which
	// declares rules over patterns and binds no clause variables.
	schema bool
	// ordinarySchema: index and constraint DDL, run in a schema transaction.
	ordinarySchema bool
	// catalog: a catalog command (databases, composite databases, aliases,
	// privileges), run on the database manager without a graph transaction.
	catalog bool
	// definition: a CREATE that defines a catalog, schema, procedure or
	// knowledge-policy object instead of writing a pattern.
	definition bool
	// administration: one of Neo4j's administration commands, routed to the
	// system database and refused after USE (isAdministrationBody).
	administration bool
	// systemRouted: an administration command the system database runs by
	// itself; with a leading USE it is refused instead (useAdministrationCommandError).
	systemRouted bool
	// transactionControl: a bare BEGIN, COMMIT or ROLLBACK [TRANSACTION].
	transactionControl bool
	// creates: a CREATE statement (a pattern or a definition).
	creates bool
	// terminate: TERMINATE TRANSACTION[S].
	terminate bool
	// procedureDefinition: CREATE [OR REPLACE] PROCEDURE, whose $names
	// declare the procedure's arguments.
	procedureDefinition bool
	// knowledgePolicy: NornicDB's knowledge-policy DDL.
	knowledgePolicy bool
	// privilegeShaped: GRANT / REVOKE / DENY shaped as a privilege statement
	// (isPrivilegeCommandShaped); another statement starting with these words
	// is a syntax error.
	privilegeShaped bool
	// ownSyntax: a statement with a syntax of its own, which canonicalization
	// keeps as written but for its comments (hasOwnStatementSyntax).
	ownSyntax bool
	// optionsDefinition and limitsAlteration: forms whose option maps and
	// limit values their command owner reads, so the lexical map-key and
	// numeric checks do not apply (CREATE … INDEX … OPTIONS, CREATE
	// CONSTRAINT … REQUIRE, ALTER DATABASE … SET LIMIT).
	optionsDefinition, limitsAlteration bool
	// compositeRootCommand: a command a composite database runs at its root,
	// without a constituent target (isCompositeRootShow).
	compositeRootCommand bool
	// command: the command the statement names, which selects its owner
	// (readStatementCommand); commandNone for a graph query.
	command statementCommand
	// schemaCommand: a schema command's form (readSchemaCommand).
	schemaCommand schemaCommand
	// schemaPermission and adminPermission: the entitlement the statement
	// needs beyond read and write.
	schemaPermission, adminPermission bool
	// change: the state a command changes by itself (its row's change); a
	// graph query's effects are the grammar's (readStatementExecutionContract).
	change stateChange
	// version: the language version its CYPHER preamble states, "5" when it
	// states none (readStatementBody).
	version string
	// opening: how the statement opens (readStatementOpening); an EXPLAIN,
	// PROFILE or USE prefix opens it.
	opening statementOpening
	// startWord: the statement's first word as typed, upper-cased.
	startWord string
	// prefixMode: the statement's EXPLAIN or PROFILE prefix (EXPLAIN plans
	// the statement and runs nothing).
	prefixMode statementPrefixMode
}

// statementPrefixMode is a statement's execution prefix.
type statementPrefixMode uint8

const (
	prefixNone statementPrefixMode = iota
	prefixExplain
	prefixProfile
)

// declaresRules reports a schema or definition command: it declares rules or
// objects instead of evaluating expressions. A SHOW command evaluates its
// YIELD / WHERE / RETURN tail.
func (c statementClass) declaresRules() bool { return !c.show && (c.schema || c.definition) }

// classifyStatement is the one statement-kind decider. It reads the
// execution-mode and leading USE prefixes once, so EXPLAIN GRANT … and
// USE db CREATE CONSTRAINT … have their body's kind, and classifies the body.
func classifyStatement(cypher string) statementClass {
	if class, ok := statementTextClasses.get(cypher); ok {
		return class
	}
	body, used, version, prefixed, prefixMode := readStatementBody(cypher)
	class := classifyStatementBody(body)
	class.prefixMode = prefixMode
	class.systemRouted = class.administration && !used
	class.version = version
	if prefixed {
		class.opening = openingClause
	}
	class.startWord = readLeadingWords(cypher).at(0)
	statementTextClasses.put(cypher, class)
	return class
}

// statementClasses and statementTextClasses memoize classification per
// statement body and per full statement text: it depends only on the text,
// and every consumer of one statement asks for it.
var (
	statementClasses     = newBoundedCache[string, statementClass](4096, boundedCacheStringShardIndex)
	statementTextClasses = newBoundedCache[string, statementClass](4096, boundedCacheStringShardIndex)
)

// classifyStatementBody classifies a statement body (statementBody).
func classifyStatementBody(body string) statementClass {
	if class, ok := statementClasses.get(body); ok {
		return class
	}
	class := readStatementClass(body)
	statementClasses.put(body, class)
	return class
}

// readStatementClass derives every fact from the body's command identity
// (readStatementCommand) and its row in statementCommandFacts, and, for a
// graph query, from the grammar's query starts. Nothing else names command
// families.
func readStatementClass(body string) statementClass {
	w := readLeadingWords(body)
	command := readStatementCommand(body)
	row := statementCommandFacts[command]
	class := statementClass{
		opening:              readStatementOpening(w),
		command:              command,
		extension:            row.extension,
		show:                 row.show,
		terminate:            row.terminate,
		schema:               row.schemaShape,
		ordinarySchema:       row.ordinarySchema,
		catalog:              row.catalog,
		definition:           row.definition,
		administration:       row.administration,
		knowledgePolicy:      row.knowledgePolicy,
		procedureDefinition:  row.procedureDefinition,
		privilegeShaped:      row.privilegeShaped,
		ownSyntax:            row.ownSyntax,
		schemaPermission:     row.schemaPermission,
		adminPermission:      row.adminPermission,
		change:               row.change,
		creates:              w.at(0) == "CREATE",
		compositeRootCommand: row.compositeRoot,
		transactionControl:   row.transactionControl,
	}
	class.commandOwned = row.extension || row.catalog || row.show || row.diagnosed
	class.graphQuery = command == commandNone && isGraphQueryBody(body, w)
	switch command {
	case commandSchema:
		class.schemaCommand = readSchemaCommand(w)
		class.optionsDefinition = findKeywordIndex(body, "OPTIONS") >= 0 && class.schemaCommand != schemaCreateConstraint ||
			class.schemaCommand == schemaCreateConstraint && findKeywordIndex(body, "REQUIRE") >= 0
		class.ownSyntax = class.schemaCommand == schemaCreateConstraint && isConstraintContract(body)
	case commandAlterDatabaseLimit:
		class.limitsAlteration = true
	}
	return class
}

// statementBody strips execution modes and leading USE clauses, in any order,
// with the shared mode and USE readers. A malformed USE stays in the body; its
// own reader reports it.
func statementBody(cypher string) string {
	body, _, _, _, _ := readStatementBody(cypher)
	return body
}

// readStatementBody is statementBody that also reports whether it read a USE.
// It also reads past a CYPHER preamble.
func readStatementBody(cypher string) (string, bool, string, bool, statementPrefixMode) {
	used, prefixed, prefixMode := false, false, prefixNone
	version := "5"
	for {
		mode, body := parseExecutionMode(cypher)
		prefixed = prefixed || mode != ModeNormal
		switch {
		case mode == ModeExplain:
			prefixMode = prefixExplain
		case mode == ModeProfile && prefixMode == prefixNone:
			prefixMode = prefixProfile
		}
		body = strings.TrimSpace(body)
		body = body[statementTextStart(body):]
		// A CYPHER preamble (CYPHER 5, option groups) precedes the statement;
		// its last version number is the statement's language version.
		if stripped, changed, number, _ := readCypherPreamble(body); changed {
			if number != "" {
				version = number
			}
			cypher = stripped
			continue
		}
		_, remaining, found, err := fabric.ReadUseClause(context.Background(), body, false, cyphertext.AcceptedSource)
		if err != nil || !found || remaining == body {
			return body, used, version, prefixed, prefixMode
		}
		used, prefixed = true, true
		cypher = remaining
	}
}

// isGraphQueryBody reports whether a statement body (classifyStatement) is a
// Cypher graph query: it starts with a word that opens the grammar's single
// query (antlr.IsGraphQueryStart), and a CREATE is followed by a pattern
// ("CREATE (" or "CREATE p = "). CREATE INDEX / CONSTRAINT / DATABASE / USER
// and the other CREATE ... definitions are not. A USE left in the body is
// malformed and keeps the graph query's checks.
func isGraphQueryBody(query string, w statementWords) bool {
	if w.count == 0 || !antlr.IsGraphQueryStart(w.at(0)) {
		return false
	}
	if w.at(0) != "CREATE" {
		return true
	}
	start := queryGapEnd(query, 0) + len("CREATE")
	rest := query[queryGapEnd(query, start):]
	if strings.HasPrefix(rest, "(") {
		return true
	}
	name, ok := cyphertext.ScanIdentifier(rest, 0)
	return ok && name.Value != "" && strings.HasPrefix(rest[queryGapEnd(rest, name.End):], "=")
}
