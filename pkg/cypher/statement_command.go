package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"

	"github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// statementCommand is the command a statement body names. It is the one
// recognition of a command statement: every command fact (statementFacts) is
// read from its row in statementCommandFacts, and the statement router, the
// SHOW dispatcher, the schema dispatcher and result-column compilation select
// their command owner from it. Graph queries are commandNone.
type statementCommand uint8

const (
	commandNone statementCommand = iota
	// SHOW and TERMINATE commands (executeShowStatement).
	commandShowConstraints
	commandShowIndexes
	commandShowKnowledgePolicies
	commandShowProcedures
	commandShowFunctions
	commandShowCompositeDatabases
	commandShowConstituents
	commandShowDefaultDatabase
	commandShowUsers
	commandShowCurrentUser
	commandShowTransactions
	commandTerminateTransactions
	commandShowPrivileges
	commandShowUnsupportedAdministration
	commandShowUserDefined
	commandShowServers
	commandShowDatabases
	commandShowDatabase
	commandShowAliases
	commandShowSettings
	commandShowLimits
	commandShowAdministrationOther
	commandShowOther
	commandTerminateOther
	// Other commands (executeWithoutTransaction).
	commandAlterDatabase
	commandAlterDatabaseLimit
	commandAlterDatabaseOption
	commandProcedurePrivilege
	commandUnsupportedPrivilege
	commandMalformedPrivilege
	commandCreateProcedure
	commandCreateKnowledgePolicy
	commandAlterKnowledgePolicy
	commandDropKnowledgePolicy
	commandRetentionPolicy
	commandShowRetentionPolicy
	commandDropRetentionPolicy
	commandSchema
	commandCreateSchemaOther
	commandCreateCompositeDatabase
	commandCreateOrReplaceDatabase
	commandCreateOrReplaceOther
	commandCreateDatabase
	commandCreateDatabaseOptions
	commandCreateAlias
	commandDropCompositeDatabase
	commandDropDatabase
	commandDropAlias
	commandDropProcedure
	commandDropRole
	commandDropSchemaOther
	commandDropAdministration
	commandInvalidDrop
	commandAlterCompositeDatabase
	commandAdministrationUnsupported
	commandCreateAdministrationUnsupported
	// Transaction control: a bare BEGIN, COMMIT or ROLLBACK [TRANSACTION], and
	// a BEGIN … COMMIT / ROLLBACK script candidate.
	commandBegin
	commandCommit
	commandRollback
	commandBeginScript
)

// stateChange is the state a command changes by itself.
type stateChange uint8

const (
	changeNone stateChange = iota
	changeSchema
	changeAdministration
	// changeAdministrationCreate: an administration CREATE, which keeps the
	// implicit write wrapper used before procedure admission.
	changeAdministrationCreate
)

// commandRow is one command's facts. statementClass reads every command fact
// from its row; no other list names command families.
type commandRow struct {
	// show / terminate: a SHOW or TERMINATE command.
	show, terminate bool
	// diagnosed: its command owner reads and diagnoses its syntax although
	// it is neither an extension nor a catalog command (every SHOW is).
	diagnosed bool
	// extension: NornicDB reads it beside the shared grammar.
	extension bool
	// schemaShape: declares a schema rule (CREATE/DROP CONSTRAINT, INDEX, …);
	// ordinarySchema: index and constraint DDL run in a schema transaction.
	schemaShape, ordinarySchema bool
	// catalog: run on the database manager without a graph transaction.
	catalog bool
	// definition: a CREATE that defines an object instead of writing a pattern.
	definition bool
	// administration: one of Neo4j's administration commands, routed to the
	// system database and refused after USE.
	administration bool
	// knowledgePolicy, procedureDefinition, privilegeShaped, ownSyntax: the
	// command's own syntax families.
	knowledgePolicy, procedureDefinition, privilegeShaped, ownSyntax bool
	// compositeRoot: a composite database runs it at its root.
	compositeRoot bool
	// transactionControl: a bare BEGIN, COMMIT or ROLLBACK.
	transactionControl bool
	// schemaPermission / adminPermission: the entitlement it needs.
	schemaPermission, adminPermission bool
	// change: the state the command changes by itself.
	change stateChange
}

var (
	rowShowRead      = commandRow{show: true}
	rowShowAdmin     = commandRow{show: true, administration: true}
	rowSchemaDDL     = commandRow{extension: true, schemaShape: true, ordinarySchema: true, schemaPermission: true, compositeRoot: true, change: changeSchema}
	rowCatalog       = commandRow{catalog: true, definition: true, administration: true, adminPermission: true, compositeRoot: true, change: changeAdministration}
	rowCatalogCreate = commandRow{catalog: true, definition: true, administration: true, adminPermission: true, compositeRoot: true, change: changeAdministrationCreate}
	rowPrivilege     = commandRow{extension: true, catalog: true, definition: true, administration: true, adminPermission: true, compositeRoot: true, change: changeAdministration}
	rowPolicyChange  = commandRow{extension: true, knowledgePolicy: true, ownSyntax: true, adminPermission: true, change: changeAdministration}
)

// statementCommandFacts is the one table of command facts.
var statementCommandFacts = map[statementCommand]commandRow{
	commandShowConstraints:                 {show: true, compositeRoot: true},
	commandShowIndexes:                     {show: true, compositeRoot: true},
	commandShowKnowledgePolicies:           {show: true, extension: true, knowledgePolicy: true, ownSyntax: true},
	commandShowProcedures:                  {show: true, compositeRoot: true},
	commandShowFunctions:                   {show: true, compositeRoot: true},
	commandShowCompositeDatabases:          {show: true, compositeRoot: true},
	commandShowConstituents:                {show: true, compositeRoot: true},
	commandShowDefaultDatabase:             rowShowAdmin,
	commandShowUsers:                       {show: true, administration: true, adminPermission: true},
	commandShowCurrentUser:                 rowShowAdmin,
	commandShowTransactions:                {show: true, adminPermission: true},
	commandTerminateTransactions:           {terminate: true, adminPermission: true, change: changeAdministration},
	commandShowPrivileges:                  {show: true, extension: true, catalog: true, definition: true, administration: true, adminPermission: true, compositeRoot: true},
	commandShowUnsupportedAdministration:   rowShowAdmin,
	commandShowUserDefined:                 rowShowRead,
	commandShowServers:                     rowShowAdmin,
	commandShowDatabases:                   {show: true, catalog: true, definition: true, administration: true, compositeRoot: true},
	commandShowDatabase:                    {show: true, administration: true, compositeRoot: true},
	commandShowAliases:                     {show: true, administration: true, compositeRoot: true},
	commandShowSettings:                    {show: true, compositeRoot: true},
	commandShowLimits:                      {show: true, extension: true, compositeRoot: true},
	commandShowAdministrationOther:         rowShowAdmin,
	commandShowOther:                       rowShowRead,
	commandTerminateOther:                  {terminate: true},
	commandAlterDatabase:                   rowCatalog,
	commandAlterDatabaseLimit:              {extension: true, catalog: true, definition: true, administration: true, adminPermission: true, compositeRoot: true, change: changeAdministration},
	commandAlterDatabaseOption:             {extension: true, catalog: true, definition: true, administration: true, adminPermission: true, compositeRoot: true, change: changeAdministration},
	commandProcedurePrivilege:              {extension: true, catalog: true, definition: true, administration: true, privilegeShaped: true, adminPermission: true, compositeRoot: true, change: changeAdministration},
	commandUnsupportedPrivilege:            {extension: true, catalog: true, definition: true, administration: true, privilegeShaped: true, adminPermission: true, compositeRoot: true, change: changeAdministration},
	commandMalformedPrivilege:              rowPrivilege,
	commandCreateProcedure:                 {extension: true, definition: true, procedureDefinition: true, ownSyntax: true, adminPermission: true, change: changeAdministration},
	commandCreateKnowledgePolicy:           {extension: true, knowledgePolicy: true, ownSyntax: true, definition: true, adminPermission: true, change: changeAdministration},
	commandAlterKnowledgePolicy:            rowPolicyChange,
	commandDropKnowledgePolicy:             rowPolicyChange,
	commandRetentionPolicy:                 {ownSyntax: true},
	commandShowRetentionPolicy:             {ownSyntax: true, diagnosed: true},
	commandDropRetentionPolicy:             {ownSyntax: true, diagnosed: true},
	commandSchema:                          rowSchemaDDL,
	commandCreateSchemaOther:               {extension: true, schemaShape: true, change: changeSchema},
	commandCreateCompositeDatabase:         rowCatalogCreate,
	commandCreateOrReplaceDatabase:         {extension: true, schemaShape: true, ownSyntax: true, catalog: true, definition: true, administration: true, adminPermission: true, compositeRoot: true, change: changeAdministrationCreate},
	commandCreateOrReplaceOther:            {extension: true, schemaShape: true, ownSyntax: true, change: changeAdministration},
	commandCreateDatabase:                  rowCatalogCreate,
	commandCreateDatabaseOptions:           {extension: true, catalog: true, definition: true, administration: true, adminPermission: true, compositeRoot: true, change: changeAdministrationCreate},
	commandCreateAlias:                     rowCatalogCreate,
	commandDropCompositeDatabase:           rowCatalog,
	commandDropDatabase:                    rowCatalog,
	commandDropAlias:                       rowCatalog,
	commandDropProcedure:                   {extension: true, adminPermission: true, change: changeAdministration},
	commandDropRole:                        {extension: true, administration: true, change: changeAdministration},
	commandDropSchemaOther:                 {extension: true, schemaShape: true, change: changeSchema},
	commandDropAdministration:              {administration: true, diagnosed: true, change: changeAdministration},
	commandInvalidDrop:                     {diagnosed: true},
	commandAlterCompositeDatabase:          rowCatalog,
	commandAdministrationUnsupported:       {administration: true, change: changeAdministration},
	commandCreateAdministrationUnsupported: {administration: true, change: changeAdministrationCreate},
	commandBegin:                           {transactionControl: true, compositeRoot: true},
	commandCommit:                          {transactionControl: true, compositeRoot: true},
	commandRollback:                        {transactionControl: true, compositeRoot: true},
	commandBeginScript:                     {compositeRoot: true},
}

// schemaCommand is a schema command's form (executeSchemaCommand).
type schemaCommand uint8

const (
	schemaCommandNone schemaCommand = iota
	schemaCreateConstraint
	schemaDropConstraint
	schemaDropIndex
	schemaCreateFulltextIndex
	schemaCreateVectorIndex
	schemaCreateRangeIndex
	schemaCreateTypedPropertyIndex
	schemaCreateLookupIndex
	schemaCreateIndex
)

// statementWords is a statement body's leading reserved words (up to six).
type statementWords struct {
	words [6]string
	count int
	// end is the offset just after the last word read.
	end int
	// tail is where the text after the last word read starts, past gaps.
	tail int
}

func (w statementWords) at(i int) string {
	if i < w.count {
		return w.words[i]
	}
	return ""
}

// is reports whether the words from i on are exactly want.
func (w statementWords) is(i int, want ...string) bool {
	for k, word := range want {
		if w.at(i+k) != word {
			return false
		}
	}
	return true
}

func (w statementWords) oneOf(i int, want ...string) bool {
	for _, word := range want {
		if w.at(i) == word {
			return true
		}
	}
	return false
}

// statementTextStart is where a statement's text starts: past leading
// whitespace, comments and a byte order mark.
func statementTextStart(body string) int {
	i := queryGapEnd(body, 0)
	if strings.HasPrefix(body[i:], "\ufeff") {
		i = queryGapEnd(body, i+len("\ufeff"))
	}
	return i
}

// readLeadingWords reads up to six leading words of a statement body,
// upper-cased, skipping whitespace and comments. Reading stops at a quoted
// name or a word with a non-ASCII letter: these are reserved grammar words,
// and Unicode case folding must not turn a name into one.
func readLeadingWords(body string) statementWords {
	var w statementWords
	i := statementTextStart(body)
	for w.count < len(w.words) && i < len(body) {
		end := scanReservedWord(body, i)
		if end == i {
			break
		}
		w.words[w.count] = upperASCII(body[i:end])
		w.count++
		w.end = end
		i = queryGapEnd(body, end)
	}
	w.tail = i
	return w
}

// statementStartWord returns the word text starts with, and whether it opens
// a statement: a start of the shared grammar's script (antlr.IsStatementStart),
// a command the owner recognizes (readStatementCommand: START DATABASE, RENAME
// …, which the shared grammar does not have), or another statement NornicDB
// reads outside the grammar: LOAD CSV and a conditional query (WHEN … THEN …).
func statementStartWord(text string) (string, bool) {
	w := readLeadingWords(text)
	start := w.at(0)
	return start, antlr.IsStatementStart(start) || start == "LOAD" || start == "WHEN" || readStatementCommand(text) != commandNone
}

// scanReservedWord returns the end of the ASCII word at i, i when there is
// none. Cypher whitespace, including non-ASCII whitespace, ends a word; any
// other non-ASCII byte makes it a name.
func scanReservedWord(s string, i int) int {
	end := i
	for end < len(s) {
		c := s[end]
		if c >= 0x80 {
			if end > i && cyphertext.IsWhitespaceAt(s, end) {
				return end
			}
			return i
		}
		if c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || end > i && c >= '0' && c <= '9' {
			end++
			continue
		}
		break
	}
	return end
}

// statementOpening is how a statement body opens: with a word NornicDB
// accepts at a statement's start, or not.
type statementOpening uint8

const (
	openingNone statementOpening = iota
	openingClause
)

// statementOpeningWords are the words NornicDB accepts at the start of a
// statement body ("query must start with a valid clause" otherwise).
// Statements whose start word the grammar accepts but cannot open on its own
// (SET, REMOVE, FINISH, UNION, …) are not among them.
var statementOpeningWords = map[string]bool{
	"MATCH": true, "CREATE": true, "MERGE": true, "DELETE": true, "DETACH": true, "CALL": true,
	"RETURN": true, "WITH": true, "UNWIND": true, "OPTIONAL": true, "DROP": true, "SHOW": true,
	"FOREACH": true, "LOAD": true, "EXPLAIN": true, "PROFILE": true, "ALTER": true, "USE": true,
	"BEGIN": true, "COMMIT": true, "ROLLBACK": true, "TERMINATE": true, "LET": true, "FILTER": true,
	"FOR": true,
}

// readStatementOpening reads how a statement body opens.
func readStatementOpening(w statementWords) statementOpening {
	if statementOpeningWords[w.at(0)] {
		return openingClause
	}
	return openingNone
}

// readStatementCommand is the one recognition of a command statement. It
// reads the body's leading words, testing the more specific forms first, and
// the few forms a command's facts depend on (OPTIONS, SET LIMIT, …).
func readStatementCommand(body string) statementCommand {
	w := readLeadingWords(body)
	switch w.at(0) {
	case "SHOW", "TERMINATE":
		return readShowCommand(body, w)
	case "ALTER":
		switch {
		case w.is(1, "DATABASE"):
			switch {
			case findKeywordIndex(body, "SET LIMIT") >= 0:
				return commandAlterDatabaseLimit
			case findKeywordIndex(body, "SET OPTION") >= 0 || findKeywordIndex(body, "REMOVE OPTION") >= 0:
				return commandAlterDatabaseOption
			}
			return commandAlterDatabase
		case w.is(1, "COMPOSITE", "DATABASE"):
			return commandAlterCompositeDatabase
		case w.is(1, "DECAY", "PROFILE"), w.is(1, "PROMOTION", "PROFILE"), w.is(1, "PROMOTION", "POLICY"):
			return commandAlterKnowledgePolicy
		case w.is(1, "RETENTION"):
			return commandRetentionPolicy
		case w.oneOf(1, "USER", "ROLE", "SERVER", "ALIAS"), w.is(1, "CURRENT", "USER"):
			return commandAdministrationUnsupported
		}
	case "GRANT", "REVOKE", "DENY":
		if _, ok, _ := parseProcedurePrivilegeCommand(body); ok {
			return commandProcedurePrivilege
		}
		if w.at(0) != "DENY" && w.oneOf(1, "ROLE", "ROLES") || findKeywordIndex(body, "ON") > 0 {
			return commandUnsupportedPrivilege
		}
		return commandMalformedPrivilege
	case "RENAME", "START", "STOP", "ENABLE", "DRYRUN", "DEALLOCATE", "REALLOCATE":
		return commandAdministrationUnsupported
	case "BEGIN", "COMMIT", "ROLLBACK":
		if (w.count == 1 || w.count == 2 && w.at(1) == "TRANSACTION") && w.tail == len(body) {
			switch w.at(0) {
			case "BEGIN":
				return commandBegin
			case "COMMIT":
				return commandCommit
			}
			return commandRollback
		}
		if w.at(0) == "BEGIN" {
			return commandBeginScript
		}
	case "CREATE":
		return readCreateCommand(body, w)
	case "DROP":
		return readDropCommand(w)
	}
	return commandNone
}

func readCreateCommand(body string, w statementWords) statementCommand {
	switch {
	case w.is(1, "PROCEDURE"), w.is(1, "OR", "REPLACE", "PROCEDURE"):
		return commandCreateProcedure
	case w.is(1, "DECAY", "PROFILE"), w.is(1, "PROMOTION", "PROFILE"), w.is(1, "PROMOTION", "POLICY"):
		return commandCreateKnowledgePolicy
	case w.is(1, "RETENTION"):
		return commandRetentionPolicy
	case w.is(1, "CONSTRAINT"), w.is(1, "INDEX"),
		w.oneOf(1, "RANGE", "FULLTEXT", "VECTOR", "TEXT", "POINT", "LOOKUP") && w.is(2, "INDEX"):
		return commandSchema
	case w.is(1, "COMPOSITE", "DATABASE"):
		return commandCreateCompositeDatabase
	case w.is(1, "OR", "REPLACE"):
		if findKeywordIndex(body, "DATABASE") >= 0 {
			return commandCreateOrReplaceDatabase
		}
		return commandCreateOrReplaceOther
	case w.is(1, "DATABASE"):
		if findKeywordIndex(body, "OPTIONS") >= 0 {
			return commandCreateDatabaseOptions
		}
		return commandCreateDatabase
	case w.is(1, "ALIAS"):
		return commandCreateAlias
	case w.oneOf(1, "USER", "ROLE", "SERVER"):
		return commandCreateAdministrationUnsupported
	case w.oneOf(1, "FULLTEXT", "VECTOR", "RANGE", "TEXT", "POINT", "LOOKUP", "BTREE"):
		return commandCreateSchemaOther
	}
	return commandNone
}

func readDropCommand(w statementWords) statementCommand {
	switch {
	case w.is(1, "COMPOSITE", "DATABASE"):
		return commandDropCompositeDatabase
	case w.is(1, "DATABASE"):
		return commandDropDatabase
	case w.is(1, "ALIAS"):
		return commandDropAlias
	case w.is(1, "CONSTRAINT"), w.is(1, "INDEX"):
		return commandSchema
	case w.is(1, "DECAY", "PROFILE"), w.is(1, "PROMOTION", "PROFILE"), w.is(1, "PROMOTION", "POLICY"):
		return commandDropKnowledgePolicy
	case w.is(1, "RETENTION"):
		return commandDropRetentionPolicy
	case w.is(1, "PROCEDURE"):
		return commandDropProcedure
	case w.is(1, "ROLE"):
		return commandDropRole
	case w.oneOf(1, "FULLTEXT", "VECTOR", "RANGE", "TEXT", "POINT", "LOOKUP", "BTREE"):
		return commandDropSchemaOther
	case w.oneOf(1, "USER", "SERVER"):
		return commandDropAdministration
	}
	return commandInvalidDrop
}

// readShowCommand reads a SHOW or TERMINATE command.
func readShowCommand(body string, w statementWords) statementCommand {
	if w.at(0) == "TERMINATE" {
		if w.oneOf(1, "TRANSACTIONS", "TRANSACTION") {
			return commandTerminateTransactions
		}
		return commandTerminateOther
	}
	if _, isConstraints, _ := showConstraintKindOf(body, false); isConstraints {
		return commandShowConstraints
	}
	switch {
	case w.oneOf(1, "FULLTEXT", "RANGE", "VECTOR", "LOOKUP") && w.oneOf(2, "INDEXES", "INDEX"), w.oneOf(1, "INDEXES", "INDEX"):
		return commandShowIndexes
	case w.is(1, "DECAY", "PROFILES"), w.is(1, "PROMOTION", "PROFILES"), w.is(1, "PROMOTION", "POLICIES"):
		return commandShowKnowledgePolicies
	case w.is(1, "RETENTION"):
		return commandShowRetentionPolicy
	case w.is(1, "PROCEDURES"):
		return commandShowProcedures
	case w.is(1, "FUNCTIONS"):
		return commandShowFunctions
	case w.is(1, "COMPOSITE", "DATABASES"):
		return commandShowCompositeDatabases
	case w.is(1, "CONSTITUENTS"):
		return commandShowConstituents
	case w.is(1, "DEFAULT", "DATABASE"), w.is(1, "HOME", "DATABASE"):
		return commandShowDefaultDatabase
	case w.is(1, "USERS"):
		return commandShowUsers
	case w.is(1, "CURRENT", "USER"):
		return commandShowCurrentUser
	case w.oneOf(1, "TRANSACTIONS", "TRANSACTION"):
		return commandShowTransactions
	case w.oneOf(1, "PRIVILEGES", "PRIVILEGE"),
		w.oneOf(1, "ROLES", "ROLE") && (findKeywordIndex(showCommandHead(body), "PRIVILEGES") > 0 || findKeywordIndex(showCommandHead(body), "PRIVILEGE") > 0):
		return commandShowPrivileges
	case w.oneOf(1, "SERVERS", "SERVER"):
		return commandShowServers
	case w.is(1, "USER", "DEFINED"):
		return commandShowUserDefined
	case w.oneOf(1, "ROLES", "ROLE", "USER"):
		return commandShowUnsupportedAdministration
	case w.is(1, "DATABASES"):
		return commandShowDatabases
	case w.is(1, "DATABASE"):
		return commandShowDatabase
	case w.is(1, "ALIASES"):
		return commandShowAliases
	case w.oneOf(1, "SETTINGS", "SETTING"):
		return commandShowSettings
	case w.is(1, "LIMITS"):
		return commandShowLimits
	case w.oneOf(1, "SUPPORTED", "POPULATED", "ALIAS"),
		w.is(1, "ALL") && w.oneOf(2, "ROLE", "ROLES", "PRIVILEGE", "PRIVILEGES"):
		return commandShowAdministrationOther
	}
	return commandShowOther
}

// readSchemaCommand reads a schema command's form.
func readSchemaCommand(w statementWords) schemaCommand {
	switch {
	case w.is(0, "CREATE", "CONSTRAINT"):
		return schemaCreateConstraint
	case w.is(0, "DROP", "CONSTRAINT"):
		return schemaDropConstraint
	case w.is(0, "DROP", "INDEX"):
		return schemaDropIndex
	case w.is(0, "CREATE", "FULLTEXT", "INDEX"):
		return schemaCreateFulltextIndex
	case w.is(0, "CREATE", "VECTOR", "INDEX"):
		return schemaCreateVectorIndex
	case w.is(0, "CREATE", "RANGE", "INDEX"):
		return schemaCreateRangeIndex
	case w.is(0, "CREATE", "TEXT", "INDEX"), w.is(0, "CREATE", "POINT", "INDEX"):
		return schemaCreateTypedPropertyIndex
	case w.is(0, "CREATE", "LOOKUP", "INDEX"):
		return schemaCreateLookupIndex
	case w.is(0, "CREATE", "INDEX"):
		return schemaCreateIndex
	}
	return schemaCommandNone
}

// isConstraintContract reports CREATE CONSTRAINT … REQUIRE { … }, a
// constraint contract with one predicate per line.
func isConstraintContract(body string) bool {
	require := keywordIndex(body, "REQUIRE")
	if require < 0 {
		return false
	}
	open := queryGapEnd(body, require+len("REQUIRE"))
	return open < len(body) && body[open] == '{'
}
