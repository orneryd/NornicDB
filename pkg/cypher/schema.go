// Schema command parsing and execution for Cypher.
//
// This file implements Neo4j schema management commands:
//   - CREATE CONSTRAINT
//   - CREATE NO OVERLAP CONSTRAINT (NornicDB's temporal no-overlap head)
//   - CREATE INDEX
//   - CREATE RANGE INDEX
//   - CREATE FULLTEXT INDEX
//   - CREATE VECTOR INDEX
package cypher

import (
	"context"
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strconv"
	"strings"

	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// isCompositeRoot returns true if the storage engine is a composite database root.
// Schema DDL and introspection commands must be rejected on composite roots —
// callers must target a specific constituent via USE <composite>.<alias>.
func isCompositeRoot(engine storage.Engine) bool {
	for engine != nil {
		if _, native := engine.(*transactionStorageWrapper); native {
			return false
		}
		if root, ok := engine.(interface{ IsComposite() bool }); ok {
			return root.IsComposite()
		}
		inner, ok := engine.(storage.EngineUnwrapper)
		if !ok {
			return false
		}
		next := inner.GetInnerEngine()
		if next == engine {
			return false
		}
		engine = next
	}
	return false
}

// isCompositeAllowedCommand returns true for system/admin commands that are
// valid at composite root level without requiring a constituent target.
func isCompositeAllowedCommand(cypher string) bool {
	upper := upperASCII(strings.TrimSpace(cypher))
	prefixes := []string{
		"SHOW DATABASE", "SHOW COMPOSITE", "SHOW CONSTITUENTS",
		"SHOW ALIASES", "SHOW LIMITS", "SHOW PROCEDURES", "SHOW FUNCTIONS", "SHOW SETTING",
		// Schema introspection/DDL commands pass through to their own handlers
		// which return more specific composite-root error messages.
		"SHOW INDEX", "SHOW FULLTEXT INDEX", "SHOW RANGE INDEX", "SHOW VECTOR INDEX", "SHOW LOOKUP INDEX",
		"SHOW CONSTRAINT",
		"CREATE INDEX", "CREATE RANGE INDEX", "CREATE FULLTEXT INDEX", "CREATE VECTOR INDEX", "CREATE LOOKUP INDEX", "CREATE TEXT INDEX", "CREATE POINT INDEX",
		"CREATE CONSTRAINT", "CREATE NO OVERLAP CONSTRAINT",
		"DROP INDEX", "DROP CONSTRAINT",
		"CREATE DATABASE", "DROP DATABASE",
		"CREATE COMPOSITE", "DROP COMPOSITE", "ALTER COMPOSITE",
		"CREATE ALIAS", "DROP ALIAS",
		"BEGIN", "COMMIT", "ROLLBACK",
	}
	for _, p := range prefixes {
		if strings.HasPrefix(upper, p) {
			return true
		}
	}
	return false
}

// schemaDDLTargetError validates the graph owning a schema mutation before
// transaction capability or mutation admission. Composite roots coordinate
// constituents and deliberately have no local schema transaction owner.
func schemaDDLTargetError(engine storage.Engine) error {
	if isCompositeRoot(engine) {
		return localizedError(localization.CypherSchemaCompositeDDLNotAllowed(), nil)
	}
	return nil
}

// executeSchemaCommand handles CREATE CONSTRAINT and CREATE INDEX commands.
func (e *StorageExecutor) executeSchemaCommand(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if err := schemaDDLTargetError(e.storage); err != nil {
		return nil, err
	}
	if err := e.prepareSchemaTransaction(ctx); err != nil {
		return nil, err
	}
	if schema := e.storage.GetSchema(); schema != nil {
		release := schema.LockSchemaMutation()
		defer release()
	}
	resumeWrites, err := pauseAsyncWritesForSchemaDDL(e.storage)
	defer resumeWrites()
	if err != nil {
		return nil, err
	}

	upper := upperASCII(cypher)

	// Order matters: check more specific patterns first
	var run func(context.Context, string) (*ExecuteResult, error)
	statement := strings.TrimSpace(cypher)
	if isCreateConstraintStatement(ctx, statement) || strings.Contains(upper, "CREATE CONSTRAINT") {
		run = e.executeCreateConstraint
	} else if strings.Contains(upper, "DROP CONSTRAINT") {
		run = e.executeDropConstraint
	} else if startsWithKeywords(ctx, cypher, "DROP", "INDEX") {
		run = e.executeDropIndex
	} else if strings.Contains(upper, "CREATE FULLTEXT INDEX") {
		run = e.executeCreateFulltextIndex
	} else if strings.Contains(upper, "CREATE VECTOR INDEX") {
		run = e.executeCreateIndex
	} else if strings.Contains(upper, "CREATE RANGE INDEX") {
		run = e.executeCreateIndex
	} else if startsWithKeywords(ctx, cypher, "CREATE", "TEXT INDEX") || startsWithKeywords(ctx, cypher, "CREATE", "POINT INDEX") {
		run = e.executeCreateTypedPropertyIndex
	} else if startsWithKeywords(ctx, cypher, "CREATE", "LOOKUP INDEX") {
		run = e.executeCreateLookupIndex
	} else if strings.Contains(upper, "CREATE INDEX") {
		run = e.executeCreateIndex
	} else {
		return nil, localizedError(localization.CypherSchemaUnknownCommand(cypher), nil)
	}
	result, err := e.countSchemaChanges(ctx, cypher, run)
	if err == nil {
		if wrapper, ok := e.storage.(*transactionStorageWrapper); ok {
			err = wrapper.tx.StageSchemaChanges()
		}
	}
	if err != nil && isCreateConstraintStatement(ctx, statement) {
		var violation *storage.ConstraintViolationError
		if errors.As(err, &violation) {
			return nil, &classifiedCypherError{cause: err, code: "Neo.DatabaseError.Schema.ConstraintCreationFailed", detail: "ConstraintCreationFailed"}
		}
	}

	// Invalidate query cache — cached SHOW INDEXES/CONSTRAINTS results are now stale.
	if err == nil && e.cache != nil {
		e.cache.Invalidate()
	}

	return result, err
}

// countSchemaChanges runs a schema command and reports the indexes and
// constraints it created or dropped in the result's counters, as Neo4j's
// summary does (#507): CREATE INDEX -> indexes_added 1, DROP CONSTRAINT ->
// constraints_removed 1, and nothing for an IF [NOT] EXISTS that did nothing.
// The counts are the difference in the schema's objects across the command
// (SchemaObjectCounts), so every CREATE / DROP form is counted the same way.
func (e *StorageExecutor) countSchemaChanges(ctx context.Context, cypher string, run func(context.Context, string) (*ExecuteResult, error)) (*ExecuteResult, error) {
	schema := e.storage.GetSchema()
	if schema == nil {
		return run(ctx, cypher)
	}
	indexesBefore, constraintsBefore := schema.SchemaObjectCounts()
	result, err := run(ctx, cypher)
	if err != nil || result == nil {
		return result, err
	}
	indexesAfter, constraintsAfter := schema.SchemaObjectCounts()
	if indexesAfter == indexesBefore && constraintsAfter == constraintsBefore {
		return result, nil
	}
	if result.Stats == nil {
		result.Stats = &QueryStats{}
	}
	result.Stats.IndexesAdded += max(indexesAfter-indexesBefore, 0)
	result.Stats.IndexesRemoved += max(indexesBefore-indexesAfter, 0)
	result.Stats.ConstraintsAdded += max(constraintsAfter-constraintsBefore, 0)
	result.Stats.ConstraintsRemoved += max(constraintsBefore-constraintsAfter, 0)
	return result, nil
}

// pauseAsyncWritesForSchemaDDL previously flushed and held every async write
// cache in the engine stack while a schema command validated stored data
// (#700). Writes now commit synchronously through their transactions, so
// there is no cached write to hold: a schema command observes every
// acknowledged write without a pause. The returned function is always safe to
// call.
func pauseAsyncWritesForSchemaDDL(_ storage.Engine) (func(), error) {
	return func() {}, nil
}

// constraintDeclarationHead is the CREATE head a statement declares a
// constraint with, and the REQUIRE family that head declares. A head that
// declares no family admits every requirement, as Neo4j's CREATE CONSTRAINT
// does.
type constraintDeclarationHead struct {
	text   string
	family constraintRequirementKind
}

// constraintDeclarationHeadTemporal is the head that declares the temporal
// no-overlap family.
const constraintDeclarationHeadTemporal = "CREATE NO OVERLAP CONSTRAINT"

// createConstraintHeadAt returns the CREATE head cypher declares a constraint
// with, or the zero head when it declares none. CREATE CONSTRAINT is Neo4j's
// head and declares no family: it admits every requirement, including the
// deprecated tuple spelling of a temporal no-overlap constraint. CREATE NO
// OVERLAP CONSTRAINT declares the temporal no-overlap family, whose one
// requirement names the validity window in a temporal.interval( … ) call and
// the grouping key after GROUP BY.
func createConstraintHeadAt(ctx context.Context, cypher string) constraintDeclarationHead {
	switch {
	case startsWithKeywords(ctx, cypher, "CREATE", "NO OVERLAP CONSTRAINT"):
		return constraintDeclarationHead{text: constraintDeclarationHeadTemporal, family: constraintRequirementTemporal}
	case startsWithKeywords(ctx, cypher, "CREATE", "CONSTRAINT"):
		return constraintDeclarationHead{text: "CREATE CONSTRAINT"}
	}
	return constraintDeclarationHead{}
}

// isCreateConstraintStatement reports whether cypher declares a constraint, in
// either head createConstraintHeadAt reads.
func isCreateConstraintStatement(ctx context.Context, cypher string) bool {
	return createConstraintHeadAt(ctx, cypher).text != ""
}

// executeCreateConstraint handles CREATE CONSTRAINT and CREATE NO OVERLAP
// CONSTRAINT commands.
//
// Supported syntax (Neo4j 5.x):
//
//	CREATE CONSTRAINT constraint_name IF NOT EXISTS FOR (n:Label) REQUIRE n.property IS UNIQUE
//
// Supported syntax (Neo4j 4.x):
//
//	CREATE CONSTRAINT FOR (n:Label) REQUIRE (n.first, n.last) IS UNIQUE
//
// Supported syntax (NornicDB temporal no-overlap, interval spelling):
//
//	CREATE NO OVERLAP CONSTRAINT name FOR (n:Account)
//	REQUIRE temporal.interval(n.starts_at, n.ends_at) GROUP BY n.accountId
//
// The tuple spelling of that requirement — CREATE CONSTRAINT name FOR
// (n:Account) REQUIRE (n.accountId, n.starts_at, n.ends_at) IS TEMPORAL NO
// OVERLAP — was shipped first, is deprecated, and stays accepted; both compile
// to the same constraint definition.
func (e *StorageExecutor) executeCreateConstraint(ctx context.Context, cypher string) (*ExecuteResult, error) {
	onPosition := keywordIndexFrom(ctx, cypher, "ON", 0, defaultKeywordScanOpts())
	forPosition := keywordIndexFrom(ctx, cypher, "FOR", 0, defaultKeywordScanOpts())
	if onPosition >= 0 && (forPosition < 0 || onPosition < forPosition) {
		return nil, localizedError(localization.CypherSchemaInvalidSyntax("CREATE CONSTRAINT"), nil)
	}
	// Every spelling of a uniqueness or key requirement becomes the one the
	// shapes below read (canonicalConstraintRequirement).
	cypher, err := e.canonicalConstraintRequirement(ctx, cypher)
	if err != nil {
		return nil, err
	}
	// Detect IF NOT EXISTS to pass through to AddConstraint for duplicate-schema handling.
	ifNotExists := strings.Contains(upperASCII(cypher), "IF NOT EXISTS")
	// A head that declares a requirement family admits only that family's one
	// requirement. Read it here, before any shape parses, binds or registers
	// anything: a contract block, MAX COUNT, ALLOWED, IS UNIQUE and every other
	// shape are refused for a head that declared the temporal no-overlap family.
	if head := createConstraintHeadAt(ctx, cypher); head.family == constraintRequirementTemporal {
		declaration, recognized, err := e.parseCreateConstraintPropertyDDL(ctx, cypher)
		if err != nil {
			return nil, err
		}
		if !recognized || declaration.requirement.kind != head.family {
			return nil, constraintTemporalRequirementError()
		}
		return e.executeCreateConstraintProperty(ctx, declaration, ifNotExists)
	}
	// Select the contract shape before execution; stored-data validation does
	// not participate in the syntax decision.
	if contract, err := e.parseCreateConstraintContractDDL(ctx, cypher); err == nil {
		return e.executeCreateConstraintContract(ctx, cypher, contract, ifNotExists)
	}

	// One owner reads every property declaration this prefix can spell; the
	// requirement it returns decides the constraint the statement creates.
	if declaration, recognized, err := e.parseCreateConstraintPropertyDDL(ctx, cypher); recognized {
		if err != nil {
			return nil, err
		}
		return e.executeCreateConstraintProperty(ctx, declaration, ifNotExists)
	}

	if parsed, err := e.parseCreateConstraintCardinalityDDL(ctx, cypher); err == nil {
		constraintName := parsed.name
		if constraintName == "" {
			directionSuffix := "outgoing"
			if parsed.direction == "INCOMING" {
				directionSuffix = "incoming"
			}
			constraintName = fmt.Sprintf("constraint_%s_max_%s_%d", parsed.relType, directionSuffix, parsed.maxCount)
		}
		if _, err := e.schemaOptionMap(ctx, parsed.options, "cardinality constraint"); err != nil {
			return nil, err
		}
		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintCardinality,
			EntityType: storage.ConstraintEntityRelationship,
			Label:      parsed.relType,
			MaxCount:   parsed.maxCount,
			Direction:  parsed.direction,
		}
		if err := e.addSchemaConstraint(ctx, constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	} else if isCreateConstraintStatement(ctx, cypher) &&
		strings.Contains(upperASCII(cypher), "REQUIRE MAX COUNT") {
		return nil, err
	}

	if parsed, err := e.parseCreateConstraintPolicyDDL(ctx, cypher); err == nil {
		constraintName := parsed.name
		if constraintName == "" {
			constraintName = fmt.Sprintf("constraint_%s_%s_%s_%s", parsed.sourceLabel, parsed.relType, parsed.targetLabel, lowerASCII(parsed.policyMode))
		}
		if _, err := e.schemaOptionMap(ctx, parsed.options, "policy constraint"); err != nil {
			return nil, err
		}
		constraint := storage.Constraint{
			Name:        constraintName,
			Type:        storage.ConstraintPolicy,
			EntityType:  storage.ConstraintEntityRelationship,
			Label:       parsed.relType,
			SourceLabel: parsed.sourceLabel,
			TargetLabel: parsed.targetLabel,
			PolicyMode:  parsed.policyMode,
		}
		if err := e.addSchemaConstraint(ctx, constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	} else if isCreateConstraintStatement(ctx, cypher) &&
		(strings.Contains(upperASCII(cypher), "REQUIRE ALLOWED") || strings.Contains(upperASCII(cypher), "REQUIRE DISALLOWED")) {
		return nil, err
	}

	return nil, localizedError(localization.CypherSchemaInvalidSyntax("CREATE CONSTRAINT"), nil)
}

// executeDropIndex handles DROP INDEX commands.
//
// Supported syntax:
//
//	DROP INDEX index_name
//	DROP INDEX index_name IF EXISTS
func (e *StorageExecutor) executeDropIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if err := schemaDDLTargetError(e.storage); err != nil {
		return nil, err
	}

	rest := strings.TrimSpace(strings.TrimSpace(cypher)[len("DROP INDEX"):])
	if rest == "" {
		return nil, localizedError(localization.CypherSchemaNameRequired("DROP INDEX", "index"), nil)
	}
	name, ifExists, ok := parseDropSchemaName(rest)
	if !ok {
		return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherSchemaInvalidSyntax("DROP INDEX"))
	}

	if err := e.dropIndexByName(ctx, name, ifExists); err != nil {
		return nil, err
	}
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

// parseDropSchemaName reads the rest of DROP INDEX / DROP CONSTRAINT: one
// name, plain or backtick-quoted, with IF EXISTS after it (Neo4j's form) or
// before it. Anything else is no such statement: Neo4j's SyntaxError for
// DROP INDEX name -> text (#528), where the text was read as part of the
// name.
func parseDropSchemaName(rest string) (name string, ifExists, ok bool) {
	at := 0
	if matchKeywordAt(rest, at, "IF") {
		if next := skipASCIISpaces(rest, at+len("IF"), len(rest)); matchKeywordAt(rest, next, "EXISTS") {
			ifExists, at = true, skipASCIISpaces(rest, next+len("EXISTS"), len(rest))
		}
	}
	written, end, scanned := scanSymbolicName(rest, at)
	if !scanned {
		return "", false, false
	}
	at = skipASCIISpaces(rest, end, len(rest))
	if !ifExists && matchKeywordAt(rest, at, "IF") {
		if next := skipASCIISpaces(rest, at+len("IF"), len(rest)); matchKeywordAt(rest, next, "EXISTS") {
			ifExists, at = true, skipASCIISpaces(rest, next+len("EXISTS"), len(rest))
		}
	}
	if at != len(rest) {
		return "", false, false
	}
	return symbolicNameValue(written), ifExists, true
}

// dropIndexByName removes the named index from the schema and tears down the
// in-memory data that belongs to it. It is the single drop path for DROP INDEX
// and the db.index.vector.drop / db.index.fulltext.drop procedures. A missing
// index is a Neo.DatabaseError.Schema.IndexDropFailed error unless ifExists is set.
func (e *StorageExecutor) dropIndexByName(ctx context.Context, name string, ifExists bool) error {
	// Look up the schema entry BEFORE dropping it so we can also tear down
	// what the index alone owns: its registered vector space. The search
	// service's vectors are not the index's: every vector-shaped property of
	// a node or relationship has one entry, kept equal to storage by every
	// installer whatever indexes exist, and read by every vector index on the
	// property, by hybrid search and by the Qdrant API. They stay, so another
	// vector index on the same property keeps serving its nodes and an index
	// created again serves the stored nodes at once, as Neo4j fills a new
	// index from existing data (NOR-404). Nothing is retired, so no work
	// admitted before the drop can bring back a retired entry (NOR-386).
	var droppedVectorIndex *storage.VectorIndex
	var population *search.VectorIndexPopulation
	if schema := e.storage.GetSchema(); schema != nil {
		if vi, ok := readVectorIndexDefinition(ctx, schema, name); ok && vi != nil {
			copyVI := *vi
			droppedVectorIndex = &copyVI
			// The index's population record (FAILED, or one still running)
			// ends with the index, once its drop commits.
			population = readVectorIndexPopulation(ctx, e.search(), name)
		}
	}

	if err := e.storage.GetSchema().DropIndex(name); err != nil {
		var missing *localization.LocalizedError
		if errors.As(err, &missing) && missing.Message.ID == localization.MessageStorageSchemaIndexNotFound {
			if ifExists {
				return nil
			}
			return newSemanticError("Neo.DatabaseError.Schema.IndexDropFailed", "MissingIndex", err.Error())
		}
		return err
	}

	// Unregister the index's vector space after the schema entry is gone so
	// concurrent reads can't see a half-dropped index.
	if droppedVectorIndex != nil {
		e.withdrawVectorIndexPublication(ctx, name)
		if err := e.afterCommit(context.Background(), func(context.Context) error {
			e.unregisterVectorSpace(name)
			population.Retire()
			return nil
		}); err != nil {
			return err
		}
	}

	// Invalidate query cache — cached SHOW INDEXES results are now stale.
	if e.cache != nil {
		e.cache.Invalidate()
	}
	return nil
}

// executeDropConstraint handles DROP CONSTRAINT commands.
func (e *StorageExecutor) executeDropConstraint(ctx context.Context, cypher string) (*ExecuteResult, error) {
	trimmed := strings.TrimSpace(cypher)
	upper := upperASCII(trimmed)
	if !strings.HasPrefix(upper, "DROP CONSTRAINT") {
		return nil, localizedError(localization.CypherSchemaInvalidSyntax("DROP CONSTRAINT"), nil)
	}

	rest := strings.TrimSpace(trimmed[len("DROP CONSTRAINT"):])
	if rest == "" {
		return nil, localizedError(localization.CypherSchemaInvalidSyntax("DROP CONSTRAINT"), nil)
	}

	name, ifExists, ok := parseDropSchemaName(rest)
	if !ok {
		return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherSchemaInvalidSyntax("DROP CONSTRAINT"))
	}

	if err := e.storage.GetSchema().DropConstraint(name); err != nil {
		var missing *localization.LocalizedError
		if errors.As(err, &missing) && missing.Message.ID == localization.MessageStorageSchemaConstraintNotFound {
			if ifExists {
				return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
			}
			return nil, newSemanticError("Neo.DatabaseError.Schema.ConstraintDropFailed", "MissingConstraint", err.Error())
		}
		return nil, err
	}

	if e.cache != nil {
		e.cache.Invalidate()
	}

	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

type parsedCreateIndexDDL struct {
	indexName        string
	label            string
	relationshipType string
	properties       []string
	isRelationship   bool
	options          schemaOptions
}

func keywordSpanAt(s string, pos int, keyword string) (int, bool) {
	ks, ke := trimKeywordWSBounds(keyword)
	return keywordMatchAt(s, pos, keyword, ks, ke)
}

func parseOptionalDDLName(ctx context.Context, segment string) (string, error) {
	segment = strings.TrimSpace(segment)
	if segment == "" {
		return "", nil
	}

	ifPos := keywordIndexFrom(ctx, segment, "IF NOT EXISTS", 0, defaultKeywordScanOpts())
	if ifPos < 0 {
		name, rest, ok := cutSchemaName(segment)
		if !ok || rest != "" {
			return "", localizedError(localization.CypherSchemaInvalidIdentifierSegment(), nil)
		}
		return name, nil
	}

	ifEnd, ok := keywordSpanAt(segment, ifPos, "IF NOT EXISTS")
	if !ok || strings.TrimSpace(segment[ifEnd:]) != "" {
		return "", localizedError(localization.CypherSchemaInvalidIfNotExistsClause(), nil)
	}

	namePart := strings.TrimSpace(segment[:ifPos])
	if namePart == "" {
		return "", nil
	}
	name, rest, ok := cutSchemaName(namePart)
	if !ok || rest != "" {
		return "", localizedError(localization.CypherSchemaInvalidIdentifierSegment(), nil)
	}
	return name, nil
}

// propertySyntax is the property-list syntax an index ON clause family admits.
// The two families differ only there, so one reader owns the shape, the
// delimiter reading and the OPTIONS split, and each caller names its syntax.
type propertySyntax int

const (
	// propertySyntaxOrdinary is the range and vector index form: a parenthesized
	// or bare property expression, with no EACH prefix and no bracketed list.
	propertySyntaxOrdinary propertySyntax = iota
	// propertySyntaxFulltext is the fulltext index form: an optional EACH prefix
	// and a bracketed or parenthesized property list.
	propertySyntaxFulltext
)

// propertiesSegment reads an index ON clause's property expression and the
// OPTIONS tail that follows it: the one owner of that reading, shared by the
// range/vector index forms and the fulltext form. The families differ only in
// what they admit before the property list - fulltext also reads an EACH prefix
// and a bracketed list, and names the bracketed form "ON EACH" in its error -
// so the caller states its syntax while the shape, the delimiter reading and
// the OPTIONS split stay here.
func propertiesSegment(ctx context.Context, onClause string, syntax propertySyntax) (props string, tail string, err error) {
	onClause = strings.TrimSpace(onClause)
	if onClause == "" {
		return "", "", localizedError(localization.CypherSchemaEmptyClause("ON"), nil)
	}

	if syntax == propertySyntaxFulltext && startsWithKeywordFold(onClause, "EACH") {
		eachEnd, ok := keywordSpanAt(onClause, 0, "EACH")
		if !ok {
			return "", "", localizedError(localization.CypherSchemaInvalidClause("EACH"), nil)
		}
		onClause = strings.TrimSpace(onClause[eachEnd:])
		if onClause == "" {
			return "", "", localizedError(localization.CypherSchemaEmptyClause("ON"), nil)
		}
	}

	if syntax == propertySyntaxFulltext && strings.HasPrefix(onClause, "[") {
		inside, rest, ok := extractBracketSection(onClause)
		if !ok {
			return "", "", localizedError(localization.CypherSchemaInvalidClause("ON EACH"), nil)
		}
		return strings.TrimSpace(inside), strings.TrimSpace(rest), nil
	}

	if strings.HasPrefix(onClause, "(") {
		inside, rest, ok := extractParenSection(onClause)
		if !ok {
			return "", "", localizedError(localization.CypherSchemaInvalidClause("ON"), nil)
		}
		return strings.TrimSpace(inside), strings.TrimSpace(rest), nil
	}

	// Without a delimiter the clause is the property expression up to an
	// OPTIONS tail, which the shared DDL tail owner splits out.
	props, tail = splitDDLOptionsTail(ctx, onClause)
	return props, tail, nil
}

// schemaPatternTarget retains entity identity and borrowed source spans.
// Variable decoding is paid only by consumers that need its identity.
// Fulltext lists need source provenance before interpreting pipe separators.
type schemaPatternTarget struct {
	isRelationship bool
	variableSource string
	name           string
	// nameSource retains quote and separator provenance for fulltext type lists.
	nameSource string
	// direction reads the connector immediately beside the relationship bracket:
	// OUTGOING for "->", INCOMING for "<-", empty when the pattern is
	// undirected. Only a caller whose contract is directional requires one.
	direction string
}

// parseForPatternTarget reads the entity a FOR pattern names and its
// <variable>:<labelOrType> interior: a node pattern's (var:Label), or a
// relationship pattern's (a)-[var:TYPE]-(b), whose label or type is the
// bracketed section's. A relationship pattern also carries the direction its
// connectors spell: a leading "->" suffix is OUTGOING and takes precedence,
// otherwise a trailing "<-" prefix is INCOMING. Arrows inside quoted endpoint
// names are data, and an undirected pattern has no direction. It is the one
// owner of that reading: the index pattern parser, the constraint contract's
// FOR clause, the relationship-key DDL and the directional cardinality DDL all
// take the label, type, variable and direction from it, and each keeps its own
// statement-specific admission and error around it.
func parseForPatternTarget(forClause string) (target schemaPatternTarget, ok bool) {
	forClause = strings.TrimSpace(forClause)
	if forClause == "" {
		return schemaPatternTarget{}, false
	}

	if strings.Contains(forClause, "[") {
		bracketStart := strings.Index(forClause, "[")
		inside, rest, bracketed := extractBracketSection(forClause[bracketStart:])
		if !bracketed {
			return schemaPatternTarget{}, false
		}
		prefix := strings.TrimSpace(forClause[:bracketStart])
		suffix := strings.TrimSpace(rest)
		if !strings.Contains(prefix, "(") || !strings.Contains(suffix, "(") {
			return schemaPatternTarget{}, false
		}
		colon := strings.Index(inside, ":")
		if colon < 0 {
			return schemaPatternTarget{}, false
		}
		target.variableSource = strings.TrimSpace(inside[:colon])
		target.nameSource = strings.TrimSpace(inside[colon+1:])
		target.name = cyphertext.SymbolicNameValue(target.nameSource)
		if target.name == "" {
			return schemaPatternTarget{}, false
		}
		target.isRelationship = true
		switch {
		case strings.HasPrefix(suffix, "->"):
			target.direction = "OUTGOING"
		case strings.HasSuffix(prefix, "<-"):
			target.direction = "INCOMING"
		}
		return target, true
	}

	inside, rest, matched := extractParenSection(forClause)
	if !matched || strings.TrimSpace(rest) != "" {
		return schemaPatternTarget{}, false
	}
	colon := strings.Index(inside, ":")
	if colon < 0 {
		return schemaPatternTarget{}, false
	}
	target.variableSource = strings.TrimSpace(inside[:colon])
	target.nameSource = strings.TrimSpace(inside[colon+1:])
	target.name = cyphertext.SymbolicNameValue(target.nameSource)
	if target.name == "" {
		return schemaPatternTarget{}, false
	}
	return target, true
}

func parseCreateIndexForPattern(forClause string) (label string, relationshipType string, isRelationship bool, err error) {
	forClause = strings.TrimSpace(forClause)
	if forClause == "" {
		return "", "", false, localizedError(localization.CypherSchemaEmptyClause("FOR"), nil)
	}

	target, ok := parseForPatternTarget(forClause)
	if !ok {
		clause := "node FOR"
		if strings.Contains(forClause, "[") {
			clause = "relationship FOR"
		}
		return "", "", false, localizedError(localization.CypherSchemaInvalidClause(clause), nil)
	}
	if target.isRelationship {
		return "", target.name, true, nil
	}
	return target.name, "", false, nil
}

func (e *StorageExecutor) parseCreateIndexDDL(ctx context.Context, cypher string, prefixKeyword string) (parsedCreateIndexDDL, error) {
	q := strings.TrimSpace(cypher)
	if q == "" {
		return parsedCreateIndexDDL{}, localizedError(localization.CypherSchemaEmptyStatement(), nil)
	}

	prefixPos := keywordIndexFrom(ctx, q, prefixKeyword, 0, defaultKeywordScanOpts())
	if prefixPos != 0 {
		return parsedCreateIndexDDL{}, localizedError(localization.CypherSchemaInvalidKeyword("prefix"), nil)
	}
	prefixEnd, ok := keywordSpanAt(q, 0, prefixKeyword)
	if !ok {
		return parsedCreateIndexDDL{}, localizedError(localization.CypherSchemaInvalidKeyword("prefix"), nil)
	}

	forPos := keywordIndexFrom(ctx, q, "FOR", prefixEnd, defaultKeywordScanOpts())
	if forPos < 0 {
		return parsedCreateIndexDDL{}, localizedError(localization.CypherSchemaMissingKeyword("FOR"), nil)
	}
	forEnd, ok := keywordSpanAt(q, forPos, "FOR")
	if !ok {
		return parsedCreateIndexDDL{}, localizedError(localization.CypherSchemaInvalidKeyword("FOR"), nil)
	}

	onPos := keywordIndexFrom(ctx, q, "ON", forEnd, defaultKeywordScanOpts())
	if onPos < 0 {
		return parsedCreateIndexDDL{}, localizedError(localization.CypherSchemaMissingKeyword("ON"), nil)
	}
	onEnd, ok := keywordSpanAt(q, onPos, "ON")
	if !ok {
		return parsedCreateIndexDDL{}, localizedError(localization.CypherSchemaInvalidKeyword("ON"), nil)
	}

	name, err := parseOptionalDDLName(ctx, q[prefixEnd:forPos])
	if err != nil {
		return parsedCreateIndexDDL{}, err
	}

	label, relType, isRelationship, err := parseCreateIndexForPattern(q[forEnd:onPos])
	if err != nil {
		return parsedCreateIndexDDL{}, err
	}

	propsSegment, tail, err := propertiesSegment(ctx, q[onEnd:], propertySyntaxOrdinary)
	if err != nil {
		return parsedCreateIndexDDL{}, err
	}
	options, err := schemaOptionsTail(tail)
	if err != nil {
		return parsedCreateIndexDDL{}, err
	}
	// ON EACH [...] is fulltext-index syntax; for CREATE [RANGE|VECTOR] INDEX
	// every item must be a plain variable.property reference, otherwise the
	// leftover text (e.g. "p]") would be stored as the property name.
	if startsWithKeywordFold(propsSegment, "EACH") || strings.HasPrefix(propsSegment, "[") ||
		!qualifiedIndexPropertiesWellFormed(propsSegment) {
		return parsedCreateIndexDDL{}, localizedError(localization.CypherSchemaInvalidClause("ON"), nil)
	}

	properties := indexPropertiesFromOnClause(propsSegment, false)
	if len(properties) == 0 {
		return parsedCreateIndexDDL{}, localizedError(localization.CypherSchemaIndexPropertiesRequired(), nil)
	}

	return parsedCreateIndexDDL{
		indexName:        name,
		label:            label,
		relationshipType: relType,
		properties:       properties,
		isRelationship:   isRelationship,
		options:          options,
	}, nil
}

func (e *StorageExecutor) parseCreateIndexLegacyDDL(ctx context.Context, cypher, prefix string) (parsedCreateIndexDDL, error) {
	q := strings.TrimSpace(cypher)
	if q == "" {
		return parsedCreateIndexDDL{}, localizedError(localization.CypherSchemaEmptyStatement(), nil)
	}

	prefixPos := keywordIndexFrom(ctx, q, prefix, 0, defaultKeywordScanOpts())
	if prefixPos != 0 {
		return parsedCreateIndexDDL{}, localizedError(localization.CypherSchemaInvalidKeyword("prefix"), nil)
	}
	prefixEnd, ok := keywordSpanAt(q, 0, prefix)
	if !ok {
		return parsedCreateIndexDDL{}, localizedError(localization.CypherSchemaInvalidKeyword("prefix"), nil)
	}

	onPos := keywordIndexFrom(ctx, q, "ON", prefixEnd, defaultKeywordScanOpts())
	if onPos < 0 {
		return parsedCreateIndexDDL{}, localizedError(localization.CypherSchemaMissingKeyword("ON"), nil)
	}
	onEnd, ok := keywordSpanAt(q, onPos, "ON")
	if !ok {
		return parsedCreateIndexDDL{}, localizedError(localization.CypherSchemaInvalidKeyword("ON"), nil)
	}

	name, err := parseOptionalDDLName(ctx, q[prefixEnd:onPos])
	if err != nil {
		return parsedCreateIndexDDL{}, err
	}

	onClause := strings.TrimSpace(q[onEnd:])
	if !strings.HasPrefix(onClause, ":") {
		return parsedCreateIndexDDL{}, localizedError(localization.CypherSchemaInvalidClause("legacy ON"), nil)
	}
	onClause = strings.TrimSpace(onClause[1:])

	openParen := strings.Index(onClause, "(")
	if openParen < 0 {
		return parsedCreateIndexDDL{}, localizedError(localization.CypherSchemaInvalidClause("legacy ON"), nil)
	}
	label := cyphertext.SymbolicNameValue(strings.TrimSpace(onClause[:openParen]))
	if label == "" {
		return parsedCreateIndexDDL{}, localizedError(localization.CypherSchemaInvalidClause("legacy ON"), nil)
	}

	inside, tail, ok := extractParenSection(onClause[openParen:])
	if !ok {
		return parsedCreateIndexDDL{}, localizedError(localization.CypherSchemaInvalidClause("legacy ON"), nil)
	}
	options, err := schemaOptionsTail(strings.TrimSpace(tail))
	if err != nil {
		return parsedCreateIndexDDL{}, err
	}

	properties := indexPropertiesFromOnClause(inside, true)
	if len(properties) == 0 {
		return parsedCreateIndexDDL{}, localizedError(localization.CypherSchemaIndexPropertiesRequired(), nil)
	}

	return parsedCreateIndexDDL{
		indexName:      name,
		label:          label,
		properties:     properties,
		isRelationship: false,
		options:        options,
	}, nil
}

type parsedCreateFulltextIndexDDL struct {
	indexName         string
	label             string
	labels            []string
	relationshipTypes []string
	properties        []string
	isRelationship    bool
	options           schemaOptions
}

func parseCreateFulltextForPattern(forClause string) (label string, relationshipTypes []string, isRelationship bool, err error) {
	target, ok := parseForPatternTarget(forClause)
	if !ok {
		// Neo4j compatibility: allow non-parenthesized node pattern `n:Label`.
		if !strings.Contains(forClause, "[") {
			parts := strings.SplitN(strings.TrimSpace(forClause), ":", 2)
			if len(parts) == 2 {
				compatLabel := cyphertext.SymbolicNameValue(strings.TrimSpace(parts[1]))
				if compatLabel != "" {
					return compatLabel, nil, false, nil
				}
			}
		}
		_, _, _, err = parseCreateIndexForPattern(forClause)
		return "", nil, false, err
	}
	if !target.isRelationship {
		return target.name, nil, false, nil
	}
	relTypes, err := parseFulltextRelationshipTypes(target.nameSource)
	if err != nil {
		return "", nil, false, err
	}
	return "", relTypes, true, nil
}

func (e *StorageExecutor) parseCreateFulltextIndexDDL(ctx context.Context, cypher string) (parsedCreateFulltextIndexDDL, error) {
	q := strings.TrimSpace(cypher)
	if q == "" {
		return parsedCreateFulltextIndexDDL{}, localizedError(localization.CypherSchemaEmptyStatement(), nil)
	}

	prefixPos := keywordIndexFrom(ctx, q, "CREATE FULLTEXT INDEX", 0, defaultKeywordScanOpts())
	if prefixPos != 0 {
		return parsedCreateFulltextIndexDDL{}, localizedError(localization.CypherSchemaInvalidKeyword("prefix"), nil)
	}
	prefixEnd, ok := keywordSpanAt(q, 0, "CREATE FULLTEXT INDEX")
	if !ok {
		return parsedCreateFulltextIndexDDL{}, localizedError(localization.CypherSchemaInvalidKeyword("prefix"), nil)
	}

	forPos := keywordIndexFrom(ctx, q, "FOR", prefixEnd, defaultKeywordScanOpts())
	if forPos < 0 {
		return parsedCreateFulltextIndexDDL{}, localizedError(localization.CypherSchemaMissingKeyword("FOR"), nil)
	}
	forEnd, ok := keywordSpanAt(q, forPos, "FOR")
	if !ok {
		return parsedCreateFulltextIndexDDL{}, localizedError(localization.CypherSchemaInvalidKeyword("FOR"), nil)
	}

	onPos := keywordIndexFrom(ctx, q, "ON", forEnd, defaultKeywordScanOpts())
	if onPos < 0 {
		return parsedCreateFulltextIndexDDL{}, localizedError(localization.CypherSchemaMissingKeyword("ON"), nil)
	}
	onEnd, ok := keywordSpanAt(q, onPos, "ON")
	if !ok {
		return parsedCreateFulltextIndexDDL{}, localizedError(localization.CypherSchemaInvalidKeyword("ON"), nil)
	}

	name, err := parseOptionalDDLName(ctx, q[prefixEnd:forPos])
	if err != nil {
		return parsedCreateFulltextIndexDDL{}, err
	}
	if name == "" {
		return parsedCreateFulltextIndexDDL{}, localizedError(localization.CypherSchemaIndexNameRequired(), nil)
	}

	label, relationshipTypes, isRelationship, err := parseCreateFulltextForPattern(q[forEnd:onPos])
	if err != nil {
		return parsedCreateFulltextIndexDDL{}, err
	}
	var labels []string
	if !isRelationship {
		pattern := strings.TrimSpace(q[forEnd:onPos])
		if inside, _, ok := extractParenSection(pattern); ok {
			pattern = inside
		}
		_, targets, _ := strings.Cut(pattern, ":")
		labels, err = parseFulltextRelationshipTypes(targets)
		if err != nil {
			return parsedCreateFulltextIndexDDL{}, err
		}
	}

	propsSegment, tail, err := propertiesSegment(ctx, q[onEnd:], propertySyntaxFulltext)
	if err != nil {
		return parsedCreateFulltextIndexDDL{}, err
	}
	options, err := schemaOptionsTail(tail)
	if err != nil {
		return parsedCreateFulltextIndexDDL{}, err
	}

	properties := indexPropertiesFromOnClause(propsSegment, false)
	if len(properties) == 0 {
		return parsedCreateFulltextIndexDDL{}, localizedError(localization.CypherSchemaFulltextPropertiesRequired(), nil)
	}

	return parsedCreateFulltextIndexDDL{
		indexName:         name,
		label:             label,
		labels:            labels,
		relationshipTypes: relationshipTypes,
		properties:        properties,
		isRelationship:    isRelationship,
		options:           options,
	}, nil
}

type parsedConstraintForRequireDDL struct {
	variable       string
	name           string
	label          string
	isRelationship bool
	requireExpr    string
	options        schemaOptions
	// family is the requirement kind the declaration's CREATE head declares,
	// and "" when its head declares none (createConstraintHeadAt).
	family constraintRequirementKind
}

type parsedCardinalityConstraintDDL struct {
	name      string
	relType   string
	direction string // OUTGOING | INCOMING
	maxCount  int
	options   schemaOptions
}

type parsedPolicyConstraintDDL struct {
	name        string
	sourceLabel string
	relType     string
	targetLabel string
	policyMode  string // ALLOWED | DISALLOWED
	options     schemaOptions
}

func splitDDLOptionsTail(ctx context.Context, s string) (expr string, optionsTail string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ""
	}
	if idx := keywordIndexFrom(ctx, s, "OPTIONS", 0, defaultKeywordScanOpts()); idx >= 0 {
		return strings.TrimSpace(s[:idx]), strings.TrimSpace(s[idx:])
	}
	return s, ""
}

// parseCreateConstraintForRequireParts parses the shared CREATE [NO OVERLAP]
// CONSTRAINT [name] FOR <pattern> REQUIRE prefix used by every constraint DDL
// shape. It returns the parsed optional name, the FOR-pattern span, and the
// REQUIRE tail with any OPTIONS tail still attached, so a shape whose REQUIRE
// is not a predicate reads that tail itself. Error messages are shared so every
// constraint shape rejects malformed prefixes identically.
func (e *StorageExecutor) parseCreateConstraintForRequireParts(ctx context.Context, cypher string) (name string, patternSpan string, requireText string, err error) {
	q := strings.TrimSpace(cypher)
	if q == "" {
		return "", "", "", localizedError(localization.CypherSchemaEmptyStatement(), nil)
	}
	header := createConstraintHeadAt(ctx, q).text
	if header == "" {
		return "", "", "", localizedError(localization.CypherSchemaInvalidKeyword("prefix"), nil)
	}
	prefixEnd, ok := keywordSpanAt(q, 0, header)
	if !ok {
		return "", "", "", localizedError(localization.CypherSchemaInvalidKeyword("prefix"), nil)
	}
	forPos := keywordIndexFrom(ctx, q, "FOR", prefixEnd, defaultKeywordScanOpts())
	if forPos < 0 {
		// FOR-based declarations do not admit the legacy ON/ASSERT shape.
		// Classify that shape once at the shared prefix, without changing
		// the missing-keyword diagnostic for malformed FOR declarations.
		if keywordIndexFrom(ctx, q, "ON", prefixEnd, defaultKeywordScanOpts()) >= 0 {
			return "", "", "", localizedError(localization.CypherSchemaUnsupportedCreateConstraintShape(), nil)
		}
		return "", "", "", localizedError(localization.CypherSchemaMissingKeyword("FOR"), nil)
	}
	forEnd, ok := keywordSpanAt(q, forPos, "FOR")
	if !ok {
		return "", "", "", localizedError(localization.CypherSchemaInvalidKeyword("FOR"), nil)
	}
	reqPos := keywordIndexFrom(ctx, q, "REQUIRE", forEnd, defaultKeywordScanOpts())
	if reqPos < 0 {
		return "", "", "", localizedError(localization.CypherSchemaMissingKeyword("REQUIRE"), nil)
	}
	reqEnd, ok := keywordSpanAt(q, reqPos, "REQUIRE")
	if !ok {
		return "", "", "", localizedError(localization.CypherSchemaInvalidKeyword("REQUIRE"), nil)
	}
	name, err = parseOptionalDDLName(ctx, q[prefixEnd:forPos])
	if err != nil {
		return "", "", "", err
	}
	return name, q[forEnd:reqPos], q[reqEnd:], nil
}

// parseCreateConstraintForRequireHead parses that prefix and its one optional
// OPTIONS tail: the predicate reading every constraint shape whose REQUIRE is a
// predicate takes, with the tail admitted by the schema OPTIONS owner.
func (e *StorageExecutor) parseCreateConstraintForRequireHead(ctx context.Context, cypher string) (name string, patternSpan string, requireExpr string, options schemaOptions, err error) {
	name, patternSpan, requireText, err := e.parseCreateConstraintForRequireParts(ctx, cypher)
	if err != nil {
		return "", "", "", schemaOptions{}, err
	}
	requireExpr, tail := splitDDLOptionsTail(ctx, requireText)
	options, err = schemaOptionsTail(tail)
	if err != nil {
		return "", "", "", schemaOptions{}, err
	}
	return name, patternSpan, requireExpr, options, nil
}

func (e *StorageExecutor) parseCreateConstraintForRequireDDL(ctx context.Context, cypher string) (parsedConstraintForRequireDDL, error) {
	name, patternSpan, requireExpr, options, err := e.parseCreateConstraintForRequireHead(ctx, cypher)
	if err != nil {
		return parsedConstraintForRequireDDL{}, err
	}
	target, ok := parseForPatternTarget(patternSpan)
	if !ok {
		return parsedConstraintForRequireDDL{}, localizedError(localization.CypherSchemaInvalidPattern("FOR"), nil)
	}
	return parsedConstraintForRequireDDL{name: name, label: target.name, variable: cyphertext.SymbolicNameValue(target.variableSource), isRelationship: target.isRelationship, requireExpr: requireExpr, options: options, family: createConstraintHeadAt(ctx, cypher).family}, nil
}

func parseCardinalityRequireExpr(ctx context.Context, requireExpr string) (int, bool) {
	requireExpr = strings.TrimSpace(requireExpr)
	idx := keywordIndexFrom(ctx, requireExpr, "MAX COUNT", 0, defaultKeywordScanOpts())
	if idx != 0 {
		return 0, false
	}
	end, ok := keywordSpanAt(requireExpr, 0, "MAX COUNT")
	if !ok {
		return 0, false
	}
	raw := strings.TrimSpace(requireExpr[end:])
	if raw == "" {
		return 0, false
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 1 {
		return 0, false
	}
	return v, true
}

func (e *StorageExecutor) parseCreateConstraintCardinalityDDL(ctx context.Context, cypher string) (parsedCardinalityConstraintDDL, error) {
	name, patternSpan, requireExpr, options, err := e.parseCreateConstraintForRequireHead(ctx, cypher)
	if err != nil {
		return parsedCardinalityConstraintDDL{}, err
	}
	target, ok := parseForPatternTarget(patternSpan)
	// The shared reader also admits a node pattern and an undirected
	// relationship; a cardinality contract is directional, so it requires a
	// relationship whose arrows spell one.
	if !ok || !target.isRelationship || target.direction == "" {
		return parsedCardinalityConstraintDDL{}, localizedError(localization.CypherSchemaInvalidPattern("cardinality FOR"), nil)
	}
	maxCount, ok := parseCardinalityRequireExpr(ctx, requireExpr)
	if !ok {
		if startsWithKeywordFold(strings.TrimSpace(requireExpr), "MAX COUNT") {
			end, spanOK := keywordSpanAt(strings.TrimSpace(requireExpr), 0, "MAX COUNT")
			if spanOK {
				raw := strings.TrimSpace(strings.TrimSpace(requireExpr)[end:])
				if raw != "" {
					digitsOnly := isAllDigits(raw)
					if digitsOnly {
						v, convErr := strconv.Atoi(raw)
						if convErr != nil || v < 1 {
							return parsedCardinalityConstraintDDL{}, localizedError(localization.CypherSchemaPositiveMaxCountRequired(raw), convErr)
						}
					}
				}
			}
		}
		return parsedCardinalityConstraintDDL{}, localizedError(localization.CypherSchemaInvalidClause("cardinality require"), nil)
	}

	return parsedCardinalityConstraintDDL{name: name, relType: target.name, direction: target.direction, maxCount: maxCount, options: options}, nil
}

func parseNodeLabelOnlyPattern(inner string) (string, bool) {
	inner = strings.TrimSpace(inner)
	if inner == "" {
		return "", false
	}
	if !strings.HasPrefix(inner, ":") {
		return "", false
	}
	label := cyphertext.SymbolicNameValue(strings.TrimSpace(inner[1:]))
	if label == "" {
		return "", false
	}
	return label, true
}

func parsePolicyForClause(forClause string) (sourceLabel string, relType string, targetLabel string, ok bool) {
	forClause = strings.TrimSpace(forClause)
	if !strings.HasPrefix(forClause, "(") {
		return "", "", "", false
	}
	leftInside, leftRest, ok := extractParenSection(forClause)
	if !ok {
		return "", "", "", false
	}
	sourceLabel, ok = parseNodeLabelOnlyPattern(leftInside)
	if !ok {
		return "", "", "", false
	}

	leftRest = strings.TrimSpace(leftRest)
	if !strings.HasPrefix(leftRest, "-") {
		return "", "", "", false
	}
	bracketStart := strings.Index(leftRest, "[")
	if bracketStart < 0 {
		return "", "", "", false
	}
	relInside, relRest, ok := extractBracketSection(leftRest[bracketStart:])
	if !ok {
		return "", "", "", false
	}
	colon := strings.Index(relInside, ":")
	if colon < 0 {
		return "", "", "", false
	}
	relType = cyphertext.SymbolicNameValue(strings.TrimSpace(relInside[colon+1:]))
	if relType == "" {
		return "", "", "", false
	}

	relRest = strings.TrimSpace(relRest)
	if !strings.HasPrefix(relRest, "->") {
		return "", "", "", false
	}
	relRest = strings.TrimSpace(relRest[2:])
	if !strings.HasPrefix(relRest, "(") {
		return "", "", "", false
	}
	rightInside, trailing, ok := extractParenSection(relRest)
	if !ok || strings.TrimSpace(trailing) != "" {
		return "", "", "", false
	}
	targetLabel, ok = parseNodeLabelOnlyPattern(rightInside)
	if !ok {
		return "", "", "", false
	}
	return sourceLabel, relType, targetLabel, true
}

func parsePolicyModeRequireExpr(requireExpr string) (string, bool) {
	requireExpr = strings.TrimSpace(requireExpr)
	if startsWithKeywordFold(requireExpr, "ALLOWED") {
		end, ok := keywordSpanAt(requireExpr, 0, "ALLOWED")
		if ok && strings.TrimSpace(requireExpr[end:]) == "" {
			return "ALLOWED", true
		}
	}
	if startsWithKeywordFold(requireExpr, "DISALLOWED") {
		end, ok := keywordSpanAt(requireExpr, 0, "DISALLOWED")
		if ok && strings.TrimSpace(requireExpr[end:]) == "" {
			return "DISALLOWED", true
		}
	}
	return "", false
}

func (e *StorageExecutor) parseCreateConstraintPolicyDDL(ctx context.Context, cypher string) (parsedPolicyConstraintDDL, error) {
	name, patternSpan, requireExpr, options, err := e.parseCreateConstraintForRequireHead(ctx, cypher)
	if err != nil {
		return parsedPolicyConstraintDDL{}, err
	}
	source, relType, target, ok := parsePolicyForClause(patternSpan)
	if !ok {
		return parsedPolicyConstraintDDL{}, localizedError(localization.CypherSchemaInvalidPattern("policy FOR"), nil)
	}
	mode, ok := parsePolicyModeRequireExpr(requireExpr)
	if !ok {
		return parsedPolicyConstraintDDL{}, localizedError(localization.CypherSchemaInvalidClause("policy require"), nil)
	}

	return parsedPolicyConstraintDDL{name: name, sourceLabel: source, relType: relType, targetLabel: target, policyMode: mode, options: options}, nil
}

// addSchemaConstraint adds constraint and, when it is a single-property
// uniqueness or node key constraint, fills the property index it owns from
// the stored nodes (#875), as addPropertyIndex fills a new index: equality
// and IN seeks then use it inside transactions as they use an index of its
// own. A constraint whose fill fails is dropped again, since a half-filled
// index would make seeks drop rows.
func (e *StorageExecutor) addSchemaConstraint(ctx context.Context, constraint storage.Constraint, ifNotExists bool) error {
	schema := e.storage.GetSchema()
	needed, err := schema.ConstraintCreationRequired(ctx, constraint, ifNotExists)
	if err != nil {
		return err
	}
	if !needed {
		return nil
	}
	if err := storage.ValidateConstraintOnCreationForEngine(ctx, e.storage, constraint); err != nil {
		return err
	}
	_, existed := schema.ConstraintPropertyIndex(constraint.Name)
	registered := schema.HasConstraintNamed(constraint.Name)
	if err := schema.AddConstraint(constraint, ifNotExists); err != nil {
		return err
	}
	// A new temporal constraint's history index is authoritative when its
	// label is empty; over existing nodes it isn't until the next rebuild
	// (#1126). IF NOT EXISTS on an existing constraint changes nothing.
	if !registered && constraint.Type == storage.ConstraintTemporal {
		if err := storage.MarkNewTemporalConstraintsForEngine(e.storage, schema, []storage.Constraint{constraint}); err != nil {
			return err
		}
	}
	// A relationship constraint's equality index is not a schema property index:
	// its postings are relationship IDs, so the storage owner fills it from the
	// stored relationships of the type here, as a node constraint's owned index is
	// filled below. A match after the declaration is then an exact-value lookup
	// instead of a scan of every relationship of the type. A fill that fails drops
	// the constraint again, since lookups trust the index to hold every value.
	if constraint.EffectiveEntityType() == storage.ConstraintEntityRelationship {
		if err := storage.PrepareRelationshipPropertyEqualityIndexForEngine(ctx, e.storage, constraint); err != nil {
			_ = schema.DropConstraint(constraint.Name)
			return err
		}
		return nil
	}
	index, owned := schema.ConstraintPropertyIndex(constraint.Name)
	if existed || !owned {
		return nil
	}
	if err := e.backfillCompositeIndex(ctx, index.Name, index.Label, index.Properties); err != nil {
		_ = schema.DropConstraint(constraint.Name)
		return err
	}
	if constraint.Type == storage.ConstraintUnique && constraint.EntityType != storage.ConstraintEntityRelationship && len(constraint.Properties) == 1 {
		return storage.RefreshUniqueConstraintValuesForEngine(ctx, e.storage, schema)
	}
	return nil
}

// addPropertyIndex creates an equality index (arity-1 or composite) and,
// when it didn't exist yet, fills it from the label's nodes. An existing
// index is left as it is: it is already maintained, and filling it again
// would list its nodes twice. A new index whose fill fails is dropped again:
// index lookups trust an index to hold every node with the value, so a
// half-filled one would drop rows.
func (e *StorageExecutor) addPropertyIndex(ctx context.Context, name, label string, properties []string) error {
	schema := e.storage.GetSchema()
	_, existed := schema.GetCompositeIndex(name)
	if err := schema.AddCompositeIndex(name, label, properties); err != nil {
		return err
	}
	if existed {
		return nil
	}
	if err := e.backfillCompositeIndex(ctx, name, label, properties); err != nil {
		_ = schema.DropIndex(name)
		return err
	}
	return nil
}

// backfillCompositeIndex fills a newly created equality index from the
// label's stored nodes, those the visibility layer hides from reads included:
// an index lookup finds them once they are shown again, and a NODE KEY check
// that trusts the index sees their values. A new index whose fill fails is
// dropped by addPropertyIndex / addSchemaConstraint, since index lookups
// trust an index to hold every matching node.
func (e *StorageExecutor) backfillCompositeIndex(ctx context.Context, name, label string, properties []string) error {
	schema := e.storage.GetSchema()
	if schema == nil {
		return nil
	}
	nodes, err := storage.GetStoredNodesByLabel(ctx, e.storage, label)
	if err != nil {
		return localizedError(localization.CypherSchemaBackfillIndexFailed(label, err), err)
	}
	entries := make(map[storage.NodeID]map[string]interface{}, len(nodes))
	for _, node := range nodes {
		if node == nil || node.Properties == nil {
			continue
		}
		entries[storage.EnsureNodeIDDatabasePrefixForEngine(e.storage, node.ID)] = node.Properties
	}
	if err := schema.BackfillCompositeIndex(ctx, name, entries); err != nil {
		property := ""
		if len(properties) > 0 {
			property = properties[0]
		}
		return localizedError(localization.CypherSchemaBackfillPropertyIndexFailed(label, property, err), err)
	}
	return nil
}

// qualifiedIndexPropertiesWellFormed reports whether every comma-separated item
// of an index ON clause is a variable.property reference whose two parts are
// identifiers or backtick-quoted names. An empty list is left to the caller's
// "properties required" error.
func qualifiedIndexPropertiesWellFormed(propertiesStr string) bool {
	if strings.TrimSpace(propertiesStr) == "" {
		return true
	}
	for _, part := range splitTopLevelCommaKeepEmpty(propertiesStr) {
		if _, _, ok := schemaPropertyReference(part, schemaReferenceUngrouped); !ok {
			return false
		}
	}
	return true
}

// indexPropertiesFromOnClause reads an ON clause's comma-separated items with
// the shared quote-aware readers: a comma or a dot inside a backtick-quoted
// name is part of that name, never a separator, and an item is one
// variable.property reference. allowBare additionally admits the bare property
// names of the legacy ON :Label(prop) index syntax.
func indexPropertiesFromOnClause(propertiesStr string, allowBare bool) []string {
	parts := splitTopLevelComma(propertiesStr)
	properties := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if _, property, ok := splitPostfixPropertyAccess(part); ok {
			if property != "" {
				properties = append(properties, property)
			}
			continue
		}
		if !allowBare {
			continue
		}
		if property, ok := isOneSymbolicName(part); ok && property != "" {
			properties = append(properties, property)
		}
	}
	return properties
}

// executeCreateFulltextIndex handles CREATE FULLTEXT INDEX commands.
//
// Supported syntax:
//
//	CREATE FULLTEXT INDEX index_name [IF NOT EXISTS]
//	FOR (n:Label) ON EACH [n.prop1, n.prop2]
//
//	CREATE FULLTEXT INDEX index_name [IF NOT EXISTS]
//	FOR ()-[r:RelType]-() ON EACH [r.prop1, r.prop2]
//
// The relationship form binds an index to a relationship type (or
// types — Neo4j allows `:R1|R2`); the runtime then scopes
// db.index.fulltext.queryRelationships to those types instead of
// scanning every edge.
func (e *StorageExecutor) executeCreateFulltextIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, localizedError(localization.CypherSchemaManagerUnavailable(), nil)
	}

	parsed, err := e.parseCreateFulltextIndexDDL(ctx, cypher)
	if err != nil {
		if errors.Is(err, nerrors.ErrInvalidFulltextRelationshipTypes) {
			return nil, &classifiedCypherError{cause: err, code: "Neo.ClientError.Statement.SyntaxError", detail: "UnexpectedSyntax"}
		}
		return nil, err
	}
	if _, err := e.schemaOptionMap(ctx, parsed.options, "fulltext index"); err != nil {
		return nil, err
	}

	entityType := storage.ConstraintEntityNode
	targets := parsed.labels
	if parsed.isRelationship {
		entityType = storage.ConstraintEntityRelationship
		targets = parsed.relationshipTypes
	}
	create, err := e.admitIndexCreationForTargets(ctx, cypher, parsed.indexName, "FULLTEXT", targets, parsed.properties, entityType)
	if err != nil {
		return nil, err
	}
	if !create {
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if parsed.isRelationship {
		if err := schema.AddFulltextIndex(parsed.indexName, parsed.relationshipTypes, parsed.properties, storage.ConstraintEntityRelationship); err != nil {
			return nil, localizedError(localization.CypherSchemaAddFulltextRelationshipIndexFailed(err), err)
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if err := schema.AddFulltextIndex(parsed.indexName, parsed.labels, parsed.properties, storage.ConstraintEntityNode); err != nil {
		return nil, localizedError(localization.CypherSchemaAddFulltextIndexFailed(err), err)
	}

	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

// parseDomainValueList parses a comma-separated list of literal values from a domain constraint.
// Supports strings ('value' or "value"), integers, and floats.
func parseDomainValueList(raw string) ([]interface{}, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	var values []interface{}
	for _, item := range splitTopLevelComma(raw) {
		token := strings.TrimSpace(item)
		if token == "" {
			continue
		}
		if value, quoted := cyphertext.DecodeString(token, cyphertext.AcceptedSource); quoted {
			values = append(values, value)
		} else if isCypherStringQuoteByte(token[0]) {
			return nil, localizedError(localization.CypherSchemaUnterminatedString(token), nil)
		} else {
			// Numeric or boolean literal
			// Try integer
			if intVal, err := strconv.ParseInt(token, 10, 64); err == nil {
				values = append(values, intVal)
			} else if floatVal, err := strconv.ParseFloat(token, 64); err == nil {
				values = append(values, floatVal)
			} else if token == "true" {
				values = append(values, true)
			} else if token == "false" {
				values = append(values, false)
			} else {
				// Treat as bare string
				values = append(values, token)
			}
		}

	}

	return values, nil
}

func parseFulltextRelationshipTypes(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nerrors.ErrInvalidFulltextRelationshipTypes
	}

	types := make([]string, 0, 2)
	var cur strings.Builder

	flush := func() error {
		name, ok := isOneSymbolicName(cur.String())
		cur.Reset()
		if !ok || name == "" {
			return nerrors.ErrInvalidFulltextRelationshipTypes
		}
		types = append(types, name)
		return nil
	}

	for index := 0; index < len(raw); index++ {
		if isCypherBacktickByte(raw[index]) {
			end, closed := scanCypherQuotedText(raw, index, raw[index])
			if !closed {
				return nil, nerrors.ErrInvalidFulltextRelationshipTypes
			}
			cur.WriteString(raw[index:end])
			index = end - 1
			continue
		}
		switch raw[index] {
		case '|':
			if err := flush(); err != nil {
				return nil, err
			}
		default:
			cur.WriteByte(raw[index])
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return types, nil
}
