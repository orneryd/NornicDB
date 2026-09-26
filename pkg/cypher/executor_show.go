package cypher

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/config/dbconfig"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// ===== SHOW Commands (Neo4j compatibility) =====

func (e *StorageExecutor) executeShowSettings(_ context.Context, cypher string) (*ExecuteResult, error) {
	query := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(cypher), ";"))
	upper := strings.ToUpper(query)
	prefix := "SHOW SETTINGS"
	if !strings.HasPrefix(upper, prefix) {
		prefix = "SHOW SETTING"
	}
	if !strings.HasPrefix(upper, prefix) {
		return nil, localizedError(localization.CypherAdminInvalidSyntax("SHOW SETTINGS"), nil)
	}
	tail := strings.TrimSpace(query[len(prefix):])

	selected := make(map[string]struct{})
	if tail != "" {
		for _, rawName := range strings.Split(tail, ",") {
			name := strings.Trim(strings.TrimSpace(rawName), "`'\"")
			if name == "" {
				return nil, localizedError(localization.CypherAdminInvalidSyntax("SHOW SETTINGS"), nil)
			}
			selected[name] = struct{}{}
		}
	}

	definitions := dbconfig.Settings()
	snapshot := SettingsSnapshot{}
	if e.settingsResolver != nil {
		snapshot = e.settingsResolver()
	}
	sort.Slice(definitions, func(i, j int) bool { return definitions[i].Name < definitions[j].Name })
	rows := make([][]interface{}, 0, len(definitions))
	seen := make(map[string]struct{}, len(definitions))
	for _, definition := range definitions {
		if _, duplicate := seen[definition.Name]; duplicate {
			continue
		}
		seen[definition.Name] = struct{}{}
		if len(selected) > 0 {
			if _, ok := selected[definition.Name]; !ok {
				continue
			}
		}
		value := definition.DefaultValue
		startupValue := definition.DefaultValue
		configuredValue, explicitlySet := snapshot.Configured[definition.Name]
		if explicitlySet {
			value = configuredValue
		}
		if activeValue, ok := snapshot.Active[definition.Name]; ok {
			value = activeValue
			startupValue = activeValue
		}
		if definition.Redacted {
			if value != "" {
				value = "<REDACTED>"
			}
			if startupValue != "" {
				startupValue = "<REDACTED>"
			}
		}
		rows = append(rows, []interface{}{
			definition.Name,
			value,
			definition.Dynamic,
			definition.DefaultValue,
			definition.Description,
			startupValue,
			explicitlySet,
			append([]string(nil), definition.ValidValues...),
			definition.Deprecated,
		})
	}

	return withShowDefaultColumns(&ExecuteResult{
		Columns: []string{"name", "value", "isDynamic", "defaultValue", "description", "startupValue", "isExplicitlySet", "validValues", "isDeprecated"},
		Rows:    rows,
	}, showSettingsDefaultColumns), nil
}

// executeShowIndexes handles SHOW INDEXES command
func (e *StorageExecutor) executeShowIndexes(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if isCompositeRoot(e.storage) {
		return nil, localizedError(localization.CypherResidualCompositeShowTargetRequired("SHOW INDEXES"), nil)
	}
	schema := e.storage.GetSchema()
	rows := [][]interface{}{}
	upper := strings.ToUpper(strings.TrimSpace(cypher))
	indexTypeFilter := ""
	switch {
	case strings.HasPrefix(upper, "SHOW FULLTEXT INDEX"):
		indexTypeFilter = "FULLTEXT"
	case strings.HasPrefix(upper, "SHOW RANGE INDEX"):
		indexTypeFilter = "RANGE"
	case strings.HasPrefix(upper, "SHOW VECTOR INDEX"):
		indexTypeFilter = "VECTOR"
	}
	if schema != nil {
		indexes := schema.GetIndexes()
		rows = make([][]interface{}, 0, len(indexes))
		for i, idx := range indexes {
			idxMap, ok := idx.(map[string]interface{})
			if !ok {
				continue
			}

			name := idxMap["name"]
			idxType := idxMap["type"]
			if idxType == "PROPERTY" || idxType == "COMPOSITE" {
				idxType = "RANGE"
			}
			if indexTypeFilter != "" && !strings.EqualFold(fmt.Sprintf("%v", idxType), indexTypeFilter) {
				continue
			}

			var labelsOrTypes interface{} = []string{}
			var properties interface{} = []string{}
			if l, ok := idxMap["label"].(string); ok && l != "" {
				labelsOrTypes = []string{l}
			} else if ls, ok := idxMap["labels"]; ok {
				labelsOrTypes = ls
			}
			if p, ok := idxMap["property"].(string); ok && p != "" {
				properties = []string{p}
			} else if ps, ok := idxMap["properties"]; ok {
				properties = ps
			}

			// Determine entity type (default NODE)
			entityType := "NODE"
			if et, ok := idxMap["entityType"].(string); ok && et != "" {
				entityType = et
			}

			// Determine owning constraint (nil if standalone)
			var owningConstraint interface{}
			if oc, ok := idxMap["owningConstraint"].(string); ok && oc != "" {
				owningConstraint = oc
			}

			// trackedSince, options, failureMessage and createStatement
			// (the last four columns) aren't known.
			rows = append(rows, []interface{}{
				int64(i + 1),      // id
				name,              // name
				"ONLINE",          // state
				100.0,             // populationPercent
				idxType,           // type
				entityType,        // entityType
				labelsOrTypes,     // labelsOrTypes
				properties,        // properties
				"nornicdb+schema", // indexProvider
				owningConstraint,  // owningConstraint
				nil,               // lastRead
				int64(0),          // readCount
				nil, nil, nil, nil,
			})
		}
	}

	return withShowDefaultColumns(&ExecuteResult{
		Columns: append(append([]string(nil), showIndexesDefaultColumns...), "trackedSince", "options", "failureMessage", "createStatement"),
		Rows:    rows,
	}, showIndexesDefaultColumns), nil
}

// executeShowConstraints handles SHOW CONSTRAINTS command
func (e *StorageExecutor) executeShowConstraints(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if isCompositeRoot(e.storage) {
		return nil, localizedError(localization.CypherResidualCompositeShowTargetRequired("SHOW CONSTRAINTS"), nil)
	}
	if isShowConstraintContractsCommand(cypher) {
		return e.executeShowConstraintContracts(ctx)
	}
	schema := e.storage.GetSchema()
	rows := [][]interface{}{}

	if schema != nil {
		constraints := schema.GetAllConstraints()
		for i, constraint := range constraints {
			var ownedIndex interface{}
			if constraint.OwnedIndex != "" {
				ownedIndex = constraint.OwnedIndex
			}
			// Direction and maxCount for cardinality constraints.
			var direction, maxCount interface{}
			if constraint.Type == storage.ConstraintCardinality {
				direction = constraint.Direction
				maxCount = int64(constraint.MaxCount)
			}
			// Source/target labels and policy mode for policy constraints.
			var sourceLabel, targetLabel, policyMode interface{}
			if constraint.Type == storage.ConstraintPolicy {
				sourceLabel = constraint.SourceLabel
				targetLabel = constraint.TargetLabel
				policyMode = constraint.PolicyMode
			}
			rows = append(rows, []interface{}{
				int64(i + 1),
				constraint.Name,
				string(constraint.Type),
				string(constraint.EffectiveEntityType()),
				[]string{constraint.Label},
				constraint.Properties,
				ownedIndex,
				nil,
				nil, // options
				nil, // createStatement
				direction,
				maxCount,
				sourceLabel,
				targetLabel,
				policyMode,
			})
		}

		offset := len(rows)
		for i, constraint := range schema.GetAllPropertyTypeConstraints() {
			rows = append(rows, []interface{}{
				int64(offset + i + 1),
				constraint.Name,
				string(storage.ConstraintPropertyType),
				string(constraint.EffectiveEntityType()),
				[]string{constraint.Label},
				[]string{constraint.Property},
				nil,
				string(constraint.ExpectedType),
				nil, nil, // options, createStatement
				nil, nil, nil, nil, nil,
			})
		}
	}

	// Neo4j's full set, then NornicDB's cardinality / policy constraint
	// columns, which only YIELD * or YIELD <column> show.
	return withShowDefaultColumns(&ExecuteResult{
		Columns: append(append([]string(nil), showConstraintsDefaultColumns...), "options", "createStatement", "direction", "maxCount", "sourceLabel", "targetLabel", "policyMode"),
		Rows:    rows,
	}, showConstraintsDefaultColumns), nil
}

// showTailKeywords start the part of a SHOW command after the command itself.
var showTailKeywords = []string{"YIELD", "WHERE", "RETURN", "ORDER BY", "SKIP", "LIMIT"}

// showCommandHead returns the SHOW command without its YIELD / WHERE /
// RETURN / ORDER BY / SKIP / LIMIT tail, which applyShowTail handles.
func showCommandHead(cypher string) string {
	query := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(cypher), ";"))
	end := len(query)
	for _, keyword := range showTailKeywords {
		if index := findKeywordIndexInContext(query, keyword); index >= 0 && index < end {
			end = index
		}
	}
	return strings.TrimSpace(query[:end])
}

// executeShowWithTail runs a SHOW command: run lists every row of the command
// (given the command without its tail), and applyShowTail applies the tail.
// Every SHOW command goes through it, so YIELD, WHERE, RETURN, aggregation,
// ORDER BY and SKIP / LIMIT behave the same for all of them.
func (e *StorageExecutor) executeShowWithTail(ctx context.Context, cypher string, run func(context.Context, string) (*ExecuteResult, error)) (*ExecuteResult, error) {
	result, err := run(ctx, showCommandHead(cypher))
	if err != nil {
		return nil, err
	}
	sortShowRowsByName(result)
	return e.applyShowTail(ctx, cypher, result, showDefaultColumns(result))
}

// sortShowRowsByName orders a SHOW listing by its name column, as Neo4j lists
// every SHOW command (indexes and constraints by name, not creation order).
// A listing without a string name column keeps its order.
func sortShowRowsByName(result *ExecuteResult) {
	column := -1
	for i, name := range result.Columns {
		if name == "name" {
			column = i
			break
		}
	}
	if column < 0 {
		return
	}
	name := func(row []interface{}) (string, bool) {
		if column >= len(row) {
			return "", false
		}
		value, ok := row[column].(string)
		return value, ok
	}
	sorted := true
	for i, row := range result.Rows {
		current, ok := name(row)
		if !ok {
			return
		}
		if i > 0 {
			if previous, _ := name(result.Rows[i-1]); previous > current {
				sorted = false
			}
		}
	}
	if !sorted {
		sort.SliceStable(result.Rows, func(i, j int) bool {
			left, _ := name(result.Rows[i])
			right, _ := name(result.Rows[j])
			return left < right
		})
	}
}

// Neo4j's SHOW commands list a default set of columns, and YIELD * or YIELD
// <column> the full set (#690). The listings build the full set in Neo4j's
// order (NornicDB-only columns after it); these are the default sets.
var (
	showFunctionsDefaultColumns   = []string{"name", "category", "description"}
	showSettingsDefaultColumns    = []string{"name", "value", "isDynamic", "defaultValue", "description"}
	showProceduresDefaultColumns  = []string{"name", "description", "mode", "worksOnSystem"}
	showDatabasesDefaultColumns   = []string{"name", "type", "aliases", "access", "address", "role", "writer", "requestedStatus", "currentStatus", "statusMessage", "default", "home", "constituents"}
	showIndexesDefaultColumns     = []string{"id", "name", "state", "populationPercent", "type", "entityType", "labelsOrTypes", "properties", "indexProvider", "owningConstraint", "lastRead", "readCount"}
	showConstraintsDefaultColumns = []string{"id", "name", "type", "entityType", "labelsOrTypes", "properties", "ownedIndex", "propertyType"}
)

// showDefaultColumns returns the default columns of a SHOW listing, which the
// listing records under the "showDefaultColumns" metadata key; nil when every
// column is a default one.
func showDefaultColumns(result *ExecuteResult) []string {
	if result == nil || result.Metadata == nil {
		return nil
	}
	defaults, _ := result.Metadata["showDefaultColumns"].([]string)
	return defaults
}

// withShowDefaultColumns records the default columns of a SHOW listing.
func withShowDefaultColumns(result *ExecuteResult, defaults []string) *ExecuteResult {
	if result.Metadata == nil {
		result.Metadata = make(map[string]interface{}, 1)
	}
	result.Metadata["showDefaultColumns"] = defaults
	return result
}

// projectShowColumns keeps columns of result, in that order.
func projectShowColumns(result *ExecuteResult, columns []string) *ExecuteResult {
	index := make(map[string]int, len(result.Columns))
	for i, column := range result.Columns {
		index[column] = i
	}
	rows := make([][]interface{}, len(result.Rows))
	cells := make([]interface{}, len(result.Rows)*len(columns))
	for r, row := range result.Rows {
		projected := cells[r*len(columns) : (r+1)*len(columns) : (r+1)*len(columns)]
		for c, column := range columns {
			if i, ok := index[column]; ok && i < len(row) {
				projected[c] = row[i]
			}
		}
		rows[r] = projected
	}
	return &ExecuteResult{Columns: append([]string(nil), columns...), Rows: rows}
}

// applyShowTail applies a SHOW command's tail to its rows with Neo4j's SHOW
// grammar:
//
//	SHOW … WHERE <predicate>
//	SHOW … YIELD <items> [ORDER BY …] [SKIP n] [LIMIT n] [WHERE …] [RETURN …]
//
// Without YIELD, the result has the command's default columns (defaults;
// nil: all of them), as has SHOW … WHERE; YIELD * and YIELD <column> reach
// every column.
//
// YIELD selects and renames columns; its ORDER BY / SKIP / LIMIT page the
// rows before its WHERE filters them; RETURN (with aggregation, DISTINCT,
// ORDER BY and SKIP / LIMIT expressions) runs over all remaining rows. The
// paging, filter and RETURN run as one pipeline (runPipelineClauses), the
// same row operators as every other clause. Any other form (RETURN after a
// WHERE without YIELD, WITH, a WHERE before YIELD's ORDER BY, a non-literal
// YIELD SKIP / LIMIT) is a SyntaxError, as in Neo4j.
func (e *StorageExecutor) applyShowTail(ctx context.Context, cypher string, result *ExecuteResult, defaults []string) (*ExecuteResult, error) {
	query := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(cypher), ";"))
	head := showCommandHead(query)
	if len(head) == len(query) {
		if defaults != nil {
			return projectShowColumns(result, defaults), nil
		}
		return result, nil
	}
	tail := strings.TrimSpace(query[len(head):])
	invalid := func() error {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidShowClause",
			"invalid SHOW command: expected WHERE, or YIELD [ORDER BY] [SKIP] [LIMIT] [WHERE] [RETURN]")
	}
	if !startsWithKeywordFold(tail, "YIELD") {
		if !startsWithKeywordFold(tail, "WHERE") {
			return nil, invalid()
		}
		where := strings.TrimSpace(tail[len("WHERE"):])
		for _, keyword := range []string{"RETURN", "ORDER BY", "SKIP", "LIMIT", "WITH", "YIELD"} {
			if topLevelKeywordIndex(where, keyword) >= 0 {
				return nil, invalid()
			}
		}
		tail = "YIELD * WHERE " + where
		if defaults != nil {
			result = projectShowColumns(result, defaults)
		}
	}
	body := strings.TrimSpace(tail[len("YIELD"):])

	// Segment boundaries, in the only order Neo4j accepts.
	segments := []string{"ORDER BY", "SKIP", "LIMIT", "WHERE", "RETURN"}
	positions := make([]int, len(segments))
	itemsEnd := len(body)
	for i, keyword := range segments {
		positions[i] = topLevelKeywordIndex(body, keyword)
		if positions[i] >= 0 && positions[i] < itemsEnd {
			itemsEnd = positions[i]
		}
	}
	returnIndex := positions[4]
	head4 := body
	if returnIndex >= 0 {
		head4 = body[:returnIndex]
	}
	for _, keyword := range []string{"WITH", "MATCH", "UNWIND", "CALL", "CREATE", "MERGE", "SET", "DELETE", "REMOVE", "FOREACH", "OPTIONAL MATCH"} {
		if topLevelKeywordIndex(head4, keyword) >= 0 {
			return nil, invalid()
		}
	}
	last := -1
	for i := 0; i < 4; i++ {
		position := topLevelKeywordIndex(head4, segments[i])
		if position < 0 {
			continue
		}
		if position < last {
			return nil, invalid()
		}
		last = position
	}
	segmentText := func(i int) string {
		start := topLevelKeywordIndex(head4, segments[i])
		if start < 0 {
			return ""
		}
		end := len(head4)
		for j := 0; j < 4; j++ {
			if position := topLevelKeywordIndex(head4, segments[j]); position > start && position < end {
				end = position
			}
		}
		return strings.TrimSpace(head4[start+len(segments[i]) : end])
	}
	for _, i := range []int{1, 2} {
		if value := segmentText(i); value != "" && !strings.HasPrefix(value, "$") {
			if integer, literal := parseLiteralValueFromComputedRow(value); !literal {
				return nil, invalid()
			} else if _, isInteger := integer.(int64); !isInteger {
				return nil, invalid()
			}
		}
	}

	items := strings.TrimSpace(body[:itemsEnd])
	yield := parseYieldClause("CALL show() YIELD " + items)
	if yield == nil || items == "" {
		return nil, invalid()
	}
	// The yielded columns keep their original names too: YIELD's ORDER BY and
	// WHERE may use either (YIELD name AS indexName WHERE name = 'x'), as in
	// Neo4j. Without RETURN, the YIELD items are the result.
	outputs := append([]string(nil), result.Columns...)
	projected := &ExecuteResult{Columns: append([]string(nil), result.Columns...), Rows: result.Rows}
	if !yield.yieldAll {
		if err := validateYieldColumnsExist(result.Columns, yield); err != nil {
			return nil, err
		}
		outputs = outputs[:0]
		index := make(map[string]int, len(result.Columns))
		for i, column := range result.Columns {
			index[column] = i
		}
		sources := make([]int, 0, len(yield.items))
		for _, item := range yield.items {
			name := item.name
			if item.alias != "" {
				name = item.alias
			}
			outputs = append(outputs, name)
			if name != item.name {
				projected.Columns = append(projected.Columns, name)
				sources = append(sources, index[item.name])
			}
		}
		if len(sources) > 0 {
			projected.Rows = make([][]interface{}, len(result.Rows))
			for r, row := range result.Rows {
				extended := append(append(make([]interface{}, 0, len(row)+len(sources)), row...), make([]interface{}, len(sources))...)
				for s, source := range sources {
					if source < len(row) {
						extended[len(row)+s] = row[source]
					}
				}
				projected.Rows[r] = extended
			}
		}
	}

	var clauses strings.Builder
	paging := ""
	for i, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if value := segmentText(i); value != "" {
			paging += " " + keyword + " " + value
		}
	}
	if paging != "" {
		clauses.WriteString("WITH *" + paging + " ")
	}
	if where := segmentText(3); where != "" {
		clauses.WriteString("WITH * WHERE " + where + " ")
	}
	if returnIndex >= 0 {
		clauses.WriteString(strings.TrimSpace(body[returnIndex:]))
	} else {
		quoted := make([]string, len(outputs))
		for i, column := range outputs {
			quoted[i] = column
			if name, next, ok := scanIdentifierToken(column, 0); !ok || name != column || next != len(column) {
				quoted[i] = "`" + strings.ReplaceAll(column, "`", "``") + "` AS `" + strings.ReplaceAll(column, "`", "``") + "`"
			}
		}
		clauses.WriteString("RETURN " + strings.Join(quoted, ", "))
	}
	return e.executeCallTail(ctx, projected, clauses.String())
}

func (e *StorageExecutor) executeShowConstraintContracts(ctx context.Context) (*ExecuteResult, error) {
	schema := e.storage.GetSchema()
	rows := [][]interface{}{}
	if schema != nil {
		contracts := schema.GetAllConstraintContracts()
		for _, contract := range contracts {
			compiledCount := int64(0)
			runtimeCount := int64(0)
			for _, entry := range contract.Entries {
				if strings.HasPrefix(entry.Kind, "primitive-") {
					compiledCount++
				} else {
					runtimeCount++
				}
			}
			rows = append(rows, []interface{}{
				contract.Name,
				contract.TargetEntityType,
				contract.TargetLabelOrType,
				int64(len(contract.Entries)),
				compiledCount,
				runtimeCount,
				contract.Definition,
			})
		}
	}
	return &ExecuteResult{
		Columns: []string{"name", "targetEntityType", "targetLabelOrType", "entryCount", "compiledEntryCount", "runtimeEntryCount", "definition"},
		Rows:    rows,
	}, nil
}

// executeShowProcedures handles SHOW PROCEDURES command
func (e *StorageExecutor) executeShowProcedures(ctx context.Context, cypher string) (*ExecuteResult, error) {
	ensureBuiltInProceduresRegistered()
	registered := ListRegisteredProcedures()
	procedures := make([][]interface{}, 0, len(registered))
	for _, p := range registered {
		description := p.Description
		if p.DescriptionMessage.ID != "" && e.localizationRenderer != nil {
			if rendered, _, err := e.localizationRenderer.Render(ctx, p.DescriptionMessage); err == nil {
				description = rendered
			}
		}
		arguments := make([]interface{}, 0, len(p.Params))
		for _, param := range p.Params {
			arguments = append(arguments, map[string]interface{}{"name": param.Name, "type": param.Type, "description": "", "isDeprecated": false})
		}
		returns := make([]interface{}, 0, len(p.Returns))
		for _, column := range p.Returns {
			returns = append(returns, map[string]interface{}{"name": column.Name, "type": column.Type, "description": "", "isDeprecated": false})
		}
		// admin, rolesExecution, rolesBoostedExecution, deprecatedBy and
		// option aren't known.
		procedures = append(procedures, []interface{}{p.Name, description, string(p.Mode), p.WorksOnSystem, p.Signature, arguments, returns, nil, nil, nil, false, nil, nil})
	}

	return withShowDefaultColumns(&ExecuteResult{
		Columns: []string{"name", "description", "mode", "worksOnSystem", "signature", "argumentDescription", "returnDescription", "admin", "rolesExecution", "rolesBoostedExecution", "isDeprecated", "deprecatedBy", "option"},
		Rows:    procedures,
	}, showProceduresDefaultColumns), nil
}

// executeShowFunctions handles SHOW FUNCTIONS command
// showFunctionsTable lists the built-in functions SHOW FUNCTIONS reports.
// Each function: name, category (Neo4j's category names; kalman.* is
// NornicDB's own), signature, description, aggregating.
var showFunctionsTable = [][]interface{}{
	// Scalar functions
	{"id", "Scalar", "id(entity :: ANY) :: INTEGER", "Returns the id of a node or relationship", false},
	{"elementId", "Scalar", "elementId(entity :: ANY) :: STRING", "Returns the element id of a node or relationship", false},
	{"labels", "List", "labels(node :: NODE) :: LIST<STRING>", "Returns labels of a node", false},
	{"type", "Scalar", "type(relationship :: RELATIONSHIP) :: STRING", "Returns the type of a relationship", false},
	{"keys", "List", "keys(entity :: ANY) :: LIST<STRING>", "Returns the property keys of a node or relationship", false},
	{"properties", "Scalar", "properties(entity :: ANY) :: MAP", "Returns all properties of a node or relationship", false},
	{"coalesce", "Scalar", "coalesce(expression :: ANY...) :: ANY", "Returns first non-null value", false},
	{"head", "Scalar", "head(list :: LIST<ANY>) :: ANY", "Returns the first element of a list", false},
	{"last", "Scalar", "last(list :: LIST<ANY>) :: ANY", "Returns the last element of a list", false},
	{"tail", "List", "tail(list :: LIST<ANY>) :: LIST<ANY>", "Returns all but the first element of a list", false},
	{"size", "Scalar", "size(list :: LIST<ANY>) :: INTEGER", "Returns the number of elements in a list", false},
	{"length", "Scalar", "length(path :: PATH) :: INTEGER", "Returns the length of a path", false},
	{"reverse", "String", "reverse(original :: LIST<ANY> | STRING) :: LIST<ANY> | STRING", "Reverses a list or string", false},
	{"range", "List", "range(start :: INTEGER, end :: INTEGER, step :: INTEGER = 1) :: LIST<INTEGER>", "Returns a list of integers", false},
	{"toString", "String", "toString(expression :: ANY) :: STRING", "Converts expression to string", false},
	{"toInteger", "Scalar", "toInteger(expression :: ANY) :: INTEGER", "Converts expression to integer", false},
	{"toFloat", "Scalar", "toFloat(expression :: ANY) :: FLOAT", "Converts expression to float", false},
	{"toBoolean", "Scalar", "toBoolean(expression :: ANY) :: BOOLEAN", "Converts expression to boolean", false},
	{"toLower", "String", "toLower(original :: STRING) :: STRING", "Converts string to lowercase", false},
	{"toUpper", "String", "toUpper(original :: STRING) :: STRING", "Converts string to uppercase", false},
	{"trim", "String", "trim(original :: STRING) :: STRING", "Trims whitespace from string", false},
	{"ltrim", "String", "ltrim(original :: STRING) :: STRING", "Trims leading whitespace", false},
	{"rtrim", "String", "rtrim(original :: STRING) :: STRING", "Trims trailing whitespace", false},
	{"replace", "String", "replace(original :: STRING, search :: STRING, replace :: STRING) :: STRING", "Replaces all occurrences", false},
	{"split", "String", "split(original :: STRING, splitDelimiter :: STRING) :: LIST<STRING>", "Splits string by delimiter", false},
	{"substring", "String", "substring(original :: STRING, start :: INTEGER, length :: INTEGER = NULL) :: STRING", "Returns substring", false},
	{"left", "String", "left(original :: STRING, length :: INTEGER) :: STRING", "Returns left part of string", false},
	{"right", "String", "right(original :: STRING, length :: INTEGER) :: STRING", "Returns right part of string", false},
	// Math functions
	{"abs", "Numeric", "abs(expression :: NUMBER) :: NUMBER", "Returns absolute value", false},
	{"ceil", "Numeric", "ceil(expression :: FLOAT) :: INTEGER", "Returns ceiling value", false},
	{"floor", "Numeric", "floor(expression :: FLOAT) :: INTEGER", "Returns floor value", false},
	{"round", "Numeric", "round(expression :: FLOAT) :: INTEGER", "Rounds to nearest integer", false},
	{"sign", "Numeric", "sign(expression :: NUMBER) :: INTEGER", "Returns sign of number", false},
	{"sqrt", "Logarithmic", "sqrt(expression :: FLOAT) :: FLOAT", "Returns square root", false},
	{"rand", "Numeric", "rand() :: FLOAT", "Returns random float between 0 and 1", false},
	{"randomUUID", "Scalar", "randomUUID() :: STRING", "Returns a random UUID", false},
	{"sin", "Trigonometric", "sin(expression :: FLOAT) :: FLOAT", "Returns sine", false},
	{"cos", "Trigonometric", "cos(expression :: FLOAT) :: FLOAT", "Returns cosine", false},
	{"tan", "Trigonometric", "tan(expression :: FLOAT) :: FLOAT", "Returns tangent", false},
	{"log", "Logarithmic", "log(expression :: FLOAT) :: FLOAT", "Returns natural logarithm", false},
	{"log10", "Logarithmic", "log10(expression :: FLOAT) :: FLOAT", "Returns base-10 logarithm", false},
	{"exp", "Logarithmic", "exp(expression :: FLOAT) :: FLOAT", "Returns e raised to power", false},
	{"pi", "Trigonometric", "pi() :: FLOAT", "Returns pi constant", false},
	{"e", "Logarithmic", "e() :: FLOAT", "Returns Euler's number", false},
	// Temporal functions
	{"timestamp", "Scalar", "timestamp() :: INTEGER", "Returns current timestamp in milliseconds", false},
	{"datetime", "Temporal", "datetime(input :: ANY = NULL) :: DATETIME", "Creates a datetime", false},
	{"date", "Temporal", "date(input :: ANY = NULL) :: DATE", "Creates a date", false},
	{"time", "Temporal", "time(input :: ANY = NULL) :: TIME", "Creates a time", false},
	// Aggregation functions
	{"count", "Aggregating", "count(expression :: ANY) :: INTEGER", "Returns count", true},
	{"sum", "Aggregating", "sum(expression :: NUMBER) :: NUMBER", "Returns sum", true},
	{"avg", "Aggregating", "avg(expression :: NUMBER) :: FLOAT", "Returns average", true},
	{"min", "Aggregating", "min(expression :: ANY) :: ANY", "Returns minimum", true},
	{"max", "Aggregating", "max(expression :: ANY) :: ANY", "Returns maximum", true},
	{"collect", "Aggregating", "collect(expression :: ANY) :: LIST<ANY>", "Collects values into list", true},
	// Predicate functions
	{"exists", "Predicate", "exists(expression :: ANY) :: BOOLEAN", "Returns true if expression is not null", false},
	{"isEmpty", "Predicate", "isEmpty(list :: LIST<ANY> | MAP | STRING) :: BOOLEAN", "Returns true if empty", false},
	{"all", "Predicate", "all(variable IN list WHERE predicate) :: BOOLEAN", "Returns true if all match", false},
	{"any", "Predicate", "any(variable IN list WHERE predicate) :: BOOLEAN", "Returns true if any match", false},
	{"none", "Predicate", "none(variable IN list WHERE predicate) :: BOOLEAN", "Returns true if none match", false},
	{"single", "Predicate", "single(variable IN list WHERE predicate) :: BOOLEAN", "Returns true if exactly one matches", false},
	// Spatial functions
	{"point", "Spatial", "point(input :: MAP) :: POINT", "Creates a point", false},
	{"distance", "Spatial", "distance(point1 :: POINT, point2 :: POINT) :: FLOAT", "Returns distance between points", false},
	{"polygon", "Spatial", "polygon(points :: LIST<POINT>) :: POLYGON", "Creates a polygon from a list of points", false},
	{"lineString", "Spatial", "lineString(points :: LIST<POINT>) :: LINESTRING", "Creates a lineString from a list of points", false},
	{"point.intersects", "Spatial", "point.intersects(point :: POINT, polygon :: POLYGON) :: BOOLEAN", "Checks if point intersects with polygon", false},
	{"point.contains", "Spatial", "point.contains(polygon :: POLYGON, point :: POINT) :: BOOLEAN", "Checks if polygon contains point", false},
	// Vector functions
	{"vector.similarity.cosine", "Vector", "vector.similarity.cosine(vector1 :: LIST<FLOAT>, vector2 :: LIST<FLOAT>) :: FLOAT", "Cosine similarity", false},
	{"vector.similarity.euclidean", "Vector", "vector.similarity.euclidean(vector1 :: LIST<FLOAT>, vector2 :: LIST<FLOAT>) :: FLOAT", "Euclidean similarity", false},
	// Kalman filter functions
	{"kalman.init", "Kalman", "kalman.init(config? :: MAP) :: STRING", "Create new Kalman filter state (basic scalar filter for noise smoothing)", false},
	{"kalman.process", "Kalman", "kalman.process(measurement :: FLOAT, state :: STRING, target? :: FLOAT) :: MAP", "Process measurement, returns {value, state}", false},
	{"kalman.predict", "Kalman", "kalman.predict(state :: STRING, steps :: INTEGER) :: FLOAT", "Predict state n steps into the future", false},
	{"kalman.state", "Kalman", "kalman.state(state :: STRING) :: FLOAT", "Get current state estimate from state JSON", false},
	{"kalman.reset", "Kalman", "kalman.reset(state :: STRING) :: STRING", "Reset filter state to initial values", false},
	{"kalman.velocity.init", "Kalman", "kalman.velocity.init(initialPos? :: FLOAT, initialVel? :: FLOAT) :: STRING", "Create 2-state Kalman filter (position + velocity for trend tracking)", false},
	{"kalman.velocity.process", "Kalman", "kalman.velocity.process(measurement :: FLOAT, state :: STRING) :: MAP", "Process measurement, returns {value, velocity, state}", false},
	{"kalman.velocity.predict", "Kalman", "kalman.velocity.predict(state :: STRING, steps :: INTEGER) :: FLOAT", "Predict position n steps into the future", false},
	{"kalman.adaptive.init", "Kalman", "kalman.adaptive.init(config? :: MAP) :: STRING", "Create adaptive Kalman filter (auto-switches between basic and velocity modes)", false},
	{"kalman.adaptive.process", "Kalman", "kalman.adaptive.process(measurement :: FLOAT, state :: STRING) :: MAP", "Process measurement, returns {value, mode, state}", false},
}

var (
	showFunctionRowsOnce  sync.Once
	showFunctionRowsCache [][]interface{}
)

// showFunctionRows returns SHOW FUNCTIONS' full rows, built once from
// showFunctionsTable. Callers copy the rows they hand out.
func showFunctionRows() [][]interface{} {
	showFunctionRowsOnce.Do(func() {
		rows := make([][]interface{}, 0, len(showFunctionsTable))
		for _, function := range showFunctionsTable {
			signature, _ := function[2].(string)
			arguments, returns := functionSignatureDescriptions(signature)
			// rolesExecution, rolesBoostedExecution and deprecatedBy aren't known.
			rows = append(rows, []interface{}{function[0], function[1], function[3], signature, true, arguments, returns, function[4], nil, nil, false, nil})
		}
		// Listed by name (sortShowRowsByName then finds them in order).
		sort.SliceStable(rows, func(i, j int) bool { return fmt.Sprint(rows[i][0]) < fmt.Sprint(rows[j][0]) })
		showFunctionRowsCache = rows
	})
	return showFunctionRowsCache
}

func (e *StorageExecutor) executeShowFunctions(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// The listing is static: its rows (with the argument and return
	// descriptions parsed from each signature) are built once, and each call
	// gets its own copy of them.
	cached := showFunctionRows()
	rows := make([][]interface{}, len(cached))
	var cells []interface{}
	for i, row := range cached {
		if len(cells) < len(row) {
			cells = make([]interface{}, len(row)*(len(cached)-i))
		}
		rows[i] = cells[:len(row):len(row)]
		copy(rows[i], row)
		cells = cells[len(row):]
	}
	return withShowDefaultColumns(&ExecuteResult{
		Columns: []string{"name", "category", "description", "signature", "isBuiltIn", "argumentDescription", "returnDescription", "aggregating", "rolesExecution", "rolesBoostedExecution", "isDeprecated", "deprecatedBy"},
		Rows:    rows,
	}, showFunctionsDefaultColumns), nil
}

// functionSignatureDescriptions derives SHOW FUNCTIONS' argumentDescription
// (a list of {name, type, description, isDeprecated} maps) and
// returnDescription from a signature "f(a :: T, b :: U = d) :: R". An argument
// without a declared type is ANY.
func functionSignatureDescriptions(signature string) ([]interface{}, string) {
	open := strings.IndexByte(signature, '(')
	if open < 0 {
		return []interface{}{}, ""
	}
	closing := findMatchingParen(signature, open)
	if closing < 0 {
		return []interface{}{}, ""
	}
	returns := ""
	if rest := strings.TrimSpace(signature[closing+1:]); strings.HasPrefix(rest, "::") {
		returns = strings.TrimSpace(rest[2:])
	}
	arguments := []interface{}{}
	for _, part := range splitTopLevelComma(signature[open+1 : closing]) {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, typeName := part, "ANY"
		if separator := strings.Index(part, "::"); separator >= 0 {
			name = strings.TrimSpace(part[:separator])
			typeName = strings.TrimSpace(part[separator+2:])
			if defaultValue := strings.Index(typeName, "="); defaultValue >= 0 {
				typeName = strings.TrimSpace(typeName[:defaultValue])
			}
		}
		arguments = append(arguments, map[string]interface{}{"name": strings.TrimSuffix(name, "?"), "type": typeName, "description": "", "isDeprecated": false})
	}
	return arguments, returns
}

// executeShowDatabase handles SHOW DATABASE command (singular - shows current database)
func (e *StorageExecutor) executeShowDatabase(ctx context.Context, cypher string) (*ExecuteResult, error) {
	nodeCount, _ := e.storage.NodeCount()
	edgeCount, _ := e.storage.EdgeCount()

	// Try to get database name from context or use default
	dbName := "nornic" // Default fallback

	// Priority 1: Check context for :USE database command
	if useDB := GetUseDatabaseFromContext(ctx); useDB != "" {
		dbName = useDB
	} else if e.dbManager != nil {
		// Priority 2: Try to infer database name from storage (if it's a NamespacedEngine)
		if namespacedEngine, ok := e.storage.(interface{ Namespace() string }); ok {
			if namespace := namespacedEngine.Namespace(); namespace != "" {
				dbName = namespace
			}
		}
		// Priority 3: Use default database from dbManager
		// Note: DatabaseManagerInterface doesn't expose DefaultDatabaseName,
		// so we can't call it directly. The NamespacedEngine namespace should
		// already be set correctly by the server layer.
	}

	return withShowDefaultColumns(&ExecuteResult{
		Columns: showDatabasesColumns,
		Rows:    [][]interface{}{showDatabaseRow(e.dbManager, dbName, "standard", "online", true)},
		Stats: &QueryStats{
			NodesCreated:         int(nodeCount),
			RelationshipsCreated: int(edgeCount),
		},
	}, showDatabasesDefaultColumns), nil
}

// executeShowDatabases handles SHOW DATABASES command (plural - lists all databases).
//
// Returns a list of all databases with their metadata including name, type, status,
// and whether they are the default database. This command requires DatabaseManager
// to be set via SetDatabaseManager().
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetDatabaseManager(dbManager)
//	result, err := executor.Execute(ctx, "SHOW DATABASES", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//	for _, row := range result.Rows {
//		// emit "Database: %s (type: %s, status: %s)" for row[0], row[1], row[7]
//	}
//
// Returns Neo4j-compatible format with columns:
//   - name: Database name
//   - type: Database type (standard, system)
//   - access: Access mode (read-write)
//   - address: Server address
//   - role: Server role (primary)
//   - writer: Whether writes are allowed
//   - requestedStatus: Requested status
//   - currentStatus: Current status (online, offline)
//   - statusMessage: Status message
//   - default: Whether this is the default database
//   - home: Whether this is the home database
//   - constituents: Constituent databases (empty for single databases)
func (e *StorageExecutor) executeShowDatabases(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, localizedError(localization.CypherAdminDatabaseManagerUnavailable("SHOW DATABASES"), nil)
	}

	databases := e.dbManager.ListDatabases()
	rows := make([][]interface{}, 0, len(databases))

	for _, db := range databases {
		rows = append(rows, showDatabaseRow(e.dbManager, db.Name(), db.Type(), db.Status(), db.IsDefault()))
	}

	return withShowDefaultColumns(&ExecuteResult{
		Columns: showDatabasesColumns,
		Rows:    rows,
	}, showDatabasesDefaultColumns), nil
}

// showDatabasesColumns is SHOW DATABASES' full column set, in Neo4j's order.
var showDatabasesColumns = []string{"name", "type", "aliases", "access", "databaseID", "serverID", "address", "role", "writer", "requestedStatus", "currentStatus", "statusMessage", "default", "home", "currentPrimariesCount", "currentSecondariesCount", "requestedPrimariesCount", "requestedSecondariesCount", "creationTime", "lastStartTime", "lastStopTime", "store", "lastCommittedTxn", "replicationLag", "constituents", "options"}

// showDatabaseRow is one SHOW DATABASES row in showDatabasesColumns order.
// The cluster and store columns NornicDB has no value for are null.
func showDatabaseRow(manager DatabaseManagerInterface, name, databaseType, status string, isDefault bool) []interface{} {
	aliases := []string{}
	if manager != nil {
		for alias := range manager.ListAliases(name) {
			aliases = append(aliases, alias)
		}
		sort.Strings(aliases)
	}
	return []interface{}{
		name, databaseType, aliases, "read-write",
		nil, nil, // databaseID, serverID
		"localhost:7687", "primary", true,
		status, status, "",
		isDefault, isDefault,
		nil, nil, nil, nil, // primaries / secondaries counts
		nil, nil, nil, // creationTime, lastStartTime, lastStopTime
		nil, nil, nil, // store, lastCommittedTxn, replicationLag
		[]string{},
		nil, // options
	}
}

// executeCreateDatabase handles CREATE DATABASE command.
//
// Creates a new database with the specified name. Supports optional IF NOT EXISTS
// clause to avoid errors when the database already exists. This command requires
// DatabaseManager to be set via SetDatabaseManager().
//
// Syntax:
//   - CREATE DATABASE name
//   - CREATE DATABASE name IF NOT EXISTS
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetDatabaseManager(dbManager)
//
//	// Create database
//	result, err := executor.Execute(ctx, "CREATE DATABASE tenant_a", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Create with IF NOT EXISTS (idempotent)
//	result, err = executor.Execute(ctx, "CREATE DATABASE tenant_a IF NOT EXISTS", nil)
//
// Returns:
//   - Success: Result with database name in single row
//   - Error: If database already exists (unless IF NOT EXISTS is used)
//   - Error: If DatabaseManager is not configured
func (e *StorageExecutor) executeCreateDatabase(ctx context.Context, cypher string) (*ExecuteResult, error) {
	e.logEvent(slog.LevelDebug, localization.CypherCreateDatabaseInvokedEvent(len(cypher)))
	if e.dbManager == nil {
		return nil, localizedError(localization.CypherAdminDatabaseManagerUnavailable("CREATE DATABASE"), nil)
	}

	// Find "CREATE DATABASE" keyword position (with flexible whitespace)
	createDbIdx := findMultiWordKeywordIndex(cypher, "CREATE", "DATABASE")
	if createDbIdx == -1 {
		return nil, localizedError(localization.CypherAdminInvalidSyntax("CREATE DATABASE"), nil)
	}

	// Skip "CREATE" and whitespace to find "DATABASE"
	startPos := createDbIdx + len("CREATE")
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}
	// Skip "DATABASE" and whitespace
	if startPos+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("DATABASE")], "DATABASE") {
		startPos += len("DATABASE")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
	}

	if startPos >= len(cypher) {
		return nil, localizedError(localization.CypherAdminDatabaseNameExpected("CREATE DATABASE"), nil)
	}

	// Find end of database name (whitespace, end of string, or "IF NOT EXISTS")
	// Check for "IF NOT EXISTS" first (with flexible whitespace)
	// This is a 3-word keyword: IF NOT EXISTS
	ifNotIdx := findMultiWordKeywordIndex(cypher[startPos:], "IF", "NOT")
	var ifNotExistsIdx int = -1
	if ifNotIdx >= 0 {
		// Found "IF NOT" - check if "EXISTS" follows
		afterIfNot := startPos + ifNotIdx + len("IF")
		// Skip whitespace after "IF"
		for afterIfNot < len(cypher) && isWhitespace(cypher[afterIfNot]) {
			afterIfNot++
		}
		// Check for "NOT"
		if afterIfNot+len("NOT") <= len(cypher) && strings.EqualFold(cypher[afterIfNot:afterIfNot+len("NOT")], "NOT") {
			afterNot := afterIfNot + len("NOT")
			// Skip whitespace after "NOT"
			for afterNot < len(cypher) && isWhitespace(cypher[afterNot]) {
				afterNot++
			}
			// Check for "EXISTS"
			if afterNot+len("EXISTS") <= len(cypher) && strings.EqualFold(cypher[afterNot:afterNot+len("EXISTS")], "EXISTS") {
				// Found "IF NOT EXISTS" - database name ends before "IF"
				ifNotExistsIdx = ifNotIdx
			}
		}
	}
	var dbNameEnd int
	if ifNotExistsIdx >= 0 {
		// Database name ends before "IF NOT EXISTS"
		dbNameEnd = startPos + ifNotExistsIdx
	} else {
		// No IF NOT EXISTS - database name goes to end of query
		dbNameEnd = len(cypher)
	}

	// Extract database name (trim whitespace)
	rawDBName := strings.TrimSpace(cypher[startPos:dbNameEnd])
	dbName, err := unquoteBacktickIdentifier(rawDBName)
	if err != nil {
		return nil, localizedError(localization.CypherAdminInvalidIdentifier(rawDBName), err)
	}
	if dbName == "" {
		return nil, localizedError(localization.CypherAdminDatabaseNameEmpty("CREATE DATABASE"), nil)
	}

	// Validate database name (basic validation)
	if strings.ContainsAny(dbName, " \t\n\r") {
		return nil, localizedError(localization.CypherAdminInvalidDatabaseName(dbName), nil)
	}

	// Check if already exists
	if e.dbManager.Exists(dbName) {
		if ifNotExistsIdx >= 0 {
			// IF NOT EXISTS - return success with no error
			return &ExecuteResult{
				Columns: []string{"name"},
				Rows:    [][]interface{}{{dbName}},
			}, nil
		}
		return nil, localizedError(localization.CypherAdminDatabaseAlreadyExists(dbName), nil)
	}

	// Create database
	err = e.dbManager.CreateDatabase(dbName)
	if err != nil {
		e.logEvent(slog.LevelError, localization.CypherCreateDatabaseFailedEvent())
		return nil, localizedError(localization.CypherAdminCreateDatabaseFailed(dbName, err), err)
	}
	e.logEvent(slog.LevelInfo, localization.CypherCreateDatabaseSucceededEvent())

	return &ExecuteResult{
		Columns: []string{"name"},
		Rows:    [][]interface{}{{dbName}},
	}, nil
}

// executeDropDatabase handles DROP DATABASE command.
//
// Deletes a database and all its data. Supports optional IF EXISTS clause to
// avoid errors when the database doesn't exist. This command requires
// DatabaseManager to be set via SetDatabaseManager().
//
// Syntax:
//   - DROP DATABASE name
//   - DROP DATABASE name IF EXISTS
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetDatabaseManager(dbManager)
//
//	// Drop database
//	result, err := executor.Execute(ctx, "DROP DATABASE tenant_a", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Drop with IF EXISTS (idempotent)
//	result, err = executor.Execute(ctx, "DROP DATABASE tenant_a IF EXISTS", nil)
//
// Warning: This operation permanently deletes all data in the database.
// The default and system databases cannot be dropped.
//
// Returns:
//   - Success: Result with database name in single row (empty if IF EXISTS and not found)
//   - Error: If database doesn't exist (unless IF EXISTS is used)
//   - Error: If DatabaseManager is not configured
func (e *StorageExecutor) executeDropDatabase(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, localizedError(localization.CypherAdminDatabaseManagerUnavailable("DROP DATABASE"), nil)
	}

	// Find "DROP DATABASE" keyword position (with flexible whitespace)
	dropDbIdx := findMultiWordKeywordIndex(cypher, "DROP", "DATABASE")
	if dropDbIdx == -1 {
		return nil, localizedError(localization.CypherAdminInvalidSyntax("DROP DATABASE"), nil)
	}

	// Skip "DROP" and whitespace to find "DATABASE"
	startPos := dropDbIdx + len("DROP")
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}
	// Skip "DATABASE" and whitespace
	if startPos+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("DATABASE")], "DATABASE") {
		startPos += len("DATABASE")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
	}
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}

	if startPos >= len(cypher) {
		return nil, localizedError(localization.CypherAdminDatabaseNameExpected("DROP DATABASE"), nil)
	}

	// Find end of database name (whitespace, end of string, or "IF EXISTS")
	// Check for "IF EXISTS" first (with flexible whitespace)
	ifExistsIdx := findMultiWordKeywordIndex(cypher[startPos:], "IF", "EXISTS")
	var dbNameEnd int
	if ifExistsIdx >= 0 {
		dbNameEnd = startPos + ifExistsIdx
		ifExistsIdx = startPos + ifExistsIdx // Absolute position
	} else {
		// No IF EXISTS - database name goes to end of query
		dbNameEnd = len(cypher)
	}

	// Extract database name (trim whitespace)
	rawDBName := strings.TrimSpace(cypher[startPos:dbNameEnd])
	dbName, err := unquoteBacktickIdentifier(rawDBName)
	if err != nil {
		return nil, localizedError(localization.CypherAdminInvalidIdentifier(rawDBName), err)
	}
	if dbName == "" {
		return nil, localizedError(localization.CypherAdminDatabaseNameEmpty("DROP DATABASE"), nil)
	}

	// Validate database name (basic validation)
	if strings.ContainsAny(dbName, " \t\n\r") {
		return nil, localizedError(localization.CypherAdminInvalidDatabaseName(dbName), nil)
	}

	// Check if exists
	if !e.dbManager.Exists(dbName) {
		if ifExistsIdx >= 0 {
			// IF EXISTS - return success with no error
			return &ExecuteResult{
				Columns: []string{"name"},
				Rows:    [][]interface{}{},
			}, nil
		}
		return nil, localizedError(localization.CypherAdminDatabaseDoesNotExist(dbName), nil)
	}

	// Drop database
	err = e.dbManager.DropDatabase(dbName)
	if err != nil {
		return nil, localizedError(localization.CypherAdminDropDatabaseFailed(dbName, err), err)
	}

	return &ExecuteResult{
		Columns: []string{"name"},
		Rows:    [][]interface{}{{dbName}},
	}, nil
}

// executeCreateAlias handles CREATE ALIAS command (Neo4j-compatible).
//
// Creates an alias for a database. Aliases allow referencing databases with
// alternative names, useful for database renaming, environment mapping, etc.
//
// Syntax:
//   - CREATE ALIAS alias_name FOR DATABASE database_name
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetDatabaseManager(dbManager)
//
//	// Create alias
//	result, err := executor.Execute(ctx, "CREATE ALIAS main FOR DATABASE tenant_primary_2024", nil)
//
// Returns:
//   - Success: Result with alias name in single row
//   - Error: If alias already exists or database doesn't exist
func (e *StorageExecutor) executeCreateAlias(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, localizedError(localization.CypherAdminDatabaseManagerUnavailable("CREATE ALIAS"), nil)
	}

	// Find "CREATE ALIAS" keyword position
	createAliasIdx := findMultiWordKeywordIndex(cypher, "CREATE", "ALIAS")
	if createAliasIdx == -1 {
		return nil, localizedError(localization.CypherAdminInvalidSyntax("CREATE ALIAS"), nil)
	}

	// Skip "CREATE" and whitespace to find "ALIAS"
	startPos := createAliasIdx + len("CREATE")
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}
	// Skip "ALIAS" and whitespace
	if startPos+len("ALIAS") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("ALIAS")], "ALIAS") {
		startPos += len("ALIAS")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
	}

	if startPos >= len(cypher) {
		return nil, localizedError(localization.CypherAdminAliasNameExpected("CREATE ALIAS"), nil)
	}

	// Find "FOR DATABASE" to separate alias name from database name
	forIdx := findMultiWordKeywordIndex(cypher[startPos:], "FOR", "DATABASE")
	if forIdx == -1 {
		return nil, localizedError(localization.CypherAdminTermExpected("CREATE ALIAS", "FOR DATABASE"), nil)
	}

	// Extract alias name (trim all whitespace)
	aliasName := strings.TrimSpace(cypher[startPos : startPos+forIdx])
	// Remove all whitespace from alias name (handle cases where whitespace variations
	// might have left whitespace in the extracted name)
	aliasName = strings.ReplaceAll(aliasName, " ", "")
	aliasName = strings.ReplaceAll(aliasName, "\t", "")
	aliasName = strings.ReplaceAll(aliasName, "\n", "")
	aliasName = strings.ReplaceAll(aliasName, "\r", "")

	if aliasName == "" {
		return nil, localizedError(localization.CypherAdminAliasNameEmpty("CREATE ALIAS"), nil)
	}

	// Validate alias name (should not contain whitespace after cleaning)
	if strings.ContainsAny(aliasName, " \t\n\r") {
		return nil, localizedError(localization.CypherAdminInvalidAliasName(aliasName), nil)
	}

	// Skip "FOR" and whitespace
	dbStartPos := startPos + forIdx + len("FOR")
	for dbStartPos < len(cypher) && isWhitespace(cypher[dbStartPos]) {
		dbStartPos++
	}
	// Skip "DATABASE" and whitespace
	if dbStartPos+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[dbStartPos:dbStartPos+len("DATABASE")], "DATABASE") {
		dbStartPos += len("DATABASE")
		for dbStartPos < len(cypher) && isWhitespace(cypher[dbStartPos]) {
			dbStartPos++
		}
	}

	if dbStartPos >= len(cypher) {
		return nil, localizedError(localization.CypherAdminDatabaseNameExpected("CREATE ALIAS"), nil)
	}

	// Extract database name (rest of query, trim all whitespace)
	dbName := strings.TrimSpace(cypher[dbStartPos:])
	// Remove all whitespace from database name
	dbName = strings.ReplaceAll(dbName, " ", "")
	dbName = strings.ReplaceAll(dbName, "\t", "")
	dbName = strings.ReplaceAll(dbName, "\n", "")
	dbName = strings.ReplaceAll(dbName, "\r", "")

	if dbName == "" {
		return nil, localizedError(localization.CypherAdminDatabaseNameEmpty("CREATE ALIAS"), nil)
	}

	// Validate database name (should not contain whitespace after cleaning)
	if strings.ContainsAny(dbName, " \t\n\r") {
		return nil, localizedError(localization.CypherAdminInvalidDatabaseName(dbName), nil)
	}

	// Create alias
	err := e.dbManager.CreateAlias(aliasName, dbName)
	if err != nil {
		return nil, localizedError(localization.CypherAdminCreateAliasFailed(aliasName, dbName, err), err)
	}

	return &ExecuteResult{
		Columns: []string{"alias"},
		Rows:    [][]interface{}{{aliasName}},
	}, nil
}

// executeDropAlias handles DROP ALIAS command (Neo4j-compatible).
//
// Removes an alias for a database.
//
// Syntax:
//   - DROP ALIAS alias_name
//   - DROP ALIAS alias_name IF EXISTS
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetDatabaseManager(dbManager)
//
//	// Drop alias
//	result, err := executor.Execute(ctx, "DROP ALIAS main", nil)
//
// Returns:
//   - Success: Result with alias name in single row (empty if IF EXISTS and not found)
//   - Error: If alias doesn't exist (unless IF EXISTS is used)
func (e *StorageExecutor) executeDropAlias(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, localizedError(localization.CypherAdminDatabaseManagerUnavailable("DROP ALIAS"), nil)
	}

	// Find "DROP ALIAS" keyword position
	dropAliasIdx := findMultiWordKeywordIndex(cypher, "DROP", "ALIAS")
	if dropAliasIdx == -1 {
		return nil, localizedError(localization.CypherAdminInvalidSyntax("DROP ALIAS"), nil)
	}

	// Skip "DROP" and whitespace to find "ALIAS"
	startPos := dropAliasIdx + len("DROP")
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}
	// Skip "ALIAS" and whitespace
	if startPos+len("ALIAS") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("ALIAS")], "ALIAS") {
		startPos += len("ALIAS")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
	}

	if startPos >= len(cypher) {
		return nil, localizedError(localization.CypherAdminAliasNameExpected("DROP ALIAS"), nil)
	}

	// Find end of alias name (whitespace, end of string, or "IF EXISTS")
	ifExistsIdx := findMultiWordKeywordIndex(cypher[startPos:], "IF", "EXISTS")
	var aliasNameEnd int
	if ifExistsIdx >= 0 {
		aliasNameEnd = startPos + ifExistsIdx
	} else {
		aliasNameEnd = len(cypher)
	}

	// Extract alias name (trim all whitespace)
	aliasName := strings.TrimSpace(cypher[startPos:aliasNameEnd])
	// Remove all whitespace from alias name
	aliasName = strings.ReplaceAll(aliasName, " ", "")
	aliasName = strings.ReplaceAll(aliasName, "\t", "")
	aliasName = strings.ReplaceAll(aliasName, "\n", "")
	aliasName = strings.ReplaceAll(aliasName, "\r", "")

	if aliasName == "" {
		return nil, localizedError(localization.CypherAdminAliasNameEmpty("DROP ALIAS"), nil)
	}

	// Validate alias name (should not contain whitespace after cleaning)
	if strings.ContainsAny(aliasName, " \t\n\r") {
		return nil, localizedError(localization.CypherAdminInvalidAliasName(aliasName), nil)
	}

	// Check if alias exists (by checking if it resolves)
	_, err := e.dbManager.ResolveDatabase(aliasName)
	if err != nil {
		if ifExistsIdx >= 0 {
			// IF EXISTS - return success with no error
			return &ExecuteResult{
				Columns: []string{"alias"},
				Rows:    [][]interface{}{},
			}, nil
		}
		return nil, localizedError(localization.CypherAdminAliasDoesNotExist(aliasName), nil)
	}

	// Drop alias
	err = e.dbManager.DropAlias(aliasName)
	if err != nil {
		return nil, localizedError(localization.CypherAdminDropAliasFailed(aliasName, err), err)
	}

	return &ExecuteResult{
		Columns: []string{"alias"},
		Rows:    [][]interface{}{{aliasName}},
	}, nil
}

// executeShowAliases handles SHOW ALIASES command (Neo4j-compatible).
//
// Lists all database aliases, optionally filtered by database.
//
// Syntax:
//   - SHOW ALIASES
//   - SHOW ALIASES FOR DATABASE database_name
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetDatabaseManager(dbManager)
//
//	// List all aliases
//	result, err := executor.Execute(ctx, "SHOW ALIASES", nil)
//
//	// List aliases for specific database
//	result, err = executor.Execute(ctx, "SHOW ALIASES FOR DATABASE tenant_a", nil)
//
// Returns:
//   - Success: Result with alias and database columns
func (e *StorageExecutor) executeShowAliases(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, localizedError(localization.CypherResidualShowAliasesManagerUnavailable(), nil)
	}

	// Find "SHOW ALIASES" keyword position
	showAliasesIdx := findMultiWordKeywordIndex(cypher, "SHOW", "ALIASES")
	if showAliasesIdx == -1 {
		return nil, localizedError(localization.CypherResidualShowAliasesSyntaxInvalid(), nil)
	}

	// Skip "SHOW" and whitespace to find "ALIASES"
	startPos := showAliasesIdx + len("SHOW")
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}
	// Skip "ALIASES" and whitespace
	if startPos+len("ALIASES") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("ALIASES")], "ALIASES") {
		startPos += len("ALIASES")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
	}

	// Check for "FOR DATABASE" clause
	var databaseName string
	if startPos < len(cypher) {
		forIdx := findMultiWordKeywordIndex(cypher[startPos:], "FOR", "DATABASE")
		if forIdx >= 0 {
			// Extract database name
			dbStartPos := startPos + forIdx + len("FOR")
			for dbStartPos < len(cypher) && isWhitespace(cypher[dbStartPos]) {
				dbStartPos++
			}
			// Skip "DATABASE" and whitespace
			if dbStartPos+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[dbStartPos:dbStartPos+len("DATABASE")], "DATABASE") {
				dbStartPos += len("DATABASE")
				for dbStartPos < len(cypher) && isWhitespace(cypher[dbStartPos]) {
					dbStartPos++
				}
			}
			if dbStartPos < len(cypher) {
				databaseName = strings.TrimSpace(cypher[dbStartPos:])
			}
		}
	}

	// List aliases
	aliases := e.dbManager.ListAliases(databaseName)

	// Format results
	rows := make([][]interface{}, 0, len(aliases))
	for alias, dbName := range aliases {
		rows = append(rows, []interface{}{alias, dbName})
	}

	return &ExecuteResult{
		Columns: []string{"alias", "database"},
		Rows:    rows,
	}, nil
}

// executeAlterDatabase handles ALTER DATABASE SET LIMIT command.
//
// Sets resource limits for a database. Supports setting individual limits
// or multiple limits in a single command.
//
// Syntax:
//   - ALTER DATABASE database_name SET LIMIT limit_name = value
//   - ALTER DATABASE database_name SET LIMIT limit_name1 = value1, limit_name2 = value2
//
// Supported limit names:
//   - max_nodes: Maximum number of nodes (int64)
//   - max_edges: Maximum number of edges (int64)
//   - max_bytes: Maximum storage size in bytes (int64)
//   - max_query_time: Maximum query execution time (duration string, e.g., "60s", "5m")
//   - max_results: Maximum number of query results (int64)
//   - max_concurrent_queries: Maximum concurrent queries (int)
//   - max_connections: Maximum connections (int)
//   - max_queries_per_second: Maximum queries per second (int)
//   - max_writes_per_second: Maximum writes per second (int)
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetDatabaseManager(dbManager)
//
//	// Set max nodes limit
//	result, err := executor.Execute(ctx, "ALTER DATABASE tenant_a SET LIMIT max_nodes = 1000000", nil)
//
//	// Set multiple limits
//	result, err = executor.Execute(ctx, "ALTER DATABASE tenant_a SET LIMIT max_nodes = 1000000, max_edges = 5000000", nil)
//
// Returns:
//   - Success: Result with database name
//   - Error: If database doesn't exist or syntax is invalid
func (e *StorageExecutor) executeAlterDatabase(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, localizedError(localization.CypherAdminDatabaseManagerUnavailable("ALTER DATABASE"), nil)
	}

	// Find "ALTER DATABASE" keyword position
	alterDbIdx := findMultiWordKeywordIndex(cypher, "ALTER", "DATABASE")
	if alterDbIdx == -1 {
		return nil, localizedError(localization.CypherAdminInvalidSyntax("ALTER DATABASE"), nil)
	}

	// Skip "ALTER" and whitespace to find "DATABASE"
	startPos := alterDbIdx + len("ALTER")
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}
	// Skip "DATABASE" and whitespace
	if startPos+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("DATABASE")], "DATABASE") {
		startPos += len("DATABASE")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
	} else {
		return nil, localizedError(localization.CypherAdminKeywordExpected("ALTER DATABASE", "DATABASE"), nil)
	}

	// Find "SET LIMIT" keyword
	setLimitIdx := findMultiWordKeywordIndex(cypher[startPos:], "SET", "LIMIT")
	if setLimitIdx == -1 {
		return nil, localizedError(localization.CypherAdminClauseExpected("ALTER DATABASE", "SET LIMIT"), nil)
	}

	// Extract database name (between "DATABASE" and "SET LIMIT")
	dbNameEnd := startPos + setLimitIdx
	databaseName := strings.TrimSpace(cypher[startPos:dbNameEnd])
	if databaseName == "" {
		return nil, localizedError(localization.CypherAdminDatabaseNameExpected("ALTER DATABASE"), nil)
	}

	// Skip "SET LIMIT" and whitespace
	limitStartPos := startPos + setLimitIdx + len("SET")
	for limitStartPos < len(cypher) && isWhitespace(cypher[limitStartPos]) {
		limitStartPos++
	}
	if limitStartPos+len("LIMIT") <= len(cypher) && strings.EqualFold(cypher[limitStartPos:limitStartPos+len("LIMIT")], "LIMIT") {
		limitStartPos += len("LIMIT")
		for limitStartPos < len(cypher) && isWhitespace(cypher[limitStartPos]) {
			limitStartPos++
		}
	} else {
		return nil, localizedError(localization.CypherAdminKeywordExpected("ALTER DATABASE", "LIMIT"), nil)
	}

	// Get existing limits or create new ones
	existingLimitsInterface, err := e.dbManager.GetDatabaseLimits(databaseName)
	if err != nil {
		return nil, localizedError(localization.CypherAdminDatabaseNotFound(databaseName, err), err)
	}

	// Type assert to *multidb.Limits
	var existingLimits *multidb.Limits
	if existingLimitsInterface != nil {
		var ok bool
		existingLimits, ok = existingLimitsInterface.(*multidb.Limits)
		if !ok {
			return nil, localizedError(localization.CypherAdminInvalidLimitsType(), nil)
		}
	}

	// Create new limits if none exist, otherwise deep copy
	var limits *multidb.Limits
	if existingLimits == nil {
		limits = &multidb.Limits{}
	} else {
		// Deep copy to avoid modifying the original
		limits = &multidb.Limits{
			Storage: multidb.StorageLimits{
				MaxNodes: existingLimits.Storage.MaxNodes,
				MaxEdges: existingLimits.Storage.MaxEdges,
				MaxBytes: existingLimits.Storage.MaxBytes,
			},
			Query: multidb.QueryLimits{
				MaxQueryTime:         existingLimits.Query.MaxQueryTime,
				MaxResults:           existingLimits.Query.MaxResults,
				MaxConcurrentQueries: existingLimits.Query.MaxConcurrentQueries,
			},
			Connection: multidb.ConnectionLimits{
				MaxConnections: existingLimits.Connection.MaxConnections,
			},
			Rate: multidb.RateLimits{
				MaxQueriesPerSecond: existingLimits.Rate.MaxQueriesPerSecond,
				MaxWritesPerSecond:  existingLimits.Rate.MaxWritesPerSecond,
			},
		}
	}

	// Parse limit assignments: "limit_name = value, limit_name2 = value2"
	limitClause := strings.TrimSpace(cypher[limitStartPos:])
	if limitClause == "" {
		return nil, localizedError(localization.CypherAdminLimitAssignmentExpected(), nil)
	}

	// Split by comma to handle multiple limits
	assignments := strings.Split(limitClause, ",")
	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		if assignment == "" {
			continue
		}

		// Parse "limit_name = value"
		parts := strings.SplitN(assignment, "=", 2)
		if len(parts) != 2 {
			return nil, localizedError(localization.CypherAdminInvalidLimitAssignment(assignment), nil)
		}

		limitName := strings.TrimSpace(strings.ToLower(parts[0]))
		limitValue := strings.TrimSpace(parts[1])

		// Parse and set the limit based on name
		switch limitName {
		case "max_nodes":
			val, err := strconv.ParseInt(limitValue, 10, 64)
			if err != nil {
				return nil, localizedError(localization.CypherAdminInvalidLimitValue(limitName, err), err)
			}
			limits.Storage.MaxNodes = val

		case "max_edges":
			val, err := strconv.ParseInt(limitValue, 10, 64)
			if err != nil {
				return nil, localizedError(localization.CypherAdminInvalidLimitValue(limitName, err), err)
			}
			limits.Storage.MaxEdges = val

		case "max_bytes":
			val, err := strconv.ParseInt(limitValue, 10, 64)
			if err != nil {
				return nil, localizedError(localization.CypherAdminInvalidLimitValue(limitName, err), err)
			}
			limits.Storage.MaxBytes = val

		case "max_query_time":
			duration, err := time.ParseDuration(limitValue)
			if err != nil {
				return nil, localizedError(localization.CypherAdminInvalidDurationLimitValue(limitName, err), err)
			}
			limits.Query.MaxQueryTime = duration

		case "max_results":
			val, err := strconv.ParseInt(limitValue, 10, 64)
			if err != nil {
				return nil, localizedError(localization.CypherAdminInvalidLimitValue(limitName, err), err)
			}
			limits.Query.MaxResults = val

		case "max_concurrent_queries":
			val, err := strconv.Atoi(limitValue)
			if err != nil {
				return nil, localizedError(localization.CypherAdminInvalidLimitValue(limitName, err), err)
			}
			limits.Query.MaxConcurrentQueries = val

		case "max_connections":
			val, err := strconv.Atoi(limitValue)
			if err != nil {
				return nil, localizedError(localization.CypherAdminInvalidLimitValue(limitName, err), err)
			}
			limits.Connection.MaxConnections = val

		case "max_queries_per_second":
			val, err := strconv.Atoi(limitValue)
			if err != nil {
				return nil, localizedError(localization.CypherAdminInvalidLimitValue(limitName, err), err)
			}
			limits.Rate.MaxQueriesPerSecond = val

		case "max_writes_per_second":
			val, err := strconv.Atoi(limitValue)
			if err != nil {
				return nil, localizedError(localization.CypherAdminInvalidLimitValue(limitName, err), err)
			}
			limits.Rate.MaxWritesPerSecond = val

		default:
			return nil, localizedError(localization.CypherAdminUnknownLimitName(limitName), nil)
		}
	}

	// Update limits in database manager (pass as interface{} to match interface)
	err = e.dbManager.SetDatabaseLimits(databaseName, limits)
	if err != nil {
		return nil, localizedError(localization.CypherAdminSetDatabaseLimitsFailed(databaseName, err), err)
	}

	return &ExecuteResult{
		Columns: []string{"database"},
		Rows:    [][]interface{}{{databaseName}},
	}, nil
}

// executeShowLimits handles SHOW LIMITS command.
//
// Lists resource limits for a database in Neo4j-compatible format.
//
// Syntax:
//   - SHOW LIMITS FOR DATABASE database_name
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetDatabaseManager(dbManager)
//
//	// Show limits for database
//	result, err := executor.Execute(ctx, "SHOW LIMITS FOR DATABASE tenant_a", nil)
//
// Returns:
//   - Success: Result with limit information in Neo4j-compatible format
//   - Error: If database doesn't exist
func (e *StorageExecutor) executeShowLimits(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, localizedError(localization.CypherAdminDatabaseManagerUnavailable("SHOW LIMITS"), nil)
	}

	// Find "SHOW LIMITS" keyword position
	showLimitsIdx := findMultiWordKeywordIndex(cypher, "SHOW", "LIMITS")
	if showLimitsIdx == -1 {
		return nil, localizedError(localization.CypherAdminInvalidSyntax("SHOW LIMITS"), nil)
	}

	// Skip "SHOW" and whitespace to find "LIMITS"
	startPos := showLimitsIdx + len("SHOW")
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}
	// Skip "LIMITS" and whitespace
	if startPos+len("LIMITS") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("LIMITS")], "LIMITS") {
		startPos += len("LIMITS")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
	} else {
		return nil, localizedError(localization.CypherAdminKeywordExpected("SHOW LIMITS", "LIMITS"), nil)
	}

	// Check for "FOR DATABASE" clause
	forDbIdx := findMultiWordKeywordIndex(cypher[startPos:], "FOR", "DATABASE")
	if forDbIdx == -1 {
		return nil, localizedError(localization.CypherAdminClauseExpected("SHOW LIMITS", "FOR DATABASE"), nil)
	}

	// Skip "FOR" and whitespace
	dbNameStart := startPos + forDbIdx + len("FOR")
	for dbNameStart < len(cypher) && isWhitespace(cypher[dbNameStart]) {
		dbNameStart++
	}
	// Skip "DATABASE" and whitespace
	if dbNameStart+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[dbNameStart:dbNameStart+len("DATABASE")], "DATABASE") {
		dbNameStart += len("DATABASE")
		for dbNameStart < len(cypher) && isWhitespace(cypher[dbNameStart]) {
			dbNameStart++
		}
	} else {
		return nil, localizedError(localization.CypherAdminKeywordExpected("SHOW LIMITS", "DATABASE"), nil)
	}

	// Extract database name (rest of query)
	databaseName := strings.TrimSpace(cypher[dbNameStart:])
	if databaseName == "" {
		return nil, localizedError(localization.CypherAdminDatabaseNameExpected("SHOW LIMITS"), nil)
	}

	// Get limits from database manager
	limitsInterface, err := e.dbManager.GetDatabaseLimits(databaseName)
	if err != nil {
		return nil, localizedError(localization.CypherAdminDatabaseNotFound(databaseName, err), err)
	}

	// Type assert to *multidb.Limits
	var limits *multidb.Limits
	if limitsInterface != nil {
		var ok bool
		limits, ok = limitsInterface.(*multidb.Limits)
		if !ok {
			return nil, localizedError(localization.CypherAdminInvalidLimitsType(), nil)
		}
	}

	// If no limits set, return empty/unlimited values
	if limits == nil {
		limits = &multidb.Limits{}
	}

	// Format limits in Neo4j-compatible format
	// Neo4j returns: name, type, value, description
	rows := make([][]interface{}, 0)

	// Storage limits
	if limits.Storage.MaxNodes > 0 {
		rows = append(rows, []interface{}{databaseName, "max_nodes", limits.Storage.MaxNodes, "Maximum number of nodes"})
	}
	if limits.Storage.MaxEdges > 0 {
		rows = append(rows, []interface{}{databaseName, "max_edges", limits.Storage.MaxEdges, "Maximum number of edges"})
	}
	if limits.Storage.MaxBytes > 0 {
		rows = append(rows, []interface{}{databaseName, "max_bytes", limits.Storage.MaxBytes, "Maximum storage size in bytes"})
	}

	// Query limits
	if limits.Query.MaxQueryTime > 0 {
		rows = append(rows, []interface{}{databaseName, "max_query_time", limits.Query.MaxQueryTime.String(), "Maximum query execution time"})
	}
	if limits.Query.MaxResults > 0 {
		rows = append(rows, []interface{}{databaseName, "max_results", limits.Query.MaxResults, "Maximum number of query results"})
	}
	if limits.Query.MaxConcurrentQueries > 0 {
		rows = append(rows, []interface{}{databaseName, "max_concurrent_queries", limits.Query.MaxConcurrentQueries, "Maximum concurrent queries"})
	}

	// Connection limits
	if limits.Connection.MaxConnections > 0 {
		rows = append(rows, []interface{}{databaseName, "max_connections", limits.Connection.MaxConnections, "Maximum concurrent connections"})
	}

	// Rate limits
	if limits.Rate.MaxQueriesPerSecond > 0 {
		rows = append(rows, []interface{}{databaseName, "max_queries_per_second", limits.Rate.MaxQueriesPerSecond, "Maximum queries per second"})
	}
	if limits.Rate.MaxWritesPerSecond > 0 {
		rows = append(rows, []interface{}{databaseName, "max_writes_per_second", limits.Rate.MaxWritesPerSecond, "Maximum writes per second"})
	}

	// If no limits are set, return a single row indicating unlimited
	if len(rows) == 0 {
		rows = append(rows, []interface{}{databaseName, "unlimited", nil, "No limits configured (unlimited)"})
	}

	return &ExecuteResult{
		Columns: []string{"database", "limit", "value", "description"},
		Rows:    rows,
	}, nil
}

// truncateQuery truncates a query string to maxLen characters for error messages
func truncateQuery(query string, maxLen int) string {
	query = strings.TrimSpace(query)
	if len(query) <= maxLen {
		return query
	}
	return query[:maxLen] + "..."
}
