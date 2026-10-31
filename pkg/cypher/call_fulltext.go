// Author: Codex implementation lane, 2026-10-26 (NOR-148).
package cypher

// Author: declared-list-owners lane (Claude), 2026-10-30: result headers come from procedure_result_columns.go.

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// ========================================
// Neo4j Fulltext Index Procedures
// ========================================

// callDbIndexFulltextQueryNodes implements db.index.fulltext.queryNodes
// Syntax: CALL db.index.fulltext.queryNodes('indexName', query) YIELD node, score
//
// This is the primary text search procedure for:
//   - Keyword-based memory search
//   - Content discovery
//   - Text matching across node properties
//
// Parameters:
//   - indexName: Name of the fulltext index (from CREATE FULLTEXT INDEX)
//   - query: Search query string (supports AND, OR, NOT, wildcards)
//
// Returns:
//   - node: The matched node with all properties
//   - score: BM25-like relevance score
//
// Scoring Algorithm:
//
//	Uses a simplified BM25-like scoring that considers:
//	- Term frequency (TF): How often query terms appear
//	- Inverse document frequency (IDF): How rare terms are
//	- Field length normalization: Shorter fields score higher
//
// builtInFulltextIndexes are the fulltext index names that need no CREATE
// FULLTEXT INDEX: they search the default searchable properties.
var builtInFulltextIndexes = map[string]bool{
	"default":     true,
	"node_search": true,
}

// fulltextIndexNeedsNoDeclaration reports a name that exists without CREATE: the
// empty name and the built-in indexes.
func fulltextIndexNeedsNoDeclaration(indexName string) bool {
	return indexName == "" || builtInFulltextIndexes[indexName]
}

// requireFulltextIndex is Neo4j's check shared by the fulltext query
// procedures (nodes and relationships): querying a fulltext index that
// doesn't exist (declared is false) fails, unless the name is a built-in one
// or empty.
func requireFulltextIndex(indexName string, declared bool) error {
	if declared || fulltextIndexNeedsNoDeclaration(indexName) {
		return nil
	}
	return localizedError(localization.CypherSpecializedCallsFulltextIndexNotFound(indexName), nil)
}

func (e *StorageExecutor) callDbIndexFulltextQueryNodes(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: columnNames(nodeScoreColumns),
		Rows:    [][]interface{}{},
	}
	indexName, query, opts, err := fulltextQueryArguments("db.index.fulltext.queryNodes", args)
	if err != nil {
		return nil, err
	}
	if query == "" {
		return result, nil
	}

	// Get index configuration if it exists
	var targetLabels []string
	var targetProperties []string

	schema := e.storage.GetSchema()
	if schema != nil {
		if ftIdx, exists := schema.GetFulltextIndex(indexName); exists {
			targetLabels = ftIdx.Labels
			targetProperties = ftIdx.Properties
		}
	}

	if err := requireFulltextIndex(indexName, len(targetProperties) > 0); err != nil {
		return nil, err
	}

	// Default searchable properties if no index config or using built-in index
	// Includes additional searchable properties: path, workerRole, requirements
	if len(targetProperties) == 0 {
		targetProperties = []string{"content", "text", "title", "name", "description", "body", "summary", "path", "workerRole", "requirements"}
	}

	wildcard := isFulltextWildcard(query)
	presence, isPresence := fulltextFieldPresenceQuery(query)
	if wildcard || isPresence {
		if isPresence && !containsString(targetProperties, presence) {
			return result, nil
		}
		if err := e.fulltextUnrankedRows(ctx, indexName, targetLabels, targetProperties, false, wildcard, presence, opts, result); err != nil {
			return nil, err
		}
		applyFulltextOptions(result, opts)
		return result, nil
	}
	hits, err := e.fulltextHits(ctx, indexName, query, targetLabels, targetProperties, false)
	if err != nil {
		return nil, err
	}
	for _, hit := range hits {
		result.Rows = append(result.Rows, []interface{}{storage.CopyNode(hit.row.node), hit.score})
	}
	applyFulltextOptions(result, opts)

	return result, nil
}

// buildNodeFulltextDoc canonicalizes a node into an ftDoc: every declared
// index property is lower-cased into the properties map, all property
// values are concatenated into contentLower for BM25 scoring, and the
// token count is captured for length normalization. Returns nil if the
// node has no content in any indexed property (empty-doc filter is
// preserved from the pre-parser path).
func buildNodeFulltextDoc(node *storage.Node, properties []string) *ftDoc {
	if node == nil {
		return nil
	}
	return buildFulltextDocFromProperties(node.Properties, extractTextContent(node, properties))
}

// buildFulltextDocFromProperties builds the shared ftDoc: every non-empty
// property becomes a lowercased search field with its raw value preserved,
// and the entity-specific content string supplies the searchable body. The
// node and edge builders differ only in how they extract that content.
func buildFulltextDocFromProperties(properties map[string]interface{}, content string) *ftDoc {
	props := make(map[string]string, len(properties))
	rawProps := make(map[string]string, len(properties))
	for k, v := range properties {
		if v == nil {
			continue
		}
		raw := fmt.Sprintf("%v", v)
		if raw == "" {
			continue
		}
		props[lowerASCII(k)] = lowerASCII(raw)
		rawProps[lowerASCII(k)] = raw
	}
	if content == "" {
		return nil
	}
	contentLower := lowerASCII(content)
	return &ftDoc{
		properties:    props,
		rawProperties: rawProps,
		contentLower:  contentLower,
		contentTokenN: len(strings.Fields(content)),
	}
}

// decodeCypherStringLiteral strips surrounding single or double quotes from a
// Cypher string literal and undoes the standard backslash-escape encoding
// (`\\` -> `\`, `\'` -> `'`, `\"` -> `"`, `\n`, `\t`, `\r`). This is what
// parameter substitution produces in valueToLiteral for values that end up
// as the fulltext query string; without this decode step a user query of
// `Cloud\Trail` reaches the Lucene parser as `Cloud\\Trail`, defeating the
// escape rule.
func decodeCypherStringLiteral(s string) string {
	if decoded, quoted := cyphertext.DecodeString(s, cyphertext.AcceptedSource); quoted {
		return decoded
	}
	return s
}

type fulltextQueryOptions struct {
	skip  int
	limit int
}

// fulltextQueryArguments reads db.index.fulltext.queryNodes /
// queryRelationships(indexName, queryString, options = {}): the index name
// and query are STRINGs the procedure needs, and options' skip and limit are
// non-negative whole numbers.
func fulltextQueryArguments(procedure string, args []interface{}) (string, string, fulltextQueryOptions, error) {
	opts := fulltextQueryOptions{skip: 0, limit: -1}
	indexName, err := requiredProcedureString(procedure, args, 0, "indexName")
	if err != nil {
		return "", "", opts, err
	}
	query, err := requiredProcedureString(procedure, args, 1, "queryString")
	if err != nil {
		return "", "", opts, err
	}
	options, err := optionalProcedureMap(procedure, args, 2, "options")
	if err != nil {
		return "", "", opts, err
	}
	if v, ok := options["skip"]; ok {
		i, convOK := fulltextOptionToInt(v)
		if !convOK || i < 0 {
			return "", "", opts, localizedError(localization.CypherSpecializedCallsFulltextOptionInvalid("skip", v), nil)
		}
		opts.skip = i
	}
	if v, ok := options["limit"]; ok {
		i, convOK := fulltextOptionToInt(v)
		if !convOK || i < 0 {
			return "", "", opts, localizedError(localization.CypherSpecializedCallsFulltextOptionInvalid("limit", v), nil)
		}
		opts.limit = i
	}
	return indexName, query, opts, nil
}

func fulltextOptionToInt(v interface{}) (int, bool) {
	return valueconvert.ToInt(v, valueconvert.IntegerFloat|valueconvert.IntegerWholeFloat|valueconvert.IntegerStrings|valueconvert.IntegerTrimStrings)
}

func applyFulltextOptions(result *ExecuteResult, opts fulltextQueryOptions) {
	if result == nil || len(result.Rows) == 0 {
		return
	}
	if opts.skip > 0 {
		if opts.skip >= len(result.Rows) {
			result.Rows = [][]interface{}{}
			return
		}
		result.Rows = result.Rows[opts.skip:]
	}
	if opts.limit >= 0 && opts.limit < len(result.Rows) {
		result.Rows = result.Rows[:opts.limit]
	}
}

// isFulltextWildcard reports whether the query should be treated as
// the Lucene "match every document" pattern — bare "*" or "*:*". An
// empty query is handled separately by the caller (early-return with
// no rows). Trimming + lowercasing keeps `" * "`, `*:*`, and `*` all
// canonical.
func isFulltextWildcard(query string) bool {
	q := lowerASCII(strings.TrimSpace(query))
	return q == "*" || q == "*:*"
}

// fulltextFieldPresenceQuery recognizes the Lucene field-presence
// shorthand `<prop>:*` (every document that has a non-empty value for
// the named property). Returns the property name and true on a match.
// The property name is preserved case-as-written so the caller can
// look it up in node.Properties without lowercasing.
//
// The full wildcard form ("*", "*:*") is recognized by isFulltextWildcard
// and short-circuited before this function is reached.
func fulltextFieldPresenceQuery(query string) (string, bool) {
	q := strings.TrimSpace(query)
	colon := strings.Index(q, ":")
	if colon <= 0 || colon == len(q)-1 {
		return "", false
	}
	name := strings.TrimSpace(q[:colon])
	value := strings.TrimSpace(q[colon+1:])
	if name == "" || value != "*" {
		return "", false
	}
	// Reject names that contain whitespace or operator characters; that
	// would mean the query is something like `name OR foo:*` (a more
	// complex Lucene expression that needs the BM25 path).
	for _, r := range name {
		if !cyphertext.NamePart(r) {
			return "", false
		}
	}
	return name, true
}

// nodeHasNonEmptyProperty reports whether node carries a property
// named propName whose stringified value is non-empty. The "non-empty"
// check matches Neo4j-Lucene behavior: a field with an empty string
// value is treated as having no posting and is excluded from
// `<field>:*` queries.
func nodeHasNonEmptyProperty(node *storage.Node, propName string) bool {
	if node == nil {
		return false
	}
	val, ok := node.Properties[propName]
	if !ok || val == nil {
		return false
	}
	if s, ok := val.(string); ok && s == "" {
		return false
	}
	return true
}

// matchesLabels checks if a node has any of the target labels (empty = all match)
func matchesLabels(node *storage.Node, targetLabels []string) bool {
	if len(targetLabels) == 0 {
		return true
	}
	for _, nl := range node.Labels {
		for _, tl := range targetLabels {
			if nl == tl {
				return true
			}
		}
	}
	return false
}

// extractTextContent extracts searchable text content from a node
func extractTextContent(node *storage.Node, properties []string) string {
	var content strings.Builder

	for _, propName := range properties {
		if val, ok := node.Properties[propName]; ok {
			content.WriteString(fmt.Sprintf("%v ", val))
		}
	}

	return strings.TrimSpace(content.String())
}

// isBackslashEscaped reads decoded Lucene phrase text, not Cypher tokens.
func isBackslashEscaped(s string, idx int) bool {
	if idx <= 0 {
		return false
	}
	backslashes := 0
	for i := idx - 1; i >= 0 && s[i] == '\\'; i-- {
		backslashes++
	}
	return backslashes%2 == 1
}
