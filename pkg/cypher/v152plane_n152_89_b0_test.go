package cypher

// NOR-403 whole-scope reproduction on the shipped v1.5.1-equivalent tree
// (d6ec7766). Reproduction only: this file adds no production seam and no
// production change.
//
// Provenance and limits
//
// The historical lab jobs that obtained the exact statement
// (i20261104203608-62168-bb5c, i20261104203638-50224-92f1,
// i20261104203145-22016-d7c7) and their private PD corpus are not present in
// this repository or its history (bounded search for the job ids and for
// "NOR403"/"NOR-403" returns nothing outside these files). The statement below
// is therefore the recorded equivalent shape from
// procedure_yield_subquery_scope_test.go (TestProcedureYieldWhereEligibilityStates)
// carried onto the NOR-403 label set the report names
// (PDDocument/PDCollection/PDVersion/PDSummary) and the original tail
//
//	RETURN node, score ORDER BY score DESC, node.id LIMIT $limit
//
// It is not a replay of the private workload and it does not claim to be the
// exact historical statement text.
//
// What is asserted through the public Execute path only
//
//   - Row identity, score, ordering and LIMIT truncation of the filtered
//     procedure statement against a hand-recorded decision table.
//   - Full filter truth: the complete multi-EXISTS predicate decides on every
//     branch (document collection membership, summary currency, collection
//     owner, document owner with version and source match), not on a single
//     EXISTS. The statement text, the recorded decisions and an independent Go
//     filter model are cross-checked.
//   - Cold (fresh executor), warm (retained corpus) and query-cache
//     executions, plus stability across a schema publication.
//   - That the actual search service, not a per-statement fallback, serves the
//     corpus, and that the callback population is exactly the index membership.
//
// The small isolated fixture is deterministic: identical two-token source
// bodies produce identical scores, so the recorded ordering exercises both the
// score term and the node.id tie-break.

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testutil/memorypressure"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/caseinputs"
	"github.com/stretchr/testify/require"
)

const (
	// nor403FulltextIndex is the one fulltext index the statement queries. Both
	// source and summary rows are members, which is why the eligibility
	// predicate distinguishes them by label.
	nor403FulltextIndex = caseinputs.NOR403FulltextIndex
	// nor403QueryTerm matches every row of the isolated corpus, so the raw and
	// filtered populations are both fully known.
	nor403QueryTerm = "boiler"
)

// nor403RawStatement is the same procedure call and ranked projection without
// the eligibility predicate: the raw-row reference the filter is measured
// against.
const nor403RawStatement = caseinputs.NOR403RawStatement

// nor403EligibilityStatement is the recorded equivalent of the NOR-403
// statement: db.index.fulltext.queryNodes yields node/score, the full
// multi-EXISTS eligibility predicate filters the raw rows, and the public
// RETURN/ORDER BY/LIMIT tail is unchanged.
const nor403EligibilityStatement = caseinputs.NOR403EligibilityStatement

// nor403SchemaStatements are the schema objects the production database
// reports: PDDocument.id, PDCollection.id and PDVersion.id UNIQUE plus an
// ONLINE property index on PDVersion.document, and the fulltext index over both
// indexed labels.
var nor403SchemaStatements = []string{
	"CREATE CONSTRAINT nor403_pddocument_id FOR (d:PDDocument) REQUIRE d.id IS UNIQUE",
	"CREATE CONSTRAINT nor403_pdcollection_id FOR (c:PDCollection) REQUIRE c.id IS UNIQUE",
	"CREATE CONSTRAINT nor403_pdversion_id FOR (v:PDVersion) REQUIRE v.id IS UNIQUE",
	"CREATE INDEX nor403_pdversion_document FOR (v:PDVersion) ON (v.document)",
	"CREATE FULLTEXT INDEX " + nor403FulltextIndex + " FOR (n:PDSource|PDSummary) ON EACH [n.text]",
}

// nor403RequiredReasons are the deciding branches the isolated corpus must
// cover. Each name is the reason recorded on at least one row of the decision
// table below, so the test proves the complete predicate is evaluated instead
// of a single EXISTS.
var nor403RequiredReasons = []string{
	"document-membership-absent",
	"document-archived-collection",
	"document-open-collection",
	"summary-collection-owner-open",
	"summary-collection-owner-archived",
	"summary-not-current",
	"summary-document-owner-version-match",
	"summary-document-owner-archived-document",
	"summary-document-owner-version-mismatch",
	"summary-document-owner-source-mismatch",
	"summary-owner-kind-unhandled",
}

// nor403Collection is one PDCollection. A nil archived value is a collection
// without the property, which coalesce(archived, false) reads as open.
type nor403Collection struct {
	id       string
	archived *bool
}

// nor403Document is one PDDocument with its PDCollection memberships.
type nor403Document struct {
	id               string
	currentVersionID string
	memberships      []string
}

// nor403Version is one PDVersion. document and id are the property-map values
// the predicate matches; current_source_id is the summary currency source.
type nor403Version struct {
	id, document, currentSourceID string
}

// nor403Row is one fulltext-indexed node: a PDSource, or a PDSummary when
// summary is set. documentID "" is an absent document_id property, current nil
// is an absent current property. visible and reason are the hand-recorded
// decision for the full predicate, written independently of the Go model below;
// generated corpora record visible from that model instead.
type nor403Row struct {
	id         string
	documentID string
	summary    bool
	current    *bool
	ownerKind  string
	ownerID    string
	sourceIDs  []string
	text       string
	visible    bool
	reason     string
}

// nor403Truth is the independent Go model of the predicate, used to cross-check
// the recorded decisions here and to decide eligibility in the generated
// benchmark corpus and the single-EXISTS control.
type nor403Truth struct {
	collections map[string]*bool
	documents   map[string]nor403Document
	versions    map[string]nor403Version
}

func nor403Bool(value bool) *bool { return &value }

var nor403IsolatedCollections = []nor403Collection{
	{id: "c01", archived: nor403Bool(false)},
	{id: "c02", archived: nor403Bool(true)},
	{id: "c03", archived: nor403Bool(false)},
	{id: "c04"}, // archived property absent: coalesce(archived, false) is open
}

var nor403IsolatedDocuments = []nor403Document{
	{id: "d01", currentVersionID: "v01"},
	{id: "d02", currentVersionID: "v02", memberships: []string{"c02"}},
	{id: "d03", currentVersionID: "v03", memberships: []string{"c02", "c03"}},
	{id: "d04", currentVersionID: "v04", memberships: []string{"c03"}},
	{id: "d05", currentVersionID: "v05", memberships: []string{"c02"}},
	{id: "d06", currentVersionID: "v08"}, // v08 belongs to d08: property-map mismatch
	{id: "d07", memberships: []string{"c04"}},
	{id: "d08", currentVersionID: "v08", memberships: []string{"c04", "c02"}},
}

var nor403IsolatedVersions = []nor403Version{
	{id: "v01", document: "d01", currentSourceID: "s01"},
	{id: "v02", document: "d02", currentSourceID: "s02"},
	{id: "v03", document: "d03", currentSourceID: "s03"},
	{id: "v04", document: "d04", currentSourceID: "s04"},
	{id: "v05", document: "d05", currentSourceID: "s05"},
	{id: "v08", document: "d08", currentSourceID: "s08"},
}

// nor403IsolatedRows is the decision table. Every row body is two tokens for
// sources and three for summaries, so sources tie with each other and summaries
// tie with each other: the recorded order therefore exercises the node.id
// tie-break, not only the score term.
var nor403IsolatedRows = []nor403Row{
	{id: "s01", documentID: "d01", text: "boiler alpha", visible: true, reason: "document-membership-absent"},
	{id: "s02", documentID: "d02", text: "boiler beta", visible: false, reason: "document-archived-collection"},
	{id: "s03", documentID: "d03", text: "boiler gamma", visible: true, reason: "document-open-collection"},
	{id: "s04", documentID: "d04", text: "boiler delta", visible: true, reason: "document-open-collection"},
	{id: "s05", documentID: "d05", text: "boiler epsilon", visible: false, reason: "document-archived-collection"},
	{id: "s06", text: "boiler zeta", visible: true, reason: "document-membership-absent"},
	{id: "s07", documentID: "d99", text: "boiler eta", visible: true, reason: "document-membership-absent"},
	{id: "s08", documentID: "d08", text: "boiler theta", visible: true, reason: "document-open-collection"},
	{id: "s09", documentID: "d06", text: "boiler iota", visible: true, reason: "document-membership-absent"},
	{id: "s10", documentID: "d07", text: "boiler kappa", visible: true, reason: "document-open-collection"},

	{id: "m01", summary: true, current: nor403Bool(true), ownerKind: "collection", ownerID: "c03", sourceIDs: []string{"s04"}, text: "boiler summary one", visible: true, reason: "summary-collection-owner-open"},
	{id: "m02", summary: true, current: nor403Bool(true), ownerKind: "collection", ownerID: "c02", text: "boiler summary two", visible: false, reason: "summary-collection-owner-archived"},
	{id: "m03", summary: true, current: nor403Bool(false), ownerKind: "collection", ownerID: "c03", text: "boiler summary three", visible: false, reason: "summary-not-current"},
	{id: "m04", summary: true, current: nor403Bool(true), ownerKind: "document", ownerID: "d04", sourceIDs: []string{"s04"}, text: "boiler summary four", visible: true, reason: "summary-document-owner-version-match"},
	{id: "m05", summary: true, current: nor403Bool(true), ownerKind: "document", ownerID: "d05", sourceIDs: []string{"s05"}, text: "boiler summary five", visible: false, reason: "summary-document-owner-archived-document"},
	{id: "m06", summary: true, current: nor403Bool(true), ownerKind: "document", ownerID: "d06", sourceIDs: []string{"s08"}, text: "boiler summary six", visible: false, reason: "summary-document-owner-version-mismatch"},
	{id: "m07", summary: true, current: nor403Bool(true), ownerKind: "document", ownerID: "d07", sourceIDs: []string{"s10"}, text: "boiler summary seven", visible: false, reason: "summary-document-owner-version-mismatch"},
	{id: "m08", summary: true, ownerKind: "collection", ownerID: "c03", text: "boiler summary eight", visible: false, reason: "summary-not-current"},
	{id: "m09", summary: true, current: nor403Bool(true), ownerKind: "workspace", ownerID: "c03", text: "boiler summary nine", visible: false, reason: "summary-owner-kind-unhandled"},
	{id: "m10", summary: true, current: nor403Bool(true), ownerKind: "document", ownerID: "d03", sourceIDs: []string{"s03"}, text: "boiler summary ten", visible: true, reason: "summary-document-owner-version-match"},
	{id: "m11", summary: true, current: nor403Bool(true), ownerKind: "document", ownerID: "d08", sourceIDs: []string{"s08"}, text: "boiler summary eleven", visible: true, reason: "summary-document-owner-version-match"},
	{id: "m12", summary: true, current: nor403Bool(true), ownerKind: "document", ownerID: "d01", sourceIDs: []string{"s06"}, text: "boiler summary twelve", visible: false, reason: "summary-document-owner-source-mismatch"},
	{id: "m13", summary: true, current: nor403Bool(true), ownerKind: "document", ownerID: "d01", sourceIDs: []string{"s01", "s06"}, text: "boiler summary thirteen", visible: true, reason: "summary-document-owner-version-match"},
	{id: "m14", summary: true, documentID: "d02", current: nor403Bool(true), ownerKind: "collection", ownerID: "c03", text: "boiler summary fourteen", visible: false, reason: "document-archived-collection"},
}

// nor403ObservedRow is one projected row: the node's id property and the
// procedure's score.
type nor403ObservedRow struct {
	ID    string
	Score float64
}

// nor403Fixture is one isolated corpus plus its recorded decisions and the
// independent filter model.
type nor403Fixture struct {
	store storage.Engine
	rows  []nor403Row
	byID  map[string]nor403Row
	truth *nor403Truth
}

func newNOR403Truth(collections []nor403Collection, documents []nor403Document, versions []nor403Version) *nor403Truth {
	truth := &nor403Truth{
		collections: make(map[string]*bool, len(collections)),
		documents:   make(map[string]nor403Document, len(documents)),
		versions:    make(map[string]nor403Version, len(versions)),
	}
	for _, collection := range collections {
		truth.collections[collection.id] = collection.archived
	}
	for _, document := range documents {
		truth.documents[document.id] = document
	}
	for _, version := range versions {
		truth.versions[version.id] = version
	}
	return truth
}

// memberships reports whether the predicate's pattern can match a PDDocument
// with a PDCollection MEMBER_OF edge for documentID.
func (truth *nor403Truth) memberships(documentID string) (members bool, open bool) {
	document, ok := truth.documents[documentID]
	if !ok {
		return false, false
	}
	for _, collectionID := range document.memberships {
		archived, exists := truth.collections[collectionID]
		if !exists {
			continue
		}
		members = true
		if archived == nil || !*archived {
			open = true
		}
	}
	return members, open
}

// documentVisible is
//
//	NOT EXISTS { (:PDDocument {id: documentID})-[:MEMBER_OF]->(:PDCollection) }
//	OR EXISTS { ...->(open_in:PDCollection) WHERE NOT coalesce(open_in.archived, false) }
func (truth *nor403Truth) documentVisible(documentID string) bool {
	members, open := truth.memberships(documentID)
	return !members || open
}

// summaryDocumentMatches is the document-owner EXISTS of the summary branch.
func (truth *nor403Truth) summaryDocumentMatches(ownerID string, sourceIDs []string) bool {
	document, ok := truth.documents[ownerID]
	if !ok || document.currentVersionID == "" {
		return false
	}
	version, ok := truth.versions[document.currentVersionID]
	if !ok || version.document != document.id {
		return false
	}
	if !nor403ContainsString(sourceIDs, version.currentSourceID) {
		return false
	}
	return truth.documentVisible(document.id)
}

// sourceVisible evaluates the complete predicate for one indexed row.
func (truth *nor403Truth) sourceVisible(row nor403Row) bool {
	if !truth.documentVisible(row.documentID) {
		return false
	}
	if !row.summary {
		return true
	}
	if row.current == nil || !*row.current {
		return false
	}
	switch row.ownerKind {
	case "collection":
		archived, exists := truth.collections[row.ownerID]
		return exists && (archived == nil || !*archived)
	case "document":
		return truth.summaryDocumentMatches(row.ownerID, row.sourceIDs)
	}
	return false
}

func nor403ContainsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func nor403QuotedList(values []string) string {
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = fmt.Sprintf("%q", value)
	}
	return strings.Join(quoted, ", ")
}

func nor403ObservedIDs(rows []nor403ObservedRow) []string {
	ids := make([]string, len(rows))
	for i, row := range rows {
		ids[i] = row.ID
	}
	return ids
}

// nor403RankedRows is the one owner of the statement's ORDER BY score DESC,
// node.id and LIMIT projection: it keeps the raw rows whose id keep accepts and
// applies the ranked window. The recorded decision table, the generated
// benchmark corpus and the single-EXISTS control all select through it.
func nor403RankedRows(raw []nor403ObservedRow, limit int, keep func(id string) bool) []nor403ObservedRow {
	kept := make([]nor403ObservedRow, 0, len(raw))
	for _, row := range raw {
		if keep(row.ID) {
			kept = append(kept, row)
		}
	}
	sort.SliceStable(kept, func(i, j int) bool {
		if kept[i].Score != kept[j].Score {
			return kept[i].Score > kept[j].Score
		}
		return kept[i].ID < kept[j].ID
	})
	if limit >= 0 && limit < len(kept) {
		kept = kept[:limit]
	}
	return kept
}

// recordedEligible reports the hand-recorded decision for one isolated row.
func (fixture *nor403Fixture) recordedEligible(id string) bool {
	row, ok := fixture.byID[id]
	return ok && row.visible
}

// nor403SeedStatements renders the tables as public Cypher writes, so the corpus
// is created through the same Execute path the assertions use.
func nor403SeedStatements(collections []nor403Collection, documents []nor403Document, versions []nor403Version, rows []nor403Row) []string {
	statements := make([]string, 0, len(collections)+len(documents)*2+len(versions)+len(rows))
	for _, collection := range collections {
		if collection.archived == nil {
			statements = append(statements, fmt.Sprintf("CREATE (:PDCollection {id: %q})", collection.id))
			continue
		}
		statements = append(statements, fmt.Sprintf("CREATE (:PDCollection {id: %q, archived: %t})", collection.id, *collection.archived))
	}
	for _, document := range documents {
		if document.currentVersionID == "" {
			statements = append(statements, fmt.Sprintf("CREATE (:PDDocument {id: %q})", document.id))
		} else {
			statements = append(statements, fmt.Sprintf("CREATE (:PDDocument {id: %q, current_version_id: %q})", document.id, document.currentVersionID))
		}
		for _, collectionID := range document.memberships {
			statements = append(statements,
				fmt.Sprintf("MATCH (d:PDDocument {id: %q}), (c:PDCollection {id: %q}) CREATE (d)-[:MEMBER_OF]->(c)", document.id, collectionID))
		}
	}
	for _, version := range versions {
		statements = append(statements,
			fmt.Sprintf("CREATE (:PDVersion {id: %q, document: %q, current_source_id: %q})", version.id, version.document, version.currentSourceID))
	}
	for _, row := range rows {
		properties := []string{fmt.Sprintf("id: %q", row.id), fmt.Sprintf("text: %q", row.text)}
		if row.documentID != "" {
			properties = append(properties, fmt.Sprintf("document_id: %q", row.documentID))
		}
		label := "PDSource"
		if row.summary {
			label = "PDSummary"
			if row.current != nil {
				properties = append(properties, fmt.Sprintf("current: %t", *row.current))
			}
			properties = append(properties,
				fmt.Sprintf("owner_kind: %q", row.ownerKind),
				fmt.Sprintf("owner_id: %q", row.ownerID),
				"source_ids: ["+nor403QuotedList(row.sourceIDs)+"]")
		}
		statements = append(statements, fmt.Sprintf("CREATE (:%s {%s})", label, strings.Join(properties, ", ")))
	}
	return statements
}

// newNOR403Fixture seeds the isolated corpus through public Cypher and returns
// it with its recorded decisions.
func newNOR403Fixture(t *testing.T) *nor403Fixture {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor403")
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	for _, statement := range nor403SchemaStatements {
		_, err := exec.Execute(t.Context(), statement, nil)
		require.NoError(t, err, statement)
	}
	for _, statement := range nor403SeedStatements(nor403IsolatedCollections, nor403IsolatedDocuments, nor403IsolatedVersions, nor403IsolatedRows) {
		_, err := exec.Execute(t.Context(), statement, nil)
		require.NoError(t, err, statement)
	}
	byID := make(map[string]nor403Row, len(nor403IsolatedRows))
	for _, row := range nor403IsolatedRows {
		byID[row.id] = row
	}
	return &nor403Fixture{
		store: store,
		rows:  nor403IsolatedRows,
		byID:  byID,
		truth: newNOR403Truth(nor403IsolatedCollections, nor403IsolatedDocuments, nor403IsolatedVersions),
	}
}

// newExecutor builds one public entry point over the same store, with the
// result cache policy the case asks for.
func (fixture *nor403Fixture) newExecutor(policy QueryCachePolicy) *StorageExecutor {
	return NewStorageExecutor(fixture.store, policy)
}

// nor403ObservedRows projects node/score rows and asserts the declared columns
// and value shapes of the public procedure.
func nor403ObservedRows(tb testing.TB, result *ExecuteResult) []nor403ObservedRow {
	tb.Helper()
	require.Equal(tb, []string{"node", "score"}, result.Columns,
		"the statement projects node and score in that order")
	rows := make([]nor403ObservedRow, 0, len(result.Rows))
	for _, row := range result.Rows {
		require.Len(tb, row, 2)
		node, ok := row[0].(*storage.Node)
		require.True(tb, ok, "the procedure yields the node entity, got %T", row[0])
		id, ok := node.Properties["id"].(string)
		require.True(tb, ok, "every corpus row carries a string id, got %T", node.Properties["id"])
		score, ok := row[1].(float64)
		require.True(tb, ok, "the procedure yields a float64 score, got %T", row[1])
		require.Positive(tb, score, "a matched row keeps a positive score")
		rows = append(rows, nor403ObservedRow{ID: id, Score: score})
	}
	return rows
}

// nor403RunStatement executes one ranked procedure statement with the query
// term and limit parameters.
func nor403RunStatement(tb testing.TB, exec *StorageExecutor, statement, term string, limit int64) []nor403ObservedRow {
	tb.Helper()
	result, err := exec.Execute(tb.Context(), statement,
		map[string]interface{}{"query": term, "limit": limit})
	require.NoError(tb, err, statement)
	return nor403ObservedRows(tb, result)
}

// nor403AssertRankedOrder checks the ORDER BY score DESC, node.id contract on an
// already projected row list.
func nor403AssertRankedOrder(t *testing.T, rows []nor403ObservedRow) {
	t.Helper()
	for i := 1; i < len(rows); i++ {
		previous, current := rows[i-1], rows[i]
		if previous.Score == current.Score {
			require.Less(t, previous.ID, current.ID,
				"equal scores order by node.id ascending: %s then %s", previous.ID, current.ID)
			continue
		}
		require.Greater(t, previous.Score, current.Score,
			"rows order by score descending: %s (%f) then %s (%f)", previous.ID, previous.Score, current.ID, current.Score)
	}
}

// nor403AssertEligibilityStatement runs one execution state (cold, warm,
// cache-disabled or after a schema publication) and compares the public rows to
// the recorded decisions.
func nor403AssertEligibilityStatement(t *testing.T, exec *StorageExecutor, fixture *nor403Fixture, state string) {
	t.Helper()
	raw := nor403RunStatement(t, exec, nor403RawStatement, nor403QueryTerm, int64(len(fixture.rows)))
	require.Len(t, raw, len(fixture.rows), "%s: every isolated row matches the term", state)
	nor403AssertRankedOrder(t, raw)

	full := nor403RunStatement(t, exec, nor403EligibilityStatement, nor403QueryTerm, int64(len(fixture.rows)))
	expectedFull := nor403RankedRows(raw, len(fixture.rows), fixture.recordedEligible)
	require.NotEmpty(t, expectedFull, "%s: the isolated corpus keeps eligible rows", state)
	require.Equal(t, expectedFull, full, "%s: filtered rows, scores and order", state)
	nor403AssertRankedOrder(t, full)
	require.Less(t, len(full), len(raw),
		"%s: the eligibility predicate removes raw rows instead of passing all of them", state)

	// Every returned node is a raw row with the filter's own score.
	rawScore := make(map[string]float64, len(raw))
	for _, row := range raw {
		rawScore[row.ID] = row.Score
	}
	for _, row := range full {
		require.Equal(t, rawScore[row.ID], row.Score, "%s: %s keeps its raw score", state, row.ID)
	}

	const limit = 3
	truncated := nor403RunStatement(t, exec, nor403EligibilityStatement, nor403QueryTerm, limit)
	require.Equal(t, nor403RankedRows(raw, limit, fixture.recordedEligible), truncated,
		"%s: LIMIT $limit truncation", state)
	require.Len(t, truncated, limit, "%s: the eligible population exceeds the requested limit", state)
	t.Logf("%s: %d raw rows, %d eligible rows; eligible order %v; limit %d rows %v",
		state, len(raw), len(full), nor403ObservedIDs(full), limit, nor403ObservedIDs(truncated))

	// The tie-break is exercised, not merely the score order: at least two
	// eligible rows share a score.
	tied := false
	for i := 1; i < len(full); i++ {
		if full[i-1].Score == full[i].Score {
			tied = true
			break
		}
	}
	require.True(t, tied, "%s: the corpus exercises the node.id tie-break", state)
}

// TestNOR403FulltextExistsEligibility reproduces the statement's row, filter,
// order and limit behavior on an isolated deterministic corpus under both
// statement parsers, in cold, warm and query-cache execution states.
func TestNOR403FulltextExistsEligibility(t *testing.T) {
	memorypressure.WithoutRuntimePressure(t)

	// Statement-shape guard: the recorded statement keeps the full predicate.
	require.Equal(t, 6, strings.Count(nor403EligibilityStatement, "EXISTS {"),
		"the full multi-EXISTS predicate is under test, not a single EXISTS")
	require.Contains(t, nor403EligibilityStatement, "YIELD node, score")
	require.Contains(t, nor403EligibilityStatement, "RETURN node, score ORDER BY score DESC, node.id LIMIT $limit")

	forEachParser(t, func(t *testing.T) {
		fixture := newNOR403Fixture(t)

		// Recorded decisions agree with the independent filter model, and the
		// corpus covers every deciding branch.
		reasons := make([]string, 0, len(fixture.rows))
		visible := 0
		for _, row := range fixture.rows {
			reasons = append(reasons, row.reason)
			if row.visible {
				visible++
			}
			require.Equal(t, row.visible, fixture.truth.sourceVisible(row),
				"row %s: the filter model disagrees with the recorded decision (%s)", row.id, row.reason)
		}
		require.Subset(t, reasons, nor403RequiredReasons,
			"every deciding branch of the predicate has a recorded row")
		require.Equal(t, 13, visible, "the isolated corpus records thirteen eligible rows")

		// The declared schema is visible to the executor, and the property
		// index on PDVersion.document is one the planner can use.
		schema := fixture.store.GetSchema()
		require.Len(t, schema.GetAllConstraints(), 3)
		_, ok := schema.GetFulltextIndex(nor403FulltextIndex)
		require.True(t, ok, "the fulltext index the statement queries is declared")
		explain, err := fixture.newExecutor(QueryCachePolicy{MaxEntries: 0, TTL: 0}).Execute(t.Context(),
			"EXPLAIN MATCH (v:PDVersion {document: 'd04'}) RETURN v.id", nil)
		require.NoError(t, err)
		plan := executionPlanFromResult(t, explain)
		require.Contains(t, collectOperatorTypes(plan.Root), "NodeIndexSeek",
			"the declared PDVersion.document property index is planner-usable")

		modes := []struct {
			name   string
			policy QueryCachePolicy
			fresh  bool
			runs   int
		}{
			{name: "cold/cache-disabled", policy: QueryCachePolicy{MaxEntries: 0, TTL: 0}, fresh: true, runs: 2},
			{name: "warm/cache-disabled", policy: QueryCachePolicy{MaxEntries: 0, TTL: 0}, runs: 2},
			{name: "warm/query-cache", policy: QueryCachePolicy{MaxEntries: 64, TTL: 5 * time.Minute}, runs: 2},
		}
		for _, mode := range modes {
			t.Run(mode.name, func(t *testing.T) {
				var exec *StorageExecutor
				if !mode.fresh {
					exec = fixture.newExecutor(mode.policy)
				}
				for run := 0; run < mode.runs; run++ {
					if mode.fresh {
						// A fresh executor owns no retained corpus: the first
						// execution of each run is a cold acquisition.
						exec = fixture.newExecutor(mode.policy)
					}
					if mode.policy.MaxEntries == 0 {
						require.Nil(t, exec.cache, "MaxEntries:0 disables the result cache")
					} else {
						require.NotNil(t, exec.cache, "the warm case must actually hold a result cache")
					}
					nor403AssertEligibilityStatement(t, exec, fixture, fmt.Sprintf("%s run %d", mode.name, run+1))
				}
			})
		}

		// A schema publication between executions must not change either the
		// corpus the callback serves or the filtered rows.
		t.Run("schema-publication", func(t *testing.T) {
			exec := fixture.newExecutor(QueryCachePolicy{MaxEntries: 0, TTL: 0})
			nor403AssertEligibilityStatement(t, exec, fixture, "before-publication")
			_, err := exec.Execute(t.Context(), "CREATE INDEX nor403_probe FOR (n:NOR403Probe) ON (n.value)", nil)
			require.NoError(t, err)
			nor403AssertEligibilityStatement(t, exec, fixture, "after-publication")
		})

		// The actual search service owns the corpus: its callback population is
		// exactly the index membership and it is retained, not rebuilt per
		// statement.
		t.Run("search-service-callback", func(t *testing.T) {
			exec := fixture.newExecutor(QueryCachePolicy{MaxEntries: 0, TTL: 0})
			require.Nil(t, exec.search(), "a fresh executor holds no search service before the first fulltext read")
			result, err := exec.Execute(t.Context(),
				`CALL db.index.fulltext.queryNodes('`+nor403FulltextIndex+`', '*') YIELD node RETURN node.id AS id ORDER BY id`, nil)
			require.NoError(t, err)
			require.Equal(t, []string{"id"}, result.Columns)
			ids := make([]string, 0, len(result.Rows))
			for _, row := range result.Rows {
				id, ok := row[0].(string)
				require.True(t, ok)
				ids = append(ids, id)
			}
			expected := make([]string, 0, len(fixture.rows))
			for _, row := range fixture.rows {
				expected = append(expected, row.id)
			}
			sort.Strings(expected)
			require.Equal(t, expected, ids,
				"the service corpus is exactly the fulltext index membership")

			service := exec.search()
			require.NotNil(t, service, "the fulltext procedure binds the actual search service")
			nor403AssertEligibilityStatement(t, exec, fixture, "after-service-binding")
			require.Same(t, service, exec.search(),
				"the retained corpus owner is reused across statement executions")
		})
	})
}
