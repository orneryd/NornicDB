package temporalguide

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const ledger = "canonical-graph-ledger"

// Ownership exercises the published relationship pattern over the caller's
// executor, HTTP or Bolt adapter. All fixtures are owned by the caller.
func Ownership(t *testing.T, run Runner) {
	t.Helper()
	execute := func(query string, params map[string]interface{}) [][]interface{} {
		t.Helper()
		rows, err := run(query, params)
		require.NoError(t, err, query)
		return rows
	}
	for _, name := range []string{"account-key", "asset-key", "ownership-constraint", "ownership-first"} {
		execute(Example(t, ledger, name), nil)
	}
	at := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := run("CREATE (:Account {id:'alice'})", nil)
	require.Error(t, err, "documented identity must be unique")
	_, err = run("CREATE (:Asset {id:'asset-42'})", nil)
	require.Error(t, err, "documented asset identity must be unique")
	execute(strings.ReplaceAll(Example(t, ledger, "ownership-transfer"), "id:'bob'", "id:'missing'"), nil)
	require.Equal(t, [][]interface{}{{"alice"}}, execute("MATCH (owner:Account)-[r:OWNS]->(:Asset {id:'asset-42'}) WHERE r.ends_at IS NULL RETURN owner.id AS owner", nil), "missing next owner must leave the old epoch open")
	_, err = run(`MATCH (b:Account {id:'bob'}),(a:Asset {id:'asset-42'}) CREATE (b)-[:OWNS {assetId:a.id,starts_at:datetime('2026-01-15T00:00:00Z'),ends_at:null}]->(a)`, nil)
	require.Error(t, err, "an open epoch excludes a competing owner")
	execute(Example(t, ledger, "ownership-transfer"), nil)
	business := Example(t, ledger, "ownership-at")
	require.Equal(t, [][]interface{}{{"alice"}}, execute(business, nil))
	require.Equal(t, [][]interface{}{{"bob"}}, execute(replaceBusinessTime(business, "2026-02-01T00:00:00Z"), nil), "shared boundary belongs to the next owner")
	require.Empty(t, execute(replaceBusinessTime(business, "2025-12-31T00:00:00Z"), nil))
	rows := execute(Example(t, ledger, "ownership-recorded"), map[string]interface{}{"recordedAt": at})
	require.Len(t, rows, 1)
	require.Len(t, rows[0], 1)
	row, ok := rows[0][0].(map[string]interface{})
	require.True(t, ok, "historical row must be a map, got %T", rows[0][0])
	require.Equal(t, "alice", row["owner"])
	require.Nil(t, row["ends_at"], "historical record predates closing Alice's epoch")
	assertTime(t, row["starts_at"], "2026-01-01T00:00:00Z")
	// Retrying an already-applied transfer has no matching old open epoch.
	execute(Example(t, ledger, "ownership-transfer"), nil)
	require.Equal(t, [][]interface{}{{"bob"}}, execute(replaceBusinessTime(business, "2026-02-01T00:00:00Z"), nil))
	require.Equal(t, [][]interface{}{{"alice"}, {"bob"}}, execute("MATCH (a:Account)-[:OWNS]->(:Asset {id:'asset-42'}) RETURN a.id AS owner ORDER BY owner", nil))
	execute(Example(t, ledger, "ownership-open-past"), nil)
	require.Empty(t, execute(`CALL db.temporal.asOf($at,'MATCH (:Account)-[:OWNS]->(a:Asset {id:$id}) RETURN a.id AS id',{id:'legacy-asset'}) YIELD row RETURN row`, map[string]interface{}{"at": at}), "an explicit past epoch does not backdate its database commit")
	require.Equal(t, [][]interface{}{{"alice"}}, execute(Example(t, ledger, "ownership-past-query"), nil))
	_, err = run(`MATCH (b:Account {id:'bob'}),(a:Asset {id:'legacy-asset'}) CREATE (b)-[:OWNS {assetId:a.id,starts_at:null,ends_at:datetime('2020-01-01T00:00:00Z')}]->(a)`, nil)
	require.Error(t, err, "unbounded past reserves earlier time against another owner")
	execute(`MATCH (b:Account {id:'bob'}),(a:Asset {id:'legacy-asset'}) CREATE (b)-[:OWNS {assetId:a.id,starts_at:datetime('2026-02-01T00:00:00Z'),ends_at:null}]->(a)`, nil)
	require.Equal(t, [][]interface{}{{"bob"}}, execute(strings.ReplaceAll(replaceBusinessTime(business, "2026-02-01T00:00:00Z"), "asset-42", "legacy-asset"), nil))
	// An independent asset can have an overlapping ownership epoch.
	execute(`MATCH (a:Account {id:'alice'}) CREATE (asset:Asset {id:'asset-43'}) CREATE (a)-[:OWNS {assetId:asset.id,starts_at:datetime('2026-01-01T00:00:00Z'),ends_at:null}]->(asset)`, nil)
	// Failure later in the transfer statement must not leave a closed epoch.
	_, err = run(`MATCH (a:Account {id:'bob'})-[old:OWNS]->(asset:Asset {id:'asset-42'}) WITH old,asset,a SET old.ends_at=datetime('2026-03-01T00:00:00Z') CREATE (a)-[:OWNS {assetId:asset.id,starts_at:datetime('2026-02-15T00:00:00Z'),ends_at:null}]->(asset)`, nil)
	require.Error(t, err)
	require.Equal(t, [][]interface{}{{"bob"}}, execute(replaceBusinessTime(business, "2026-04-01T00:00:00Z"), nil), "rejected transfer rolls back its preceding SET")
	execute("MATCH (:Account {id:'alice'})-[r:OWNS]->(:Asset {id:'asset-42'}) DELETE r", nil)
	require.Empty(t, execute(business, nil), "deleting the explicit epoch removes that application history")
	rows = execute(Example(t, ledger, "ownership-recorded"), map[string]interface{}{"recordedAt": at})
	require.Empty(t, rows, "historical read must not resurrect the deleted relationship")
}

func replaceBusinessTime(query, at string) string {
	// Both comparisons deliberately use the same literal in the published query.
	return strings.ReplaceAll(query, "2026-01-15T00:00:00Z", at)
}

func assertTime(t *testing.T, value interface{}, want string) {
	t.Helper()
	var got time.Time
	switch v := value.(type) {
	case time.Time:
		got = v
	case interface{ TemporalTime() time.Time }:
		got = v.TemporalTime()
	case string:
		var err error
		got, err = time.Parse(time.RFC3339Nano, v)
		if err != nil {
			got, err = time.Parse("2006-01-02T15:04Z07:00", v)
		}
		require.NoError(t, err)
	default:
		t.Fatalf("unexpected temporal wire value %T: %v", value, value)
	}
	require.Equal(t, want, got.UTC().Format(time.RFC3339Nano))
}

// GenericLedger exercises every executable example in the optional fact model.
func GenericLedger(t *testing.T, run Runner) {
	t.Helper()
	for _, name := range []string{"fact-key", "fact-epochs", "fact-first", "fact-replace"} {
		_, err := run(Example(t, ledger, name), nil)
		require.NoError(t, err, name)
	}
	rows, err := run(Example(t, ledger, "fact-current"), nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{89.99}}, rows)
	rows, err = run(`MATCH (:FactKey {subject:'product-123',predicate:'price'})-[:HAS_VERSION]->(v:FactVersion) RETURN v.amount AS amount ORDER BY amount`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{89.99}, {99.99}}, rows)
	_, err = run(Example(t, ledger, "fact-event"), nil)
	require.NoError(t, err)
	rows, err = run(`MATCH (m:MutationEvent)-[:AFFECTS]->(v:FactVersion) RETURN m.actor AS actor,v.amount AS amount`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"bob", 89.99}}, rows)
}

// Contracts covers both published block examples and the guarantees implied by
// their mixed native and boolean entries.
func Contracts(t *testing.T, run Runner) {
	t.Helper()
	for _, name := range []string{"employment-contract", "price-contract"} {
		_, err := run(Example(t, "managing-constraints", name), nil)
		require.NoError(t, err, name)
	}
	rows, err := run("SHOW CONSTRAINT CONTRACTS", nil)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	counts := map[string][]int{"works_at_contract": {9, 5, 4}, "price_contract": {3, 2, 1}}
	for _, row := range rows {
		expected, ok := counts[row[0].(string)]
		require.True(t, ok)
		for i, want := range expected {
			require.EqualValues(t, want, row[i+3])
		}
	}
	_, err = run(`CREATE (:Person {id:'p1',tenant:'t1'}),(:Company {id:'c1',tenant:'t1'}),(:Company {id:'c2',tenant:'t2'})`, nil)
	require.NoError(t, err)
	create := `MATCH (a:Person {id:'p1'}),(b:Company {id:$company}) CREATE (a)-[:WORKS_AT $props]->(b)`
	base := func(id, start, end string) map[string]interface{} {
		return map[string]interface{}{"id": id, "tenant": "t1", "externalId": id, "employeeId": "p1", "startedAt": start, "endedAt": end, "role": "Engineer", "status": "active", "hoursPerWeek": int64(40)}
	}
	write := func(p map[string]interface{}, company string) error {
		_, err := run(create, map[string]interface{}{"company": company, "props": p})
		return err
	}
	require.NoError(t, write(base("w1", "2026-01-01", "2026-02-01"), "c1"))
	require.Error(t, write(base("w2", "2026-01-15", "2026-03-01"), "c1"))
	require.NoError(t, write(base("w2", "2026-02-01", "2026-03-01"), "c1"))
	for _, tc := range []struct {
		name   string
		change func(map[string]interface{})
	}{
		{"duplicate id", func(p map[string]interface{}) { p["id"] = "w1" }},
		{"missing start", func(p map[string]interface{}) { delete(p, "startedAt") }},
		{"wrong role type", func(p map[string]interface{}) { p["role"] = int64(3) }},
		{"duplicate relationship key", func(p map[string]interface{}) { p["externalId"] = "w1" }},
		{"missing grouping component", func(p map[string]interface{}) { delete(p, "employeeId") }},
		{"bad status", func(p map[string]interface{}) { p["status"] = "paused" }},
		{"nonpositive hours", func(p map[string]interface{}) { p["hoursPerWeek"] = int64(0) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := base("rejected", "2026-03-01", "2026-04-01")
			tc.change(p)
			require.Error(t, write(p, "c1"))
		})
	}
	require.Error(t, write(base("cross-tenant", "2026-03-01", "2026-04-01"), "c2"))
	_, err = run(`MATCH (a:Person {id:'p1'}) CREATE (a)-[:WORKS_AT $props]->(a)`, map[string]interface{}{"props": base("self", "2026-03-01", "2026-04-01")})
	require.Error(t, err)
	rows, err = run("MATCH ()-[r:WORKS_AT]->() RETURN r.id AS id ORDER BY id", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"w1"}, {"w2"}}, rows, "all rejected contract writes must leave no relationship")
	price := func(id, currency, start, end string, amount float64) map[string]interface{} {
		return map[string]interface{}{"id": id, "productId": "product-123", "currency": currency, "starts_at": start, "ends_at": end, "amount": amount}
	}
	for i, tc := range []struct {
		p        map[string]interface{}
		accepted bool
	}{
		{price("usd1", "USD", "2026-01-01", "2026-02-01", 99.99), true},
		{price("usd2", "USD", "2026-01-15", "2026-03-01", 89.99), false},
		{price("eur1", "EUR", "2026-01-15", "2026-03-01", 89.99), true},
		{price("usd2", "USD", "2026-02-01", "2026-03-01", 89.99), true},
		{price("bad", "USD", "2026-03-01", "2026-04-01", -1), false},
	} {
		_, err = run("CREATE (:Price $props)", map[string]interface{}{"props": tc.p})
		if tc.accepted {
			require.NoError(t, err, "price case %d", i)
		} else {
			require.Error(t, err, "price case %d", i)
		}
	}
}

// History executes the history guide's actual setup, update and parameterized
// historical query, demonstrating that the commit cutoff is not an epoch bound.
func History(t *testing.T, run Runner) {
	t.Helper()
	_, err := run(Example(t, "historical-reads-mvcc-retention", "history-first"), nil)
	require.NoError(t, err)
	at := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = run(Example(t, "historical-reads-mvcc-retention", "history-update"), nil)
	require.NoError(t, err)
	rows, err := run(Example(t, "historical-reads-mvcc-retention", "history-read"), map[string]interface{}{"at": at})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	row, ok := rows[0][0].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "acct-42", row["accountId"])
	require.EqualValues(t, 100, row["balance"])
	rows, err = run("MATCH (a:Account {accountId:'acct-42'}) RETURN a.balance AS balance", nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.EqualValues(t, 200, rows[0][0])
}

// EpochBounds qualifies the guide's optional shape contract, rather than
// pretending that a no-overlap constraint alone validates every property type.
func EpochBounds(t *testing.T, run Runner) {
	t.Helper()
	for _, item := range []struct{ guide, name string }{{ledger, "ownership-constraint"}, {"managing-constraints", "ownership-shape"}} {
		_, err := run(Example(t, item.guide, item.name), nil)
		require.NoError(t, err)
	}
	_, err := run("CREATE (:Account {id:'alice'})", nil)
	require.NoError(t, err)
	start := "starts_at:datetime('2026-01-01T00:00:00Z')"
	for i, tc := range []struct {
		name, fields string
		accepted     bool
	}{
		{"closed", start + ",ends_at:datetime('2026-02-01T00:00:00Z')", true},
		{"null end", start + ",ends_at:null", true},
		{"missing end", start, true},
		{"missing start", "ends_at:datetime('2026-02-01T00:00:00Z')", true},
		{"null start", "starts_at:null,ends_at:datetime('2026-02-01T00:00:00Z')", true},
		{"both null", "starts_at:null,ends_at:null", false},
		{"both missing", "", false},
		{"untyped start", "starts_at:'2026-01-01',ends_at:null", false},
		{"numeric start", "starts_at:1767225600,ends_at:null", false},
		{"bad end", start + ",ends_at:'not-a-time'", false},
		{"numeric end", start + ",ends_at:1769904000", false},
		{"zero length", start + ",ends_at:datetime('2026-01-01T00:00:00Z')", false},
		{"reversed", start + ",ends_at:datetime('2025-12-31T00:00:00Z')", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := fmt.Sprintf("bounds-%d", i)
			fields := tc.fields
			if fields != "" {
				fields = "," + fields
			}
			query := fmt.Sprintf("MATCH (a:Account {id:'alice'}) CREATE (asset:Asset {id:$id}) CREATE (a)-[:OWNS {assetId:asset.id%s}]->(asset)", fields)
			_, err := run(query, map[string]interface{}{"id": id})
			if tc.accepted {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			rows, err := run("MATCH (a:Asset {id:$id}) RETURN count(a) AS count", map[string]interface{}{"id": id})
			require.NoError(t, err)
			require.Len(t, rows, 1)
			want := 0
			if tc.accepted {
				want = 1
			}
			require.EqualValues(t, want, rows[0][0], "a rejected epoch must roll back the asset created in the same statement")
		})
	}
}

// OwnershipScript executes the downloadable example as source, and asserts its
// final effects independently of the individual guide-fence scenarios.
func OwnershipScript(t *testing.T, run Runner) {
	t.Helper()
	data, err := os.ReadFile("../../docs/examples/temporal-ownership.cypher")
	require.NoError(t, err)
	text := string(data)
	var last [][]interface{}
	for _, span := range cyphertext.StatementSpans(text) {
		query := strings.TrimSpace(text[span.Start:span.End])
		if query == "" {
			continue
		}
		last, err = run(query, nil)
		require.NoError(t, err, query)
	}
	require.Equal(t, [][]interface{}{{"alice"}}, last)
	rows, err := run("MATCH (owner:Account)-[r:OWNS]->(:Asset {id:'asset-42'}) WHERE r.ends_at IS NULL RETURN owner.id AS owner", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"bob"}}, rows)
}

// CostExamples verifies the declarations used to explain index/scan costs.
// The cost claims themselves require the separately scoped benchmark.
func CostExamples(t *testing.T, run Runner) {
	t.Helper()
	for _, name := range []string{"cost-node", "cost-relationship"} {
		_, err := run(Example(t, "../performance/temporal-read-and-constraint-costs", name), nil)
		require.NoError(t, err)
	}
	p := map[string]interface{}{"tenantId": "t1", "accountId": "a1", "assetId": "asset1", "starts_at": "2026-01-01", "ends_at": "2026-02-01"}
	_, err := run("CREATE (:Account $props)", map[string]interface{}{"props": p})
	require.NoError(t, err)
	_, err = run("CREATE (:Account $props)", map[string]interface{}{"props": p})
	require.Error(t, err)
	p["tenantId"] = "t2"
	_, err = run("CREATE (:Account $props)", map[string]interface{}{"props": p})
	require.NoError(t, err)
	_, err = run("CREATE (:Endpoint {id:1}),(:Endpoint {id:2})", nil)
	require.NoError(t, err)
	edge := `MATCH (a:Endpoint {id:1}),(b:Endpoint {id:2}) CREATE (a)-[:OWNS $props]->(b)`
	_, err = run(edge, map[string]interface{}{"props": p})
	require.NoError(t, err)
	_, err = run(edge, map[string]interface{}{"props": p})
	require.Error(t, err)
	p["assetId"] = "asset2"
	_, err = run(edge, map[string]interface{}{"props": p})
	require.NoError(t, err)
}
