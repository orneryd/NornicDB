package main

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	math "github.com/orneryd/nornicdb/pkg/math/libm"
)

// Shared Northwind seed construction, used identically by the Bolt and the
// embedded LadybugDB backends so every engine in the sweep seeds the exact
// same deterministic dataset with the same Cypher statements.

var ensureIndexQueries = []string{
	// PK / FK indexes — required so MATCH-by-ID in UNWIND batches hits
	// the fast path instead of scanning the full label population.
	"CREATE INDEX category_id IF NOT EXISTS FOR (n:Category) ON (n.categoryID)",
	"CREATE INDEX supplier_id IF NOT EXISTS FOR (n:Supplier) ON (n.supplierID)",
	"CREATE INDEX customer_id IF NOT EXISTS FOR (n:Customer) ON (n.customerID)",
	"CREATE INDEX product_id IF NOT EXISTS FOR (n:Product) ON (n.productID)",
	"CREATE INDEX order_id IF NOT EXISTS FOR (n:Order) ON (n.orderID)",
	"CREATE INDEX product_category_fk IF NOT EXISTS FOR (n:Product) ON (n._categoryID)",
	"CREATE INDEX product_supplier_fk IF NOT EXISTS FOR (n:Product) ON (n._supplierID)",
	"CREATE INDEX order_customer_fk IF NOT EXISTS FOR (n:Order) ON (n._customerID)",
	// Name indexes — ORDER BY tiebreaker columns used by the query
	// suite. Declared up front so engines plan sort-merges identically
	// and neither penalises the tiebreaker on a cold cache.
	"CREATE INDEX product_name IF NOT EXISTS FOR (n:Product) ON (n.productName)",
	"CREATE INDEX customer_name IF NOT EXISTS FOR (n:Customer) ON (n.companyName)",
	"CREATE INDEX category_name IF NOT EXISTS FOR (n:Category) ON (n.categoryName)",
}

const categoriesCreateCypher = `UNWIND $rows AS row
 CREATE (:Category {categoryID: row.categoryID, categoryName: row.categoryName, description: row.description})`

const suppliersCreateCypher = `UNWIND $rows AS row
 CREATE (:Supplier {supplierID: row.supplierID, companyName: row.companyName, contactName: row.contactName,
                    country: row.country, region: row.region, phone: row.phone, notes: row.notes})`

const customersCreateCypher = `UNWIND $rows AS row
 CREATE (:Customer {customerID: row.customerID, companyName: row.companyName, contactName: row.contactName,
                    country: row.country, city: row.city, address: row.address})`

// Seeding strategy: do NOT use `UNWIND $rows MATCH ... CREATE` — that
// pattern falls back to a label scan per row on NornicDB's planner and
// seeding takes ~5ms per row (minutes at 50k scale). Instead, write
// nodes in two steps:
//  1. Bulk CREATE each node type with its foreign-key ids as properties
//     (no MATCH in the UNWIND — fast, ~20µs per node).
//  2. Wire edges in a single "cross-join" MATCH a, b WHERE a.fk = b.pk
//     CREATE (a)-[:REL]->(b) pass, once per relationship type.
const productsCreateCypher = `UNWIND $rows AS row
 MATCH (c:Category {categoryID: row.categoryID})
 MATCH (s:Supplier {supplierID: row.supplierID})
 CREATE (p:Product {productID: row.productID, productName: row.productName, sku: row.sku,
                    unitPrice: row.unitPrice, unitsInStock: row.unitsInStock, discontinued: row.discontinued,
                    description: row.description, tags: row.tags})
 CREATE (p)-[:PART_OF]->(c)
 CREATE (s)-[:SUPPLIES]->(p)`

// Orders are seeded in two passes: order nodes + PURCHASED edges first,
// ORDERS line edges second (nested UNWIND with inline property maps is
// rejected by NornicDB's parser).
const ordersCreateCypher = `UNWIND $rows AS row
 MATCH (c:Customer {customerID: row.customerID})
 CREATE (o:Order {orderID: row.orderID, shipCity: row.shipCity, shipCountry: row.shipCountry,
                  orderDate: row.orderDate, notes: row.notes})
 CREATE (c)-[:PURCHASED]->(o)`

const orderLinesCreateCypher = `UNWIND $rows AS row
 MATCH (o:Order {orderID: row.orderID})
 MATCH (p:Product {productID: row.productID})
 CREATE (o)-[:ORDERS {quantity: row.quantity, discount: row.discount}]->(p)`

// seedPhase is one UNWIND batch phase of the seed.
type seedPhase struct {
	name   string
	cypher string
	rows   []map[string]any
}

// seedPlan is the complete deterministic seed for one engine run.
type seedPlan struct {
	phases []seedPhase
}

// buildSeedPlan constructs every seed phase. Randomness is derived only
// from cfg.seed, so the Bolt engines and the embedded LadybugDB engine
// receive byte-identical payloads.
func buildSeedPlan(cfg seedConfig) seedPlan {
	r := newRNG(cfg.seed)
	plan := seedPlan{}

	// --- Categories ---
	catRows := make([]map[string]any, 0, cfg.categories)
	for i := 0; i < cfg.categories; i++ {
		name := fmt.Sprintf("Category-%d-%s", i+1, r.pick(adjectives))
		desc := r.joinN(descBlocks, 1, 3)
		catRows = append(catRows, map[string]any{
			"categoryID":   int64(i + 1),
			"categoryName": name,
			"description":  desc,
		})
	}
	plan.phases = append(plan.phases, seedPhase{name: "categories", cypher: categoriesCreateCypher, rows: catRows})

	// --- Suppliers ---
	supRows := make([]map[string]any, 0, cfg.suppliers)
	for i := 0; i < cfg.suppliers; i++ {
		supRows = append(supRows, map[string]any{
			"supplierID":  int64(i + 1),
			"companyName": fmt.Sprintf("%s %s Supply Co. #%d", r.pick(adjectives), r.pick(nouns), i+1),
			"contactName": fmt.Sprintf("%s %s", r.pick(firstNames), r.pick(lastNames)),
			"country":     r.pick(countries),
			"region":      r.pick(regions),
			"phone":       fmt.Sprintf("+%d-%03d-%03d-%04d", 1+r.IntN(99), r.IntN(1000), r.IntN(1000), r.IntN(10000)),
			"notes":       r.joinN(descBlocks, 0, 2),
		})
	}
	plan.phases = append(plan.phases, seedPhase{name: "suppliers", cypher: suppliersCreateCypher, rows: supRows})

	// --- Customers ---
	custRows := make([]map[string]any, 0, cfg.customers)
	for i := 0; i < cfg.customers; i++ {
		custRows = append(custRows, map[string]any{
			"customerID":  int64(i + 1),
			"companyName": fmt.Sprintf("%s %s LLC #%d", r.pick(adjectives), r.pick(nouns), i+1),
			"contactName": fmt.Sprintf("%s %s", r.pick(firstNames), r.pick(lastNames)),
			"country":     r.pick(countries),
			"city":        r.pick(cities),
			"address":     r.joinN(descBlocks, 0, 2),
		})
	}
	plan.phases = append(plan.phases, seedPhase{name: "customers", cypher: customersCreateCypher, rows: custRows})

	// --- Products (+ PART_OF, SUPPLIES) ---
	prodRows := make([]map[string]any, 0, cfg.products)
	for i := 0; i < cfg.products; i++ {
		prodRows = append(prodRows, map[string]any{
			"productID":    int64(i + 1),
			"productName":  fmt.Sprintf("%s %s %d", r.pick(adjectives), r.pick(nouns), i+1),
			"sku":          fmt.Sprintf("SKU-%06d-%c%c", i+1, 'A'+r.IntN(26), 'A'+r.IntN(26)),
			"unitPrice":    math.Round((0.5+r.Float64()*199.5)*100) / 100,
			"unitsInStock": int64(r.IntN(500)),
			"discontinued": r.IntN(20) == 0,
			"description":  r.joinN(descBlocks, 1, 4),
			"tags":         anySlice(r.uniqueSubset(tagPool, 1+r.IntN(4))),
			"categoryID":   int64((i % cfg.categories) + 1),
			"supplierID":   int64((i % cfg.suppliers) + 1),
		})
	}
	plan.phases = append(plan.phases, seedPhase{name: "products", cypher: productsCreateCypher, rows: prodRows})

	// --- Orders (+ PURCHASED), then ORDERS lines ---
	ordRows := make([]map[string]any, 0, cfg.orders)
	lineRows := make([]map[string]any, 0, cfg.orders*(cfg.orderLinesMin+cfg.orderLinesMax)/2)
	for i := 0; i < cfg.orders; i++ {
		lines := cfg.orderLinesMin
		if cfg.orderLinesMax > cfg.orderLinesMin {
			lines += r.IntN(cfg.orderLinesMax - cfg.orderLinesMin + 1)
		}
		orderID := int64(10000 + i)
		ordRows = append(ordRows, map[string]any{
			"orderID":     orderID,
			"customerID":  int64(r.IntN(cfg.customers) + 1),
			"shipCity":    r.pick(cities),
			"shipCountry": r.pick(countries),
			"orderDate":   time.Now().Add(-time.Duration(r.IntN(365)) * 24 * time.Hour).Unix(),
			"notes":       r.joinN(notesBlocks, 0, 2),
		})
		for j := 0; j < lines; j++ {
			lineRows = append(lineRows, map[string]any{
				"orderID":   orderID,
				"productID": int64(r.IntN(cfg.products) + 1),
				"quantity":  int64(1 + r.IntN(25)),
				"discount":  math.Round(r.Float64()*25*100) / 100,
			})
		}
	}
	plan.phases = append(plan.phases, seedPhase{name: "orders", cypher: ordersCreateCypher, rows: ordRows})
	plan.phases = append(plan.phases, seedPhase{name: "order-lines", cypher: orderLinesCreateCypher, rows: lineRows})
	return plan
}

// planSeedNodesAndRelationships returns the (nodes, relationships) a plan
// creates, matching the Bolt seeder's historical accounting.
func planSeedNodesAndRelationships(plan seedPlan) (nodes, relationships int) {
	for _, phase := range plan.phases {
		switch phase.name {
		case "products":
			nodes += len(phase.rows)
			relationships += len(phase.rows) * 2 // PART_OF + SUPPLIES
		case "order-lines":
			relationships += len(phase.rows)
		default:
			nodes += len(phase.rows)
			if phase.name == "orders" {
				relationships += len(phase.rows) // PURCHASED
			}
		}
	}
	return nodes, relationships
}

// planApproxBytes sums the approximate payload size of every phase, matching
// the Bolt seeder's accounting.
func planApproxBytes(plan seedPlan) int64 {
	var total int64
	for _, phase := range plan.phases {
		total += approxBytes(phase.rows)
	}
	return total
}

// seedCountQueries verifies what is actually on disk after seeding. Shared by
// the Bolt and Ladybug backends so every engine reports the same entities.
var seedCountQueries = []struct {
	field string
	query string
}{
	{"categories", "MATCH (n:Category) RETURN count(n) AS n"},
	{"suppliers", "MATCH (n:Supplier) RETURN count(n) AS n"},
	{"customers", "MATCH (n:Customer) RETURN count(n) AS n"},
	{"products", "MATCH (n:Product) RETURN count(n) AS n"},
	{"orders", "MATCH (n:Order) RETURN count(n) AS n"},
	{"part_of_edges", "MATCH ()-[r:PART_OF]->() RETURN count(r) AS n"},
	{"supplies_edges", "MATCH ()-[r:SUPPLIES]->() RETURN count(r) AS n"},
	{"purchased_edges", "MATCH ()-[r:PURCHASED]->() RETURN count(r) AS n"},
	{"orders_edges", "MATCH ()-[r:ORDERS]->() RETURN count(r) AS n"},
}

// setSeedCount assigns one verified count into the SeedCounts report.
func setSeedCount(sc *SeedCounts, field string, n int64) {
	switch field {
	case "categories":
		sc.Categories = n
	case "suppliers":
		sc.Suppliers = n
	case "customers":
		sc.Customers = n
	case "products":
		sc.Products = n
	case "orders":
		sc.Orders = n
	case "part_of_edges":
		sc.PartOfEdges = n
	case "supplies_edges":
		sc.SuppliesEdges = n
	case "purchased_edges":
		sc.PurchasedEdges = n
	case "orders_edges":
		sc.OrdersEdges = n
	}
}

// foldChainedCreates returns a variant of a seed statement with chained
// CREATE clauses folded into one comma-separated CREATE pattern list. Kuzu
// (LadybugDB) rejects chained CREATE clauses, and FalkorDB's planner handles
// the folded form more reliably on large UNWIND batches. The Bolt engines
// keep the original Neo4j-flavored statements unchanged.
func foldChainedCreates(phaseName, cypher string) string {
	switch phaseName {
	case "products":
		return strings.Replace(cypher,
			"\n CREATE (p)-[:PART_OF]->(c)\n CREATE (s)-[:SUPPLIES]->(p)",
			", (p)-[:PART_OF]->(c), (s)-[:SUPPLIES]->(p)", 1)
	case "orders":
		return strings.Replace(cypher,
			"\n CREATE (c)-[:PURCHASED]->(o)",
			", (c)-[:PURCHASED]->(o)", 1)
	default:
		return cypher
	}
}

var falkorIndexRe = regexp.MustCompile(`^CREATE INDEX \S+ IF NOT EXISTS FOR (.+)$`)

// falkorIndexQuery rewrites a Neo4j-style named CREATE INDEX statement into
// FalkorDB's supported form: no index name and no IF NOT EXISTS clause.
// The second return value is false when the statement does not match the
// expected pattern (and was returned unchanged).
func falkorIndexQuery(q string) (string, bool) {
	m := falkorIndexRe.FindStringSubmatch(strings.TrimSpace(q))
	if m == nil {
		return q, false
	}
	return "CREATE INDEX FOR " + m[1], true
}

var ladybugOrderLabelRe = regexp.MustCompile(`:Order\b`)

// ladybugDialect adapts the shared Cypher to LadybugDB: ORDER is a keyword to its parser, so the Order
// label must be backtick-quoted wherever it is used. The other engines receive the statements unchanged.
func ladybugDialect(cypher string) string {
	return ladybugOrderLabelRe.ReplaceAllString(cypher, ":`Order`")
}
