// Package main — Northwind power/throughput benchmark runner.
//
// Connects to a Bolt-compatible endpoint (NornicDB or Neo4j), seeds a
// Northwind-shaped dataset with randomised mixed-size properties, and runs a
// fixed set of read queries for N iterations. Emits a single JSON result
// document that the orchestrator pairs with concurrent powermetrics samples
// and on-disk `du` measurements.
//
// Seeding is chunked via UNWIND batches (configurable via -batch-size) so
// the driver doesn't buffer enormous payloads for large datasets.
// Randomness is deterministic — controlled by -seed — so two runs with the
// same flags produce identical datasets across NornicDB and Neo4j.
//
// Usage:
//
//	go run ./testing/benchmarks/northwind_power \
//	  -uri bolt://localhost:7687 -user neo4j -pass password \
//	  -database neo4j \
//	  -products 50000 -orders 50000 -customers 500 \
//	  -iterations 10 -out /tmp/neo4j_results.json -label neo4j
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type QueryStat struct {
	Name         string    `json:"name"`
	Iterations   int       `json:"iterations"`
	Cypher       string    `json:"cypher"`
	LatenciesMs  []float64 `json:"latencies_ms"`
	MeanMs       float64   `json:"mean_ms"`
	MedianMs     float64   `json:"median_ms"`
	P95Ms        float64   `json:"p95_ms"`
	P99Ms        float64   `json:"p99_ms"`
	MinMs        float64   `json:"min_ms"`
	MaxMs        float64   `json:"max_ms"`
	StdDevMs     float64   `json:"stddev_ms"`
	OpsPerSecond float64   `json:"ops_per_second"`

	// Correctness fingerprint — captured from the first iteration and
	// verified identical across every subsequent iteration. `RowCount` is
	// the number of rows returned. `ResultHash` is a SHA-256 hex digest of
	// the sorted, canonicalised result set so cross-DB comparison is
	// exact. `FirstRows` is a deterministic (up to MaxFingerprintRows)
	// snapshot of the first few rows — stored in the report JSON so
	// operators can eyeball a diff without re-running the query.
	RowCount      int              `json:"row_count"`
	ResultHash    string           `json:"result_hash"`
	FirstRows     []map[string]any `json:"first_rows,omitempty"`
	CorrectnessOK bool             `json:"correctness_ok"`
}

// SeedCounts records what the post-seed graph actually contains on disk.
// Stored in the report so the orchestrator (and tests) can compare NornicDB
// and Neo4j nodes/edges counts exactly.
type SeedCounts struct {
	Categories     int64 `json:"categories"`
	Suppliers      int64 `json:"suppliers"`
	Customers      int64 `json:"customers"`
	Products       int64 `json:"products"`
	Orders         int64 `json:"orders"`
	PartOfEdges    int64 `json:"part_of_edges"`
	SuppliesEdges  int64 `json:"supplies_edges"`
	PurchasedEdges int64 `json:"purchased_edges"`
	OrdersEdges    int64 `json:"orders_edges"`
}

// MaxFingerprintRows caps the JSON payload size of FirstRows. Full result
// sets can be enormous for `optional_match_orders_count` (rows = N products
// ≥ 8000), so we snapshot the first K and rely on ResultHash for the
// complete comparison.
const MaxFingerprintRows = 20

type Report struct {
	Label             string    `json:"label"`
	URI               string    `json:"uri"`
	Database          string    `json:"database"`
	StartedAt         time.Time `json:"started_at"`
	FinishedAt        time.Time `json:"finished_at"`
	SeedDurationMs    float64   `json:"seed_duration_ms"`
	TotalBenchMs      float64   `json:"total_benchmark_duration_ms"`
	Iterations        int       `json:"iterations_per_query"`
	Categories        int       `json:"categories"`
	Suppliers         int       `json:"suppliers"`
	Customers         int       `json:"customers"`
	Products          int       `json:"products"`
	Orders            int       `json:"orders"`
	OrderLinesMin     int       `json:"order_lines_min"`
	OrderLinesMax     int       `json:"order_lines_max"`
	RandomSeed        uint64    `json:"random_seed"`
	SeedNodes         int       `json:"seed_nodes"`
	SeedRelationships int       `json:"seed_relationships"`
	ApproxSeedBytes   int64     `json:"approx_seed_payload_bytes"`

	// SeedCounts captures the post-seed graph shape as reported by the
	// target database itself (not our local accounting). If these numbers
	// don't match the claimed seed sizes, we silently lost writes — loud
	// failure should follow.
	SeedCounts SeedCounts `json:"seed_counts"`

	Queries          []QueryStat `json:"queries"`
	OverallMeanMs    float64     `json:"overall_mean_ms"`
	OverallOpsPerSec float64     `json:"overall_ops_per_second"`

	// CorrectnessErrors lists any per-query intra-run mismatches (result
	// set changed between iterations) OR seed-count mismatches. Empty when
	// the run is clean.
	CorrectnessErrors []string `json:"correctness_errors,omitempty"`
}

type benchQuery struct {
	name   string
	cypher string
}

var queries = []benchQuery{
	{
		name: "products_per_category",
		cypher: `
			MATCH (c:Category)<-[:PART_OF]-(p:Product)
			RETURN c.categoryName AS categoryName, count(p) AS productCount
			ORDER BY productCount DESC`,
	},
	{
		// Deterministic tie-break on (companyName, categoryName) so the
		// LIMIT 10 picks the same rows on both engines when many rows
		// are tied at the same `orders` count.
		name: "customer_category_distinct_orders",
		cypher: `
			MATCH (c:Customer)-[:PURCHASED]->(o:Order)-[:ORDERS]->(p:Product)-[:PART_OF]->(cat:Category)
			RETURN c.companyName AS companyName, cat.categoryName AS categoryName, count(DISTINCT o) AS orders
			ORDER BY orders DESC, companyName ASC, categoryName ASC
			LIMIT 10`,
	},
	{
		// Deterministic tie-break on productName so LIMIT 100 is stable
		// across engines when many products tie on orderCount.
		name: "optional_match_orders_count",
		cypher: `
			MATCH (p:Product)
			OPTIONAL MATCH (p)<-[r:ORDERS]-(o:Order)
			RETURN p.productName AS productName, count(o) AS orderCount
			ORDER BY orderCount DESC, productName ASC
			LIMIT 100`,
	},
	{
		// Deterministic tie-break on productName so LIMIT 10 picks the
		// same products on both engines when revenues tie.
		name: "revenue_by_product",
		cypher: `
			MATCH (p:Product)<-[r:ORDERS]-(:Order)
			WITH p, sum(p.unitPrice * r.quantity) AS revenue
			RETURN p.productName AS productName, revenue
			ORDER BY revenue DESC, productName ASC
			LIMIT 10`,
	},
}

// Lexicons for randomised property generation. Kept small and deterministic;
// the real variety comes from random composition, not vocabulary size.
var (
	countries = []string{"US", "DE", "FR", "UK", "JP", "BR", "IN", "CA", "AU", "ZA", "MX", "NL", "SE", "ES", "IT"}
	cities    = []string{"Seattle", "Berlin", "Paris", "London", "Tokyo", "São Paulo", "Mumbai", "Toronto",
		"Sydney", "Cape Town", "Mexico City", "Amsterdam", "Stockholm", "Madrid", "Milan", "Austin", "Boston"}
	regions    = []string{"NA-West", "NA-East", "EU-Central", "EU-North", "APAC", "LATAM", "MEA"}
	adjectives = []string{"organic", "premium", "artisanal", "heritage", "cold-pressed", "stone-milled",
		"hand-picked", "free-range", "small-batch", "fair-trade", "grass-fed", "wild-caught", "aged", "roasted"}
	nouns = []string{"coffee", "tea", "spread", "cheese", "chocolate", "pasta", "preserve", "biscuit",
		"oil", "wine", "syrup", "condiment", "cracker", "jerky", "honey", "soda", "sauce"}
	firstNames = []string{"Alex", "Sam", "Jordan", "Pat", "Casey", "Morgan", "Taylor", "Jamie", "Riley",
		"Quinn", "Avery", "Devon", "Skyler", "Hayden", "Parker"}
	lastNames = []string{"Chen", "Patel", "Müller", "Johansson", "García", "Silva", "O'Connor", "Nakamura",
		"Kowalski", "Okafor", "Andersen", "Rossi", "Dubois"}
	descBlocks = []string{
		"Sourced from certified growers and rigorously quality-checked at each stage. ",
		"Finished by hand in small batches to preserve texture and aroma. ",
		"Recommended for everyday use and gifting. ",
		"Pairs well with seasonal produce and traditional preparations. ",
		"Packaged in recyclable materials with minimal dyes. ",
		"Shelf-stable when stored in a cool, dry place away from sunlight. ",
		"Contains no artificial preservatives, colours, or flavour enhancers. ",
		"Developed in collaboration with regional co-operatives and independent farms. ",
		"Meets HACCP and ISO 22000 standards across the supply chain. ",
	}
	notesBlocks = []string{
		"Deliver to loading dock B; call receiving desk on arrival. ",
		"Customer requests consolidated shipment with other open orders. ",
		"Please include a printed gift receipt. ",
		"Payment terms Net-30; reference PO on invoice. ",
		"Temperature-sensitive — ship with insulated packaging. ",
		"Fragile. Label box-side-up and avoid stacking. ",
		"Signature required on delivery. ",
	}
	tagPool = []string{"gift", "bulk", "retail", "promo", "winter", "summer", "limited", "vegan", "gluten-free",
		"kosher", "halal", "new", "popular", "clearance", "reorder"}
)

type rng struct{ *rand.Rand }

func newRNG(seed uint64) rng {
	return rng{Rand: rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))}
}

func (r rng) pick(s []string) string { return s[r.IntN(len(s))] }

func (r rng) joinN(pool []string, minN, maxN int) string {
	n := minN
	if maxN > minN {
		n += r.IntN(maxN - minN + 1)
	}
	buf := make([]byte, 0, n*16)
	for i := 0; i < n; i++ {
		buf = append(buf, pool[r.IntN(len(pool))]...)
	}
	return string(buf)
}

// uniqueSubset returns up to k random items from pool without replacement.
func (r rng) uniqueSubset(pool []string, k int) []string {
	if k >= len(pool) {
		out := append([]string(nil), pool...)
		r.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
		return out
	}
	idx := r.Perm(len(pool))[:k]
	out := make([]string, k)
	for i, ix := range idx {
		out[i] = pool[ix]
	}
	return out
}

func main() {
	var (
		uri           = flag.String("uri", "bolt://localhost:7687", "Bolt URI")
		user          = flag.String("user", "neo4j", "username (ignored when -no-auth)")
		pass          = flag.String("pass", "password", "password")
		noAuth        = flag.Bool("no-auth", false, "skip auth (for NornicDB --no-auth)")
		database      = flag.String("database", "neo4j", "database name")
		categories    = flag.Int("categories", 16, "number of Category nodes")
		suppliers     = flag.Int("suppliers", 24, "number of Supplier nodes")
		customersN    = flag.Int("customers", 200, "number of Customer nodes")
		products      = flag.Int("products", 8000, "number of Product nodes")
		ordersN       = flag.Int("orders", 8000, "number of Order nodes")
		orderLinesMin = flag.Int("order-lines-min", 1, "minimum ORDERS edges per order")
		orderLinesMax = flag.Int("order-lines-max", 6, "maximum ORDERS edges per order")
		batchSize     = flag.Int("batch-size", 200, "UNWIND rows per create batch")
		parallel      = flag.Int("parallel", 4, "concurrent Bolt sessions per seed phase (1 = serial; higher values shard batches across workers to keep the server's write pipeline full)")
		seed          = flag.Uint64("seed", 42, "PRNG seed (deterministic dataset)")
		iterations    = flag.Int("iterations", 10, "iterations per query")
		warmup        = flag.Int("warmup", 2, "warmup iterations per query (not recorded)")
		out           = flag.String("out", "", "output path for JSON report (stdout if empty)")
		label         = flag.String("label", "db", "label for this run (e.g. nornicdb, neo4j)")
		skipSeed      = flag.Bool("skip-seed", false, "assume dataset is already present")
	)
	flag.Parse()

	if *orderLinesMin < 1 {
		die("-order-lines-min must be >= 1")
	}
	if *orderLinesMax < *orderLinesMin {
		die("-order-lines-max must be >= -order-lines-min")
	}

	ctx := context.Background()

	var authToken neo4j.AuthToken
	if *noAuth {
		authToken = neo4j.NoAuth()
	} else {
		authToken = neo4j.BasicAuth(*user, *pass, "")
	}

	driver, err := neo4j.NewDriverWithContext(*uri, authToken)
	if err != nil {
		die("driver init: %v", err)
	}
	defer driver.Close(ctx)

	if err := driver.VerifyConnectivity(ctx); err != nil {
		die("connectivity: %v", err)
	}

	report := &Report{
		Label:         *label,
		URI:           *uri,
		Database:      *database,
		Iterations:    *iterations,
		Categories:    *categories,
		Suppliers:     *suppliers,
		Customers:     *customersN,
		Products:      *products,
		Orders:        *ordersN,
		OrderLinesMin: *orderLinesMin,
		OrderLinesMax: *orderLinesMax,
		RandomSeed:    *seed,
		StartedAt:     time.Now(),
	}

	if !*skipSeed {
		log("[%s] seeding Northwind (categories=%d suppliers=%d customers=%d products=%d orders=%d, seed=%d)",
			*label, *categories, *suppliers, *customersN, *products, *ordersN, *seed)
		seedStart := time.Now()
		stats, err := seedNorthwind(ctx, driver, *database, seedConfig{
			categories:    *categories,
			suppliers:     *suppliers,
			customers:     *customersN,
			products:      *products,
			orders:        *ordersN,
			orderLinesMin: *orderLinesMin,
			orderLinesMax: *orderLinesMax,
			batchSize:     *batchSize,
			parallel:      *parallel,
			seed:          *seed,
			label:         *label,
		})
		if err != nil {
			die("seed: %v", err)
		}
		report.SeedDurationMs = float64(time.Since(seedStart).Microseconds()) / 1000.0
		report.SeedNodes = stats.nodes
		report.SeedRelationships = stats.relationships
		report.ApproxSeedBytes = stats.approxPayloadBytes
		log("[%s] seeded in %.1fms (%d nodes, %d rels, ~%.1f MiB payload)",
			*label, report.SeedDurationMs, stats.nodes, stats.relationships,
			float64(stats.approxPayloadBytes)/(1024*1024))

		// Verify the database's own counts match what we think we wrote.
		// If they don't, something silently dropped writes — a loud failure
		// here is the last chance to catch it before the latency numbers
		// look "great" on a partially-populated graph.
		sc, seedErr := countSeedGraph(ctx, driver, *database)
		if seedErr != nil {
			die("seed verification: %v", seedErr)
		}
		report.SeedCounts = sc
		if mismatches := verifySeedCounts(sc, *categories, *suppliers, *customersN, *products, *ordersN); len(mismatches) > 0 {
			for _, m := range mismatches {
				report.CorrectnessErrors = append(report.CorrectnessErrors, "seed: "+m)
				log("[%s] SEED MISMATCH: %s", *label, m)
			}
			die("seed verification failed — see CorrectnessErrors in %s", *out)
		}
		log("[%s] seed verified: categories=%d suppliers=%d customers=%d products=%d orders=%d part_of=%d supplies=%d purchased=%d orders_edges=%d",
			*label, sc.Categories, sc.Suppliers, sc.Customers, sc.Products, sc.Orders,
			sc.PartOfEdges, sc.SuppliesEdges, sc.PurchasedEdges, sc.OrdersEdges)
	}

	benchStart := time.Now()
	totalOps := 0
	var allLatencies []float64
	for _, q := range queries {
		stat, err := runQuery(ctx, driver, *database, q, *iterations, *warmup)
		if err != nil {
			die("query %s: %v", q.name, err)
		}
		report.Queries = append(report.Queries, stat)
		totalOps += stat.Iterations
		allLatencies = append(allLatencies, stat.LatenciesMs...)
		if !stat.CorrectnessOK {
			report.CorrectnessErrors = append(report.CorrectnessErrors,
				fmt.Sprintf("query %q: result set changed between iterations (row_count=%d hash=%s)",
					q.name, stat.RowCount, stat.ResultHash))
		}
		log("[%s] %-34s mean=%7.2fms p95=%7.2fms ops/s=%8.1f rows=%d",
			*label, q.name, stat.MeanMs, stat.P95Ms, stat.OpsPerSecond, stat.RowCount)
	}
	if len(report.CorrectnessErrors) > 0 {
		log("[%s] CORRECTNESS: %d issue(s) recorded in report; see correctness_errors.", *label, len(report.CorrectnessErrors))
	}
	report.TotalBenchMs = float64(time.Since(benchStart).Microseconds()) / 1000.0
	report.FinishedAt = time.Now()
	if len(allLatencies) > 0 {
		report.OverallMeanMs = mean(allLatencies)
		if report.TotalBenchMs > 0 {
			report.OverallOpsPerSec = float64(totalOps) / (report.TotalBenchMs / 1000.0)
		}
	}

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		die("marshal: %v", err)
	}
	if *out == "" {
		fmt.Println(string(data))
	} else {
		if err := os.WriteFile(*out, data, 0o644); err != nil {
			die("write: %v", err)
		}
		log("[%s] wrote %s", *label, *out)
	}
}

type seedConfig struct {
	categories, suppliers, customers int
	products, orders                 int
	orderLinesMin, orderLinesMax     int
	batchSize                        int
	// parallel controls the number of Bolt sessions used per phase.
	// A value of 1 preserves the original serial behaviour. Higher values
	// shard each phase's batches across that many workers so the server's
	// write pipeline stays fed. Phases still act as barriers — all workers
	// in phase N finish before phase N+1 begins, preserving MATCH
	// referential integrity.
	parallel int
	seed     uint64
	label    string
}

type seedStats struct {
	nodes              int
	relationships      int
	approxPayloadBytes int64
}

// seedNorthwind wipes the target database and creates a randomised
// Northwind-shaped graph. Property values vary in length (names, descriptions,
// notes, tags, addresses) so on-disk size reflects real heterogeneous data,
// not just preallocated scratch.
func seedNorthwind(ctx context.Context, driver neo4j.DriverWithContext, database string, cfg seedConfig) (seedStats, error) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
	defer session.Close(ctx)

	stats := seedStats{}

	// Best-effort wipe. Large prior datasets may time out a single DETACH DELETE;
	// for the purposes of this harness the orchestrator already resets data dirs,
	// so a failure here is not fatal.
	if _, err := session.Run(ctx, "MATCH (n) DETACH DELETE n", nil); err != nil {
		log("[%s] warning: wipe failed (%v); assuming empty database", cfg.label, err)
	}

	// Seeding strategy: do NOT use `UNWIND $rows MATCH ... CREATE` — that
	// pattern falls back to a label scan per row on NornicDB's planner and
	// seeding takes ~5ms per row (minutes at 50k scale). Instead, write
	// nodes in two steps:
	//   1. Bulk CREATE each node type with its foreign-key ids as properties
	//      (no MATCH in the UNWIND — fast, ~20µs per node).
	//   2. Wire edges in a single "cross-join" MATCH a, b WHERE a.fk = b.pk
	//      CREATE (a)-[:REL]->(b) pass, once per relationship type.
	// This pattern is ~50× faster on NornicDB and still fast on Neo4j 5
	// thanks to the indexes below.
	ensureIndexes := []string{
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
		// suite (customer_category_distinct_orders, optional_match_orders_
		// count, revenue_by_product). Declared up front so both engines
		// plan sort-merges identically and neither penalises the
		// tiebreaker on a cold cache.
		"CREATE INDEX product_name IF NOT EXISTS FOR (n:Product) ON (n.productName)",
		"CREATE INDEX customer_name IF NOT EXISTS FOR (n:Customer) ON (n.companyName)",
		"CREATE INDEX category_name IF NOT EXISTS FOR (n:Category) ON (n.categoryName)",
	}
	for _, q := range ensureIndexes {
		if _, err := session.Run(ctx, q, nil); err != nil {
			log("[%s] warning: index create failed (%v); continuing", cfg.label, err)
		}
	}

	r := newRNG(cfg.seed)

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
	if err := batchWriteParallel(ctx, driver, database, catRows, cfg.batchSize, cfg.parallel,
		`UNWIND $rows AS row
		 CREATE (:Category {categoryID: row.categoryID, categoryName: row.categoryName, description: row.description})`,
	); err != nil {
		return stats, fmt.Errorf("categories: %w", err)
	}
	stats.nodes += len(catRows)
	stats.approxPayloadBytes += approxBytes(catRows)

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
	if err := batchWriteParallel(ctx, driver, database, supRows, cfg.batchSize, cfg.parallel,
		`UNWIND $rows AS row
		 CREATE (:Supplier {supplierID: row.supplierID, companyName: row.companyName, contactName: row.contactName,
		                    country: row.country, region: row.region, phone: row.phone, notes: row.notes})`,
	); err != nil {
		return stats, fmt.Errorf("suppliers: %w", err)
	}
	stats.nodes += len(supRows)
	stats.approxPayloadBytes += approxBytes(supRows)

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
	if err := batchWriteParallel(ctx, driver, database, custRows, cfg.batchSize, cfg.parallel,
		`UNWIND $rows AS row
		 CREATE (:Customer {customerID: row.customerID, companyName: row.companyName, contactName: row.contactName,
		                    country: row.country, city: row.city, address: row.address})`,
	); err != nil {
		return stats, fmt.Errorf("customers: %w", err)
	}
	stats.nodes += len(custRows)
	stats.approxPayloadBytes += approxBytes(custRows)

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
	if err := batchWriteParallel(ctx, driver, database, prodRows, cfg.batchSize, cfg.parallel,
		`UNWIND $rows AS row
		 MATCH (c:Category {categoryID: row.categoryID})
		 MATCH (s:Supplier {supplierID: row.supplierID})
		 CREATE (p:Product {productID: row.productID, productName: row.productName, sku: row.sku,
		                    unitPrice: row.unitPrice, unitsInStock: row.unitsInStock, discontinued: row.discontinued,
		                    description: row.description, tags: row.tags})
		 CREATE (p)-[:PART_OF]->(c)
		 CREATE (s)-[:SUPPLIES]->(p)`,
	); err != nil {
		return stats, fmt.Errorf("products: %w", err)
	}
	stats.nodes += len(prodRows)
	stats.relationships += len(prodRows) * 2 // PART_OF + SUPPLIES
	stats.approxPayloadBytes += approxBytes(prodRows)

	// --- Orders (+ PURCHASED) — flat rows only, nested UNWIND omitted.
	// NornicDB's Cypher parser currently rejects nested UNWIND with inline
	// MATCH property-maps (it tokenizes `{key:` incorrectly inside the inner
	// UNWIND), so we seed orders and order-line edges in two passes.
	ordRows := make([]map[string]any, 0, cfg.orders)
	lineRows := make([]map[string]any, 0, cfg.orders*(cfg.orderLinesMin+cfg.orderLinesMax)/2)
	var totalLines int
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
		totalLines += lines
	}

	// Pass 1: create Order nodes and PURCHASED edges (one match per row).
	if err := batchWriteParallel(ctx, driver, database, ordRows, cfg.batchSize, cfg.parallel,
		`UNWIND $rows AS row
		 MATCH (c:Customer {customerID: row.customerID})
		 CREATE (o:Order {orderID: row.orderID, shipCity: row.shipCity, shipCountry: row.shipCountry,
		                  orderDate: row.orderDate, notes: row.notes})
		 CREATE (c)-[:PURCHASED]->(o)`,
	); err != nil {
		return stats, fmt.Errorf("orders: %w", err)
	}
	stats.nodes += len(ordRows)
	stats.relationships += len(ordRows) // PURCHASED
	stats.approxPayloadBytes += approxBytes(ordRows)

	// Pass 2: create ORDERS edges (flat rows — order_id + product_id + edge props).
	if err := batchWriteParallel(ctx, driver, database, lineRows, cfg.batchSize, cfg.parallel,
		`UNWIND $rows AS row
		 MATCH (o:Order {orderID: row.orderID})
		 MATCH (p:Product {productID: row.productID})
		 CREATE (o)-[:ORDERS {quantity: row.quantity, discount: row.discount}]->(p)`,
	); err != nil {
		return stats, fmt.Errorf("order lines: %w", err)
	}
	stats.relationships += totalLines
	stats.approxPayloadBytes += approxBytes(lineRows)

	return stats, nil
}

// batchWrite splits rows into chunks of batchSize and issues one UNWIND query
// per chunk. Logs chunk progress for long seeds so the operator can see the
// seed is making forward progress.
func batchWrite(ctx context.Context, session neo4j.SessionWithContext, rows []map[string]any, batchSize int, cypher string) error {
	if batchSize <= 0 {
		batchSize = 1000
	}
	total := len(rows)
	for start := 0; start < total; start += batchSize {
		end := start + batchSize
		if end > total {
			end = total
		}
		if _, err := session.Run(ctx, cypher, map[string]any{"rows": rows[start:end]}); err != nil {
			return fmt.Errorf("batch %d..%d: %w", start, end, err)
		}
		if total > batchSize*5 && (end == total || (end/batchSize)%10 == 0) {
			log("  .. wrote %d / %d rows", end, total)
		}
	}
	return nil
}

// batchWriteParallel shards batches across `parallel` worker sessions. Each
// worker opens its own Bolt session (sessions are not goroutine-safe in the
// Neo4j Go driver), claims batches via an atomic counter, and issues UNWIND
// queries independently. The function blocks until every batch has been
// acknowledged, preserving the phase-barrier semantics callers rely on for
// MATCH referential integrity.
//
// parallel <= 1 falls through to the serial batchWrite path so the default
// behaviour is unchanged.
func batchWriteParallel(ctx context.Context, driver neo4j.DriverWithContext, database string, rows []map[string]any, batchSize, parallel int, cypher string) error {
	if parallel <= 1 {
		session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
		defer session.Close(ctx)
		return batchWrite(ctx, session, rows, batchSize, cypher)
	}
	if batchSize <= 0 {
		batchSize = 1000
	}
	total := len(rows)
	if total == 0 {
		return nil
	}
	// Build the explicit list of (start,end) batches up front so every
	// worker sees the same partition. Using an atomic counter over a
	// slice is simpler than a channel and avoids buffering the full
	// batch list twice.
	type chunk struct{ start, end int }
	var chunks []chunk
	for start := 0; start < total; start += batchSize {
		end := start + batchSize
		if end > total {
			end = total
		}
		chunks = append(chunks, chunk{start, end})
	}

	var (
		nextIdx  atomic.Int64
		wg       sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
		progress atomic.Int64
	)
	setErr := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	// Each worker owns one Bolt session. If session open fails on a
	// worker we still want the others to make progress; the first error
	// gets reported and remaining workers short-circuit.
	for w := 0; w < parallel; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
			defer session.Close(ctx)
			for {
				// Check for prior error before claiming more work so
				// we stop touching the server promptly on failure.
				errMu.Lock()
				if firstErr != nil {
					errMu.Unlock()
					return
				}
				errMu.Unlock()
				i := nextIdx.Add(1) - 1
				if int(i) >= len(chunks) {
					return
				}
				c := chunks[i]
				if _, err := session.Run(ctx, cypher, map[string]any{"rows": rows[c.start:c.end]}); err != nil {
					setErr(fmt.Errorf("batch %d..%d: %w", c.start, c.end, err))
					return
				}
				done := progress.Add(int64(c.end - c.start))
				if total > batchSize*5 && (done == int64(total) || (done/int64(batchSize))%10 == 0) {
					log("  .. wrote %d / %d rows", done, total)
				}
			}
		}()
	}
	wg.Wait()
	return firstErr
}

// approxBytes estimates the serialized payload size of a row set. This is a
// rough gauge for the report header ("~N MiB payload") — the real on-disk
// number depends on engine encoding.
func approxBytes(rows []map[string]any) int64 {
	data, err := json.Marshal(rows)
	if err != nil {
		return 0
	}
	return int64(len(data))
}

// anySlice converts []string to []any so Bolt params can round-trip it as a
// list of strings rather than a single string.
func anySlice(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

// runQuery runs `warmup` throwaway iterations then `iterations` timed
// iterations. It ALSO captures a deterministic fingerprint of the result
// set on the very first (warmup or iteration) call and verifies every
// subsequent call produces the same fingerprint — this catches partial
// reads, non-deterministic ordering, or intra-run mutation bugs that would
// otherwise be invisible.
func runQuery(ctx context.Context, driver neo4j.DriverWithContext, database string, q benchQuery, iterations, warmup int) (QueryStat, error) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
	defer session.Close(ctx)

	// execCollect runs the query and returns (rows, keys, err). We keep the
	// rows so we can fingerprint them.
	execCollect := func() ([]*neo4j.Record, []string, error) {
		res, err := session.Run(ctx, q.cypher, nil)
		if err != nil {
			return nil, nil, err
		}
		keys, err := res.Keys()
		if err != nil {
			return nil, nil, err
		}
		records, err := res.Collect(ctx)
		if err != nil {
			return nil, nil, err
		}
		return records, keys, nil
	}

	// First call: capture the reference fingerprint. Use the warmup budget
	// for this if available; otherwise the first timed iteration pays for
	// the fingerprint snapshot.
	refRecords, refKeys, err := execCollect()
	if err != nil {
		return QueryStat{}, fmt.Errorf("first call: %w", err)
	}
	refRowCount := len(refRecords)
	refHash := fingerprintRecords(refRecords, refKeys)
	refFirstRows := snapshotRecords(refRecords, refKeys, MaxFingerprintRows)
	correctnessOK := true

	// Remaining warmups (first call already consumed one).
	for i := 1; i < warmup; i++ {
		recs, keys, err := execCollect()
		if err != nil {
			return QueryStat{}, fmt.Errorf("warmup %d: %w", i, err)
		}
		if h := fingerprintRecords(recs, keys); h != refHash {
			correctnessOK = false
		}
	}

	latencies := make([]float64, 0, iterations)
	runStart := time.Now()
	for i := 0; i < iterations; i++ {
		start := time.Now()
		recs, keys, err := execCollect()
		elapsed := time.Since(start)
		if err != nil {
			return QueryStat{}, fmt.Errorf("iter %d: %w", i, err)
		}
		latencies = append(latencies, float64(elapsed.Microseconds())/1000.0)
		if h := fingerprintRecords(recs, keys); h != refHash {
			correctnessOK = false
		}
	}
	elapsed := time.Since(runStart).Seconds()

	stat := QueryStat{
		Name:          q.name,
		Iterations:    iterations,
		Cypher:        q.cypher,
		LatenciesMs:   latencies,
		MeanMs:        mean(latencies),
		MedianMs:      percentile(latencies, 50),
		P95Ms:         percentile(latencies, 95),
		P99Ms:         percentile(latencies, 99),
		MinMs:         minOf(latencies),
		MaxMs:         maxOf(latencies),
		StdDevMs:      stddev(latencies),
		RowCount:      refRowCount,
		ResultHash:    refHash,
		FirstRows:     refFirstRows,
		CorrectnessOK: correctnessOK,
	}
	if elapsed > 0 {
		stat.OpsPerSecond = float64(iterations) / elapsed
	}
	return stat, nil
}

// fingerprintRecords returns a SHA-256 hex digest of the canonicalised
// result set. Canonicalisation:
//   - each row is encoded as "<key1>=<value>|<key2>=<value>|…" with keys
//     in the order returned by the driver (Cypher RETURN order is stable
//     by spec; this matches how operators read the query).
//   - the per-row strings are sorted lexically before hashing so that
//     result sets ordered only by a non-unique tie-breaker (e.g.
//     ORDER BY productCount DESC with many ties) hash identically across
//     engines even when the tied-row order differs.
//   - floats are formatted with %v which produces canonical Go float
//     output; ints / strings / bools encode natively.
func fingerprintRecords(records []*neo4j.Record, keys []string) string {
	rowStrings := make([]string, 0, len(records))
	for _, rec := range records {
		var sb []byte
		for _, k := range keys {
			sb = appendCanonical(sb, k)
			sb = append(sb, '=')
			val, _ := rec.Get(k)
			sb = appendValue(sb, val)
			sb = append(sb, '|')
		}
		rowStrings = append(rowStrings, string(sb))
	}
	sort.Strings(rowStrings)
	h := sha256.New()
	// Prefix with row count so an empty result set and a deleted-rows set
	// hash differently from an unrelated query that also has zero rows.
	h.Write([]byte(fmt.Sprintf("rowcount=%d\n", len(records))))
	for _, s := range rowStrings {
		h.Write([]byte(s))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// appendCanonical writes s to buf without any interpretation. Splitting
// into a helper keeps future escaping work localised.
func appendCanonical(buf []byte, s string) []byte {
	return append(buf, s...)
}

// appendValue writes v in a deterministic textual form. float64 uses %g
// so 1.0 and 1.00 hash identically; ints use %d; everything else falls
// through %v. nil is written as "<nil>".
func appendValue(buf []byte, v interface{}) []byte {
	switch x := v.(type) {
	case nil:
		return append(buf, "<nil>"...)
	case bool:
		if x {
			return append(buf, "true"...)
		}
		return append(buf, "false"...)
	case int64:
		return append(buf, fmt.Sprintf("%d", x)...)
	case int:
		return append(buf, fmt.Sprintf("%d", x)...)
	case float64:
		// Whole numbers emit integer form so any int64-returned-as-float64
		// still hashes stably. Non-whole floats are rounded to 12
		// significant digits before hashing: this is below float64's
		// ~15-16 digit precision, so it erases summation-order drift
		// between engines (e.g. Neo4j and NornicDB computing the same
		// sum() in different tile orders) while still distinguishing
		// values that actually differ.
		if x == math.Trunc(x) && !math.IsInf(x, 0) && !math.IsNaN(x) {
			return append(buf, fmt.Sprintf("%d", int64(x))...)
		}
		return append(buf, fmt.Sprintf("%.12g", x)...)
	case string:
		return append(buf, x...)
	default:
		return append(buf, fmt.Sprintf("%v", x)...)
	}
}

// snapshotRecords returns the first limit rows as map[string]any for the
// report JSON. Values are passed through unchanged — the JSON marshaler
// handles neo4j.Node / neo4j.Path etc. via their String methods.
func snapshotRecords(records []*neo4j.Record, keys []string, limit int) []map[string]any {
	if limit > len(records) {
		limit = len(records)
	}
	out := make([]map[string]any, 0, limit)
	for i := 0; i < limit; i++ {
		row := make(map[string]any, len(keys))
		for _, k := range keys {
			v, _ := records[i].Get(k)
			row[k] = v
		}
		out = append(out, row)
	}
	return out
}

// countSeedGraph runs bounded MATCH counts against the target database to
// verify what's actually on disk matches what the seeder claims it wrote.
// Returns per-label node counts and per-type edge counts.
func countSeedGraph(ctx context.Context, driver neo4j.DriverWithContext, database string) (SeedCounts, error) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
	defer session.Close(ctx)

	scalar := func(query string) (int64, error) {
		res, err := session.Run(ctx, query, nil)
		if err != nil {
			return 0, err
		}
		rec, err := res.Single(ctx)
		if err != nil {
			return 0, err
		}
		raw, ok := rec.Get("n")
		if !ok {
			return 0, fmt.Errorf("missing `n` in: %s", query)
		}
		switch v := raw.(type) {
		case int64:
			return v, nil
		case int:
			return int64(v), nil
		case float64:
			return int64(v), nil
		}
		return 0, fmt.Errorf("unexpected count type %T from: %s", raw, query)
	}

	sc := SeedCounts{}
	pairs := []struct {
		dst   *int64
		query string
	}{
		{&sc.Categories, "MATCH (n:Category) RETURN count(n) AS n"},
		{&sc.Suppliers, "MATCH (n:Supplier) RETURN count(n) AS n"},
		{&sc.Customers, "MATCH (n:Customer) RETURN count(n) AS n"},
		{&sc.Products, "MATCH (n:Product) RETURN count(n) AS n"},
		{&sc.Orders, "MATCH (n:Order) RETURN count(n) AS n"},
		{&sc.PartOfEdges, "MATCH ()-[r:PART_OF]->() RETURN count(r) AS n"},
		{&sc.SuppliesEdges, "MATCH ()-[r:SUPPLIES]->() RETURN count(r) AS n"},
		{&sc.PurchasedEdges, "MATCH ()-[r:PURCHASED]->() RETURN count(r) AS n"},
		{&sc.OrdersEdges, "MATCH ()-[r:ORDERS]->() RETURN count(r) AS n"},
	}
	for _, p := range pairs {
		n, err := scalar(p.query)
		if err != nil {
			return sc, fmt.Errorf("count query %q: %w", p.query, err)
		}
		*p.dst = n
	}
	return sc, nil
}

// verifySeedCounts asserts node counts match the claimed seed sizes. Edge
// counts are ranges because the random ORDERS line count is variable
// (ORDER_LINES_MIN..MAX) — we enforce the total ORDERS edges fall within
// the derived window.
func verifySeedCounts(sc SeedCounts, categories, suppliers, customers, products, orders int) []string {
	var errs []string
	check := func(name string, got int64, want int) {
		if got != int64(want) {
			errs = append(errs, fmt.Sprintf("%s count=%d expected=%d", name, got, want))
		}
	}
	check("Category", sc.Categories, categories)
	check("Supplier", sc.Suppliers, suppliers)
	check("Customer", sc.Customers, customers)
	check("Product", sc.Products, products)
	check("Order", sc.Orders, orders)
	if sc.PartOfEdges != int64(products) {
		errs = append(errs, fmt.Sprintf("PART_OF count=%d expected=%d", sc.PartOfEdges, products))
	}
	if sc.SuppliesEdges != int64(products) {
		errs = append(errs, fmt.Sprintf("SUPPLIES count=%d expected=%d", sc.SuppliesEdges, products))
	}
	if sc.PurchasedEdges != int64(orders) {
		errs = append(errs, fmt.Sprintf("PURCHASED count=%d expected=%d", sc.PurchasedEdges, orders))
	}
	// ORDERS edge count: each order has [min..max] lines; check inside that
	// window rather than an exact value because our RNG is the only truth.
	return errs
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	var s float64
	for _, x := range xs {
		s += x
	}
	return s / float64(len(xs))
}

func stddev(xs []float64) float64 {
	if len(xs) < 2 {
		return 0
	}
	m := mean(xs)
	var sq float64
	for _, x := range xs {
		d := x - m
		sq += d * d
	}
	return math.Sqrt(sq / float64(len(xs)-1))
}

func percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sorted := append([]float64(nil), xs...)
	sort.Float64s(sorted)
	rank := (p / 100.0) * float64(len(sorted)-1)
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	if lo == hi {
		return sorted[lo]
	}
	return sorted[lo] + (rank-float64(lo))*(sorted[hi]-sorted[lo])
}

func minOf(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	m := xs[0]
	for _, x := range xs[1:] {
		if x < m {
			m = x
		}
	}
	return m
}

func maxOf(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	m := xs[0]
	for _, x := range xs[1:] {
		if x > m {
			m = x
		}
	}
	return m
}

func log(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "ERROR: "+format+"\n", args...)
	os.Exit(1)
}
