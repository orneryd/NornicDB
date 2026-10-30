// Neo4j → NornicDB migration (Go).
//
// Bolt → Bolt. Reads constraints, indexes, nodes, and relationships from a
// source Neo4j instance and replays them into NornicDB using hot-path
// UNWIND/MERGE shapes from docs/performance/hot-path-query-cookbook.md.
//
// Phases (always in this order):
//
//  1. Schema  — constraints, then indexes (vector and fulltext last).
//  2. Nodes   — UNWIND $rows MERGE on _neo4j_id key, SET props.
//                Hits UnwindSimpleMergeBatch.
//  3. Edges   — UNWIND $rows MATCH (a), MATCH (b) CREATE edge.
//                Hits UnwindMultiMatchCreateBatch.
//
// Run:
//
//   go run ./scripts/migration/neo4j/migrate.go \
//       --source-url bolt://neo4j.prod:7687 --source-user neo4j --source-pass <pw> \
//       --target-url bolt://nornicdb.local:7687 \
//       --batch-size 500
//
// Flags: --skip-schema, --skip-nodes, --skip-edges, --labels A,B, --types T,U,
// --dry-run.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type config struct {
	sourceURL, sourceUser, sourcePass, sourceDB string
	targetURL, targetUser, targetPass, targetDB string
	batchSize                                   int
	skipSchema, skipNodes, skipEdges            bool
	labels, types                               string
	dryRun                                      bool
}

func parseFlags() config {
	cfg := config{}
	flag.StringVar(&cfg.sourceURL, "source-url", "", "bolt://host:7687")
	flag.StringVar(&cfg.sourceUser, "source-user", "neo4j", "")
	flag.StringVar(&cfg.sourcePass, "source-pass", "", "")
	flag.StringVar(&cfg.sourceDB, "source-database", "neo4j", "")
	flag.StringVar(&cfg.targetURL, "target-url", "bolt://localhost:7687", "")
	flag.StringVar(&cfg.targetUser, "target-user", "neo4j", "")
	flag.StringVar(&cfg.targetPass, "target-pass", "password", "")
	flag.StringVar(&cfg.targetDB, "target-database", "neo4j", "")
	flag.IntVar(&cfg.batchSize, "batch-size", 500, "rows per UNWIND batch")
	flag.BoolVar(&cfg.skipSchema, "skip-schema", false, "")
	flag.BoolVar(&cfg.skipNodes, "skip-nodes", false, "")
	flag.BoolVar(&cfg.skipEdges, "skip-edges", false, "")
	flag.StringVar(&cfg.labels, "labels", "", "comma-separated label filter")
	flag.StringVar(&cfg.types, "types", "", "comma-separated relationship-type filter")
	flag.BoolVar(&cfg.dryRun, "dry-run", false, "")
	flag.Parse()
	if cfg.sourceURL == "" || cfg.sourcePass == "" {
		log.Fatal("--source-url and --source-pass are required")
	}
	return cfg
}

func csvSet(s string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, x := range strings.Split(s, ",") {
		if t := strings.TrimSpace(x); t != "" {
			out[t] = struct{}{}
		}
	}
	return out
}

// ---- Schema ----------------------------------------------------------------

func fetchRecords(ctx context.Context, sess neo4j.SessionWithContext, q string) ([]map[string]any, error) {
	res, err := sess.Run(ctx, q, nil)
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	for res.Next(ctx) {
		out = append(out, res.Record().AsMap())
	}
	return out, res.Err()
}

func toStringSlice(v any) []string {
	xs, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(xs))
	for _, x := range xs {
		if s, ok := x.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func constraintToCypher(c map[string]any) string {
	name, _ := c["name"].(string)
	ctype := strings.ToUpper(fmt.Sprint(c["type"]))
	entity := strings.ToUpper(fmt.Sprint(c["entityType"]))
	labels := toStringSlice(c["labelsOrTypes"])
	props := toStringSlice(c["properties"])
	if len(labels) == 0 || len(props) == 0 {
		return ""
	}
	label := labels[0]

	propParts := make([]string, len(props))
	for i, p := range props {
		propParts[i] = "n." + p
	}
	propList := strings.Join(propParts, ", ")
	if len(props) > 1 {
		propList = "(" + propList + ")"
	}

	switch entity {
	case "NODE":
		switch ctype {
		case "UNIQUENESS":
			return fmt.Sprintf("CREATE CONSTRAINT %s IF NOT EXISTS FOR (n:%s) REQUIRE %s IS UNIQUE", name, label, propList)
		case "NODE_KEY":
			return fmt.Sprintf("CREATE CONSTRAINT %s IF NOT EXISTS FOR (n:%s) REQUIRE %s IS NODE KEY", name, label, propList)
		case "NODE_PROPERTY_EXISTENCE":
			return fmt.Sprintf("CREATE CONSTRAINT %s IF NOT EXISTS FOR (n:%s) REQUIRE %s IS NOT NULL", name, label, propList)
		}
	case "RELATIONSHIP":
		switch ctype {
		case "RELATIONSHIP_PROPERTY_EXISTENCE":
			return fmt.Sprintf("CREATE CONSTRAINT %s IF NOT EXISTS FOR ()-[r:%s]-() REQUIRE r.%s IS NOT NULL", name, label, props[0])
		case "RELATIONSHIP_UNIQUENESS":
			return fmt.Sprintf("CREATE CONSTRAINT %s IF NOT EXISTS FOR ()-[r:%s]-() REQUIRE r.%s IS UNIQUE", name, label, props[0])
		}
	}
	return ""
}

func indexToCypher(idx map[string]any) string {
	if oc, _ := idx["owningConstraint"].(string); oc != "" {
		return ""
	}
	name, _ := idx["name"].(string)
	itype := strings.ToUpper(fmt.Sprint(idx["type"]))
	entity := strings.ToUpper(fmt.Sprint(idx["entityType"]))
	labels := toStringSlice(idx["labelsOrTypes"])
	props := toStringSlice(idx["properties"])
	if len(labels) == 0 || len(props) == 0 || entity != "NODE" {
		return ""
	}
	label := labels[0]

	switch itype {
	case "RANGE", "BTREE":
		if len(props) == 1 {
			return fmt.Sprintf("CREATE INDEX %s IF NOT EXISTS FOR (n:%s) ON (n.%s)", name, label, props[0])
		}
		propParts := make([]string, len(props))
		for i, p := range props {
			propParts[i] = "n." + p
		}
		return fmt.Sprintf("CREATE INDEX %s IF NOT EXISTS FOR (n:%s) ON (%s)", name, label, strings.Join(propParts, ", "))
	case "FULLTEXT":
		propParts := make([]string, len(props))
		for i, p := range props {
			propParts[i] = "n." + p
		}
		return fmt.Sprintf("CREATE FULLTEXT INDEX %s IF NOT EXISTS FOR (n:%s) ON EACH [%s]", name, label, strings.Join(propParts, ", "))
	case "VECTOR":
		opts, _ := idx["options"].(map[string]any)
		cfg, _ := opts["indexConfig"].(map[string]any)
		dims, _ := cfg["vector.dimensions"].(int64)
		sim, _ := cfg["vector.similarity_function"].(string)
		if dims == 0 {
			return ""
		}
		if sim == "" {
			sim = "cosine"
		}
		return fmt.Sprintf(
			"CREATE VECTOR INDEX %s IF NOT EXISTS FOR (n:%s) ON (n.%s) "+
				"OPTIONS {indexConfig: {`vector.dimensions`: %d, `vector.similarity_function`: '%s'}}",
			name, label, props[0], dims, sim,
		)
	}
	return ""
}

func replicateSchema(ctx context.Context, src, dst neo4j.SessionWithContext, dryRun bool) error {
	fmt.Println("\n[schema]")
	cs, err := fetchRecords(ctx, src, "SHOW CONSTRAINTS")
	if err != nil {
		return fmt.Errorf("fetch constraints: %w", err)
	}
	for _, c := range cs {
		ddl := constraintToCypher(c)
		if ddl == "" {
			fmt.Printf("  · skip constraint (unmapped): %v\n", c["name"])
			continue
		}
		fmt.Printf("  → %s\n", ddl)
		if dryRun {
			continue
		}
		if _, err := dst.Run(ctx, ddl, nil); err != nil {
			return fmt.Errorf("apply constraint: %w", err)
		}
	}

	is, err := fetchRecords(ctx, src, "SHOW INDEXES")
	if err != nil {
		return fmt.Errorf("fetch indexes: %w", err)
	}
	for _, i := range is {
		ddl := indexToCypher(i)
		if ddl == "" {
			continue
		}
		fmt.Printf("  → %s\n", ddl)
		if dryRun {
			continue
		}
		if _, err := dst.Run(ctx, ddl, nil); err != nil {
			return fmt.Errorf("apply index: %w", err)
		}
	}
	return nil
}

// ---- Nodes -----------------------------------------------------------------

const nodeUpsertCypher = `
UNWIND $rows AS row
MERGE (n:_Migrated {_neo4j_id: row.id})
SET n += row.props
SET n._neo4j_labels = row.labels
`

func fetchNodeLabels(ctx context.Context, sess neo4j.SessionWithContext) ([]string, error) {
	rs, err := fetchRecords(ctx, sess, "CALL db.labels() YIELD label RETURN label")
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		if s, ok := r["label"].(string); ok {
			out = append(out, s)
		}
	}
	return out, nil
}

func streamNodes(ctx context.Context, sess neo4j.SessionWithContext, label string, batch int, yield func([]map[string]any) error) error {
	var lastID any
	for {
		var (
			res neo4j.ResultWithContext
			err error
		)
		if lastID == nil {
			q := fmt.Sprintf(
				"MATCH (n:`%s`) "+
					"RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS props "+
					"ORDER BY elementId(n) LIMIT $limit",
				label,
			)
			res, err = sess.Run(ctx, q, map[string]any{"limit": batch})
		} else {
			q := fmt.Sprintf(
				"MATCH (n:`%s`) WHERE elementId(n) > $cursor "+
					"RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS props "+
					"ORDER BY elementId(n) LIMIT $limit",
				label,
			)
			res, err = sess.Run(ctx, q, map[string]any{"cursor": lastID, "limit": batch})
		}
		if err != nil {
			return err
		}
		var rows []map[string]any
		for res.Next(ctx) {
			rows = append(rows, res.Record().AsMap())
		}
		if err := res.Err(); err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		if err := yield(rows); err != nil {
			return err
		}
		lastID = rows[len(rows)-1]["id"]
		if len(rows) < batch {
			return nil
		}
	}
}

func replicateNodes(ctx context.Context, src, dst neo4j.SessionWithContext, labels []string, filter map[string]struct{}, batch int, dryRun bool) (int, error) {
	fmt.Println("\n[nodes]")
	total := 0
	for _, label := range labels {
		if len(filter) > 0 {
			if _, ok := filter[label]; !ok {
				continue
			}
		}
		if strings.HasPrefix(label, "_") {
			continue
		}
		moved := 0
		err := streamNodes(ctx, src, label, batch, func(chunk []map[string]any) error {
			if !dryRun {
				if _, err := dst.Run(ctx, nodeUpsertCypher, map[string]any{"rows": chunk}); err != nil {
					return err
				}
			}
			moved += len(chunk)
			total += len(chunk)
			fmt.Printf("  · :%s: %d\n", label, moved)
			return nil
		})
		if err != nil {
			return total, err
		}
	}
	fmt.Printf("  total nodes upserted: %d\n", total)
	return total, nil
}

// ---- Edges -----------------------------------------------------------------

func fetchRelTypes(ctx context.Context, sess neo4j.SessionWithContext) ([]string, error) {
	rs, err := fetchRecords(ctx, sess, "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		if s, ok := r["relationshipType"].(string); ok {
			out = append(out, s)
		}
	}
	return out, nil
}

func streamEdges(ctx context.Context, sess neo4j.SessionWithContext, relType string, batch int, yield func([]map[string]any) error) error {
	var lastID any
	for {
		var (
			res neo4j.ResultWithContext
			err error
		)
		if lastID == nil {
			q := fmt.Sprintf(
				"MATCH (a)-[r:`%s`]->(b) "+
					"RETURN elementId(r) AS id, elementId(a) AS startId, elementId(b) AS endId, properties(r) AS props "+
					"ORDER BY elementId(r) LIMIT $limit",
				relType,
			)
			res, err = sess.Run(ctx, q, map[string]any{"limit": batch})
		} else {
			q := fmt.Sprintf(
				"MATCH (a)-[r:`%s`]->(b) WHERE elementId(r) > $cursor "+
					"RETURN elementId(r) AS id, elementId(a) AS startId, elementId(b) AS endId, properties(r) AS props "+
					"ORDER BY elementId(r) LIMIT $limit",
				relType,
			)
			res, err = sess.Run(ctx, q, map[string]any{"cursor": lastID, "limit": batch})
		}
		if err != nil {
			return err
		}
		var rows []map[string]any
		for res.Next(ctx) {
			rows = append(rows, res.Record().AsMap())
		}
		if err := res.Err(); err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		if err := yield(rows); err != nil {
			return err
		}
		lastID = rows[len(rows)-1]["id"]
		if len(rows) < batch {
			return nil
		}
	}
}

func edgeCreateCypher(relType string) string {
	return fmt.Sprintf(
		"UNWIND $rows AS row "+
			"MATCH (a:_Migrated {_neo4j_id: row.startId}) "+
			"MATCH (b:_Migrated {_neo4j_id: row.endId}) "+
			"CREATE (a)-[:`%s` {_neo4j_id: row.id}]->(b)",
		relType,
	)
}

func edgePropsCypher(relType string) string {
	return fmt.Sprintf(
		"UNWIND $rows AS row "+
			"MATCH (a:_Migrated {_neo4j_id: row.startId})-[r:`%s` "+
			"{_neo4j_id: row.id}]->(b:_Migrated {_neo4j_id: row.endId}) "+
			"SET r += row.props",
		relType,
	)
}

func replicateEdges(ctx context.Context, src, dst neo4j.SessionWithContext, types []string, filter map[string]struct{}, batch int, dryRun bool) (int, error) {
	fmt.Println("\n[edges]")
	total := 0
	for _, t := range types {
		if len(filter) > 0 {
			if _, ok := filter[t]; !ok {
				continue
			}
		}
		moved := 0
		createQ := edgeCreateCypher(t)
		propsQ := edgePropsCypher(t)
		err := streamEdges(ctx, src, t, batch, func(chunk []map[string]any) error {
			if !dryRun {
				if _, err := dst.Run(ctx, createQ, map[string]any{"rows": chunk}); err != nil {
					return err
				}
				hasProps := false
				for _, r := range chunk {
					if p, ok := r["props"].(map[string]any); ok && len(p) > 0 {
						hasProps = true
						break
					}
				}
				if hasProps {
					if _, err := dst.Run(ctx, propsQ, map[string]any{"rows": chunk}); err != nil {
						return err
					}
				}
			}
			moved += len(chunk)
			total += len(chunk)
			fmt.Printf("  · :%s: %d\n", t, moved)
			return nil
		})
		if err != nil {
			return total, err
		}
	}
	fmt.Printf("  total edges created: %d\n", total)
	return total, nil
}

// ---- Main ------------------------------------------------------------------

func main() {
	cfg := parseFlags()
	labelFilter := csvSet(cfg.labels)
	typeFilter := csvSet(cfg.types)

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Hour)
	defer cancel()

	srcDrv, err := neo4j.NewDriverWithContext(cfg.sourceURL, neo4j.BasicAuth(cfg.sourceUser, cfg.sourcePass, ""))
	if err != nil {
		log.Fatalf("source driver: %v", err)
	}
	defer srcDrv.Close(ctx)
	dstDrv, err := neo4j.NewDriverWithContext(cfg.targetURL, neo4j.BasicAuth(cfg.targetUser, cfg.targetPass, ""))
	if err != nil {
		log.Fatalf("target driver: %v", err)
	}
	defer dstDrv.Close(ctx)

	src := srcDrv.NewSession(ctx, neo4j.SessionConfig{DatabaseName: cfg.sourceDB, AccessMode: neo4j.AccessModeRead})
	defer src.Close(ctx)
	dst := dstDrv.NewSession(ctx, neo4j.SessionConfig{DatabaseName: cfg.targetDB, AccessMode: neo4j.AccessModeWrite})
	defer dst.Close(ctx)

	started := time.Now()

	if !cfg.skipSchema {
		if err := replicateSchema(ctx, src, dst, cfg.dryRun); err != nil {
			log.Fatalf("schema: %v", err)
		}
	}
	if !cfg.skipNodes {
		labels, err := fetchNodeLabels(ctx, src)
		if err != nil {
			log.Fatalf("fetch labels: %v", err)
		}
		if _, err := replicateNodes(ctx, src, dst, labels, labelFilter, cfg.batchSize, cfg.dryRun); err != nil {
			log.Fatalf("nodes: %v", err)
		}
	}
	if !cfg.skipEdges {
		types, err := fetchRelTypes(ctx, src)
		if err != nil {
			log.Fatalf("fetch types: %v", err)
		}
		if _, err := replicateEdges(ctx, src, dst, types, typeFilter, cfg.batchSize, cfg.dryRun); err != nil {
			log.Fatalf("edges: %v", err)
		}
	}

	fmt.Printf("\nDone in %s\n", time.Since(started).Round(time.Second))
}
