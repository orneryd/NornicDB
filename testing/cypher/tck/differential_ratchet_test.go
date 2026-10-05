package tck

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"

	"github.com/orneryd/nornicdb/testing/cypher/differential"
)

// differentialCorpusDir holds the ratcheted corpora and their known
// differences (#754).
var differentialCorpusDir = filepath.Join("testdata", "differential")

// TestDifferentialRatchetBoltMatchesPinnedNeo4j runs the sweep and the issue
// reproductions through Bolt auto-commit and Bolt explicit transactions on
// NornicDB and the pinned Neo4j, and fails when a statement differs that isn't
// in known_mismatches.jsonl (differential.Assert).
func TestDifferentialRatchetBoltMatchesPinnedNeo4j(t *testing.T) {
	referenceURI := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if referenceURI == "" {
		t.Skip("set NORNICDB_NEO4J_REFERENCE_URI to run the differential ratchet against the pinned Neo4j")
	}
	sweep, err := differential.LoadSweep(filepath.Join(differentialCorpusDir, "sweep.json.gz"))
	if err != nil {
		t.Fatal(err)
	}
	issues, err := differential.LoadIssues(filepath.Join(differentialCorpusDir, "issues.json"))
	if err != nil {
		t.Fatal(err)
	}
	referenceDriver, err := neo4j.NewDriverWithContext(referenceURI, neo4j.NoAuth())
	if err != nil {
		t.Fatalf("create Neo4j reference driver: %v", err)
	}
	defer referenceDriver.Close(context.Background())
	connectCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := waitForReference(connectCtx, referenceDriver); err != nil {
		t.Fatalf("connect to pinned Neo4j reference: %v", err)
	}

	for _, route := range []struct {
		name string
		mode TransactionMode
	}{{"bolt-auto", AutocommitMode}, {"bolt-tx", ExplicitTransactionMode}} {
		t.Run(route.name, func(t *testing.T) {
			nornicDriver, shutdown := startConformanceServer(t)
			defer shutdown()
			pair := &differential.Pair{
				Neo4j:    newBoltDifferentialExecutor(t, referenceDriver, "neo4j", route.mode),
				NornicDB: newBoltDifferentialExecutor(t, nornicDriver, "nornic", route.mode),
			}
			differential.Assert(t, filepath.Join(differentialCorpusDir, "known_mismatches.jsonl"), route.name, func() ([]differential.Result, int, error) {
				return differential.RunCorpora(context.Background(), pair, sweep, issues, t.Logf)
			})
		})
	}
}

// boltDifferentialExecutor runs statements through one Bolt route itself, so
// the outcome keeps every value's Bolt type, and resets with BoltBackend.
type boltDifferentialExecutor struct {
	driver   neo4j.DriverWithContext
	database string
	mode     TransactionMode
	backend  *BoltBackend
}

func newBoltDifferentialExecutor(t *testing.T, driver neo4j.DriverWithContext, database string, mode TransactionMode) boltDifferentialExecutor {
	return boltDifferentialExecutor{driver: driver, database: database, mode: mode,
		backend: newDifferentialBackend(t, driver, database, mode)}
}

func (executor boltDifferentialExecutor) Execute(ctx context.Context, query string) (outcome differential.Outcome) {
	session := executor.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: executor.database})
	defer session.Close(ctx)
	collect := func(result neo4j.ResultWithContext) error {
		keys, err := result.Keys()
		if err != nil {
			return err
		}
		records, err := result.Collect(ctx)
		if err != nil {
			return err
		}
		outcome.Columns = keys
		outcome.Rows = make([][]any, len(records))
		for index, record := range records {
			row := make([]any, len(record.Values))
			for column, value := range record.Values {
				row[column] = differentialOutcomeValue(value)
			}
			outcome.Rows[index] = row
		}
		return nil
	}
	var err error
	if executor.mode == ExplicitTransactionMode {
		var tx neo4j.ExplicitTransaction
		if tx, err = session.BeginTransaction(ctx); err == nil {
			var result neo4j.ResultWithContext
			if result, err = tx.Run(ctx, query, nil); err == nil {
				err = collect(result)
			}
			if err == nil {
				err = tx.Commit(ctx)
			} else {
				_ = tx.Rollback(ctx)
			}
		}
	} else {
		var result neo4j.ResultWithContext
		if result, err = session.Run(ctx, query, nil); err == nil {
			err = collect(result)
		}
	}
	if err != nil {
		var boltErr *neo4j.Neo4jError
		if errors.As(err, &boltErr) {
			return differential.Outcome{Code: boltErr.Code, Message: boltErr.Msg}
		}
		return differential.Outcome{Code: "client:" + err.Error()}
	}
	return outcome
}

func (executor boltDifferentialExecutor) Reset(ctx context.Context) error {
	return executor.backend.Reset(ctx)
}

// differentialOutcomeValue writes a Bolt value so that equal Cypher values
// encode equally on both servers and values of different types never do:
// every scalar carries its type ({"Integer": "1"}, {"Float": "1"},
// {"Date": "2020-01-02"}), entities lose their internal identities, and a
// node's labels are sorted.
func differentialOutcomeValue(value any) any {
	switch typed := value.(type) {
	case nil, bool, string:
		return typed
	case int64:
		return map[string]string{"Integer": strconv.FormatInt(typed, 10)}
	case float64:
		if math.IsNaN(typed) {
			return map[string]string{"Float": "NaN"}
		}
		return map[string]string{"Float": strconv.FormatFloat(typed, 'g', -1, 64)}
	case []byte:
		return map[string]string{"ByteArray": fmt.Sprintf("%x", typed)}
	case []any:
		items := make([]any, len(typed))
		for index, item := range typed {
			items[index] = differentialOutcomeValue(item)
		}
		return items
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			result[key] = differentialOutcomeValue(item)
		}
		return result
	case dbtype.Node:
		labels := append([]string(nil), typed.Labels...)
		sort.Strings(labels)
		return map[string]any{"Node": labels, "properties": differentialOutcomeValue(typed.Props)}
	case dbtype.Relationship:
		return map[string]any{"Relationship": typed.Type, "properties": differentialOutcomeValue(typed.Props)}
	case dbtype.Path:
		nodes := make([]any, len(typed.Nodes))
		for index, node := range typed.Nodes {
			nodes[index] = differentialOutcomeValue(node)
		}
		relationships := make([]any, len(typed.Relationships))
		for index, relationship := range typed.Relationships {
			relationships[index] = differentialOutcomeValue(relationship)
		}
		return map[string]any{"Path": nodes, "relationships": relationships}
	case dbtype.Date:
		return map[string]string{"Date": typed.Time().Format("2006-01-02")}
	case dbtype.LocalTime:
		return map[string]string{"LocalTime": typed.Time().Format("15:04:05.999999999")}
	case dbtype.Time:
		return map[string]string{"Time": typed.Time().Format("15:04:05.999999999Z07:00")}
	case dbtype.LocalDateTime:
		return map[string]string{"LocalDateTime": typed.Time().Format("2006-01-02T15:04:05.999999999")}
	case time.Time:
		return map[string]string{"DateTime": typed.Format(time.RFC3339Nano) + "[" + typed.Location().String() + "]"}
	case dbtype.Duration:
		return map[string]string{"Duration": formatBoltDuration(typed)}
	case dbtype.Point2D:
		return map[string]string{"Point": fmt.Sprintf("%d %v %v", typed.SpatialRefId, typed.X, typed.Y)}
	case dbtype.Point3D:
		return map[string]string{"Point": fmt.Sprintf("%d %v %v %v", typed.SpatialRefId, typed.X, typed.Y, typed.Z)}
	}
	return map[string]string{fmt.Sprintf("%T", value): fmt.Sprintf("%v", value)}
}
