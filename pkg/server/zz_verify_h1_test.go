// Verification tests for hunt leads L-H1-001 .. L-H1-005 (v1.5.2 verification
// campaign, H1 verifier lane, 2026-11-21). Test-only, not for merge. Each lead
// test fails exactly at the disagreement the lead states; each control next to
// it is expected to pass. Every observation is logged as one "VERIFY_H1" line.
package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// verifyH1Server is one whole server (database, authenticator with an admin
// account, server, Bolt listener and driver) over dir, built as the hunt
// harness builds it.
type verifyH1Server struct {
	t      *testing.T
	dir    string
	db     *nornicdb.DB
	auth   *auth.Authenticator
	srv    *Server
	bolt   *bolt.Server
	served chan error
	driver neo4jdriver.DriverWithContext
	closed bool
}

func verifyH1OpenErr(t *testing.T, dir string, asyncWrites bool) (*verifyH1Server, error) {
	t.Helper()
	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Memory.QueryCacheEnabled = false
	config.Database.AsyncWritesEnabled = asyncWrites
	db, err := nornicdb.Open(dir, config)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	h := &verifyH1Server{t: t, dir: dir, db: db}
	t.Cleanup(h.Close)
	h.auth, err = auth.NewAuthenticator(auth.AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-key-for-testing-only-32b"),
	}, storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system"))
	if err != nil {
		return nil, fmt.Errorf("authenticator: %w", err)
	}
	if _, err := h.auth.CreateUser(context.Background(), "admin", "password123", []auth.Role{auth.RoleAdmin}); err != nil && !errors.Is(err, auth.ErrUserExists) {
		return nil, fmt.Errorf("create user: %w", err)
	}
	serverConfig := DefaultConfig()
	serverConfig.Port = 0
	serverConfig.EmbeddingEnabled = false
	h.srv, err = New(db, h.auth, serverConfig)
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	boltConfig := bolt.DefaultConfig()
	boltConfig.MaxConnections = 64
	h.bolt = bolt.NewWithDatabaseManager(boltConfig, nil, h.srv.dbManager)
	h.served = make(chan error, 1)
	go func() { h.served <- h.bolt.Serve(listener) }()
	h.driver, err = neo4jdriver.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4jdriver.NoAuth())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.driver.VerifyConnectivity(ctx); err != nil {
		return nil, err
	}
	return h, nil
}

func verifyH1Open(t *testing.T, dir string, asyncWrites bool) *verifyH1Server {
	t.Helper()
	h, err := verifyH1OpenErr(t, dir, asyncWrites)
	require.NoError(t, err, "open %s", dir)
	return h
}

// Close stops the server cleanly (driver, Bolt, server, database) and logs a
// close error of the database.
func (h *verifyH1Server) Close() {
	if h.closed {
		return
	}
	h.closed = true
	if h.driver != nil {
		_ = h.driver.Close(context.Background())
	}
	if h.bolt != nil {
		_ = h.bolt.Close()
		select {
		case <-h.served:
		case <-time.After(10 * time.Second):
		}
	}
	if h.srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = h.srv.Stop(ctx)
		cancel()
	}
	if h.db != nil {
		if err := h.db.Close(); err != nil {
			h.t.Logf("VERIFY_H1 close database %s: err=%v", filepath.Base(h.dir), err)
		}
	}
}

// verifyH1Outcome is one Bolt statement's outcome: rows, or an error.
type verifyH1Outcome struct {
	Rows [][]any
	Err  string
}

func (o verifyH1Outcome) String() string {
	if o.Err != "" {
		return "error: " + o.Err
	}
	return fmt.Sprintf("rows=%d %v", len(o.Rows), o.Rows)
}

// run executes one statement over Bolt, auto-commit or in its own explicit
// transaction (committed when it succeeds, rolled back when it fails), as the
// hunt harness does.
func (h *verifyH1Server) run(explicit bool, query string, params map[string]any) verifyH1Outcome {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	session := h.driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "nornic"})
	defer session.Close(context.Background())
	var out verifyH1Outcome
	collect := func(result neo4jdriver.ResultWithContext) error {
		records, err := result.Collect(ctx)
		if err != nil {
			return err
		}
		for _, record := range records {
			out.Rows = append(out.Rows, append([]any(nil), record.Values...))
		}
		return nil
	}
	var err error
	if explicit {
		var tx neo4jdriver.ExplicitTransaction
		if tx, err = session.BeginTransaction(ctx); err == nil {
			var result neo4jdriver.ResultWithContext
			if result, err = tx.Run(ctx, query, params); err == nil {
				err = collect(result)
			}
			if err == nil {
				err = tx.Commit(ctx)
			} else {
				_ = tx.Rollback(context.Background())
			}
		}
	} else {
		var result neo4jdriver.ResultWithContext
		if result, err = session.Run(ctx, query, params); err == nil {
			err = collect(result)
		}
	}
	if err != nil {
		out.Err = err.Error()
	}
	return out
}

func (h *verifyH1Server) mustRun(query string) verifyH1Outcome {
	h.t.Helper()
	out := h.run(false, query, nil)
	require.Empty(h.t, out.Err, query)
	return out
}

func (h *verifyH1Server) count(label string) int64 {
	h.t.Helper()
	out := h.mustRun("MATCH (n:" + label + ") RETURN count(n)")
	require.Len(h.t, out.Rows, 1)
	return out.Rows[0][0].(int64)
}

// ---------------------------------------------------------------------------
// L-H1-001 / L-H1-002: native backups and DB.Restore.

// verifyH1Backups writes into src and takes native backups (DB.Backup) under
// base until it has one whose first byte, after the whitespace trim of
// isJSONBackupFile, is '{' or '[' ("hit") and one whose first byte is not
// ("plain"). Each write changes the length of one property, so the length of
// the backup's first record changes with it. It returns both paths.
func verifyH1Backups(t *testing.T, src *verifyH1Server, base string) (hit, plain string) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < 3000 && (hit == "" || plain == ""); i++ {
		_, err := src.db.ExecuteCypher(ctx, "MERGE (p:Pad {k: 1}) SET p.s = $s", map[string]interface{}{"s": strings.Repeat("x", i)})
		require.NoError(t, err)
		path := filepath.Join(base, fmt.Sprintf("backup-%d.bak", i))
		require.NoError(t, src.db.Backup(ctx, path), "backup %d", i)
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(data), 8)
		first := bytes.TrimSpace(data)
		looksJSON := len(first) > 0 && (first[0] == '{' || first[0] == '[')
		switch {
		case looksJSON && hit == "":
			hit = path
			t.Logf("VERIFY_H1 backup %d: size=%d first8=% x firstRecordLen(uint64 LE)=%d looksJSON=true", i, len(data), data[:8], binary.LittleEndian.Uint64(data[:8]))
		case !looksJSON && plain == "":
			plain = path
			t.Logf("VERIFY_H1 backup %d: size=%d first8=% x firstRecordLen(uint64 LE)=%d looksJSON=false", i, len(data), data[:8], binary.LittleEndian.Uint64(data[:8]))
		default:
			_ = os.Remove(path)
		}
	}
	require.NotEmpty(t, hit, "no backup in 3000 starts with '{' or '['")
	require.NotEmpty(t, plain, "no backup without a JSON-looking first byte")
	return hit, plain
}

// L-H1-001: a native backup that DB.Backup wrote without an error restores
// with DB.Restore. The backup chosen is one whose first byte happens to be '{'
// or '['. The same file also goes through the engine's native restore on a
// second fresh store, to show it is a valid native backup.
func TestVerifyH1_001_NativeBackupRestores(t *testing.T) {
	base := t.TempDir()
	src := verifyH1Open(t, filepath.Join(base, "src"), false)
	hit, _ := verifyH1Backups(t, src, base)
	want := src.mustRun("MATCH (p:Pad) RETURN size(p.s)").Rows

	native := verifyH1Open(t, filepath.Join(base, "native"), false)
	restorer, ok := native.db.GetBaseStorageForManager().(interface{ Restore(string) error })
	require.True(t, ok, "base storage has a native Restore")
	nativeErr := restorer.Restore(hit)
	t.Logf("VERIFY_H1 engine native restore of the same file: err=%v", nativeErr)
	if nativeErr == nil {
		t.Logf("VERIFY_H1 engine native restore: Pad rows %v (source %v)", native.mustRun("MATCH (p:Pad) RETURN size(p.s)").Rows, want)
	}

	target := verifyH1Open(t, filepath.Join(base, "target"), false)
	err := target.db.Restore(context.Background(), hit)
	t.Logf("VERIFY_H1 DB.Restore of a native backup starting with '{' or '[': err=%v", err)
	require.NoError(t, nativeErr, "the file is a valid native backup")
	require.NoError(t, err, "DB.Restore of a native backup written without error")
	assert.Equal(t, want, target.mustRun("MATCH (p:Pad) RETURN size(p.s)").Rows)
}

// Control: a native backup from the same store whose first byte is not '{' or
// '[' restores with DB.Restore.
func TestVerifyH1_001_Control_PlainBackupRestores(t *testing.T) {
	base := t.TempDir()
	src := verifyH1Open(t, filepath.Join(base, "src"), false)
	_, plain := verifyH1Backups(t, src, base)
	target := verifyH1Open(t, filepath.Join(base, "target"), false)
	err := target.db.Restore(context.Background(), plain)
	t.Logf("VERIFY_H1 DB.Restore of a native backup not starting with '{' or '[': err=%v", err)
	require.NoError(t, err)
	assert.Equal(t, int64(1), target.count("Pad"))
}

// L-H1-002: the restore target, closed cleanly after the lead's sequence (one
// or more successful restores, then a restore refused as in L-H1-001), opens
// again. Variants separate the two restores:
//   - failedOnly: only the refused restore;
//   - successOnly: only a successful restore (L-H9-002's condition);
//   - successThenFailed: the lead's sequence.
func TestVerifyH1_002_RestoreTargetReopens(t *testing.T) {
	base := t.TempDir()
	src := verifyH1Open(t, filepath.Join(base, "src"), false)
	hit, plain := verifyH1Backups(t, src, base)
	for _, variant := range []string{"failedOnly", "successOnly", "successThenFailed"} {
		t.Run(variant, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "restore-target")
			target := verifyH1Open(t, dir, false)
			if variant != "failedOnly" {
				require.NoError(t, target.db.Restore(context.Background(), plain), "successful restore")
			}
			if variant != "successOnly" {
				err := target.db.Restore(context.Background(), hit)
				t.Logf("VERIFY_H1 %s: refused restore err=%v", variant, err)
				require.Error(t, err, "the JSON-looking native backup is refused (L-H1-001)")
			}
			target.Close()
			again, err := verifyH1OpenErr(t, dir, false)
			t.Logf("VERIFY_H1 %s: reopen restore target: err=%v", variant, err)
			require.NoError(t, err, "reopen the restore target")
			t.Logf("VERIFY_H1 %s: reopened, Pad count %d", variant, again.count("Pad"))
		})
	}
}

// Control: a target opened the same way, with no restore, reopens.
func TestVerifyH1_002_Control_TargetWithoutRestoreReopens(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "restore-target")
	target := verifyH1Open(t, dir, false)
	target.mustRun("CREATE (:Pad {k: 1})")
	target.Close()
	again, err := verifyH1OpenErr(t, dir, false)
	t.Logf("VERIFY_H1 control: reopen target without restore: err=%v", err)
	require.NoError(t, err)
	assert.Equal(t, int64(1), again.count("Pad"))
}

// ---------------------------------------------------------------------------
// L-H1-003: auto-commit and explicit transaction keep the same graph.

var verifyH1TxSequence = []string{
	"UNWIND [0] AS m CALL { UNWIND range(1,3) AS x RETURN x }",
	"RETURN x",
	"UNWIND [0] AS m CALL { UNWIND [0] AS x RETURN 1 / x AS y }",
	"UNWIND [0] AS m CREATE (n:Nor146Admission {m: m}) WITH m CALL { UNWIND range(1,3) AS x RETURN x }",
	"UNWIND [0] AS m CALL { WITH m CREATE (n:Nor146Unit {m: m}) }",
}

const verifyH1Stmt4 = "UNWIND [0] AS m CREATE (n:Nor146Admission {m: m}) WITH m CALL { UNWIND range(1,3) AS x RETURN x }"

func verifyH1TxRun(t *testing.T, explicit bool, statements []string) (outcomes []verifyH1Outcome, admission, unit int64) {
	t.Helper()
	h := verifyH1Open(t, filepath.Join(t.TempDir(), "db"), false)
	for i, q := range statements {
		out := h.run(explicit, q, nil)
		t.Logf("VERIFY_H1 explicit=%v statement %d: %s", explicit, i+1, out)
		outcomes = append(outcomes, out)
	}
	admission, unit = h.count("Nor146Admission"), h.count("Nor146Unit")
	t.Logf("VERIFY_H1 explicit=%v graph: Nor146Admission=%d Nor146Unit=%d", explicit, admission, unit)
	return outcomes, admission, unit
}

// L-H1-003, the lead's five statements: the graph afterwards is the same.
func TestVerifyH1_003_SequenceSameGraph(t *testing.T) {
	_, autoAdm, autoUnit := verifyH1TxRun(t, false, verifyH1TxSequence)
	_, explAdm, explUnit := verifyH1TxRun(t, true, verifyH1TxSequence)
	assert.Equal(t, explUnit, autoUnit, "Nor146Unit")
	assert.Equal(t, explAdm, autoAdm, "Nor146Admission: auto-commit vs explicit transaction")
}

// L-H1-003, statement 4 alone on a fresh database: a statement that fails
// leaves nothing written, on either path; both paths keep the same graph.
func TestVerifyH1_003_Statement4Alone(t *testing.T) {
	autoOut, autoAdm, _ := verifyH1TxRun(t, false, []string{verifyH1Stmt4})
	explOut, explAdm, _ := verifyH1TxRun(t, true, []string{verifyH1Stmt4})
	if autoOut[0].Err != "" {
		assert.Equal(t, int64(0), autoAdm, "auto-commit statement failed but its CREATE is stored")
	}
	if explOut[0].Err != "" {
		assert.Equal(t, int64(0), explAdm, "explicit statement failed but its CREATE is stored")
	}
	assert.Equal(t, explAdm, autoAdm, "Nor146Admission: auto-commit vs explicit transaction")
}

// Control: statement 5 alone gives the same graph on both paths.
func TestVerifyH1_003_Control_Statement5Alone(t *testing.T) {
	_, _, autoUnit := verifyH1TxRun(t, false, []string{verifyH1TxSequence[4]})
	_, _, explUnit := verifyH1TxRun(t, true, []string{verifyH1TxSequence[4]})
	assert.Equal(t, int64(1), autoUnit)
	assert.Equal(t, int64(1), explUnit)
}

// ---------------------------------------------------------------------------
// L-H1-004: path equality over a self-loop with asynchronous writes.

const verifyH1PathQuery = "MATCH p1 = (:A)-->()\nMATCH p2 = (:A)<--()\nRETURN p1 = p2"

// verifyH1LoopRuns repeats, on one server, n times: empty the database, run
// setup, then query at once (and, when settle > 0, the same query again after
// settle). It returns how many immediate and settled reads returned want rows.
func verifyH1LoopRuns(t *testing.T, asyncWrites bool, setup, query string, n int, settle time.Duration, want int) (immediateOK, settledOK int) {
	t.Helper()
	h := verifyH1Open(t, filepath.Join(t.TempDir(), "db"), asyncWrites)
	var firstBad string
	for i := 0; i < n; i++ {
		h.mustRun("MATCH (n) DETACH DELETE n")
		h.mustRun(setup)
		out := h.run(false, query, nil)
		if out.Err == "" && len(out.Rows) == want {
			immediateOK++
		} else if firstBad == "" {
			firstBad = fmt.Sprintf("iteration %d: %s", i, out)
		}
		if settle > 0 {
			time.Sleep(settle)
			again := h.run(false, query, nil)
			if again.Err == "" && len(again.Rows) == want {
				settledOK++
			}
		}
	}
	t.Logf("VERIFY_H1 async=%v setup=%q query=%q: immediate %d/%d ok, settled(%v) %d/%d ok; first bad: %s",
		asyncWrites, setup, query, immediateOK, n, settle, settledOK, n, firstBad)
	return immediateOK, settledOK
}

// L-H1-004: with asynchronous writes, the lead's query over a self-loop
// returns one row, every time, read at once and read 200 ms later.
func TestVerifyH1_004_AsyncSelfLoopPathEquality(t *testing.T) {
	const n = 100
	immediate, settled := verifyH1LoopRuns(t, true, "CREATE (n:A)-[:LOOP]->(n)", verifyH1PathQuery, n, 200*time.Millisecond, 1)
	// Separates the self-loop from visibility: a plain relationship read at once.
	plain, _ := verifyH1LoopRuns(t, true, "CREATE (:A)-[:R]->(:B)", "MATCH (:A)-->() RETURN 1", n, 0, 1)
	loopOneWay, _ := verifyH1LoopRuns(t, true, "CREATE (n:A)-[:LOOP]->(n)", "MATCH p1 = (:A)-->() RETURN p1", n, 0, 1)
	t.Logf("VERIFY_H1 async summary: pathEquality immediate=%d settled=%d plainRel=%d loopOneWay=%d of %d", immediate, settled, plain, loopOneWay, n)
	assert.Equal(t, n, immediate, "async: lead query read at once")
	assert.Equal(t, n, settled, "async: lead query read 200 ms later")
}

// Control: the same with durable writes.
func TestVerifyH1_004_Control_DurableSelfLoopPathEquality(t *testing.T) {
	const n = 100
	immediate, _ := verifyH1LoopRuns(t, false, "CREATE (n:A)-[:LOOP]->(n)", verifyH1PathQuery, n, 0, 1)
	assert.Equal(t, n, immediate)
}

// ---------------------------------------------------------------------------
// L-H1-005: change data capture after backup and restore.

func verifyH1CDCSource(t *testing.T, base string) (*verifyH1Server, string) {
	t.Helper()
	src := verifyH1Open(t, filepath.Join(base, "src"), false)
	for _, q := range []string{"ALTER DATABASE nornic SET OPTION txLogEnrichment 'FULL'", "CREATE (:Probe {i: $i})", "CALL db.cdc.current()"} {
		t.Logf("VERIFY_H1 source %q: %s", q, src.run(false, q, nil))
	}
	return src, filepath.Join(base, "cdc.bak")
}

// L-H1-005: a database whose txLogEnrichment is FULL still captures changes
// after a native backup of it is restored into a fresh server.
func TestVerifyH1_005_CDCAfterRestore(t *testing.T) {
	base := t.TempDir()
	src, path := verifyH1CDCSource(t, base)
	require.NoError(t, src.db.Backup(context.Background(), path))
	head := make([]byte, 1)
	f, err := os.Open(path)
	require.NoError(t, err)
	_, _ = f.Read(head)
	_ = f.Close()
	require.NotContains(t, []byte("{["), head[0], "backup happens to look like JSON (L-H1-001); rerun")

	target := verifyH1Open(t, filepath.Join(base, "target"), false)
	require.NoError(t, target.db.Restore(context.Background(), path))
	after := target.run(false, "CALL db.cdc.current()", nil)
	t.Logf("VERIFY_H1 target after restore: CALL db.cdc.current(): %s", after)
	options, optErr := target.srv.dbManager.DatabaseOptions("nornic")
	t.Logf("VERIFY_H1 target after restore: database manager options(nornic)=%v err=%v", options, optErr)
	t.Logf("VERIFY_H1 target after restore: Probe count %d", target.count("Probe"))

	// Diagnostic: reload the capture modes from the restored catalog record.
	loadErr := multidb.LoadChangeCaptureModes(context.Background(), target.db.GetBaseStorageForManager(), "system")
	reloaded := target.run(false, "CALL db.cdc.current()", nil)
	t.Logf("VERIFY_H1 target after LoadChangeCaptureModes from the restored catalog (err=%v): CALL db.cdc.current(): %s", loadErr, reloaded)

	assert.Empty(t, after.Err, "CALL db.cdc.current() after restore")
}

// Control: the source server, closed and reopened, still captures changes.
func TestVerifyH1_005_Control_CDCAfterReopen(t *testing.T) {
	base := t.TempDir()
	src, _ := verifyH1CDCSource(t, base)
	src.Close()
	again := verifyH1Open(t, filepath.Join(base, "src"), false)
	out := again.run(false, "CALL db.cdc.current()", nil)
	t.Logf("VERIFY_H1 source after reopen: CALL db.cdc.current(): %s", out)
	assert.Empty(t, out.Err)
}
