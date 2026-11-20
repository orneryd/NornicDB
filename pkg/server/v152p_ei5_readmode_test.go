package server

// Earlier-issues batch 5, phase 2 (untracked probe; never committed):
// (1) Bolt read access mode with a user who may write: exact driver calls,
//     against this build (in-process Bolt server, admin user) and, when
//     NORNICDB_NEO4J_REFERENCE_URI is set, against the Neo4j reference only;
// (2) L427 over Bolt: a procedure returning (nil, nil), client-visible result.

import (
	"context"
	"fmt"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

type ei5RMCall struct {
	name string
	run  func(ctx context.Context, d neo4j.DriverWithContext, db, q string) error
}

func ei5RMConsume(ctx context.Context, r neo4j.ResultWithContext, err error) error {
	if err != nil {
		return err
	}
	_, err = r.Consume(ctx)
	return err
}

func ei5RMCalls() []ei5RMCall {
	sessionRun := func(mode neo4j.AccessMode) func(context.Context, neo4j.DriverWithContext, string, string) error {
		return func(ctx context.Context, d neo4j.DriverWithContext, db, q string) error {
			s := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: db, AccessMode: mode})
			defer s.Close(context.Background())
			r, err := s.Run(ctx, q, nil)
			return ei5RMConsume(ctx, r, err)
		}
	}
	explicit := func(mode neo4j.AccessMode) func(context.Context, neo4j.DriverWithContext, string, string) error {
		return func(ctx context.Context, d neo4j.DriverWithContext, db, q string) error {
			s := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: db, AccessMode: mode})
			defer s.Close(context.Background())
			tx, err := s.BeginTransaction(ctx)
			if err != nil {
				return fmt.Errorf("begin: %w", err)
			}
			r, err := tx.Run(ctx, q, nil)
			if err = ei5RMConsume(ctx, r, err); err != nil {
				_ = tx.Rollback(ctx)
				return fmt.Errorf("run: %w", err)
			}
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit: %w", err)
			}
			return nil
		}
	}
	managed := func(read bool) func(context.Context, neo4j.DriverWithContext, string, string) error {
		return func(ctx context.Context, d neo4j.DriverWithContext, db, q string) error {
			s := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: db})
			defer s.Close(context.Background())
			work := func(tx neo4j.ManagedTransaction) (any, error) {
				r, err := tx.Run(ctx, q, nil)
				if err != nil {
					return nil, err
				}
				return r.Consume(ctx)
			}
			var err error
			if read {
				_, err = s.ExecuteRead(ctx, work)
			} else {
				_, err = s.ExecuteWrite(ctx, work)
			}
			return err
		}
	}
	return []ei5RMCall{
		{"session(AccessModeRead).Run", sessionRun(neo4j.AccessModeRead)},
		{"session(AccessModeRead).BeginTransaction/tx.Run/tx.Commit", explicit(neo4j.AccessModeRead)},
		{"session.ExecuteRead(tx.Run)", managed(true)},
		{"neo4j.ExecuteQuery(..., ExecuteQueryWithReadersRouting())", func(ctx context.Context, d neo4j.DriverWithContext, db, q string) error {
			_, err := neo4j.ExecuteQuery(ctx, d, q, nil, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase(db), neo4j.ExecuteQueryWithReadersRouting())
			return err
		}},
		{"control session(AccessModeWrite).Run", sessionRun(neo4j.AccessModeWrite)},
		{"control session.ExecuteWrite(tx.Run)", managed(false)},
	}
}

// ei5RMMatrix runs every write statement through every call and reports the
// error and the store state after it (read through write routing).
func ei5RMMatrix(t *testing.T, tag string, d neo4j.DriverWithContext, db string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w := func(q string) ([]*neo4j.Record, error) {
		r, err := neo4j.ExecuteQuery(ctx, d, q, nil, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase(db), neo4j.ExecuteQueryWithWritersRouting())
		if err != nil {
			return nil, err
		}
		return r.Records, nil
	}
	state := func() string {
		recs, err := w("MATCH (n:Ei5RM) RETURN count(n) AS c, sum(coalesce(n.x, 0)) AS x")
		if err != nil || len(recs) == 0 {
			return fmt.Sprintf("state-error: %v", err)
		}
		c, _ := recs[0].Get("c")
		x, _ := recs[0].Get("x")
		ic := any("?")
		if ix, err := w("SHOW INDEXES YIELD name WHERE name = 'ei5rm_k' RETURN count(*) AS c"); err == nil && len(ix) > 0 {
			ic, _ = ix[0].Get("c")
		} else if err != nil {
			ic = "err:" + err.Error()
		}
		return fmt.Sprintf("nodes=%v sumX=%v index=%v", c, x, ic)
	}
	reset := func() {
		_, _ = w("MATCH (n:Ei5RM) DETACH DELETE n")
		_, _ = w("DROP INDEX ei5rm_k IF EXISTS")
		_, err := w("CREATE (:Ei5RM {k: 'seed'})")
		require.NoError(t, err)
	}
	stmts := []string{
		"CREATE (:Ei5RM {k: 'new'})",
		"MERGE (:Ei5RM {k: 'merged'})",
		"MATCH (n:Ei5RM {k: 'seed'}) SET n.x = 1",
		"MATCH (n:Ei5RM {k: 'seed'}) DETACH DELETE n",
		"CREATE INDEX ei5rm_k IF NOT EXISTS FOR (n:Ei5RM) ON (n.k)",
	}
	for _, c := range ei5RMCalls() {
		for _, q := range stmts {
			reset()
			before := state()
			err := c.run(ctx, d, db, q)
			msg := "<nil>"
			if err != nil {
				msg = err.Error()
				if len(msg) > 260 {
					msg = msg[:260]
				}
			}
			fmt.Printf("EI5_RM %s | %s | %s | before %s | error=%s | after %s\n", tag, c.name, q, before, msg, state())
		}
	}
	_, _ = w("MATCH (n:Ei5RM) DETACH DELETE n")
	_, _ = w("DROP INDEX ei5rm_k IF EXISTS")
	fmt.Printf("EI5_RM %s cleanup %s\n", tag, state())
}

func TestEi5BoltReadMode(t *testing.T) {
	if uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI"); uri != "" {
		d, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
		require.NoError(t, err)
		defer d.Close(context.Background())
		r, err := neo4j.ExecuteQuery(context.Background(), d, "CALL dbms.components() YIELD name, versions, edition RETURN name, versions, edition", nil, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("neo4j"))
		require.NoError(t, err)
		fmt.Printf("EI5_RM reference %v\n", r.Records[0].Values)
		ei5RMMatrix(t, "neo4j", d, "neo4j")
		return
	}
	ctx := t.Context()
	s, users := setupTestServer(t)

	require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: "ei5.boltnil", Mode: cypher.ProcedureModeRead, MinArgs: 0, MaxArgs: 0,
		Returns: []cypher.ProcedureColumn{{Name: "value", Type: "INTEGER"}}},
		func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error) { return nil, nil }))

	c := bolt.DefaultConfig()
	c.Port = 0
	c.RequireAuth = true
	a := bolt.NewAuthenticatorAdapter(users)
	a.SetGetEffectivePermissions(s.GetEffectivePermissions)
	c.Authenticator = a
	field := reflect.ValueOf(c).Elem().FieldByName("DatabaseExecutorOptions")
	method := reflect.ValueOf(s).MethodByName("DatabaseScopedExecutorOptions")
	if field.IsValid() && method.IsValid() {
		field.Set(method)
	}
	bs := bolt.NewWithDatabaseManager(c, nil, s.GetDatabaseManager())
	bs.SetDatabaseAccessModeResolver(s.GetDatabaseAccessModeForRoles)
	bs.SetResolvedAccessResolver(s.GetResolvedAccessForRoles)
	bs.SetProcedureGrantResolver(s.ProcedureGrantChecker)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- bs.Serve(listener) }()
	defer func() { _ = bs.Close(); <-done }()
	d, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://%s", listener.Addr()), neo4j.BasicAuth("admin", "password123", ""))
	require.NoError(t, err)
	defer d.Close(context.Background())

	collect := func(run func() (neo4j.ResultWithContext, error)) (int, error) {
		r, err := run()
		if err != nil {
			return 0, err
		}
		recs, err := r.Collect(ctx)
		return len(recs), err
	}
	for _, q := range []string{"RETURN 1 AS v", "CALL ei5.boltnil()", "CALL ei5.boltnil() YIELD value", "CALL ei5.boltnil() YIELD value RETURN value", "RETURN 2 AS v"} {
		sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
		n, err := collect(func() (neo4j.ResultWithContext, error) { return sess.Run(ctx, q, nil) })
		_ = sess.Close(context.Background())
		fmt.Printf("EI5_L427BOLT auto %q rows=%d err=%v\n", q, n, err)
	}
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	tx, err := sess.BeginTransaction(ctx)
	fmt.Printf("EI5_L427BOLT tx begin err=%v\n", err)
	if err == nil {
		for _, q := range []string{"CALL ei5.boltnil() YIELD value RETURN value", "RETURN 2 AS v"} {
			n, err := collect(func() (neo4j.ResultWithContext, error) { return tx.Run(ctx, q, nil) })
			fmt.Printf("EI5_L427BOLT tx %q rows=%d err=%v\n", q, n, err)
		}
		fmt.Printf("EI5_L427BOLT tx commit err=%v\n", tx.Commit(ctx))
	}
	_ = sess.Close(context.Background())
	sess = d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	n, err := collect(func() (neo4j.ResultWithContext, error) { return sess.Run(ctx, "RETURN 3 AS v", nil) })
	_ = sess.Close(context.Background())
	fmt.Printf("EI5_L427BOLT after (new session) rows=%d err=%v\n", n, err)

	ei5RMMatrix(t, "nornicdb", d, "nornic")
}
