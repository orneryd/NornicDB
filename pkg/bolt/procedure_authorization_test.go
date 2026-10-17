package bolt

import (
	"context"
	"net"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestBoltRejectsWriteModeProcedureForReadOnlyPrincipal(t *testing.T) {
	t.Run("autocommit", func(t *testing.T) {
		testBoltRejectsWriteModeProcedure(t, false)
	})
	t.Run("explicit transaction", func(t *testing.T) {
		testBoltRejectsWriteModeProcedure(t, true)
	})
}

func testBoltRejectsWriteModeProcedure(t *testing.T, inTransaction bool) {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	store := storage.NewMemoryEngine()
	session := newTestSession(serverConn, &cypherQueryExecutor{executor: cypher.NewStorageExecutor(store)})
	session.server = &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
	session.authenticated = true
	session.inTransaction = inTransaction
	if inTransaction {
		primeTestTransactionLifecycle(t, session)
	}
	session.authResult = &BoltAuthResult{
		Authenticated: true,
		Username:      "viewer",
		Permissions:   []string{string(auth.PermRead)},
	}

	done := make(chan error, 1)
	go func() {
		done <- session.handleRun(buildRunMessageData(
			"CALL apoc.periodic.commit($statement, {})",
			map[string]any{"statement": "CREATE (:Restricted)"},
			nil,
		))
	}()

	code, message, err := AssertFailure(t, clientConn)
	if err != nil {
		t.Fatalf("expected permission failure: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("handle RUN: %v", err)
	}
	if code != "Neo.ClientError.Security.Forbidden" {
		t.Fatalf("unexpected failure code %q: %s", code, message)
	}
	if nodes, err := store.GetNodesByLabel("Restricted"); err != nil || len(nodes) != 0 {
		t.Fatalf("refused statement changed storage: nodes=%v err=%v", nodes, err)
	}
}

func TestBoltProcedureModesRequireDeclaredPermissions(t *testing.T) {
	if err := cypher.RegisterUserProcedure(cypher.ProcedureSpec{
		Name:    "bolt.authorization.schema",
		Mode:    cypher.ProcedureModeSchema,
		MinArgs: 0,
		MaxArgs: 0,
	}, func(_ context.Context, _ *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
		return &cypher.ExecuteResult{}, nil
	}); err != nil {
		t.Fatalf("register schema procedure: %v", err)
	}

	t.Run("schema procedure requires schema", func(t *testing.T) {
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		defer clientConn.Close()

		session := newTestSession(serverConn, &cypherQueryExecutor{executor: cypher.NewStorageExecutor(storage.NewMemoryEngine())})
		session.server = &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
		session.authenticated = true
		session.authResult = &BoltAuthResult{Authenticated: true, Permissions: []string{string(auth.PermRead)}}

		done := make(chan error, 1)
		go func() { done <- session.handleRun(buildRunMessageData("CALL bolt.authorization.schema()", nil, nil)) }()
		code, message, err := AssertFailure(t, clientConn)
		if err != nil {
			t.Fatalf("expected permission failure: %v", err)
		}
		if err := <-done; err != nil {
			t.Fatalf("handle RUN: %v", err)
		}
		if code != "Neo.ClientError.Security.Forbidden" || message != "Schema operation 'create_index' on database 'nornic' is not allowed for user '' with no roles." {
			t.Fatalf("unexpected failure %q: %s", code, message)
		}
	})

	t.Run("admin procedure requires admin", func(t *testing.T) {
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		defer clientConn.Close()

		session := newTestSession(serverConn, &cypherQueryExecutor{executor: cypher.NewStorageExecutor(storage.NewMemoryEngine())})
		session.server = &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
		session.authenticated = true
		session.authResult = &BoltAuthResult{Authenticated: true, Permissions: []string{string(auth.PermRead)}}

		done := make(chan error, 1)
		go func() { done <- session.handleRun(buildRunMessageData("CALL dbms.listConfig()", nil, nil)) }()
		code, message, err := AssertFailure(t, clientConn)
		if err != nil {
			t.Fatalf("expected permission failure: %v", err)
		}
		if err := <-done; err != nil {
			t.Fatalf("handle RUN: %v", err)
		}
		if code != "Neo.ClientError.Security.Forbidden" || message != "Executing admin procedure 'dbms.listConfig' permission has not been granted for user '' with no roles." {
			t.Fatalf("unexpected failure %q: %s", code, message)
		}
	})

	t.Run("read procedure remains available", func(t *testing.T) {
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		defer clientConn.Close()

		session := newTestSession(serverConn, &cypherQueryExecutor{executor: cypher.NewStorageExecutor(storage.NewMemoryEngine())})
		session.server = &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
		session.authenticated = true
		session.authResult = &BoltAuthResult{Authenticated: true, Permissions: []string{string(auth.PermRead)}}

		done := make(chan error, 1)
		go func() { done <- session.handleRun(buildRunMessageData("CALL db.labels()", nil, nil)) }()
		if _, err := AssertSuccess(t, clientConn); err != nil {
			t.Fatalf("expected read procedure success: %v", err)
		}
		if err := <-done; err != nil {
			t.Fatalf("handle RUN: %v", err)
		}
	})
}

func TestBoltRejectsParameterizedDynamicWriteForReadOnlyPrincipal(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	executor := &cypherQueryExecutor{executor: cypher.NewStorageExecutor(storage.NewMemoryEngine())}
	session := newTestSession(serverConn, executor)
	session.server = &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
	session.authenticated = true
	session.authResult = &BoltAuthResult{Authenticated: true, Permissions: []string{string(auth.PermRead)}}

	done := make(chan error, 1)
	go func() {
		done <- session.handleRun(buildRunMessageData(
			"CALL apoc.cypher.run($statement, {})",
			map[string]any{"statement": "CREATE (:Restricted)"},
			nil,
		))
	}()

	code, message, err := AssertFailure(t, clientConn)
	if err != nil {
		t.Fatalf("expected dynamic statement permission failure: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("handle RUN: %v", err)
	}
	if code != "Neo.ClientError.Security.Forbidden" || message != "Write operations are not allowed for user '' with no roles." {
		t.Fatalf("unexpected failure %q: %s", code, message)
	}
}
