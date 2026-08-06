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

	executed := false
	session := newTestSession(serverConn, &mockExecutor{
		executeFunc: func(_ context.Context, _ string, _ map[string]any) (*QueryResult, error) {
			executed = true
			return &QueryResult{}, nil
		},
	})
	session.server = &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
	session.authenticated = true
	session.inTransaction = inTransaction
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
	if executed {
		t.Fatal("write-mode procedure reached the executor for a read-only principal")
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

		executed := false
		session := newTestSession(serverConn, &mockExecutor{
			executeFunc: func(_ context.Context, _ string, _ map[string]any) (*QueryResult, error) {
				executed = true
				return &QueryResult{}, nil
			},
		})
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
		if code != "Neo.ClientError.Security.Forbidden" || message != "Schema operations require schema permission" {
			t.Fatalf("unexpected failure %q: %s", code, message)
		}
		if executed {
			t.Fatal("schema procedure reached the executor for a non-schema principal")
		}
	})

	t.Run("DBMS procedure requires admin", func(t *testing.T) {
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		defer clientConn.Close()

		executed := false
		session := newTestSession(serverConn, &mockExecutor{
			executeFunc: func(_ context.Context, _ string, _ map[string]any) (*QueryResult, error) {
				executed = true
				return &QueryResult{}, nil
			},
		})
		session.server = &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
		session.authenticated = true
		session.authResult = &BoltAuthResult{Authenticated: true, Permissions: []string{string(auth.PermRead)}}

		done := make(chan error, 1)
		go func() { done <- session.handleRun(buildRunMessageData("CALL dbms.components()", nil, nil)) }()
		code, message, err := AssertFailure(t, clientConn)
		if err != nil {
			t.Fatalf("expected permission failure: %v", err)
		}
		if err := <-done; err != nil {
			t.Fatalf("handle RUN: %v", err)
		}
		if code != "Neo.ClientError.Security.Forbidden" || message != "Admin operations require admin permission" {
			t.Fatalf("unexpected failure %q: %s", code, message)
		}
		if executed {
			t.Fatal("DBMS procedure reached the executor for a non-admin principal")
		}
	})

	t.Run("read procedure remains available", func(t *testing.T) {
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		defer clientConn.Close()

		session := newTestSession(serverConn, &mockExecutor{
			executeFunc: func(_ context.Context, _ string, _ map[string]any) (*QueryResult, error) {
				return &QueryResult{Columns: []string{"label"}}, nil
			},
		})
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
	if code != "Neo.ClientError.Security.Forbidden" || message != "Write operations require write permission" {
		t.Fatalf("unexpected failure %q: %s", code, message)
	}
}
