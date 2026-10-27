package storage_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const remoteAuthByteQuery = "RETURN 'wire-ok' AS marker"

type remoteAuthByteFixture struct {
	user, password, scheme  string
	calls, matched, queries atomic.Int64
}

func (f *remoteAuthByteFixture) Authenticate(scheme, principal, credentials string) (*bolt.BoltAuthResult, error) {
	f.calls.Add(1)
	valid := scheme == f.scheme
	if scheme == "basic" {
		valid = valid && principal == f.user && credentials == f.password
	} else {
		valid = valid && credentials == "forwarded-token"
	}
	if !valid {
		return nil, errors.New("fixture authentication rejected")
	}
	f.matched.Add(1)
	return &bolt.BoltAuthResult{Authenticated: true, Username: "wire-fixture", PrincipalID: "sub:wire-fixture", Roles: []string{"admin"}}, nil
}
func (f *remoteAuthByteFixture) Execute(_ context.Context, query string, _ map[string]any) (*bolt.QueryResult, error) {
	if query != remoteAuthByteQuery {
		return nil, errors.New("fixture query changed")
	}
	f.queries.Add(1)
	return &bolt.QueryResult{Columns: []string{"marker"}, Rows: [][]any{{"wire-ok"}}}, nil
}
func remoteAuthByteEndpoint(t *testing.T, transport string, f *remoteAuthByteFixture) string {
	t.Helper()
	if transport == "http" {
		peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			f.calls.Add(1)
			user, password, basic := r.BasicAuth()
			valid := basic && f.scheme == "basic" && user == f.user && password == f.password
			if f.scheme == "bearer" {
				valid = r.Header.Get("Authorization") == "Bearer forwarded-token"
			}
			if !valid {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = io.WriteString(w, `{"errors":[{"code":"Neo.ClientError.Security.Unauthorized","message":"fixture authentication rejected"}]}`)
				return
			}
			f.matched.Add(1)
			var request struct {
				Statements []struct {
					Statement string `json:"statement"`
				} `json:"statements"`
			}
			if r.URL.Path != "/db/nornic/tx/commit" || json.NewDecoder(r.Body).Decode(&request) != nil || len(request.Statements) != 1 || request.Statements[0].Statement != remoteAuthByteQuery {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			f.queries.Add(1)
			_, _ = io.WriteString(w, `{"results":[{"columns":["marker"],"data":[{"row":["wire-ok"]}]}],"errors":[]}`)
		}))
		t.Cleanup(peer.Close)
		return peer.URL
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("listener setup failed")
	}
	config := bolt.DefaultConfig()
	config.RequireAuth = true
	config.Authenticator = f
	config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	peer := bolt.New(config, f)
	peer.SetDatabaseAccessMode(auth.FullDatabaseAccessMode)
	done := make(chan error, 1)
	go func() { done <- peer.Serve(listener) }()
	t.Cleanup(func() {
		if peer.Close() != nil {
			t.Error("Bolt fixture close failed")
		}
		if <-done != nil {
			t.Error("Bolt fixture serve failed")
		}
	})
	return "bolt://" + listener.Addr().String()
}

// The source form uses a real incoming Bolt DDL path so doubled backticks are
// decoded by the production credential reader before outbound authentication.
func createEscapedRemoteAuthByteRef(t *testing.T, ctx context.Context, manager *multidb.DatabaseManager, endpoint string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("coordinator listener setup failed")
	}
	config := bolt.DefaultConfig()
	config.RequireAuth = false
	config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	peer := bolt.NewWithDatabaseManager(config, nil, manager)
	done := make(chan error, 1)
	go func() { done <- peer.Serve(listener) }()
	driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
	if err != nil {
		_ = peer.Close()
		<-done
		t.Fatal("coordinator driver setup failed")
	}
	t.Cleanup(func() {
		_ = driver.Close(context.Background())
		if peer.Close() != nil {
			t.Error("coordinator close failed")
		}
		if <-done != nil {
			t.Error("coordinator serve failed")
		}
	})
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "system"})
	defer session.Close(ctx)
	query := "CREATE COMPOSITE DATABASE wire_composite ALIAS r FOR DATABASE nornic AT '" + endpoint + "' USER ` us``er ` PASSWORD ` pa``ss ` ACCESS read"
	result, err := session.Run(ctx, query, nil)
	if err != nil {
		t.Fatal("escaped DDL was rejected; details withheld")
	}
	if _, err = result.Consume(ctx); err != nil {
		t.Fatal("escaped DDL did not complete; details withheld")
	}
}

func TestRemoteAuthenticationPreservesAdmittedCredentialBytesOnWire(t *testing.T) {
	for _, transport := range []string{"http", "bolt"} {
		for _, tc := range []struct {
			name, source, user, password, token string
			reject                              bool
		}{
			{"ordinary", "literal", "service", "synthetic-wire-password", "", false},
			{"cipher-looking literal password", "literal", "service", "enc:v1:synthetic-literal-password", "", false},
			{"cipher-looking URI password", "uri", "service", "enc:v1:synthetic-literal-password", "", false},
			{"surrounding spaces", "literal", " service ", " synthetic-wire-password ", "", false},
			{"unicode whitespace", "literal", "\tservice\u00a0", "\u00a0synthetic-wire-password\t", "", false},
			{"URI decoded", "uri", " service+é ", " synthetic:wire+/@%password ", "", false},
			{"URI whitespace only", "uri", " ", "\t", "", false},
			{"explicit precedence", "explicit-priority", " service ", " synthetic-wire-password ", "Bearer ignored-forwarded", false},
			{"forwarded precedence", "uri", " service ", " synthetic-wire-password ", "  Bearer forwarded-token  ", false},
			{"escaped DDL", "ddl", " us`er ", " pa`ss ", "", false},
			{"existing literal rejection", "literal", " ", "\t", "", true},
		} {
			t.Run(transport+"/"+tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				f := &remoteAuthByteFixture{user: tc.user, password: tc.password, scheme: "basic"}
				if tc.source == "uri" && tc.token != "" {
					f.scheme = "bearer"
				}
				endpoint := remoteAuthByteEndpoint(t, transport, f)
				cfg := multidb.DefaultConfig()
				cfg.RemoteCredentialEncryptionKey = "synthetic-wire-fixture-key"
				var enginesMu sync.Mutex
				var engines []*storage.RemoteEngine
				cfg.RemoteEngineFactory = func(ref multidb.ConstituentRef, token string) (storage.Engine, error) {
					engine, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, User: ref.User, Password: ref.Password, AuthToken: token})
					if err == nil {
						enginesMu.Lock()
						engines = append(engines, engine)
						enginesMu.Unlock()
					}
					return engine, err
				}
				manager, err := multidb.NewDatabaseManager(ctx, fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), cfg)
				if err != nil {
					t.Fatal("manager setup failed")
				}
				t.Cleanup(func() {
					enginesMu.Lock()
					owned := append([]*storage.RemoteEngine(nil), engines...)
					enginesMu.Unlock()
					for _, engine := range owned {
						_ = engine.Close()
					}
					if manager.Close() != nil {
						t.Error("manager close failed")
					}
				})
				ref := multidb.ConstituentRef{Alias: "r", DatabaseName: "nornic", Type: "remote", AccessMode: "read", URI: endpoint, AuthMode: "user_password", User: tc.user, Password: tc.password}
				if tc.source == "uri" || tc.source == "explicit-priority" {
					parsed, _ := url.Parse(endpoint)
					if tc.source == "uri" {
						parsed.User = url.UserPassword(tc.user, tc.password)
						ref.AuthMode = ""
						ref.User = ""
						ref.Password = ""
					} else {
						parsed.User = url.UserPassword("ignored-user", "ignored-password")
					}
					ref.URI = parsed.String()
				}
				if tc.source == "ddl" {
					createEscapedRemoteAuthByteRef(t, ctx, manager, endpoint)
				} else {
					err = manager.CreateCompositeDatabase(ctx, "wire_composite", []multidb.ConstituentRef{ref})
					if tc.reject {
						if err == nil {
							t.Fatal("existing explicit credential admission changed")
						}
						if f.calls.Load() != 0 {
							t.Error("rejected input reached endpoint")
						}
						return
					}
					if err != nil {
						t.Fatal("admitted credential setup rejected; details withheld")
					}
				}
				engine, err := manager.GetStorageWithAuth("wire_composite.r", tc.token)
				if err != nil {
					t.Fatal("runtime engine lookup failed; details withheld")
				}
				remote, ok := engine.(*storage.RemoteEngine)
				if !ok {
					t.Fatal("unexpected runtime transport")
				}
				columns, rows, err := remote.QueryCypher(ctx, remoteAuthByteQuery, nil, storage.RemoteAccessRead)
				if err != nil {
					t.Error("admitted exact credential did not authenticate; details withheld")
					return
				}
				if len(columns) != 1 || columns[0] != "marker" || len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] != "wire-ok" {
					t.Error("non-secret public query control failed")
				}
				if f.calls.Load() == 0 || f.matched.Load() == 0 || f.queries.Load() != 1 {
					t.Error("actual auth/query boundary was not reached exactly as expected")
				}
			})
		}
	}
}
