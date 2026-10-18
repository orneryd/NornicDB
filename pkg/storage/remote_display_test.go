package storage

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
)

func TestRemoteDisplaySecretTransactionErrors(t *testing.T) {
	const credential = "synthetic-remote-credential-314159"
	for _, mode := range []string{"basic", "bearer", "uri"} {
		t.Run(mode, func(t *testing.T) {
			var upstream *httptest.Server
			upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/db/probe/tx" {
					fmt.Fprintf(w, `{"commit":%q,"errors":[]}`, upstream.URL+"/db/probe/tx/1/commit")
					return
				}
				w.WriteHeader(http.StatusUnauthorized)
				// Reflect the actual wire authorization representation.
				fmt.Fprintf(w, `{"errors":[{"code":"Neo.ClientError.Security.Unauthorized","message":%q}]}`, r.Header.Get("Authorization"))
			}))
			defer upstream.Close()
			cfg := RemoteEngineConfig{URI: upstream.URL, Database: "probe"}
			switch mode {
			case "basic":
				cfg.User, cfg.Password = "probe", credential
			case "bearer":
				cfg.AuthToken = "Bearer " + credential
			case "uri":
				cfg.URI = strings.Replace(upstream.URL, "://", "://probe:"+credential+"@", 1)
			}
			engine, err := NewRemoteEngine(cfg)
			if err != nil {
				t.Fatal("remote initialization failed (details withheld)")
			}
			defer engine.Close()
			check := func(err error) {
				t.Helper()
				if err == nil {
					t.Fatal("mock rejection produced no error")
				}
				var localized *localization.LocalizedError
				if !errors.As(err, &localized) {
					t.Fatal("localized error identity lost")
				}
				for _, text := range []string{err.Error(), localized.Error(), fmt.Sprint(localized.Message.Data)} {
					if strings.Contains(text, credential) || strings.Contains(text, base64.StdEncoding.EncodeToString([]byte("probe:"+credential))) {
						t.Error("remote error disclosed credential (details withheld)")
					}
				}
				if !strings.Contains(err.Error(), "Neo.ClientError.Security.Unauthorized") {
					t.Error("remote status code lost")
				}
			}
			_, _, err = engine.QueryCypher(context.Background(), "RETURN 1", nil)
			check(err)
			for _, operation := range []string{"query", "commit", "rollback"} {
				tx, err := engine.BeginCypherTx(context.Background())
				if err != nil {
					t.Fatal("transaction open failed (details withheld)")
				}
				switch operation {
				case "query":
					_, _, err = tx.QueryCypher(context.Background(), "RETURN 1", nil)
					defer tx.Rollback(context.Background())
				case "commit":
					err = tx.Commit(context.Background())
				case "rollback":
					err = tx.Rollback(context.Background())
				}
				check(err)
			}
		})
	}
}
