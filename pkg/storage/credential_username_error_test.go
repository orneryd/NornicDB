package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
)

func TestCredentialRemoteUserErrorProjection(t *testing.T) {
	const credential = "synthetic/token-user-871429"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok || username != credential || password != "" {
			t.Error("runtime did not consume username-only credentials")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, `{"errors":[{"code":"Neo.ClientError.Security.Unauthorized","message":%q}]}`, "provider refused "+username)
	}))
	defer upstream.Close()
	engine, err := NewRemoteEngine(RemoteEngineConfig{URI: upstream.URL, Database: "probe", User: credential})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	_, _, err = engine.QueryCypher(context.Background(), "RETURN 1", nil, RemoteAccessRead)
	if err == nil {
		t.Fatal("synthetic provider rejection was lost")
	}
	var localized *localization.LocalizedError
	if !errors.As(err, &localized) {
		t.Fatal("localized identity was lost")
	}
	for _, output := range []string{err.Error(), localized.Error(), fmt.Sprint(localized.Message.Data)} {
		if strings.Contains(output, credential) {
			t.Error("remote error discloses username-only credential after URL normalization; capture withheld")
		}
	}
}
