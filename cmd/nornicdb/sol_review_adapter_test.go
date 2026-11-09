package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/voyage"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSolReviewVoyageTruncatedError(t *testing.T) {
	const credential = "synthetic-sol-issued-access-credential-278163945"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer synthetic-sol-voyage-key-938162" {
			t.Error("runtime authentication changed")
		}
		w.WriteHeader(401)
		body := map[string]any{"access_token": credential, "diagnostic": strings.Repeat("d", 5000)}
		raw, _ := json.Marshal(body)
		_, _ = w.Write(raw)
	}))
	defer upstream.Close()
	client, err := voyage.NewClient(voyage.Config{APIKey: "synthetic-sol-voyage-key-938162", BaseURL: upstream.URL, MaxRetries: -1, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Rerank(context.Background(), "probe", []string{"document"}, voyage.RerankOptions{Model: "probe"})
	if err == nil {
		t.Fatal("upstream rejection was not returned")
	}
	if strings.Contains(err.Error(), credential) {
		t.Error("Voyage error disclosed a structured credential after the response was truncated; output withheld")
	}
	if !strings.Contains(err.Error(), fmt.Sprint(401)) {
		t.Error("status lost")
	}
}
