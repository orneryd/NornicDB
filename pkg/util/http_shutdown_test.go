package util

import (
	"context"
	"net/http"
	"testing"
)

func TestShutdownHTTPServerWithoutActiveRequests(t *testing.T) {
	server := &http.Server{}
	for range 2 {
		if err := ShutdownHTTPServer(context.Background(), server); err != nil {
			t.Fatalf("shutdown without active requests: %v", err)
		}
	}
}
