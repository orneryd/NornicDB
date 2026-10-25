package voyage

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type triageRoundTripFunc func(*http.Request) (*http.Response, error)

func (f triageRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type triageTrackedBody struct {
	io.Reader
	read   int
	closed bool
}

func (b *triageTrackedBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.read += n
	return n, err
}

func (b *triageTrackedBody) Close() error { b.closed = true; return nil }

func TestVoyageErrorBodyReadBound(t *testing.T) {
	const credential = "synthetic-issued-credential-39712684"
	const prefix = `{"message":"provider rejected request","access_token":"`
	const suffix = `"}`
	for _, size := range []int{256, 4096, 4097, 1 << 20} {
		padding := size - len(prefix) - len(credential) - len(suffix)
		body := &triageTrackedBody{Reader: strings.NewReader(prefix + credential + strings.Repeat("p", padding) + suffix)}
		client := &Client{
			apiKey:  "synthetic-request-key-318972",
			baseURL: "https://provider.example.test",
			httpClient: &http.Client{Transport: triageRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Header.Get("Authorization") != "Bearer synthetic-request-key-318972" {
					t.Error("runtime authentication changed")
				}
				return &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Retry-After": []string{"3"}}, Body: body, ContentLength: -1}, nil
			})},
		}
		err := client.postOnce(context.Background(), "/probe", nil, nil)
		var apiErr apiError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusTooManyRequests || !apiErr.Retryable() || apiErr.RetryDelay() != 3*time.Second {
			t.Fatal("bounded error read changed status or retry classification")
		}
		if body.read > 4097 || !body.closed {
			t.Errorf("error body was not bounded and closed: size=%d read=%d closed=%t", size, body.read, body.closed)
		}
		if strings.Contains(err.Error(), credential[:12]) {
			t.Error("error body disclosed a structured credential; capture withheld")
		}
		if size <= 4096 && !strings.Contains(apiErr.Body, "provider rejected request") {
			t.Error("complete error lost its public reason")
		}
		if size > 4096 && strings.Contains(apiErr.Body, "provider rejected request") {
			t.Error("incomplete oversized body was rendered")
		}
	}
}

func TestVoyageSuccessBodyKeepsLargeResponse(t *testing.T) {
	const size = 8192
	body := &triageTrackedBody{Reader: strings.NewReader(`{"value":"` + strings.Repeat("p", size) + `"}`)}
	client := &Client{baseURL: "https://provider.example.test", httpClient: &http.Client{Transport: triageRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
	})}}
	var response struct{ Value string }
	if err := client.postOnce(context.Background(), "/probe", nil, &response); err != nil || len(response.Value) != size || !body.closed {
		t.Fatal("error-body budget changed a successful response")
	}
}
