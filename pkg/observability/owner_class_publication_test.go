// Author: Codex (#1636 lane). Public retirement publication, wait and outcomes.
package observability_test

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/pkg/observability"
	"io"
	"testing"
	"time"
)

type classPublicationWriter struct{ syncFn func() error }

func (w classPublicationWriter) Write(p []byte) (int, error) { return io.Discard.Write(p) }
func (w classPublicationWriter) Sync() error                 { return w.syncFn() }

func TestOwnerClassPublicationForeignReentry(t *testing.T) {
	var provider *observability.Provider
	inner := make(chan error, 1)
	blocked := false
	failure := errors.New("publication native sync outcome")
	writer := classPublicationWriter{syncFn: func() error {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		go func() { inner <- provider.Shutdown(ctx) }()
		select {
		case err := <-inner:
			inner <- err
		case <-time.After(100 * time.Millisecond):
			blocked = true
		}
		return failure
	}}
	var err error
	provider, err = observability.New(context.Background(), observability.ObservabilityConfig{}, observability.ServiceInfo{Name: "publication"}, nil, writer)
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.Shutdown(context.Background()); !errors.Is(err, failure) {
		t.Errorf("native outcome lost: %v", err)
	}
	select {
	case err := <-inner:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("reentrant caller lost its wait cancellation: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("foreign cleanup callback could not reenter retirement")
	}
	if blocked {
		t.Error("retirement publication fence spans foreign cleanup")
	}
}

func TestOwnerClassPublicationBoundedWaitActualJoin(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	failure := errors.New("retained native sync outcome")
	writer := classPublicationWriter{syncFn: func() error { close(entered); <-release; return failure }}
	provider, err := observability.New(context.Background(), observability.ObservabilityConfig{}, observability.ServiceInfo{Name: "publication-wait"}, nil, writer)
	if err != nil {
		t.Fatal(err)
	}
	owner := make(chan error, 1)
	go func() { owner <- provider.Shutdown(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("native cleanup did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	caller := make(chan error, 1)
	go func() { caller <- provider.Shutdown(ctx) }()
	bounded := true
	select {
	case err := <-caller:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("wait did not return deadline: %v", err)
		}
	case <-time.After(150 * time.Millisecond):
		bounded = false
	}
	select {
	case <-owner:
		t.Error("original owner released before native completion")
	default:
	}
	close(release)
	if !bounded {
		select {
		case <-caller:
		case <-time.After(3 * time.Second):
			t.Fatal("bounded caller did not finish after controlled escape")
		}
		t.Error("publication wait ignored the caller deadline")
	}
	select {
	case err := <-owner:
		if !errors.Is(err, failure) {
			t.Errorf("retained native completion lost: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("native completion not joined")
	}
	if err := provider.Shutdown(context.Background()); !errors.Is(err, failure) {
		t.Errorf("eventual completion outcome not retained: %v", err)
	}
}
