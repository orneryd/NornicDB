// Author: Codex (#1636 lane). Public supervision completion and outcome class.
package lifecycle_test

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"sync"
	"testing"
	"time"
)

type classComponent struct {
	name    string
	entered chan<- struct{}
	release chan struct{}
	once    sync.Once
	result  error
}

func (c *classComponent) Name() string { return c.name }
func (c *classComponent) Start(context.Context) error {
	c.entered <- struct{}{}
	<-c.release
	return c.result
}
func (c *classComponent) Shutdown(context.Context) error { c.unblock(); return nil }
func (c *classComponent) unblock()                       { c.once.Do(func() { close(c.release) }) }

func TestOwnerClassSupervisorAllStartOutcomes(t *testing.T) {
	entered := make(chan struct{}, 2)
	first, second := errors.New("component completion A"), errors.New("component completion B")
	a := &classComponent{name: "a", entered: entered, release: make(chan struct{}), result: first}
	b := &classComponent{name: "b", entered: entered, release: make(chan struct{}), result: second}
	defer a.unblock()
	defer b.unblock()
	done := make(chan error, 1)
	go func() { done <- lifecycle.Run(context.Background(), a, b) }()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("public component did not start")
		}
	}
	a.unblock()
	b.unblock()
	select {
	case err := <-done:
		if !errors.Is(err, first) || !errors.Is(err, second) {
			t.Errorf("supervisor lost a sibling Start completion: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("supervisor did not return")
	}
}
