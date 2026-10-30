// Author: Codex. Public manager retirement must join stopped native timers.
package txsession

import (
	"context"
	"testing"
	"time"
)

func TestManagerManualExpiryJoinsStoppedTimer(t *testing.T) {
	for _, operation := range []string{"touch", "admission"} {
		t.Run(operation, func(t *testing.T) {
			manager := NewManager(time.Hour, newExecutorFactory(t))
			defer manager.Close()
			now := time.Unix(1700000000, 0)
			manager.nowFunc = func() time.Time { return now }
			session, err := manager.Open(context.Background(), "nornic")
			if err != nil {
				t.Fatal(err)
			}
			now = now.Add(2 * time.Hour)
			if operation == "touch" {
				manager.Touch(session)
			} else if lease, ok := manager.AcquireForOwner(session.ID, ""); ok {
				lease.Release()
				t.Error("expired opening admitted another request")
			}
			if _, ok := manager.Get(session.ID); ok {
				t.Error("manual expiry left its retired session available")
			}
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := manager.WaitClosed(ctx); err != nil {
				t.Fatalf("manual retirement retained its stopped native timer: %v", err)
			}
		})
	}
}
