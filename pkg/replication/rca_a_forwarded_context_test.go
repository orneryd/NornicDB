package replication

// Root-cause diagnostic (campaign v1.5.2, lane C, group G-replication-forwarded-context; run by lane A).
// Test-side only. The forwarding follower's deadline and cancellation stay on
// the follower: SendForwardApply (transport.go:738) sends a ClusterMessage
// that has no deadline field (transport.go:96-105), and the receiving read
// loop calls the handler with the connection's own context
// (transport.go:670-677). The forward-apply handler then defaults its budget
// to 30 s when that context has no deadline (handlers.go:164-169). The
// diagnostic registers a probe handler for ClusterMsgForwardApply and logs what
// context it receives when the follower's call has a 300 ms deadline.

import (
	"context"
	"testing"
	"time"
)

func TestRCAForwardedHandlerContext(t *testing.T) {
	cfg := DefaultClusterTransportConfig()
	cfg.NodeID = "leader-1"
	cfg.AuthSecret = []byte(peerAuthTestSecret)
	leader := NewClusterTransport(cfg)
	defer leader.Close()
	seen := make(chan string, 4)
	leader.RegisterHandler(ClusterMsgForwardApply, func(ctx context.Context, nodeID string, msg *ClusterMessage) (*ClusterMessage, error) {
		deadline, has := ctx.Deadline()
		budget := 30 * time.Second
		if has {
			if d := time.Until(deadline); d > 0 && d < budget {
				budget = d
			}
		}
		select {
		case <-ctx.Done():
			seen <- "RCA_FWD handler ctx ended within 3s: " + ctx.Err().Error()
		case <-time.After(3 * time.Second):
			seen <- "RCA_FWD handler ctx still live 3s after receipt (follower deadline was 300ms)"
		}
		seen <- "RCA_FWD handler ctx hasDeadline=" + map[bool]string{true: "true", false: "false"}[has] + " handlers.go budget would be " + budget.String()
		return nil, nil
	})
	addr := tlsShutdownTestAddr(t)
	lctx, lcancel := context.WithCancel(context.Background())
	defer lcancel()
	go func() { _ = leader.Listen(lctx, addr, nil) }()
	waitTCPListening(t, addr)

	fcfg := DefaultClusterTransportConfig()
	fcfg.NodeID = "follower-1"
	fcfg.AuthSecret = []byte(peerAuthTestSecret)
	follower := NewClusterTransport(fcfg)
	defer follower.Close()
	cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ccancel()
	conn, err := follower.Connect(cctx, addr)
	if err != nil {
		t.Fatal(err)
	}
	callCtx, callCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer callCancel()
	start := time.Now()
	sendErr := conn.(*ClusterConnection).SendForwardApply(callCtx, &Command{RequestID: "rca-1"}, 300*time.Millisecond)
	t.Logf("RCA_FWD follower SendForwardApply returned after %s err=%v", time.Since(start), sendErr)
	for i := 0; i < 2; i++ {
		select {
		case line := <-seen:
			t.Log(line)
		case <-time.After(10 * time.Second):
			t.Fatal("RCA_FWD handler did not report")
		}
	}
}
