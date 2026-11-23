package replication

import (
	"context"
	"log/slog"
	"time"
)

// forwardApplyRequest retains Command's flattened wire fields. Old readers
// can decode the command, while new readers also enforce the signed payload's
// deadline. This metadata never becomes part of the durable Command itself.
type forwardApplyRequest struct {
	Command          `msgpack:",inline"`
	DeadlineUnixNano int64 `msgpack:",omitempty"`
}

// dispatchRequest is the shared handler and reply publication owner.
func (c *ClusterConnection) dispatchRequest(ctx context.Context, msg *ClusterMessage, handler MessageHandler) bool {
	resp, err := handler(ctx, msg.NodeID, msg)
	if err != nil {
		c.transport.logPrintf(ctx, slog.LevelError, "[Cluster] Handler error: %v", err)
		return true
	}
	if resp == nil {
		return true
	}

	c.signMessage(resp)
	c.resources.mu.Lock()
	writeTimeout := c.writeTimeout
	if writeTimeout <= 0 {
		writeTimeout = 10 * time.Second
	}
	c.resources.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	werr := transferClusterMessage(resp, nil, c.writer, 0)
	if werr == nil {
		werr = c.writer.Flush()
	}
	c.resources.mu.Unlock()

	if werr != nil {
		c.transport.logPrintf(ctx, slog.LevelError, "[Cluster] Write error to %s: %v", c.addr, werr)
		return false
	}
	return true
}

// readDeadlineLocked preserves the timeout as an idle bound. A forwarded
// handler can exceed it; its caller/owner contexts bound preparation instead.
func (c *ClusterConnection) readDeadlineLocked() {
	deadline := time.Time{}
	if c.forwardDone == nil {
		timeout := c.readTimeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		deadline = time.Now().Add(timeout)
	}
	_ = c.resources.conn.SetReadDeadline(deadline)
}

func (c *ClusterConnection) prepareReadDeadline() {
	c.forwardMu.Lock()
	defer c.forwardMu.Unlock()
	c.readDeadlineLocked()
}

func (c *ClusterConnection) awaitForward(ctx context.Context) bool {
	c.forwardMu.Lock()
	done := c.forwardDone
	c.forwardMu.Unlock()
	if done == nil {
		return true
	}
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

// startForward leaves the reader free to observe caller EOF. The one admitted
// handler is joined by connection shutdown, while accepted writes retain the
// replicator/storage lifetime they already own.
func (c *ClusterConnection) startForward(ctx context.Context, msg *ClusterMessage, handler MessageHandler) bool {
	requestCtx, release, accepted := c.work.Acquire(ctx)
	if !accepted {
		return false
	}
	done := make(chan struct{})
	c.forwardMu.Lock()
	c.forwardDone = done
	c.readDeadlineLocked()
	c.forwardMu.Unlock()
	go func() {
		defer release()
		defer func() {
			c.forwardMu.Lock()
			c.forwardDone = nil
			c.readDeadlineLocked()
			close(done)
			c.forwardMu.Unlock()
		}()
		if !c.dispatchRequest(requestCtx, msg, handler) {
			c.work.BeginStop(nil)
		}
	}()
	return true
}
