package bolt

import (
	"crypto/tls"
	"io"
	"net"
	"syscall"
	"time"
)

func (s *Session) startMessageReader() {
	if s.reader == nil || s.messageQueue == nil || s.readErrQueue == nil {
		return
	}
	var connDone <-chan struct{}
	if s.connCtx != nil {
		connDone = s.connCtx.Done()
	}

	go func() {
		for {
			msg, err := s.readMessage()
			if err != nil {
				if s.connCancel != nil {
					s.connCancel()
				}
				s.cancelActiveRun(0)
				select {
				case s.readErrQueue <- err:
				default:
				}
				return
			}
			if msg == nil {
				continue
			}
			if msg.msgType == MsgReset || msg.msgType == MsgGoodbye {
				s.cancelActiveRun(msg.msgType)
			}
			if !s.enqueueMessage(msg, connDone) {
				return
			}
		}
	}()
}

// enqueueMessage retains bounded backpressure while observing a TCP half-close.
// The ordinary reader cannot see EOF while blocked on a full message queue.
// A kernel hangup probe never consumes payload bytes or changes read deadlines.
func (s *Session) enqueueMessage(msg *boltMessage, done <-chan struct{}) bool {
	select {
	case s.messageQueue <- msg:
		return true
	case <-done:
		return false
	default:
	}
	closed, release := newPeerCloseProbe(s.conn)
	if release != nil {
		defer release()
	}
	if closed == nil {
		select {
		case s.messageQueue <- msg:
			return true
		case <-done:
			return false
		}
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case s.messageQueue <- msg:
			return true
		case <-done:
			return false
		case <-ticker.C:
			if closed() {
				if s.connCancel != nil {
					s.connCancel()
				}
				s.cancelActiveRun(0)
				select {
				case s.readErrQueue <- io.EOF:
				default:
				}
				return false
			}
		}
	}
}

// socketConn unwraps transport adapters without reading either protocol stream.
func socketConn(conn net.Conn) syscall.Conn {
	for conn != nil {
		switch c := conn.(type) {
		case *tls.Conn:
			conn = c.NetConn()
		case *peekedConn:
			conn = c.Conn
		case *wsConn:
			if c.ws == nil {
				return nil
			}
			conn = c.ws.UnderlyingConn()
		default:
			raw, _ := conn.(syscall.Conn)
			return raw
		}
	}
	return nil
}
