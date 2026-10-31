// Author: Codex, NOR-180 admission cleanup response propagation, 2026-10-27.
package bolt

import (
	"errors"
	"fmt"
	"io"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/memory"
)

type wireAdmissionError struct {
	cause          error
	remainingChunk int
	unfinished     bool
}

func (e *wireAdmissionError) Error() string { return e.cause.Error() }
func (e *wireAdmissionError) Unwrap() error { return e.cause }

// The framing reader has stopped before this is called. Send FAILURE first,
// then drain the rejected frame without allocating its payload. Closing a TCP
// socket with unread receive data can reset it and discard the failure reply.
func (s *Session) rejectWireAdmission(admission *wireAdmissionError) error {
	if err := s.sendApplicationAdmissionFailure(admission.cause); err != nil {
		return err
	}
	if !admission.unfinished || s.conn == nil || s.reader == nil {
		return nil
	}
	if err := s.conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		return err
	}
	remaining := admission.remainingChunk
	var header [2]byte
	for {
		if _, err := io.CopyN(io.Discard, s.reader, int64(remaining)); err != nil {
			return nil
		}
		if _, err := io.ReadFull(s.reader, header[:]); err != nil {
			return nil
		}
		remaining = int(header[0])<<8 | int(header[1])
		if remaining == 0 {
			return nil
		}
	}
}

func messageDecodesPayload(kind byte) bool {
	switch kind {
	case MsgHello, MsgBegin, MsgRun, MsgPull, MsgDiscard:
		return true
	default:
		return false
	}
}

func (s *Session) sendApplicationAdmissionFailure(err error) error {
	var cleanupErr error
	if s.inTransaction {
		cleanupErr = s.rollbackExplicitTransaction(transactionTerminalRollback)
	}
	if cleanupErr != nil && !nornicerrors.HasStatusDiagnostic(cleanupErr) {
		return cleanupErr
	}
	message := localization.ApplicationMemoryPressure()
	if errors.Is(err, memory.ErrStaleSample) {
		message = localization.ApplicationMemoryUnavailable()
	}
	primary := nornicerrors.WithStatus(err, "Neo.TransientError.General.MemoryPoolOutOfMemoryError", message)
	outcome := nornicerrors.CombineTerminalOutcomes(primary, cleanupErr)
	code, text := nornicerrors.RenderStatus(outcome, s.localize)
	if err := s.sendRunFailure(code, text); err != nil {
		return err
	}
	// The response carries the primary rejection and terminal diagnostic before
	// the existing fatal cleanup result closes the connection.
	return cleanupErr
}

// readWireMessage owns accumulation for both synchronous and queued readers.
// Only the two signature bytes are read before admission. Every subsequent
// chunk grows the same lease before the message buffer grows. Disabled policy
// keeps the historical unlimited framing behavior; there is no new default cap.
func (s *Session) readWireMessage(buffer []byte) (msg *boltMessage, wire []byte, err error) {
	wire = buffer[:0]
	var policy *memory.Policy
	if s.server != nil {
		policy = s.server.memoryPolicy
	}
	request := memory.NewWireRequest(policy)
	defer request.Close()
	var prefix [2]byte
	prefixLen := 0
	total := 0
	var chunkHeaders int64
	admitted := false
	var header [2]byte
	for {
		if _, err = io.ReadFull(s.reader, header[:]); err != nil {
			return nil, wire, err
		}
		size := int(header[0])<<8 | int(header[1])
		chunkHeaders = memory.WireSize(chunkHeaders, 2)
		if size == 0 {
			if admitted {
				if err = request.Grow(int64(total), chunkHeaders); err != nil {
					return nil, wire, &wireAdmissionError{cause: err}
				}
			}
			break
		}
		if size > int(^uint(0)>>1)-total {
			return nil, wire, fmt.Errorf("Bolt message length overflow")
		}
		total += size
		take := min(size, len(prefix)-prefixLen)
		if take > 0 {
			if _, err = io.ReadFull(s.reader, prefix[prefixLen:prefixLen+take]); err != nil {
				return nil, wire, err
			}
			prefixLen += take
		}
		if prefixLen < len(prefix) {
			continue
		}
		kind := prefix[0]
		if kind >= 0xB0 && kind <= 0xBF {
			kind = prefix[1]
		}
		// Empty terminal-control messages need no decoder or reservation,
		// allowing RESET/ROLLBACK/COMMIT/GOODBYE to release existing work.
		// Extra bytes on any message still receive the accumulation bound.
		if s.server != nil && s.server.memoryPolicy != nil && (messageDecodesPayload(kind) || total > 2) {
			err = request.Grow(int64(total), chunkHeaders)
			if err != nil {
				return nil, wire, &wireAdmissionError{cause: err, remainingChunk: size - take, unfinished: true}
			}
			admitted = true
		}
		oldLen := len(wire)
		if cap(wire) < total {
			// Preserve amortized growth for large client messages. At most two
			// wire copies fit inside the existing four-times-wire forecast.
			capacity := total
			if total <= int(^uint(0)>>1)/2 {
				capacity = total * 2
			}
			grown := make([]byte, total, capacity)
			copy(grown, wire)
			wire = grown
		} else {
			wire = wire[:total]
		}
		if oldLen == 0 {
			copy(wire, prefix[:])
		}
		if _, err = io.ReadFull(s.reader, wire[total-(size-take):total]); err != nil {
			return nil, wire, err
		}
	}
	if total == 0 {
		return nil, wire, nil
	}
	if total < 2 {
		return nil, wire, fmt.Errorf("message too short: %d bytes", total)
	}
	kind, offset := wire[0], 1
	if kind >= 0xB0 && kind <= 0xBF {
		kind, offset = wire[1], 2
	}
	return &boltMessage{msgType: kind, data: wire[offset:], memoryLease: request.Move()}, wire, nil
}

// releaseIncomingReservations runs after the session stops. Closing the socket
// joins the reader before draining its queue, so no late enqueue leaks a lease.
func (s *Session) releaseIncomingReservations() {
	if s.connCancel != nil {
		s.connCancel()
	}
	if s.conn != nil {
		_ = s.conn.Close()
	}
	if s.messageReaderDone != nil {
		<-s.messageReaderDone
	}
	if s.pendingMessage != nil {
		s.pendingMessage.memoryLease.Close()
	}
	for {
		select {
		case msg, ok := <-s.messageQueue:
			if !ok {
				return
			}
			if msg != nil {
				msg.memoryLease.Close()
			}
		default:
			return
		}
	}
}
