package bolt

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The message reader can exit without sending to either queue, for example
// when its connection context ends at server shutdown while a message waits
// for queue space. The connection handler must observe that exit instead of
// waiting for a message that can never arrive.
func TestSessionHandlerJoinsReaderExit(t *testing.T) {
	t.Run("no queued message ends with EOF", func(t *testing.T) {
		s := &Session{
			messageQueue:      make(chan *boltMessage),
			readErrQueue:      make(chan error, 1),
			messageReaderDone: make(chan struct{}),
		}
		// The reader has actually exited and sent nothing.
		close(s.messageReaderDone)

		result := make(chan error, 1)
		go func() {
			_, err := s.nextQueuedMessage()
			result <- err
		}()
		select {
		case err := <-result:
			require.ErrorIs(t, err, io.EOF)
		case <-time.After(5 * time.Second):
			t.Fatal("handler kept waiting after the message reader exited")
		}
	})

	t.Run("a message queued before exit is delivered first", func(t *testing.T) {
		queued := &boltMessage{msgType: MsgReset}
		s := &Session{
			messageQueue:      make(chan *boltMessage, 1),
			readErrQueue:      make(chan error, 1),
			messageReaderDone: make(chan struct{}),
		}
		s.messageQueue <- queued
		close(s.messageReaderDone)

		msg, err := s.nextQueuedMessage()
		require.NoError(t, err)
		require.Same(t, queued, msg)

		result := make(chan error, 1)
		go func() {
			_, err := s.nextQueuedMessage()
			result <- err
		}()
		select {
		case err := <-result:
			require.ErrorIs(t, err, io.EOF)
		case <-time.After(5 * time.Second):
			t.Fatal("handler kept waiting after draining the exited reader's queue")
		}
	})
}
