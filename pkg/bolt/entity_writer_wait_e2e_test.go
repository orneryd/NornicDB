// Bolt wire end-to-end coverage for NOR-213's write-wait contract on the
// public protocol: two sessions write one existing node, or one existing
// relationship, with different properties.
//
// The schedule is the original two-session one. The first writer's
// MATCH ... SET runs in an explicit transaction and holds the entity. The
// second session's MATCH ... SET, which writes the same entity with a
// different property, waits at its write instead of failing at commit; the
// first COMMIT releases it; the second then writes on top of that commit, so
// both properties survive. Before NOR-213 the second commit failed with a
// transaction conflict.
//
// Every statement travels a real Bolt connection through the session-scoped
// explicit-transaction executor, and the storage lock-wait hook - not timing -
// orders the two writers.
package bolt

import (
	"fmt"
	"net"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nor213WireWaitLimit bounds every wait these tests make: a schedule that has
// not settled by then failed, and the test releases the lock and reports it
// instead of hanging.
const nor213WireWaitLimit = 15 * time.Second

// nor213EntityShape is one entity two sessions write: the statements that
// create it, write its two properties from each session, and read them back.
type nor213EntityShape struct {
	name string
	// seed creates the entity both sessions write.
	seed string
	// hold writes the first property, under the first writer's lock.
	hold string
	// write writes the second property from the waiting session.
	write string
	// after writes a third property from a session that must not wait.
	after string
	// readWriters returns both writers' properties.
	readWriters string
	// readAll returns both writers' properties and after's.
	readAll string
}

// nor213EntityShapes are the two entities a writer contends on: a node and a
// relationship.
func nor213EntityShapes() []nor213EntityShape {
	return []nor213EntityShape{
		{
			name:        "node",
			seed:        "CREATE (:Doc {id: $k})",
			hold:        "MATCH (n:Doc {id: $k}) SET n.a = 1",
			write:       "MATCH (n:Doc {id: $k}) SET n.b = 2",
			after:       "MATCH (n:Doc {id: $k}) SET n.c = 3",
			readWriters: "MATCH (n:Doc {id: $k}) RETURN n.a, n.b",
			readAll:     "MATCH (n:Doc {id: $k}) RETURN n.a, n.b, n.c",
		},
		{
			name:        "relationship",
			seed:        "CREATE (:Doc {id: $k})-[:HAS]->(:Part)",
			hold:        "MATCH (:Doc {id: $k})-[r:HAS]->() SET r.a = 1",
			write:       "MATCH (:Doc {id: $k})-[r:HAS]->() SET r.b = 2",
			after:       "MATCH (:Doc {id: $k})-[r:HAS]->() SET r.c = 3",
			readWriters: "MATCH (:Doc {id: $k})-[r:HAS]->() RETURN r.a, r.b",
			readAll:     "MATCH (:Doc {id: $k})-[r:HAS]->() RETURN r.a, r.b, r.c",
		},
	}
}

// nor213EntityWriteWaits signals each time a transaction starts waiting for an
// entity or constraint-key write lock another transaction holds, so these
// tests order the two sessions by the wait itself.
func nor213EntityWriteWaits(t *testing.T) <-chan struct{} {
	t.Helper()
	waits := make(chan struct{}, 64)
	t.Cleanup(storage.SetUniqueKeyLockWaitHookForTesting(func() {
		select {
		case waits <- struct{}{}:
		default:
		}
	}))
	return waits
}

func nor213DrainEntityWriteWaits(waits <-chan struct{}) {
	for {
		select {
		case <-waits:
		default:
			return
		}
	}
}

// nor213BoltStore opens the engine shape a subtest covers: durable Badger on a
// temporary directory, or the same engine with write-behind commits.
func nor213BoltStore(t *testing.T, writeBehind bool) storage.Engine {
	t.Helper()
	options := storage.BadgerOptions{DataDir: t.TempDir()}
	if writeBehind {
		options.WriteBehind = true
		options.WriteBehindInterval = time.Hour
	}
	engine, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(options))
	require.NoError(t, err)
	return storage.NewNamespacedEngine(engine, "test")
}

// nor213RunBoltStatement runs one statement on conn and reports its outcome,
// including the server's FAILURE code and message. It never fails the test, so
// a statement another goroutine is blocked on can run off the test goroutine.
func nor213RunBoltStatement(t *testing.T, conn net.Conn, query string, params map[string]any) error {
	t.Helper()
	if err := SendRun(t, conn, query, params, nil); err != nil {
		return err
	}
	msgType, msgData, err := ReadMessage(conn)
	if err != nil {
		return err
	}
	if msgType != MsgSuccess {
		return nor213BoltOutcome(msgType, msgData)
	}
	if err := SendPull(t, conn, nil); err != nil {
		return err
	}
	for {
		msgType, msgData, err = ReadMessage(conn)
		if err != nil {
			return err
		}
		if msgType == MsgRecord {
			continue
		}
		if msgType != MsgSuccess {
			return nor213BoltOutcome(msgType, msgData)
		}
		return nil
	}
}

// nor213BoltOutcome reports a message that ended a statement other than with
// SUCCESS: a FAILURE as its code and message.
func nor213BoltOutcome(msgType byte, msgData []byte) error {
	if msgType != MsgFailure {
		return fmt.Errorf("unexpected Bolt message 0x%02X", msgType)
	}
	metadata, _, err := decodePackStreamMap(msgData, 0)
	if err != nil {
		return fmt.Errorf("decode FAILURE metadata: %w", err)
	}
	code, _ := metadata["code"].(string)
	message, _ := metadata["message"].(string)
	return fmt.Errorf("%s: %s", code, message)
}

// nor213BoltBegin opens an explicit transaction on conn.
func nor213BoltBegin(t *testing.T, conn net.Conn) {
	t.Helper()
	requireNoError(t, SendBegin(t, conn, nil))
	requireNoError(t, ReadSuccess(t, conn))
}

// nor213BoltCommit ends the explicit transaction conn holds.
func nor213BoltCommit(t *testing.T, conn net.Conn) {
	t.Helper()
	requireNoError(t, SendCommit(t, conn))
	requireNoError(t, ReadSuccess(t, conn))
}

// nor213BoltExplicitWrite runs statements in an explicit transaction of its
// own on conn, committing them, and reports the first failure. It never fails
// the test, so it can run off the test goroutine.
func nor213BoltExplicitWrite(t *testing.T, conn net.Conn, params map[string]any, statements ...string) error {
	t.Helper()
	if err := SendBegin(t, conn, nil); err != nil {
		return err
	}
	if err := ReadSuccess(t, conn); err != nil {
		return err
	}
	for _, statement := range statements {
		if err := nor213RunBoltStatement(t, conn, statement, params); err != nil {
			return err
		}
	}
	if err := SendCommit(t, conn); err != nil {
		return err
	}
	return ReadSuccess(t, conn)
}

// nor213AwaitBoltStatement bounds a statement another goroutine runs on conn:
// on timeout the connection is closed so the goroutine ends, and the test
// reports the statement did not complete.
func nor213AwaitBoltStatement(t *testing.T, conn net.Conn, done <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(nor213WireWaitLimit):
		_ = conn.Close()
		select {
		case <-done:
		case <-time.After(nor213WireWaitLimit):
		}
		t.Fatalf("%s did not complete within %s", what, nor213WireWaitLimit)
		return nil
	}
}

// nor213AwaitBoltSettled waits for the goroutine a waiting statement runs in to
// leave, after the statement's connection was closed.
func nor213AwaitBoltSettled(t *testing.T, settled <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-settled:
	case <-time.After(nor213WireWaitLimit):
		t.Errorf("%s did not end after its connection closed", what)
	}
}

// nor213RequireBoltNoWriterWaits runs statement in an explicit transaction of
// its own and requires that it takes the entity's write lock without waiting:
// a writer queued behind an earlier session would make it wait.
func nor213RequireBoltNoWriterWaits(t *testing.T, port int, waits <-chan struct{}, statement string, params map[string]any) {
	t.Helper()
	writer := openBoltTestConn(t, port)
	nor213DrainEntityWriteWaits(waits)
	done := make(chan error, 1)
	go func() { done <- nor213BoltExplicitWrite(t, writer, params, statement) }()
	select {
	case err := <-done:
		requireNoError(t, err)
	case <-waits:
		_ = writer.Close()
		select {
		case <-done:
		case <-time.After(nor213WireWaitLimit):
		}
		t.Fatal("a writer of the entity waited although no session held its lock")
	}
}

// TestBoltSameEntityWritersWaitAndRetainBothProperties drives the two-session
// schedule over Bolt: the second writer waits for the first transaction, the
// first commit releases it, the second succeeds, and both writers' properties
// are committed.
func TestBoltSameEntityWritersWaitAndRetainBothProperties(t *testing.T) {
	for _, engineShape := range []struct {
		name        string
		writeBehind bool
	}{
		{"durable", false},
		{"write-behind", true},
	} {
		t.Run(engineShape.name, func(t *testing.T) {
			store := nor213BoltStore(t, engineShape.writeBehind)
			_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)
			waits := nor213EntityWriteWaits(t)
			setup := openBoltTestConn(t, port)
			for _, shape := range nor213EntityShapes() {
				key := shape.name + "-" + engineShape.name
				params := map[string]any{"k": key}
				runBoltQueryWithParams(t, setup, shape.seed, params)

				// First session: an explicit transaction whose write holds
				// the entity until it ends.
				holder := openBoltTestConn(t, port)
				nor213BoltBegin(t, holder)
				runBoltQueryWithParams(t, holder, shape.hold, params)

				// Second session: the same entity, a different property.
				waiter := openBoltTestConn(t, port)
				nor213BoltBegin(t, waiter)
				nor213DrainEntityWriteWaits(waits)
				done := make(chan error, 1)
				settled := make(chan struct{})
				go func() {
					defer close(settled)
					done <- nor213RunBoltStatement(t, waiter, shape.write, params)
				}()
				// A failed assertion must not strand the waiting statement:
				// closing both connections ends it.
				t.Cleanup(func() {
					_ = holder.Close()
					_ = waiter.Close()
					nor213AwaitBoltSettled(t, settled, key+": the second writer")
				})

				secondErr, waited := func() (error, bool) {
					select {
					case <-waits:
						return nil, true
					case err := <-done:
						return err, false
					case <-time.After(nor213WireWaitLimit):
						return nil, false
					}
				}()
				// The first commit releases the entity whether or not the
				// second writer waited.
				nor213BoltCommit(t, holder)
				if waited {
					secondErr = nor213AwaitBoltStatement(t, waiter, done, key+": the second writer")
				}
				require.NoError(t, secondErr, "%s: the second writer failed instead of waiting", key)
				require.True(t, waited, "%s: the second writer must wait for the first transaction", key)
				nor213BoltCommit(t, waiter)

				verify := openBoltTestConn(t, port)
				records := runBoltQueryWithParams(t, verify, shape.readWriters, params)
				require.Equal(t, [][]any{{int64(1), int64(2)}}, records, "%s: both writers' properties are committed", key)
				_ = verify.Close()
				_ = waiter.Close()
				_ = holder.Close()
			}
		})
	}
}

// TestBoltWaitingEntityWriterDisconnectHoldsNothing: the waiting session's
// client goes away while its statement waits for the entity. The holder's
// commit releases the entity, the abandoned session keeps no hold, and a later
// writer takes the entity without waiting and commits.
func TestBoltWaitingEntityWriterDisconnectHoldsNothing(t *testing.T) {
	for _, engineShape := range []struct {
		name        string
		writeBehind bool
	}{
		{"durable", false},
		{"write-behind", true},
	} {
		t.Run(engineShape.name, func(t *testing.T) {
			store := nor213BoltStore(t, engineShape.writeBehind)
			_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)
			waits := nor213EntityWriteWaits(t)
			setup := openBoltTestConn(t, port)
			for _, shape := range nor213EntityShapes() {
				key := shape.name + "-" + engineShape.name
				params := map[string]any{"k": key}
				runBoltQueryWithParams(t, setup, shape.seed, params)

				holder := openBoltTestConn(t, port)
				nor213BoltBegin(t, holder)
				runBoltQueryWithParams(t, holder, shape.hold, params)

				waiter := openBoltTestConn(t, port)
				nor213BoltBegin(t, waiter)
				nor213DrainEntityWriteWaits(waits)
				settled := make(chan struct{})
				go func() {
					defer close(settled)
					_ = nor213RunBoltStatement(t, waiter, shape.write, params)
				}()
				select {
				case <-waits:
				case <-time.After(nor213WireWaitLimit):
					_ = waiter.Close()
					nor213AwaitBoltSettled(t, settled, key+": the second writer")
					_ = holder.Close()
					t.Fatalf("%s: the second writer never waited for the entity", key)
				}
				// The waiting client disappears before the holder ends.
				_ = waiter.Close()
				nor213BoltCommit(t, holder)
				nor213AwaitBoltSettled(t, settled, key+": the abandoned statement")

				// A later writer takes the entity and commits; it would wait
				// for a lock the abandoned session left held.
				nor213RequireBoltNoWriterWaits(t, port, waits, shape.after, params)

				verify := openBoltTestConn(t, port)
				records := runBoltQueryWithParams(t, verify, shape.readAll, params)
				require.Equal(t, [][]any{{int64(1), nil, int64(3)}}, records, "%s: the abandoned writer committed nothing", key)
				_ = verify.Close()
				_ = holder.Close()
			}
		})
	}
}
