package fabric

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// lifecycleEnd is how a participant's commit or rollback ends.
type lifecycleEnd string

const (
	endOK      lifecycleEnd = "ok"
	endError   lifecycleEnd = "error"
	endPanic   lifecycleEnd = "panic"
	endUnknown lifecycleEnd = "unknown" // a lost acknowledgement
)

// lifecycleParticipant records how often its ends ran.
type lifecycleParticipant struct {
	name               string
	commit, rollback   lifecycleEnd
	commits, rollbacks int
}

func (p *lifecycleParticipant) bind(t *testing.T, tx *FabricTransaction) {
	t.Helper()
	run := func(how lifecycleEnd, count *int) error {
		*count++
		switch how {
		case endError:
			return fmt.Errorf("%s injected failure", p.name)
		case endUnknown:
			return fmt.Errorf("%w: %s lost acknowledgement", ErrCommitOutcomeUnknown, p.name)
		case endPanic:
			panic(p.name + " injected panic")
		}
		return nil
	}
	require.NoError(t, tx.BindParticipantCallbacks(p.name, ParticipantCallbacks{Commit: func(*SubTransaction) error { return run(p.commit, &p.commits) }, Rollback: func(*SubTransaction) error { return run(p.rollback, &p.rollbacks) }}))
}

// endTransaction runs end and returns its error and the panic it raised.
func endTransaction(end func() error) (err error, panicked any) {
	defer func() { panicked = recover() }()
	return end(), nil
}

// The coordinator's commit, for every way each participant's commit and
// rollback can end (two readers and the writer; success, error, panic, lost
// acknowledgement), ends every participant, never commits the writer after a
// reader failed, reports the writer's outcome truthfully (committed, nothing
// kept, unknown), never reports a failed cleanup as a rollback, and lets a
// panic go on only once every participant has ended. Rollback does the same
// for every combination of rollback outcomes, several failing at once
// included.
func TestFabricTransactionLifecycleMatrix(t *testing.T) {
	ends := []lifecycleEnd{endOK, endError, endPanic, endUnknown}
	rollbacks := []lifecycleEnd{endOK, endError, endPanic}
	for _, readerCommit := range ends {
		for _, writerCommit := range ends {
			for _, r1 := range rollbacks {
				for _, r2 := range rollbacks {
					for _, rw := range rollbacks {
						name := fmt.Sprintf("commit/reader=%s/writer=%s/rollbacks=%s,%s,%s", readerCommit, writerCommit, r1, r2, rw)
						t.Run(name, func(t *testing.T) {
							tx := NewFabricTransaction("lifecycle")
							readerA := &lifecycleParticipant{name: "a-reader", commit: readerCommit, rollback: r1}
							readerB := &lifecycleParticipant{name: "b-reader", commit: endOK, rollback: r2}
							writer := &lifecycleParticipant{name: "w-writer", commit: writerCommit, rollback: rw}
							for _, reader := range []*lifecycleParticipant{readerA, readerB} {
								_, err := tx.GetOrOpen(reader.name, false)
								require.NoError(t, err)
								reader.bind(t, tx)
							}
							_, err := tx.GetOrOpen(writer.name, true)
							require.NoError(t, err)
							writer.bind(t, tx)

							err, panicked := endTransaction(func() error { return tx.Commit(nil, nil) })
							subs := tx.SubTransactions()
							for _, sub := range subs {
								require.NotEqual(t, "open", sub.State, "%s left open", sub.ShardName)
							}
							require.NotEqual(t, "open", tx.State())
							require.LessOrEqual(t, writer.commits, 1)
							require.LessOrEqual(t, writer.rollbacks, 1)
							anyPanic := false
							if readerCommit != endOK {
								// A reader that fails to end: the writer never
								// commits, every participant is rolled back.
								require.Zero(t, writer.commits, "the writer commits after a reader failed")
								require.Equal(t, 1, writer.rollbacks)
								require.Equal(t, 1, readerB.rollbacks)
								require.Equal(t, "rolledback", tx.State())
								anyPanic = readerCommit == endPanic || rw == endPanic || r2 == endPanic || r1 == endPanic
								if panicked == nil {
									require.ErrorIs(t, err, ErrCommitKeptNothing)
								}
								for _, p := range []*lifecycleParticipant{readerA, readerB, writer} {
									want := "rolledback"
									if p.rollbacks > 0 && p.rollback != endOK {
										want = StateCleanupFailed
									}
									require.Equal(t, want, subs[p.name].State, p.name)
								}
								if panicked == nil && (r2 != endOK || rw != endOK) {
									require.Contains(t, err.Error(), "cleanup not confirmed")
								}
							} else {
								require.Equal(t, 1, writer.commits)
								require.Equal(t, "committed", subs[readerA.name].State)
								switch writerCommit {
								case endOK:
									require.NoError(t, err)
									require.Nil(t, panicked)
									require.Equal(t, "committed", tx.State())
									require.Zero(t, writer.rollbacks)
								case endError:
									require.Equal(t, "rolledback", tx.State())
									require.Equal(t, 1, writer.rollbacks, "a failed writer is released")
									wantCleanup := "rolledback"
									if rw != endOK {
										wantCleanup = StateCleanupFailed
									}
									require.Equal(t, wantCleanup, subs[writer.name].State, "cleanup failure must survive the commit outcome")
									anyPanic = rw == endPanic
									if panicked == nil {
										require.ErrorIs(t, err, ErrCommitKeptNothing)
										if rw != endOK {
											require.ErrorContains(t, err, "cleanup not confirmed")
										}
									}
								case endUnknown:
									require.Equal(t, StateUnknown, tx.State())
									require.Equal(t, StateUnknown, subs[writer.name].State)
									anyPanic = rw == endPanic
									if panicked == nil {
										require.ErrorIs(t, err, ErrCommitOutcomeUnknown)
										require.NotErrorIs(t, err, ErrCommitKeptNothing)
										if rw != endOK {
											require.ErrorContains(t, err, "cleanup not confirmed")
										}
									}
								case endPanic:
									// Without an outcome report a panicked
									// commit's outcome is not known.
									require.Equal(t, StateUnknown, tx.State())
									require.Equal(t, 1, writer.rollbacks)
									anyPanic = true
								}
								if writerCommit != endOK {
									wantCleanup := "rolledback"
									if rw != endOK {
										wantCleanup = StateCleanupFailed
									}
									require.Equal(t, wantCleanup, subs[writer.name].CleanupState)
									require.Equal(t, tx.State(), subs[writer.name].CommitOutcome)
								}
							}
							if anyPanic {
								require.NotNil(t, panicked, "the panic goes on")
							} else {
								require.Nil(t, panicked)
							}
							// The transaction is ended: neither end runs again.
							require.Error(t, tx.Commit(nil, nil))
							require.Error(t, tx.Rollback(nil))
						})
					}
				}
			}
		}
	}
	for _, r1 := range rollbacks {
		for _, r2 := range rollbacks {
			for _, rw := range rollbacks {
				t.Run(fmt.Sprintf("rollback/%s,%s,%s", r1, r2, rw), func(t *testing.T) {
					tx := NewFabricTransaction("lifecycle")
					participants := []*lifecycleParticipant{{name: "a", rollback: r1}, {name: "b", rollback: r2}, {name: "w", rollback: rw}}
					for i, p := range participants {
						var err error
						if i == 2 {
							_, err = tx.GetOrOpen(p.name, true)
						} else {
							_, err = tx.GetOrOpen(p.name, false)
						}
						require.NoError(t, err)
						p.bind(t, tx)
					}
					err, panicked := endTransaction(func() error { return tx.Rollback(nil) })
					subs := tx.SubTransactions()
					anyPanic, failed := false, 0
					for _, p := range participants {
						require.Equal(t, 1, p.rollbacks, "%s rolled back once", p.name)
						require.Zero(t, p.commits)
						if p.rollback == endOK {
							require.Equal(t, "rolledback", subs[p.name].State)
							continue
						}
						require.Equal(t, StateCleanupFailed, subs[p.name].State, "a failed cleanup is not reported as rolled back")
						anyPanic = anyPanic || p.rollback == endPanic
						failed++
						if panicked == nil {
							require.Contains(t, err.Error(), "shard '"+p.name+"'", "every failure is reported")
						}
					}
					require.Equal(t, "rolledback", tx.State())
					require.Equal(t, anyPanic, panicked != nil)
					if failed == 0 {
						require.NoError(t, err)
					}
				})
			}
		}
	}
}

// A writer whose commit panicked after its storage committed (its outcome
// report says so) stays committed, and the panic goes on. One whose report
// says it did not commit kept nothing; one that reports nothing usable is
// not known.
func TestFabricTransactionWriterOutcomeReport(t *testing.T) {
	for _, reported := range []string{"committed", "rolledback", StateUnknown, "garbage"} {
		t.Run(reported, func(t *testing.T) {
			tx := NewFabricTransaction("outcome")
			_, err := tx.GetOrOpen("w", true)
			require.NoError(t, err)
			rollbacks := 0
			require.NoError(t, tx.BindParticipantCallbacks("w", ParticipantCallbacks{
				Commit:   func(*SubTransaction) error { panic("after storage commit") },
				Rollback: func(*SubTransaction) error { rollbacks++; return nil },
				Outcome:  func() string { return reported },
			}))
			err, panicked := endTransaction(func() error { return tx.Commit(nil, nil) })
			require.NotNil(t, panicked)
			require.NoError(t, err)
			want := reported
			if reported == "garbage" {
				want = StateUnknown
			}
			require.Equal(t, want, tx.State())
			// Its finalizer runs either way: it releases what the panicked
			// commit held, and keeps a committed transaction committed.
			require.Equal(t, 1, rollbacks)
		})
	}
}

// A participant's error is wrapped, not replaced: the cause stays reachable.
func TestFabricTransactionCommitErrorKeepsCause(t *testing.T) {
	cause := errors.New("constraint violated")
	tx := NewFabricTransaction("cause")
	_, err := tx.GetOrOpen("w", true)
	require.NoError(t, err)
	require.NoError(t, tx.BindParticipantCallbacks("w", ParticipantCallbacks{Commit: func(*SubTransaction) error { return cause }, Rollback: func(*SubTransaction) error { return nil }}))
	err = tx.Commit(nil, nil)
	require.ErrorIs(t, err, cause)
	require.ErrorIs(t, err, ErrCommitKeptNothing)
	require.True(t, strings.Contains(err.Error(), "commit failed on shard 'w'"))
}
