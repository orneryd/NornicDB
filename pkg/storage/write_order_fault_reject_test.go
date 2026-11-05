package storage

import (
	"errors"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage/writeordertest"
)

var errInjectedRejection = errors.New("injected rejection")

// rejectingDriver is an engine driver whose fault "reject-next" makes the
// next direct write or commit be rejected without reaching the store, as a
// replication the cluster refuses is. leak makes it reach the store anyway
// (a broken layer that reports a rejection it did not enforce); attribute
// controls whether the driver reports the rejection as its fault's.
type rejectingDriver struct {
	*engineDriver
	attribute, leak bool
	armed, rejected bool
}

func (d *rejectingDriver) Fault(name string) error {
	if name != "reject-next" {
		return errors.New("unknown fault " + name)
	}
	d.armed = true
	return nil
}

func (d *rejectingDriver) FaultRejected() bool {
	r := d.rejected && d.attribute
	d.rejected = false
	return r
}

func (d *rejectingDriver) reject(apply func() (writeordertest.Outcome, error)) (writeordertest.Outcome, error) {
	if !d.armed {
		return apply()
	}
	d.armed, d.rejected = false, true
	if d.leak {
		_, _ = apply()
	}
	return writeordertest.Rejected, errInjectedRejection
}

func (d *rejectingDriver) Write(op writeordertest.Op, v int64, implicit bool) (writeordertest.Outcome, error) {
	return d.reject(func() (writeordertest.Outcome, error) { return d.engineDriver.Write(op, v, implicit) })
}

func (d *rejectingDriver) Commit(slot int) (writeordertest.Outcome, error) {
	if d.armed && !d.leak {
		d.engineDriver.Rollback(slot)
	}
	return d.reject(func() (writeordertest.Outcome, error) { return d.engineDriver.Commit(slot) })
}

// TestWriteOrderFaultRejection: a valid direct write or commit that an
// armed fault rejects passes when the store keeps no trace of it, after a
// flush and a reopen; the same rejection fails when the write reached the
// store anyway, and a rejection the driver does not attribute to its fault
// is still a rejected valid write.
func TestWriteOrderFaultRejection(t *testing.T) {
	direct := []writeordertest.Step{
		{Kind: writeordertest.Fault, Fault: "reject-next"},
		{Kind: writeordertest.Direct, Op: writeordertest.Op{Kind: writeordertest.CreateNode, Node: "n0"}},
		{Kind: writeordertest.Flush},
		{Kind: writeordertest.Reopen},
	}
	commit := []writeordertest.Step{
		{Kind: writeordertest.Fault, Fault: "reject-next"},
		{Kind: writeordertest.Begin, Slot: 1},
		{Kind: writeordertest.Stage, Slot: 1, Op: writeordertest.Op{Kind: writeordertest.CreateNode, Node: "n1"}},
		{Kind: writeordertest.Commit, Slot: 1},
		{Kind: writeordertest.Flush},
		{Kind: writeordertest.Reopen},
	}
	for _, writeBehind := range []bool{false, true} {
		for name, steps := range map[string][]writeordertest.Step{"direct": direct, "commit": commit} {
			t.Run(name, func(t *testing.T) {
				d := &rejectingDriver{engineDriver: newEngineDriver(t, writeBehind), attribute: true}
				if fail, trace := writeordertest.Run(d, steps); fail != "" {
					t.Fatalf("write-behind %v: an attributed rejection that left no trace failed: %s\n%s", writeBehind, fail, strings.Join(trace, "\n"))
				}
				d = &rejectingDriver{engineDriver: newEngineDriver(t, writeBehind), attribute: true, leak: true}
				if fail, _ := writeordertest.Run(d, steps); fail == "" {
					t.Fatalf("write-behind %v: a rejection whose write reached the store passed", writeBehind)
				}
				d = &rejectingDriver{engineDriver: newEngineDriver(t, writeBehind)}
				if fail, _ := writeordertest.Run(d, steps); !strings.Contains(fail, "rejected-valid") {
					t.Fatalf("write-behind %v: an unattributed rejection of a valid step did not fail as rejected-valid: %q", writeBehind, fail)
				}
			})
		}
	}
}
