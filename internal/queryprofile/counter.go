package queryprofile

import "sync/atomic"

// ReadCounter is the request-owned counter of one physical read: the logical
// rows and access units that read actually returned, and nothing derived from
// them.
//
// A reader adds to the counter its observer handed it (see Access), and the
// profile ledger reads the values once the statement ends. The counter holds no
// context, no identity and no reference to the statement, so a reader that
// resolved it once can add from any goroutine without another observer lookup;
// the two atomic counters are the whole state. A counter belongs to the one
// record its observer keyed it under, and is used through that pointer.
type ReadCounter struct {
	rows atomic.Int64
	hits atomic.Int64
}

// Add records rows logical rows and hits access units observed at one place. A
// nil counter - what Access returns for a statement nobody profiles - discards
// them, so an ordinary statement records nothing and allocates nothing.
func (c *ReadCounter) Add(rows, hits int64) {
	if c == nil {
		return
	}
	c.rows.Add(rows)
	c.hits.Add(hits)
}

// Values returns the rows and access units this counter holds. A nil counter
// holds none.
func (c *ReadCounter) Values() (rows, hits int64) {
	if c == nil {
		return 0, 0
	}
	return c.rows.Load(), c.hits.Load()
}
