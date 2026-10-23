package testutil

// SlowQueryWorkload performs enough real work for slow-query wiring tests to
// measure elapsed time on Windows. A trivial RETURN can finish within a clock
// tick and legitimately fall below even a one-nanosecond logging threshold.
const SlowQueryWorkload = "UNWIND range(1, 10000) AS value RETURN sum(value) AS total"
