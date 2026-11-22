package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestL9HTTPDeleteAgingRetention100(t *testing.T) {
	t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "10m")
	peer, token, driver := zzEI1cServer(t, 100)
	count := func() int64 {
		result := zzEI1cBolt(t, driver, "MATCH (n:ZZDel) RETURN count(n) AS c", nil)
		return result[0].Values[0].(int64)
	}
	settle := func() int64 {
		previous, stable := int64(-1), 0
		for range 80 {
			current := count()
			if previous == current {
				stable++
				if stable == 4 {
					return current
				}
			} else {
				stable = 0
			}
			previous = current
			time.Sleep(150 * time.Millisecond)
		}
		t.Fatal("label count did not settle")
		return -1
	}
	var durations []int64
	for cycle := 1; cycle <= 210; cycle++ {
		zzEI1cBolt(t, driver, "UNWIND range(1, $n) AS i CREATE (:ZZDel {i: i})", map[string]any{"n": 2000})
		start := time.Now()
		rows := zzEI1cHTTP(t, peer, token, "MATCH (n:ZZDel) DELETE n RETURN count(*) AS deleted")
		durations = append(durations, time.Since(start).Milliseconds())
		require.Equal(t, "[2000]", rows, fmt.Sprintf("cycle %d", cycle))
		require.Zero(t, settle())
		zzEI1cBolt(t, driver, "MATCH (n:ZZDel) DETACH DELETE n", nil)
		require.Zero(t, settle())
		if cycle%15 == 0 {
			t.Logf("L9 R2 cycles %d–%d median=%d ms", cycle-14, cycle, zzEI1cMedian(durations[cycle-15:cycle]))
		}
	}
}
