package storage

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
)

// This measures the canonical in-memory property decoder, not disk reads or
// server latency. Unselected compressed arrays should be skippable by length.
func BenchmarkExperimentalProjectedPropertySkip(b *testing.B) {
	for _, shape := range []string{"low-range", "random-int64"} {
		for _, enabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/enabled=%t", shape, enabled), func(b *testing.B) {
				previous := config.IsExperimentalPropertyStorageEnabled()
				config.SetExperimentalPropertyStorageEnabled(enabled)
				b.Cleanup(func() { config.SetExperimentalPropertyStorageEnabled(previous) })
				engine, err := testfixture.OwnResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
				if err != nil {
					b.Fatal(err)
				}
				values := make([]int64, 4096)
				for i := range values {
					hash := sha256.Sum256([]byte(fmt.Sprintf("docs/property/%d", i)))
					values[i] = int64(binary.LittleEndian.Uint64(hash[:]))
					if shape == "low-range" {
						values[i] = int64(hash[0] % 64)
					}
				}
				var body []byte
				err = engine.withUpdate(context.Background(), updateRequest{}, func(tx *badger.Txn) error {
					var err error
					body, _, err = engine.encodeNodeInTxn(tx, "bench", &Node{ID: "bench:doc", Properties: map[string]any{"numbers": values, "small": int64(7)}})
					return err
				})
				if err != nil {
					b.Fatal(err)
				}
				include := map[string]struct{}{"small": {}}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					node, err := engine.decodeNodeProjected(nil, "bench", body, include)
					if err != nil {
						b.Fatal(err)
					}
					experimentalProjectedSink = node
				}
				b.StopTimer()
			})
		}
	}
}

var experimentalProjectedSink *Node
