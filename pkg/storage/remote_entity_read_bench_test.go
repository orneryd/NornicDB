package storage

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkRemoteEntityReadAdmission(b *testing.B) {
	for _, count := range []int{1, 8, 64} {
		rows := make([][]interface{}, count)
		for i := range rows {
			rows[i] = []interface{}{map[string]interface{}{"id": fmt.Sprintf("node:%d", i), "labels": []interface{}{"Entity"}, "properties": map[string]interface{}{"rank": int64(i)}}}
		}
		engine := &RemoteEngine{transport: &fakeRemoteTransport{queryFn: func(context.Context, string, map[string]interface{}) ([]string, [][]interface{}, error) {
			return nil, rows, nil
		}}}
		b.Run(fmt.Sprintf("first/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if node, err := engine.GetFirstNodeByLabel(""); err != nil || node.ID != "node:0" {
					b.Fatalf("first node: %v", err)
				}
			}
		})
		b.Run(fmt.Sprintf("all/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if nodes, err := engine.AllNodes(); err != nil || len(nodes) != count {
					b.Fatalf("node list: %v", err)
				}
			}
		})
	}
}
