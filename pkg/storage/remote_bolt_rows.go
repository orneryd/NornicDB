package storage

import (
	"context"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/resultstream"
)

func consumeBoltRows(ctx context.Context, result neo4j.ResultWithContext, consume resultstream.Consumer) ([]string, error) {
	columns, err := result.Keys()
	if err != nil {
		return nil, err
	}
	stopped := false
	for result.Next(ctx) {
		if stopped {
			continue
		}
		record := result.Record()
		row := make([]any, len(record.Values))
		for i, value := range record.Values {
			row[i] = normalizeBoltValue(value)
		}
		more, err := consume(columns, row)
		if err != nil {
			return nil, err
		}
		stopped = !more
		if stopped && !resultstream.Drain(ctx) {
			return columns, nil
		}
	}
	if err := result.Err(); err != nil {
		return nil, err
	}
	_, err = result.Consume(ctx)
	return columns, err
}
