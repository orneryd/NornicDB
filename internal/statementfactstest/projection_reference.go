package statementfactstest

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// ProjectionOracle checks each generated public-path cell against the leased
// live reference. An absent reference configuration is not a differential pass.
type ProjectionOracle struct {
	driver           neo4j.DriverWithContext
	referenceQueries map[string]string
	expectedRows     map[string][][]any
}

func (oracle *ProjectionOracle) ReferenceCase(sample Case) Case {
	if oracle != nil {
		if query, exists := oracle.referenceQueries[sample.Name]; exists {
			sample.Query = query
		}
	}
	return sample
}

func NewProjectionOracle(t *testing.T) *ProjectionOracle {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if uri == "" {
		return nil
	}
	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(ctx)) })
	_, err = neo4j.ExecuteQuery(ctx, driver, "CREATE (:ProjectionSemanticSentinel)", nil, neo4j.EagerResultTransformer)
	require.NoError(t, err)
	return &ProjectionOracle{driver: driver}
}

func (oracle *ProjectionOracle) Check(t *testing.T, sample Case, prefix string, explicit bool) [][]any {
	if oracle == nil {
		return nil
	}
	if oracle.driver == nil {
		return oracle.expectedRows[sample.Name]
	}
	sample = oracle.ReferenceCase(sample)
	ctx := context.Background()
	session := oracle.driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	var result neo4j.ResultWithContext
	var err error
	if explicit {
		tx, failure := session.BeginTransaction(ctx)
		require.NoError(t, failure)
		defer tx.Rollback(ctx)
		result, err = tx.Run(ctx, prefix+sample.Query, nil)
	} else {
		result, err = session.Run(ctx, prefix+sample.Query, nil)
	}
	var columns []string
	var records []*neo4j.Record
	if err == nil {
		columns, err = result.Keys()
	}
	if err == nil {
		records, err = result.Collect(ctx)
	}
	if sample.Code != "" {
		require.Error(t, err, "live reference: %s", sample.Query)
		var status *neo4j.Neo4jError
		require.True(t, errors.As(err, &status), "%v", err)
		require.Equal(t, sample.Code, status.Code, "live reference: %s", sample.Query)
		return nil
	}
	require.NoError(t, err, "live reference: %s", sample.Query)
	require.Equal(t, sample.Columns, columns, "live reference: %s", sample.Query)
	rows := make([][]any, len(records))
	for index, record := range records {
		rows[index] = record.Values
	}
	if expected, known := oracle.expectedRows[sample.Name]; known {
		oracle.CheckRows(t, prefix, expected, rows)
	}
	return rows
}

// CheckRows compares protocol-neutral scalar/map/list values as well as keys.
// EXPLAIN's plan representation is deliberately outside this header contract.
func (oracle *ProjectionOracle) CheckRows(t *testing.T, prefix string, reference, actual [][]any) {
	if oracle == nil || prefix == "EXPLAIN " {
		return
	}
	if len(reference) == 0 && len(actual) == 0 {
		return
	}
	want, err := json.Marshal(reference)
	require.NoError(t, err)
	got, err := json.Marshal(actual)
	require.NoError(t, err)
	require.JSONEq(t, string(want), string(got))
}
