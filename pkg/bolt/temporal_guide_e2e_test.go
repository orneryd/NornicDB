package bolt

import (
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/internal/testfixture/temporalguide"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestTemporalGuideBoltEndToEnd(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, scenario := range []struct {
				name string
				run  func(*testing.T, temporalguide.Runner)
			}{{"ownership", temporalguide.Ownership}, {"generic ledger", temporalguide.GenericLedger}, {"constraint blocks", temporalguide.Contracts}, {"retained history", temporalguide.History}, {"epoch bounds", temporalguide.EpochBounds}, {"downloadable script", temporalguide.OwnershipScript}, {"cost declarations", temporalguide.CostExamples}} {
				t.Run(scenario.name, func(t *testing.T) {
					_, port := startBoltIntegrationServer(t, temporalHistoryBoltStore(t), boltIntegrationAutocommit)
					driver := temporalHistoryBoltDriver(t, port)
					scenario.run(t, func(query string, params map[string]interface{}) ([][]interface{}, error) {
						session := driver.NewSession(t.Context(), neo4j.SessionConfig{DatabaseName: "temporal_bolt"})
						defer func() { require.NoError(t, session.Close(t.Context())) }()
						result, err := session.Run(t.Context(), query, params)
						if err != nil {
							return nil, err
						}
						records, err := result.Collect(t.Context())
						if err != nil {
							return nil, err
						}
						rows := make([][]interface{}, len(records))
						for i, record := range records {
							rows[i] = record.Values
						}
						return rows, nil
					})
				})
			}
		})
	}
}
