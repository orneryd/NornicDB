package bolt

import (
	"bytes"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestLegacyDateTimeUnboundRelationshipCodec(t *testing.T) {
	for _, value := range legacyDateTimeCases(t) {
		for _, utc := range []bool{false, true} {
			rel := unboundRelationship{id: "r", relType: "DATETIME", properties: map[string]any{"t": value}}
			for _, input := range []any{rel, &rel} {
				wire := encodePackStreamValueIntoWithUTC(nil, input, utc)
				require.Equal(t, 1, bytes.Count(wire, dateTimeWire(value, utc)), "value/pointer %T utc=%t", input, utc)
			}
		}
	}
}

// Compare property bytes rather than entity IDs, which differ between engines.
// The direct and nested paths exercise unbound relationship encoding on the wire.
func TestLegacyDateTimeGraphPropertiesNeo4j526(t *testing.T) {
	reference := dateTimeReferenceAddress(t)
	for _, protocol := range []struct {
		name    string
		version byte
		patch   bool
	}{{"legacy44", 4, false}, {"utc44", 4, true}, {"bolt50", 5, false}} {
		for i, value := range legacyDateTimeCases(t) {
			t.Run(fmt.Sprintf("%s/%d", protocol.name, i), func(t *testing.T) {
				inner := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
				t.Cleanup(func() { require.NoError(t, inner.Close()) })
				store := storage.NewNamespacedEngine(inner, "datetime")
				for _, id := range []storage.NodeID{"a", "b"} {
					_, err := store.CreateNode(&storage.Node{ID: id, Labels: []string{"DateTimeFixture"}, Properties: map[string]any{"id": string(id), "t": value}})
					require.NoError(t, err)
				}
				require.NoError(t, store.CreateEdge(&storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "DATETIME", Properties: map[string]any{"t": value}}))
				_, port := startBoltIntegrationServerWithExplicitTx(t, store)
				conn := dateTimeWireConnect(t, fmt.Sprintf("127.0.0.1:%d", port), protocol.version, protocol.patch, false)
				ref := dateTimeWireConnect(t, reference, protocol.version, protocol.patch, true)
				// Roll back the reference fixture after reading it.
				require.NoError(t, SendMessage(ref, []byte{0xB1, MsgBegin, 0xA0}))
				require.NoError(t, ReadSuccess(t, ref))
				literal := value.Time.Format(time.RFC3339Nano)
				if value.ZoneID != "" {
					literal += "[" + value.ZoneID + "]"
				}
				expression := "datetime('" + literal + "')"
				refQuery := "CREATE p=(a:DateTimeFixture {t:" + expression + "})-[r:DATETIME {t:" + expression + "}]->(b:DateTimeFixture {t:" + expression + "}) RETURN a,r,p,[{nested:[a,r,p]}]"
				refWire := dateTimeWireQuery(t, ref, refQuery, []byte{0xA0})
				require.NoError(t, SendMessage(ref, []byte{0xB0, MsgRollback}))
				require.NoError(t, ReadSuccess(t, ref))
				query := "MATCH p=(a:DateTimeFixture {id:'a'})-[r:DATETIME]->(b:DateTimeFixture {id:'b'}) RETURN a,r,p,[{nested:[a,r,p]}]"
				wire := dateTimeWireQuery(t, conn, query, []byte{0xA0})
				utc := protocol.patch || protocol.version == 5
				want := dateTimeWire(value, utc)
				require.Equal(t, 10, bytes.Count(refWire, want), "reference properties use the negotiated DateTime bytes")
				require.Equal(t, 0, bytes.Count(refWire, dateTimeWire(value, !utc)), "reference has no opposite-family structs")
				require.Equal(t, bytes.Count(refWire, want), bytes.Count(wire, want), "all node, relationship and path properties, direct and nested")
				require.Equal(t, 0, bytes.Count(wire, dateTimeWire(value, !utc)), "no opposite-family structs")
			})
		}
	}
}
