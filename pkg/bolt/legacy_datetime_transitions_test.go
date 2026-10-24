package bolt

import (
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestLegacyDateTimeWireNamedZoneTransitions(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)
	testDateTimeWireNamedZoneTransitions(t, fmt.Sprintf("127.0.0.1:%d", port), false)
}

func TestLegacyDateTimeWireNamedZoneTransitionsNeo4j526(t *testing.T) {
	testDateTimeWireNamedZoneTransitions(t, dateTimeReferenceAddress(t), true)
}

func testDateTimeWireNamedZoneTransitions(t *testing.T, address string, reference bool) {
	t.Helper()
	for _, protocol := range []struct {
		name    string
		version byte
		patch   bool
	}{{"legacy44", 4, false}, {"utc44", 4, true}, {"bolt50", 5, false}} {
		t.Run(protocol.name, func(t *testing.T) {
			conn := dateTimeWireConnect(t, address, protocol.version, protocol.patch, reference)
			utc := protocol.patch || protocol.version == 5
			for _, fixture := range []struct{ name, wall, zone, legacyText, utcText string }{
				{"london-overlap", "2026-10-25T01:30:00Z", "Europe/London", "2026-10-25T01:30:00.000000042+01:00[Europe/London]", "2026-10-25T01:30:00.000000042Z[Europe/London]"},
				{"paris-overlap", "2026-10-25T02:30:00Z", "Europe/Paris", "2026-10-25T02:30:00.000000042+02:00[Europe/Paris]", "2026-10-25T03:30:00.000000042+01:00[Europe/Paris]"},
				{"newyork-gap", "2026-03-08T02:30:00Z", "America/New_York", "2026-03-08T03:30:00.000000042-04:00[America/New_York]", "2026-03-07T21:30:00.000000042-05:00[America/New_York]"},
			} {
				for _, shape := range []string{"scalar", "list", "map"} {
					t.Run(fixture.name+"/"+shape, func(t *testing.T) {
						wall, err := time.Parse(time.RFC3339, fixture.wall)
						require.NoError(t, err)
						signature, text := byte(0x66), fixture.legacyText
						if utc {
							signature, text = 0x69, fixture.utcText
						}
						// Construct the incoming local seconds directly, without the
						// encoder or a time.Date call in the named zone.
						wire := encodePackStreamIntInto([]byte{0xB3, signature}, wall.Unix())
						wire = encodePackStreamIntInto(wire, 42)
						wire = encodePackStreamStringInto(wire, fixture.zone)
						access := "$v"
						if shape == "list" {
							wire, access = append([]byte{0x91}, wire...), "$v[0]"
						}
						if shape == "map" {
							wire, access = append([]byte{0xA1, 0x81, 't'}, wire...), "$v.t"
						}
						params := append([]byte{0xA1, 0x81, 'v'}, wire...)
						got := dateTimeWireQuery(t, conn, "RETURN toString("+access+"), "+access+".nanosecond", params)
						require.Equal(t, encodePackStreamValue([]any{text, int64(42)}), got)
					})
				}
			}
		})
	}
}
