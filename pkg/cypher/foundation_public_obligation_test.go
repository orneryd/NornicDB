package cypher

import (
	"github.com/orneryd/nornicdb/internal/enginecontract"
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
)

func foundationDispositions(word string) []storage.BindingDisposition {
	out := make([]storage.BindingDisposition, 0, len(word))
	for _, r := range word {
		out = append(out, map[rune]storage.BindingDisposition{'O': storage.BindingOwned, 'C': storage.BindingCarried, 'D': storage.BindingDelegated, 'U': storage.BindingUnowned, 'R': storage.BindingRefused}[r])
	}
	return out
}

// These are executable adapters, with an independent disposition fixture.
// The complete Engine gate also requires adapters for the other owner packages.
var publicEngineObligationFactories = map[string]struct {
	Policy string
	Issue  func(*testing.T) FoundationPublicFixture
}{
	"github.com/orneryd/nornicdb/pkg/storage.MemoryEngine": {"OUOOOOUOUOOOOORR", func(t *testing.T) FoundationPublicFixture {
		return FoundationIssueLeafFixture(t, enginePopulationBase(t))
	}},
	"github.com/orneryd/nornicdb/pkg/storage.BadgerEngine": {"OUOOOOUOUOOOOORR", func(t *testing.T) FoundationPublicFixture {
		return FoundationIssueLeafFixture(t, enginePopulationBase(t).BadgerEngine)
	}},
	"github.com/orneryd/nornicdb/pkg/storage.CompositeEngine":               {"OCCCCCCOCOOOOORR", FoundationIssueCompositeFixture},
	"github.com/orneryd/nornicdb/pkg/storage.RemoteEngine":                  {"OCCCCCCOCOROOORR", FoundationIssueRemoteFixture},
	"github.com/orneryd/nornicdb/internal/testfixture/remotequery.Observer": {"OCCCCCCOCOROOORR", FoundationIssueRemoteObserverFixture},
	"github.com/orneryd/nornicdb/pkg/cypher.transactionStorageWrapper": {"OOOOOOOOOOOOOORR", func(t *testing.T) FoundationPublicFixture {
		return FoundationIssuePublicFixture(t, "transactionStorageWrapper", nil)
	}},
	"github.com/orneryd/nornicdb/pkg/cypher.algorithmRelationshipReader": {"CCRCCCCCCCCCCCRR", func(t *testing.T) FoundationPublicFixture {
		return FoundationIssuePublicFixture(t, "algorithmRelationshipReader", nil)
	}},
	"github.com/orneryd/nornicdb/pkg/cypher.graphIdentityStorage": {"CCRCCCCCCCCCCCRR", func(t *testing.T) FoundationPublicFixture {
		return FoundationIssuePublicFixture(t, "graphIdentityStorage", nil)
	}},
	"github.com/orneryd/nornicdb/pkg/cypher.systemGraphView": {"OCRCCCCOCCCCCORR", func(t *testing.T) FoundationPublicFixture {
		return FoundationIssuePublicFixture(t, "systemGraphView", nil)
	}},
	"github.com/orneryd/nornicdb/pkg/storage.TracedEngine": {"DDDDDDDDDDDDOORR", func(t *testing.T) FoundationPublicFixture {
		return FoundationIssuePublicFixture(t, "TracedEngine", func(e storage.Engine) storage.Engine { return storage.NewTracedEngine(e) })
	}},
	"github.com/orneryd/nornicdb/pkg/storage.WALEngine": {"DDDDDDDDODDDOORR", func(t *testing.T) FoundationPublicFixture {
		return FoundationIssuePublicFixture(t, "WALEngine", func(e storage.Engine) storage.Engine { return foundationWAL(t, e) })
	}},
	"github.com/orneryd/nornicdb/pkg/storage.NamespacedEngine": {"OCDCCCCOCOOOOORR", func(t *testing.T) FoundationPublicFixture {
		return FoundationIssuePublicFixture(t, "NamespacedEngine", func(e storage.Engine) storage.Engine { return storage.NewNamespacedEngine(e, "tenant") })
	}},
}

func TestFoundationPublicLogicalObligations(t *testing.T) {
	for name, adapter := range publicEngineObligationFactories {
		t.Run(name, func(t *testing.T) {
			enginecontract.RequirePublicProof(t, name, FoundationCheckPublicPolicy(t, name, foundationDispositions(adapter.Policy), adapter.Issue))
		})
	}
}
