package resolvers

import (
	"runtime"
	"testing"
	"weak"

	"github.com/orneryd/nornicdb/pkg/graphql/models"
	"github.com/stretchr/testify/require"
)

func TestRetiredSubscriptionsCollectible(t *testing.T) {
	cases := map[string]func(*EventBroker, *string){
		"node_created": func(b *EventBroker, id *string) {
			ch := make(chan *models.Node)
			b.nodeCreatedSubs = []subscriptionInfo{{ch: ch, id: id}}
			b.unsubscribeNodeCreated(ch)
		},
		"node_updated": func(b *EventBroker, id *string) {
			ch := make(chan *models.Node)
			b.nodeUpdatedSubs = []subscriptionInfo{{ch: ch, id: id}}
			b.unsubscribeNodeUpdated(ch)
		},
		"node_deleted": func(b *EventBroker, id *string) {
			ch := make(chan string)
			b.nodeDeletedSubs = []subscriptionInfo{{ch: ch, id: id}}
			b.unsubscribeNodeDeleted(ch)
		},
		"relationship_created": func(b *EventBroker, id *string) {
			ch := make(chan *models.Relationship)
			b.relationshipCreatedSubs = []subscriptionInfo{{ch: ch, id: id}}
			b.unsubscribeRelationshipCreated(ch)
		},
		"relationship_updated": func(b *EventBroker, id *string) {
			ch := make(chan *models.Relationship)
			b.relationshipUpdatedSubs = []subscriptionInfo{{ch: ch, id: id}}
			b.unsubscribeRelationshipUpdated(ch)
		},
		"relationship_deleted": func(b *EventBroker, id *string) {
			ch := make(chan string)
			b.relationshipDeletedSubs = []subscriptionInfo{{ch: ch, id: id}}
			b.unsubscribeRelationshipDeleted(ch)
		},
	}
	for name, retire := range cases {
		t.Run(name, func(t *testing.T) {
			owner := NewEventBroker()
			reference := func() weak.Pointer[string] {
				id := new(string)
				*id = "retired"
				ref := weak.Make(id)
				retire(owner, id)
				return ref
			}()
			runtime.GC()
			require.Nil(t, reference.Value())
			runtime.KeepAlive(owner)
		})
	}
}
