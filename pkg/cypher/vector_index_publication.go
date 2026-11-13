package cypher

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// A schema vector index has the lifecycle Neo4j 5.26 gives it: CREATE
// publishes it POPULATING, a successful population makes it ONLINE, and one
// that fails leaves it FAILED until it is dropped and created again. The
// search service owns the state (search.VectorIndexPopulation): populating an
// index means making it serve what storage holds, every stored node with its
// label (relationship of its type) with the vector the index reads, measured
// against storage. Every route that publishes a vector index goes through
// vectorIndexPublication:
// CREATE VECTOR INDEX in its own statement or an explicit transaction, the
// compatibility procedures, and DB.CreateIndex (through CREATE VECTOR INDEX).
// The index becomes visible at the commit of its schema change, so its
// population begins just before that commit (beginVectorIndexPublications)
// and runs after it (populate); a failed commit aborts it. An index the
// search service found published when it started (a restart, a restore, or a
// service that replaced the one which populated it) has no population record in
// that service: its state is the service's own build, so a failed build leaves
// it FAILED with that build's cause and a build that succeeded serves it
// ONLINE. Cypher answers such an index from storage until that build is ready.

// vectorIndexOnlineQueryTimeout is how long a vector query waits for its
// index to come ONLINE: Neo4j 5.26's INDEX_ONLINE_QUERY_TIMEOUT_SECONDS.
var vectorIndexOnlineQueryTimeout = 30 * time.Second

// vectorIndexPublication carries one created vector index from its schema
// change to its population.
type vectorIndexPublication struct {
	name   string
	target search.VectorIndexTarget
	// dimensions are the index's declared dimensions, from which a query of
	// the index decides its source.
	dimensions int
	population *search.VectorIndexPopulation
	// wrapper is the transaction that publishes the index at its commit,
	// which begins the population (begun).
	wrapper *transactionStorageWrapper
	begun   bool
}

// declareVectorIndexPublication is called before the schema change that
// creates the index named name, of the declared dimensions, on property of
// the nodes with the label (of the relationships of that type, for a
// relationship index). Inside a transaction the population begins at its
// commit; a schema change made outside one is visible at once, so its
// population begins now.
func (e *StorageExecutor) declareVectorIndexPublication(ctx context.Context, name, label, property string, dimensions int, relationship bool) *vectorIndexPublication {
	target := search.VectorIndexTarget{Label: label, Property: property, Relationship: relationship}
	publication := &vectorIndexPublication{name: name, target: target, dimensions: dimensions}
	if wrapper := e.membership(ctx).storage; wrapper != nil {
		publication.wrapper = wrapper
		wrapper.vectorPublications = append(wrapper.vectorPublications, publication)
		return publication
	}
	publication.begun = true
	publication.population = e.search().BeginVectorIndexPopulation(name, target)
	return publication
}

// abort withdraws a publication whose schema change failed.
func (p *vectorIndexPublication) abort() {
	if p == nil {
		return
	}
	if p.wrapper != nil {
		kept := p.wrapper.vectorPublications[:0]
		for _, publication := range p.wrapper.vectorPublications {
			if publication != p {
				kept = append(kept, publication)
			}
		}
		p.wrapper.vectorPublications = kept
	}
	p.population.Abort()
}

// populateVectorIndexPublication establishes the published index's
// population in svc, the database's search service, and reports one that
// failed as the publishing statement's failure; the index is then FAILED.
// Whether the service is what the index's queries read is their own
// decision, asked here for every way they read it (vectorQueryReads,
// vectorIndexServiceAnswers): when any of them reads the service, the
// population makes it serve every stored member, so each reader the index
// lets answer from the service once it is ONLINE reads the whole population,
// and a member the service refuses (a budget) leaves the index FAILED. An
// index whose queries all read storage has nothing in the service to
// populate.
func (e *StorageExecutor) populateVectorIndexPublication(ctx context.Context, p *vectorIndexPublication, svc *search.Service) error {
	if p == nil || p.population == nil {
		return nil
	}
	dimensions := p.dimensions
	if dimensions <= 0 && svc != nil {
		dimensions = svc.VectorIndexDimensions() // as a query whose vector the service can read
	}
	served := false
	for _, read := range vectorQueryReads {
		// A warm read warms a lazy service, as the query would; a passive
		// read never does. A service that can't be warmed is not what warm
		// reads read.
		if answers, _ := e.vectorIndexServiceAnswers(ctx, svc, "", p.target, dimensions, read); answers {
			served = true
			break
		}
	}
	if err := p.population.Populate(ctx, served); err != nil {
		return &classifiedCypherError{
			cause:  fmt.Errorf("vector index %q was created, but its population failed, so it is FAILED until it is dropped and created again: %w", p.name, err),
			code:   "Neo.DatabaseError.Schema.IndexCreationFailed",
			detail: "IndexCreationFailed",
		}
	}
	return nil
}

// beginVectorIndexPublications marks every vector index this transaction
// creates POPULATING in svc, the database's search service resolved by the
// committing executor, just before the commit that makes it visible.
func (w *transactionStorageWrapper) beginVectorIndexPublications(svc *search.Service) {
	for _, publication := range w.vectorPublications {
		if !publication.begun {
			publication.begun = true
			publication.population = svc.BeginVectorIndexPopulation(publication.name, publication.target)
		}
	}
}

// withdrawVectorIndexPublication withdraws, when the transaction drops the
// index named name, the publication it staged for that name: a definition
// the transaction does not publish is not populated.
func (e *StorageExecutor) withdrawVectorIndexPublication(ctx context.Context, name string) {
	wrapper := e.membership(ctx).storage
	if wrapper == nil {
		return
	}
	for _, publication := range wrapper.vectorPublications {
		if publication.name == name && !publication.begun {
			publication.abort()
			return
		}
	}
}

// abortVectorIndexPublications withdraws them when that commit fails.
func (w *transactionStorageWrapper) abortVectorIndexPublications() {
	publications := w.vectorPublications
	w.vectorPublications = nil
	for _, publication := range publications {
		publication.population.Abort()
	}
}

// publishesVectorIndex reports whether this transaction creates the vector
// index named name: a query of it in the transaction does not wait for it,
// as in Neo4j.
func (w *transactionStorageWrapper) publishesVectorIndex(name string) bool {
	for _, publication := range w.vectorPublications {
		if publication.name == name && !publication.begun {
			return true
		}
	}
	return false
}

// awaitVectorIndexOnline waits, as Neo4j's vector query procedures do, for
// the index named name to be ONLINE before a query reads it. A FAILED index,
// or one still POPULATING when the wait ends, fails the query with Neo4j's
// message.
func (e *StorageExecutor) awaitVectorIndexOnline(ctx context.Context, name string) error {
	svc := e.search()
	if svc == nil {
		return nil
	}
	if wrapper := e.membership(ctx).storage; wrapper != nil && wrapper.publishesVectorIndex(name) {
		return nil
	}
	err := svc.AwaitVectorIndexOnline(ctx, name, vectorIndexOnlineQueryTimeout)
	// One lifecycle request, not the number of internal readiness polls.
	queryprofile.Access(ctx, "VectorIndexOnlineWait", name).Add(1, 1)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, search.ErrVectorIndexFailed):
		message := fmt.Sprintf("Index `%s` entered a FAILED state. Please see database logs.", name)
		if _, failure := svc.VectorIndexState(name); failure != nil {
			message += " Cause of failure: " + failure.Error()
		}
		return newSemanticError("Neo.ClientError.Procedure.ProcedureCallFailed", "ProcedureCallFailed", message)
	case errors.Is(err, search.ErrVectorIndexNotOnline):
		return newSemanticError("Neo.ClientError.Procedure.ProcedureCallFailed", "ProcedureCallFailed", "Expected index to come online within a reasonable time.")
	}
	return err
}

// awaitVectorIndexDefinition is the schema entry of the vector index named
// name that a query reads, once it is ONLINE (awaitVectorIndexOnline), or nil
// for a name with no vector index. The entry is read again after each wait,
// since a DROP and a CREATE of the name committed during the wait replace it,
// until the entry waited for is still the current one: a query never reads a
// definition other than the one whose population it waited for.
func readVectorIndexDefinition(ctx context.Context, schema *storage.SchemaManager, name string) (*storage.VectorIndex, bool) {
	if schema == nil {
		return nil, false
	}
	index, exists := schema.GetVectorIndex(name)
	var rows int64
	if index != nil {
		rows = 1
	}
	queryprofile.Access(ctx, "VectorIndexByNameSeek", name).Add(rows, 1)
	return index, exists
}

func readVectorIndexPopulation(ctx context.Context, service *search.Service, name string) *search.VectorIndexPopulation {
	if service == nil {
		return nil
	}
	population := service.VectorIndexPopulationOf(name)
	var rows int64
	if population != nil {
		rows = 1
	}
	queryprofile.Access(ctx, "VectorIndexPopulationByNameSeek", name).Add(rows, 1)
	return population
}

func (e *StorageExecutor) awaitVectorIndexDefinition(ctx context.Context, name string) (*storage.VectorIndex, error) {
	var waited *storage.VectorIndex
	for {
		var current *storage.VectorIndex
		if schema := e.storage.GetSchema(); schema != nil {
			current, _ = readVectorIndexDefinition(ctx, schema, name)
		}
		if current == nil || current == waited {
			return current, nil
		}
		if err := e.awaitVectorIndexOnline(ctx, name); err != nil {
			return nil, err
		}
		waited = current
	}
}

// vectorIndexOnline reports whether the vector index named name may answer a
// query now: its population state, which the search service owns, is ONLINE.
// A MATCH does not read an index that is POPULATING or FAILED; it reads
// storage, as with no index.
func (e *StorageExecutor) vectorIndexOnline(ctx context.Context, name string) bool {
	state, _ := readVectorIndexState(ctx, e.search(), name)
	return state == search.VectorIndexOnline
}

const profileKindVectorIndexPopulationStateSeek = "VectorIndexPopulationStateSeek"

// readVectorIndexState observes the one resolved service that answered a state
// request. Nil services retain their existing ONLINE default without a read.
func readVectorIndexState(ctx context.Context, service *search.Service, name string) (string, error) {
	state, failure := service.VectorIndexState(name)
	if service != nil {
		queryprofile.Access(ctx, profileKindVectorIndexPopulationStateSeek, name).Add(1, 1)
	}
	return state, failure
}

// vectorIndexShowState is a vector index's state, populationPercent and
// failureMessage as SHOW INDEXES reports them.
func (e *StorageExecutor) vectorIndexShowState(ctx context.Context, name string) (string, float64, string) {
	service := e.search()
	state, failure := readVectorIndexState(ctx, service, name)
	switch state {
	case search.VectorIndexPopulating:
		return state, 0.0, ""
	case search.VectorIndexFailed:
		message := ""
		if failure != nil {
			message = failure.Error()
		}
		return state, 0.0, message
	}
	return search.VectorIndexOnline, 100.0, ""
}
