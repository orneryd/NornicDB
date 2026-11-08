// ============================================================================
// Canonical Graph Schema Bootstrap
// ============================================================================
// This script creates all constraints and indexes for Idea #7 canonical graph.
// Run once per database (schema persists across restarts).
// All statements are idempotent (safe to run multiple times).

// ----------------------------------------------------------------------------
// Entity Constraints
// ----------------------------------------------------------------------------

// Required fields
CREATE CONSTRAINT entity_id_required IF NOT EXISTS 
FOR (n:Entity) REQUIRE n.entity_id IS NOT NULL;

CREATE CONSTRAINT entity_type_required IF NOT EXISTS 
FOR (n:Entity) REQUIRE n.entity_type IS NOT NULL;

// Uniqueness
CREATE CONSTRAINT entity_id_unique IF NOT EXISTS 
FOR (n:Entity) REQUIRE n.entity_id IS UNIQUE;

// ----------------------------------------------------------------------------
// FactKey Constraints (composite uniqueness via NODE KEY)
// ----------------------------------------------------------------------------

// Required fields
CREATE CONSTRAINT fact_key_subject_required IF NOT EXISTS 
FOR (n:FactKey) REQUIRE n.subject_entity_id IS NOT NULL;

CREATE CONSTRAINT fact_key_predicate_required IF NOT EXISTS 
FOR (n:FactKey) REQUIRE n.predicate IS NOT NULL;

// Composite uniqueness (replaces computed fact_key property)
CREATE CONSTRAINT fact_key_node_key IF NOT EXISTS 
FOR (n:FactKey) REQUIRE (n.subject_entity_id, n.predicate) IS NODE KEY;

// ----------------------------------------------------------------------------
// FactVersion Constraints
// ----------------------------------------------------------------------------

// Required fields
CREATE CONSTRAINT fact_version_fact_key_required IF NOT EXISTS 
FOR (n:FactVersion) REQUIRE n.fact_key IS NOT NULL;

CREATE CONSTRAINT fact_version_valid_from_required IF NOT EXISTS 
FOR (n:FactVersion) REQUIRE n.valid_from IS NOT NULL;

CREATE CONSTRAINT fact_version_value_json_required IF NOT EXISTS 
FOR (n:FactVersion) REQUIRE n.value_json IS NOT NULL;

CREATE CONSTRAINT fact_version_asserted_at_required IF NOT EXISTS 
FOR (n:FactVersion) REQUIRE n.asserted_at IS NOT NULL;

CREATE CONSTRAINT fact_version_asserted_by_required IF NOT EXISTS 
FOR (n:FactVersion) REQUIRE n.asserted_by IS NOT NULL;

// Type constraints
CREATE CONSTRAINT fact_version_valid_from_type IF NOT EXISTS 
FOR (n:FactVersion) REQUIRE n.valid_from IS :: ZONED DATETIME;

CREATE CONSTRAINT fact_version_valid_to_type IF NOT EXISTS 
FOR (n:FactVersion) REQUIRE n.valid_to IS :: ZONED DATETIME;

CREATE CONSTRAINT fact_version_asserted_at_type IF NOT EXISTS 
FOR (n:FactVersion) REQUIRE n.asserted_at IS :: ZONED DATETIME;

// Temporal integrity (supported today): one version start per fact_key.
// Note: full temporal no-overlap must be enforced in write/query logic unless
// the engine adds native TEMPORAL NO OVERLAP constraint support.
CREATE CONSTRAINT fact_version_fact_key_valid_from_node_key IF NOT EXISTS
FOR (n:FactVersion) REQUIRE (n.fact_key, n.valid_from) IS NODE KEY;

// ----------------------------------------------------------------------------
// MutationEvent Constraints
// ----------------------------------------------------------------------------

// Required fields
CREATE CONSTRAINT mutation_event_id_required IF NOT EXISTS 
FOR (n:MutationEvent) REQUIRE n.event_id IS NOT NULL;

CREATE CONSTRAINT mutation_event_tx_id_required IF NOT EXISTS 
FOR (n:MutationEvent) REQUIRE n.tx_id IS NOT NULL;

CREATE CONSTRAINT mutation_event_actor_required IF NOT EXISTS 
FOR (n:MutationEvent) REQUIRE n.actor IS NOT NULL;

CREATE CONSTRAINT mutation_event_timestamp_required IF NOT EXISTS 
FOR (n:MutationEvent) REQUIRE n.timestamp IS NOT NULL;

CREATE CONSTRAINT mutation_event_op_type_required IF NOT EXISTS 
FOR (n:MutationEvent) REQUIRE n.op_type IS NOT NULL;

// Uniqueness
CREATE CONSTRAINT mutation_event_id_unique IF NOT EXISTS 
FOR (n:MutationEvent) REQUIRE n.event_id IS UNIQUE;

// Type constraints
CREATE CONSTRAINT mutation_event_timestamp_type IF NOT EXISTS 
FOR (n:MutationEvent) REQUIRE n.timestamp IS :: ZONED DATETIME;

CREATE CONSTRAINT mutation_event_op_type_string IF NOT EXISTS 
FOR (n:MutationEvent) REQUIRE n.op_type IS :: STRING;

CREATE CONSTRAINT mutation_event_actor_string IF NOT EXISTS 
FOR (n:MutationEvent) REQUIRE n.actor IS :: STRING;

// ----------------------------------------------------------------------------
// Evidence Constraints (optional, adjust as needed)
// ----------------------------------------------------------------------------

CREATE CONSTRAINT evidence_id_required IF NOT EXISTS 
FOR (n:Evidence) REQUIRE n.evidence_id IS NOT NULL;

CREATE CONSTRAINT evidence_id_unique IF NOT EXISTS 
FOR (n:Evidence) REQUIRE n.evidence_id IS UNIQUE;

// ----------------------------------------------------------------------------
// Relationship Constraints
// ----------------------------------------------------------------------------

CREATE CONSTRAINT current_fk_unique IF NOT EXISTS
FOR ()-[r:CURRENT]-() REQUIRE r.fact_key IS UNIQUE;

CREATE CONSTRAINT has_version_exists IF NOT EXISTS
FOR ()-[r:HAS_VERSION]-() REQUIRE r.version IS NOT NULL;

CREATE CONSTRAINT has_version_type IF NOT EXISTS
FOR ()-[r:HAS_VERSION]-() REQUIRE r.version IS :: INTEGER;

CREATE CONSTRAINT has_version_rk IF NOT EXISTS
FOR ()-[r:HAS_VERSION]-() REQUIRE (r.fact_key, r.version) IS RELATIONSHIP KEY;

// Cardinality — each FactKey may have at most one outgoing CURRENT edge
CREATE CONSTRAINT current_max_one IF NOT EXISTS
FOR ()-[r:CURRENT]->() REQUIRE MAX COUNT 1;

// Endpoint policy — only FactKey -> FactVersion is allowed for HAS_VERSION
CREATE CONSTRAINT has_version_allowed IF NOT EXISTS
FOR (:FactKey)-[r:HAS_VERSION]->(:FactVersion) REQUIRE ALLOWED;

// Endpoint policy — disallow MutationEvent directly linking to Entity via AFFECTS
CREATE CONSTRAINT no_direct_mutation_entity IF NOT EXISTS
FOR (:MutationEvent)-[r:AFFECTS]->(:Entity) REQUIRE DISALLOWED;

// ----------------------------------------------------------------------------
// Vector Indexes
// ----------------------------------------------------------------------------

// Vector index for canonical fact search
// Adjust dimensions to match your embedding model (e.g., 1024 for OpenAI, 384 for sentence-transformers)
CREATE VECTOR INDEX canonical_fact_idx IF NOT EXISTS
FOR (n:FactVersion) ON (n.embedding)
OPTIONS {indexConfig: {`vector.dimensions`: 1024, `vector.similarity_function`: 'cosine'}};

// Vector index for evidence/document search
CREATE VECTOR INDEX evidence_content_idx IF NOT EXISTS
FOR (n:Evidence) ON (n.embedding)
OPTIONS {indexConfig: {`vector.dimensions`: 1024, `vector.similarity_function`: 'cosine'}};

// ----------------------------------------------------------------------------
// Property Indexes (for efficient lookups)
// ----------------------------------------------------------------------------

// Index for entity type lookups
CREATE INDEX entity_type_idx IF NOT EXISTS 
FOR (n:Entity) ON (n.entity_type);

// Index for fact version temporal queries
CREATE INDEX fact_version_valid_from_idx IF NOT EXISTS 
FOR (n:FactVersion) ON (n.valid_from);

CREATE INDEX fact_version_fact_key_idx IF NOT EXISTS 
FOR (n:FactVersion) ON (n.fact_key);

// Index for mutation event queries
CREATE INDEX mutation_event_tx_id_idx IF NOT EXISTS 
FOR (n:MutationEvent) ON (n.tx_id);

CREATE INDEX mutation_event_timestamp_idx IF NOT EXISTS 
FOR (n:MutationEvent) ON (n.timestamp);

CREATE INDEX mutation_event_actor_idx IF NOT EXISTS 
FOR (n:MutationEvent) ON (n.actor);

// ----------------------------------------------------------------------------
// Verify Schema
// ----------------------------------------------------------------------------

// List all constraints
CALL db.constraints() YIELD name, type, labelsOrTypes, properties
RETURN name, type, labelsOrTypes, properties
ORDER BY labelsOrTypes[0], name;

// List all indexes
SHOW INDEXES;
