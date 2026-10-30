#!/usr/bin/env python3
"""
Generate comprehensive Cypher training data for NornicDB.
Includes standard Cypher syntax + NornicDB-specific extensions.

This generator creates training examples covering:
- All standard Cypher clauses and patterns
- NornicDB-specific procedures (db.txlog, db.temporal)
- Vector and fulltext search
- Canonical graph ledger patterns
- Documentation examples
"""

import json
import random
import argparse
from typing import List, Dict
from pathlib import Path


class NornicDBCypherGenerator:
    """Generate Cypher training data with NornicDB-specific features."""
    
    def __init__(self, seed: int = 42):
        random.seed(seed)
        
        # Common graph labels
        self.labels = [
            "Person", "User", "Employee", "Customer", "Student",
            "Product", "Order", "Company", "Project", "Task",
            "Post", "Comment", "Message", "File", "Document",
            "Entity", "FactKey", "FactVersion", "MutationEvent"  # Canonical graph
        ]
        
        # Common properties
        self.properties = {
            "Person": ["name", "age", "email", "city", "country", "phone"],
            "User": ["username", "email", "status", "created", "role", "last_login"],
            "Product": ["name", "price", "category", "stock", "rating", "sku"],
            "Order": ["order_id", "total", "status", "created", "customer_id"],
            "FactVersion": ["fact_key", "value_json", "valid_from", "valid_to", "asserted_at", "asserted_by"],
            "Entity": ["entity_id", "entity_type", "display_name", "created_at"],
        }
        
        self.names = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Helen", "Ivan"]
        self.cities = ["New York", "London", "Tokyo", "Paris", "Berlin", "Sydney", "Toronto", "Mumbai"]
        self.statuses = ["active", "inactive", "pending", "completed", "cancelled", "draft", "published"]
        
    def generate_standard_cypher(self, count: int) -> List[Dict]:
        """Generate standard Cypher queries."""
        examples = []
        
        templates = [
            ("Find all {label} nodes", "MATCH (n:{label}) RETURN n"),
            ("Get {label} where {prop} equals {value}", 
             "MATCH (n:{label} {{{prop}: '{value}'}}) RETURN n"),
            ("Count all {label}", "MATCH (n:{label}) RETURN count(n) as total"),
            ("Find {label} older than {age}", 
             "MATCH (n:{label}) WHERE n.age > {age} RETURN n"),
            ("Get average {prop} of {label}", 
             "MATCH (n:{label}) RETURN avg(n.{prop}) as average"),
            ("Find {name1} and {name2} who know each other",
             "MATCH (a:Person {{name: '{name1}'}})-[:KNOWS]-(b:Person {{name: '{name2}'}}) RETURN a, b"),
            ("Create a {label} named {name}",
             "CREATE (n:{label} {{name: '{name}'}}) RETURN n"),
            ("Find or create {label} named {name}",
             "MERGE (n:{label} {{name: '{name}'}}) RETURN n"),
        ]
        
        for _ in range(count):
            template = random.choice(templates)
            label = random.choice(self.labels)
            props = self.properties.get(label, ["name", "status"])
            prop = random.choice(props)
            value = random.choice(self.names + self.cities + self.statuses)
            name = random.choice(self.names)
            name1, name2 = random.sample(self.names, 2)
            age = random.randint(20, 60)
            
            examples.append({
                "instruction": "Write a Cypher query for NornicDB",
                "input": template[0].format(
                    label=label, prop=prop, value=value, name=name, 
                    name1=name1, name2=name2, age=age
                ),
                "output": template[1].format(
                    label=label, prop=prop, value=value, name=name,
                    name1=name1, name2=name2, age=age
                )
            })
        
        return examples
    
    def generate_vector_search(self, count: int) -> List[Dict]:
        """Generate vector similarity search queries."""
        examples = []
        
        templates = [
            ("Find nodes similar to '{query}' using vector search",
             "CALL db.index.vector.queryNodes('vector_idx', 10, '{query}') YIELD node, score RETURN node, score ORDER BY score DESC"),
            ("Search for similar documents to '{query}'",
             "CALL db.index.vector.queryNodes('document_idx', 5, '{query}') YIELD node, score WHERE score > 0.7 RETURN node"),
            ("Find top 10 most similar nodes to '{query}'",
             "CALL db.index.vector.queryNodes('vector_idx', 10, '{query}') YIELD node, score RETURN node, score ORDER BY score DESC LIMIT 10"),
        ]
        
        queries = [
            "product recommendations",
            "customer support",
            "technical documentation",
            "user authentication",
            "data analysis",
            "machine learning",
            "graph database",
            "vector search",
        ]
        
        for _ in range(count):
            template = random.choice(templates)
            query = random.choice(queries)
            
            examples.append({
                "instruction": "Write a Cypher query for NornicDB vector search",
                "input": template[0].format(query=query),
                "output": template[1].format(query=query)
            })
        
        return examples
    
    def generate_fulltext_search(self, count: int) -> List[Dict]:
        """Generate fulltext search queries."""
        examples = []
        
        templates = [
            ("Search for '{term}' in fulltext index",
             "CALL db.index.fulltext.queryNodes('fulltext_idx', '{term}') YIELD node, score RETURN node, score ORDER BY score DESC"),
            ("Find documents containing '{term}'",
             "CALL db.index.fulltext.queryNodes('document_idx', '{term}') YIELD node, score WHERE score > 0.5 RETURN node"),
        ]
        
        terms = ["graph", "database", "query", "search", "index", "node", "relationship", "property"]
        
        for _ in range(count):
            template = random.choice(templates)
            term = random.choice(terms)
            
            examples.append({
                "instruction": "Write a Cypher query for NornicDB fulltext search",
                "input": template[0].format(term=term),
                "output": template[1].format(term=term)
            })
        
        return examples
    
    def generate_txlog_queries(self, count: int) -> List[Dict]:
        """Generate transaction log query examples."""
        examples = []
        
        templates = [
            ("Get WAL entries from sequence {from_seq} to {to_seq}",
             "CALL db.txlog.entries({from_seq}, {to_seq}) YIELD sequence, operation, timestamp, tx_id, data RETURN sequence, operation, timestamp, tx_id ORDER BY sequence"),
            ("Get all WAL entries after sequence {seq}",
             "CALL db.txlog.entries({seq}, 0) YIELD sequence, operation, timestamp, tx_id, data RETURN sequence, operation, timestamp, tx_id ORDER BY sequence"),
            ("Find all WAL entries for transaction {tx_id}",
             "CALL db.txlog.byTxId('{tx_id}', 0) YIELD sequence, operation, timestamp, tx_id, data RETURN sequence, operation, timestamp, data ORDER BY sequence"),
            ("Get first 100 entries for transaction {tx_id}",
             "CALL db.txlog.byTxId('{tx_id}', 100) YIELD sequence, operation, timestamp, tx_id, data RETURN sequence, operation, timestamp, data ORDER BY sequence"),
        ]
        
        for _ in range(count):
            template = random.choice(templates)
            from_seq = random.randint(1000, 5000)
            to_seq = from_seq + random.randint(100, 500)
            seq = random.randint(1000, 5000)
            tx_id = f"tx-{random.randint(100000, 999999)}"
            
            examples.append({
                "instruction": "Write a Cypher query to query NornicDB transaction log",
                "input": template[0].format(from_seq=from_seq, to_seq=to_seq, seq=seq, tx_id=tx_id),
                "output": template[1].format(from_seq=from_seq, to_seq=to_seq, seq=seq, tx_id=tx_id)
            })
        
        return examples
    
    def generate_temporal_queries(self, count: int) -> List[Dict]:
        """Generate temporal query examples."""
        examples = []
        
        templates = [
            ("Check for temporal overlap in FactVersion for fact_key '{key}' from {from_date} to {to_date}",
             "CALL db.temporal.assertNoOverlap('FactVersion', 'fact_key', 'valid_from', 'valid_to', '{key}', datetime('{from_date}'), datetime('{to_date}')) YIELD ok RETURN ok"),
            ("Get FactVersion as of {date} for fact_key '{key}'",
             "CALL db.temporal.asOf('FactVersion', 'fact_key', '{key}', 'valid_from', 'valid_to', datetime('{date}')) YIELD node RETURN node"),
            ("Assert no overlap for entity '{entity}' from {from_date}",
             "CALL db.temporal.assertNoOverlap('Entity', 'entity_id', 'valid_from', 'valid_to', '{entity}', datetime('{from_date}'), null) YIELD ok RETURN ok"),
        ]
        
        keys = ["product-123|price", "user-456|status", "order-789|total"]
        entities = ["product-123", "user-456", "order-789"]
        dates = ["2024-01-15T00:00:00Z", "2024-02-20T00:00:00Z", "2024-03-10T00:00:00Z"]
        
        for _ in range(count):
            template = random.choice(templates)
            key = random.choice(keys)
            entity = random.choice(entities)
            from_date = random.choice(dates)
            to_date = random.choice(dates)
            date = random.choice(dates)
            
            examples.append({
                "instruction": "Write a Cypher query for NornicDB temporal operations",
                "input": template[0].format(key=key, entity=entity, from_date=from_date, to_date=to_date, date=date),
                "output": template[1].format(key=key, entity=entity, from_date=from_date, to_date=to_date, date=date)
            })
        
        return examples
    
    def generate_canonical_graph(self, count: int) -> List[Dict]:
        """Generate canonical graph ledger queries."""
        examples = []
        
        templates = [
            ("Create a canonical entity with id '{entity_id}'",
             "CREATE (e:Entity {{entity_id: '{entity_id}', entity_type: 'Product', display_name: '{name}', created_at: datetime()}}) RETURN e"),
            ("Create a fact key for entity '{entity_id}' with predicate '{predicate}'",
             "MERGE (fk:FactKey {{subject_entity_id: '{entity_id}', predicate: '{predicate}'}}) RETURN fk"),
            ("Create a fact version for fact_key '{fact_key}' with value '{value}'",
             "CREATE (fv:FactVersion {{fact_key: '{fact_key}', value_json: '{value}', valid_from: datetime(), valid_to: null, asserted_at: datetime(), asserted_by: 'user:alice'}}) RETURN fv"),
            ("Get current fact version for entity '{entity_id}' predicate '{predicate}'",
             "MATCH (fk:FactKey {{subject_entity_id: '{entity_id}', predicate: '{predicate}'}})-[:CURRENT]->(fv:FactVersion) RETURN fv"),
            ("Update fact version: close old and create new for '{fact_key}'",
             "MATCH (fk:FactKey {{subject_entity_id: '{entity_id}', predicate: '{predicate}'}})-[:CURRENT]->(fv_old:FactVersion) WHERE fv_old.valid_to IS NULL SET fv_old.valid_to = datetime() REMOVE fv_old:CURRENT CREATE (fv_new:FactVersion {{fact_key: '{fact_key}', value_json: '{value}', valid_from: datetime(), valid_to: null, asserted_at: datetime(), asserted_by: 'user:alice'}}) MERGE (fk)-[:CURRENT]->(fv_new) MERGE (fk)-[:HAS_VERSION]->(fv_new) RETURN fv_new"),
        ]
        
        entity_ids = ["product-123", "user-456", "order-789"]
        predicates = ["price", "status", "total", "name"]
        values = ['{"amount": 99.99, "currency": "USD"}', '{"status": "active"}', '{"total": 150.00}']
        names = self.names
        
        for _ in range(count):
            template = random.choice(templates)
            entity_id = random.choice(entity_ids)
            predicate = random.choice(predicates)
            fact_key = f"{entity_id}|{predicate}"
            value = random.choice(values)
            name = random.choice(names)
            
            examples.append({
                "instruction": "Write a Cypher query for NornicDB canonical graph ledger",
                "input": template[0].format(
                    entity_id=entity_id, predicate=predicate, fact_key=fact_key,
                    value=value, name=name
                ),
                "output": template[1].format(
                    entity_id=entity_id, predicate=predicate, fact_key=fact_key,
                    value=value, name=name
                )
            })
        
        return examples
    
    def generate_nornicdb_procedures(self, count: int) -> List[Dict]:
        """Generate NornicDB-specific procedure calls."""
        examples = []
        
        templates = [
            ("Get NornicDB version information",
             "CALL nornicdb.version() YIELD version, build, edition RETURN version, build, edition"),
            ("Get NornicDB database statistics",
             "CALL nornicdb.stats() YIELD nodes, relationships, labels, properties RETURN nodes, relationships, labels, properties"),
            ("List all database labels",
             "CALL db.labels() YIELD label RETURN label ORDER BY label"),
            ("List all relationship types",
             "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType ORDER BY relationshipType"),
            ("List all property keys",
             "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey ORDER BY propertyKey"),
            ("List all indexes",
             "CALL db.indexes() YIELD name, type, state, properties RETURN name, type, state, properties"),
            ("List all constraints",
             "CALL db.constraints() YIELD name, type, entityType, properties RETURN name, type, entityType, properties"),
        ]
        
        for _ in range(count):
            template = random.choice(templates)
            
            examples.append({
                "instruction": "Write a Cypher query to get NornicDB metadata",
                "input": template[0],
                "output": template[1]
            })
        
        return examples
    
    def generate_complex_queries(self, count: int) -> List[Dict]:
        """Generate complex multi-clause queries."""
        examples = []
        
        templates = [
            ("Find the person with the most friends",
             "MATCH (p:Person)-[:KNOWS]->() RETURN p, count(*) as friends ORDER BY friends DESC LIMIT 1"),
            ("Get top 5 most active users by post count",
             "MATCH (u:User)-[:POSTED]->(p:Post) RETURN u, count(p) as posts ORDER BY posts DESC LIMIT 5"),
            ("Find products frequently purchased together with product '{name}'",
             "MATCH (p:Product {{name: '{name}'}})<-[:PURCHASED]-(c:Customer)-[:PURCHASED]->(other:Product) WHERE p <> other RETURN other, count(*) as frequency ORDER BY frequency DESC"),
            ("Shortest path from {name1} to {name2}",
             "MATCH path = shortestPath((a:Person {{name: '{name1}'}})-[*]-(b:Person {{name: '{name2}'}})) RETURN path"),
            ("Find all nodes within 3 hops of {name}",
             "MATCH (start:Person {{name: '{name}'}})-[*1..3]-(connected) RETURN DISTINCT connected"),
        ]
        
        for _ in range(count):
            template = random.choice(templates)
            name = random.choice(self.names)
            name1, name2 = random.sample(self.names, 2)
            
            examples.append({
                "instruction": "Write a complex Cypher query for NornicDB",
                "input": template[0].format(name=name, name1=name1, name2=name2),
                "output": template[1].format(name=name, name1=name1, name2=name2)
            })
        
        return examples
    
    def generate_dataset(self, total_count: int = 2000) -> List[Dict]:
        """Generate complete dataset with balanced distribution."""
        distribution = {
            "standard": 0.30,        # 30% standard Cypher
            "vector_search": 0.10,   # 10% vector search
            "fulltext_search": 0.05, # 5% fulltext search
            "txlog": 0.10,          # 10% transaction log
            "temporal": 0.10,       # 10% temporal queries
            "canonical": 0.15,      # 15% canonical graph
            "procedures": 0.10,     # 10% NornicDB procedures
            "complex": 0.10,        # 10% complex queries
        }
        
        examples = []
        examples.extend(self.generate_standard_cypher(int(total_count * distribution["standard"])))
        examples.extend(self.generate_vector_search(int(total_count * distribution["vector_search"])))
        examples.extend(self.generate_fulltext_search(int(total_count * distribution["fulltext_search"])))
        examples.extend(self.generate_txlog_queries(int(total_count * distribution["txlog"])))
        examples.extend(self.generate_temporal_queries(int(total_count * distribution["temporal"])))
        examples.extend(self.generate_canonical_graph(int(total_count * distribution["canonical"])))
        examples.extend(self.generate_nornicdb_procedures(int(total_count * distribution["procedures"])))
        examples.extend(self.generate_complex_queries(int(total_count * distribution["complex"])))
        
        # Shuffle to mix query types
        random.shuffle(examples)
        
        return examples


def main():
    parser = argparse.ArgumentParser(description="Generate NornicDB Cypher training dataset")
    parser.add_argument("--output", type=str, default="data/nornicdb_cypher.jsonl",
                      help="Output file path")
    parser.add_argument("--count", type=int, default=2000,
                      help="Number of examples to generate")
    parser.add_argument("--seed", type=int, default=42,
                      help="Random seed for reproducibility")
    
    args = parser.parse_args()
    
    # Generate dataset
    print(f"Generating {args.count} NornicDB Cypher training examples...")
    generator = NornicDBCypherGenerator(seed=args.seed)
    examples = generator.generate_dataset(args.count)
    
    # Save to file
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        for example in examples:
            f.write(json.dumps(example) + '\n')
    
    print(f"✓ Generated {len(examples)} examples")
    print(f"✓ Saved to: {args.output}")
    
    # Print statistics
    print("\nDataset Statistics:")
    print(f"  Total examples: {len(examples)}")
    print(f"  Avg input length: {sum(len(e['input']) for e in examples) / len(examples):.1f} chars")
    print(f"  Avg output length: {sum(len(e['output']) for e in examples) / len(examples):.1f} chars")
    
    # Sample examples
    print("\nSample Examples:")
    for i, example in enumerate(random.sample(examples, 3), 1):
        print(f"\n  Example {i}:")
        print(f"    Input:  {example['input']}")
        print(f"    Output: {example['output']}")


if __name__ == "__main__":
    main()
