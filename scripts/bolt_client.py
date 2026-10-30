#!/usr/bin/env python3
"""
Bolt client for profiling - maintains persistent connection for efficiency
"""
import os
import sys
import time
from neo4j import GraphDatabase

def main():
    if len(sys.argv) < 3:
        print("Usage: bolt_client.py <uri> <query> [db_name]", file=sys.stderr)
        sys.exit(1)
    
    uri = sys.argv[1]
    query = sys.argv[2]
    db_name = sys.argv[3] if len(sys.argv) > 3 else "nornic"
    
    output_result = os.environ.get("BOLT_CLIENT_OUTPUT_RESULT") == "1"

    try:
        # Use persistent driver (reuses connections)
        driver = GraphDatabase.driver(uri, auth=None)
        
        with driver.session(database=db_name) as session:
            # Measure only the query execution time
            start_time = time.perf_counter()
            result = session.run(query)
            # Consume result to ensure query completes
            records = list(result)
            end_time = time.perf_counter()
            duration = end_time - start_time
            print(f"{duration:.9f}")
            if output_result and len(records) == 1 and len(records[0]) == 1:
                value = records[0][0]
                if isinstance(value, bool):
                    print("1" if value else "0")
                elif isinstance(value, int):
                    print(value)
                elif isinstance(value, float) and value.is_integer():
                    print(int(value))
        
        driver.close()
        sys.exit(0)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
