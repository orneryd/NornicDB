"""
Neo4j Import Script - Import Welcome Season data into Neo4j graph database via GraphQL.

This script uses the GraphQL endpoint to execute Cypher queries through the executeCypher mutation.
Neo4j handles embeddings and vector search automatically.
This script creates nodes with properties and relationships from the Excel data.

Node Types:
- POC: Points of Contact records
- WarRoom: War Room teams
- ApplicationContact: Product/Engineering contacts
- StaticKnowledge: FAQ-style knowledge
- Person: Individual contacts extracted from data
- Area: Teams/Areas/Departments
- Sheet: Source Excel sheets

Relationships:
- (:POC)-[:HAS_CONTACT]->(:Person)
- (:POC)-[:BELONGS_TO]->(:Area)
- (:POC)-[:MANAGED_BY]->(:Person)
- (:WarRoom)-[:HAS_VP]->(:Person)
- (:WarRoom)-[:HAS_PRIMARY_LEAD]->(:Person)
- (:WarRoom)-[:HAS_SECONDARY_LEAD]->(:Person)
- (:ApplicationContact)-[:HAS_PRODUCT_LEAD]->(:Person)
- (:ApplicationContact)-[:HAS_ENGINEERING_LEAD]->(:Person)
- (:Document)-[:FROM_SHEET]->(:Sheet)
- (:Person)-[:WORKS_IN]->(:Area)

Usage:
    python neo4j_import.py --verify
    python neo4j_import.py --uri https://your-graphql-endpoint/graphql/ --user admin --password secret
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Set, Tuple
import re
import requests
import json


class Neo4jImporter:
    """Import Welcome Season data into Neo4j with relationships."""
    
    # Role to label mapping for Person nodes
    ROLE_LABELS = {
        'VP': 'VP',
        'Primary Lead': 'Lead',
        'Secondary Lead': 'Lead', 
        'POC Manager': 'Manager',
        'WS Point of Contact': 'POC',
        'Engineering/Product Leader': 'Leader',
        'Product Lead': 'ProductLead',
        'Product Contact': 'ProductContact',
        'Engineering Lead': 'EngineeringLead',
        'Engineering Contact': 'EngineeringContact',
    }
    
    # Role to expertise category mapping
    ROLE_EXPERTISE = {
        'VP': 'Leadership',
        'Primary Lead': 'Leadership',
        'Secondary Lead': 'Leadership',
        'POC Manager': 'Management',
        'WS Point of Contact': 'Operations',
        'Engineering/Product Leader': 'Engineering',
        'Product Lead': 'Product',
        'Product Contact': 'Product',
        'Engineering Lead': 'Engineering',
        'Engineering Contact': 'Engineering',
    }
    
    def __init__(self, uri: str = "http://localhost:7474/graphql", user: str = "admin", password: str = "password"):
        """Initialize GraphQL connection with basic authentication."""
        self.base_url = uri
        self.auth = (user, password)
        base_dir = Path(__file__).parent
        
        # Multiple Excel files
        self.excel_file = base_dir / "WS2026 POCs - War Rooms.xlsx"
        self.coverage_schedule_file = base_dir / "CMKDigital-Emerson_WS2026 Coverage Schedule.xlsx"
        self.support_roster_file = base_dir / "2026_WS_Support_Roster.xlsx"
        
        # Track created entities for relationship building
        self.persons: Dict[str, str] = {}  # name -> node_id
        self.person_roles: Dict[str, Set[str]] = {}  # name -> set of roles
        self.areas: Dict[str, str] = {}    # area_name -> node_id
        self.schedules: Dict[str, str] = {}  # schedule_id -> node_id
        self.support_shifts: Dict[str, str] = {}  # shift_id -> node_id
        
    def close(self):
        """Close the connection (no-op for HTTP)."""
        pass
        
    def _execute_query(self, query: str, **kwargs) -> List[Dict[str, Any]]:
        """Execute a Cypher query via GraphQL API."""
        parameters = kwargs if kwargs else {}
        
        headers = {
            "Content-Type": "application/json"
        }
        
        # Wrap Cypher query in GraphQL executeCypher mutation
        graphql_query = """
        mutation ExecuteCypher($statement: String!, $parameters: JSON) {
          executeCypher(input: {statement: $statement, parameters: $parameters}) {
            columns
            rows
            rowCount
            executionTimeMs
          }
        }
        """
        
        response = requests.post(
            self.base_url,
            headers=headers,
            auth=self.auth,
            json={
                "query": graphql_query,
                "variables": {
                    "statement": query,
                    "parameters": parameters
                }
            }
        )
        
        # 200 and 202 are both success responses
        if response.status_code in (200, 202):
            data = response.json()
            
            # Check for GraphQL errors first
            if "errors" in data:
                error_msg = json.dumps(data['errors'], indent=2)
                raise Exception(f"GraphQL errors: {error_msg}")
            
            # Extract results from GraphQL response
            if "data" in data and "executeCypher" in data["data"]:
                result = data["data"]["executeCypher"]
                # Convert GraphQL result format to Neo4j-like format
                rows = result.get("rows", [])
                columns = result.get("columns", [])
                
                # Unwrap values from {"value": ...} format
                unwrapped_rows = []
                for row in rows:
                    unwrapped_row = []
                    for cell in row:
                        if isinstance(cell, dict) and "value" in cell:
                            unwrapped_row.append(cell["value"])
                        else:
                            unwrapped_row.append(cell)
                    unwrapped_rows.append(unwrapped_row)
                
                # Format as [{row: [values], meta: []}] for compatibility
                return [{"row": row, "meta": []} for row in unwrapped_rows]
            else:
                # No data returned - might be a CREATE/DELETE that succeeded
                return []
        else:
            error_text = response.text[:500] if response.text else "No error message"
            raise Exception(f"GraphQL query failed: {response.status_code} - {error_text}")
    
    def clear_database(self):
        """Clear all nodes and relationships."""
        try:
            self._execute_query("MATCH (n) DETACH DELETE n")
            print("🗑️  Database cleared")
            
            # Verify the clear worked
            result = self._execute_query("MATCH (n) RETURN count(n) as total")
            if result and result[0].get('row'):
                count = result[0]['row'][0]
                print(f"   Verified: {count} nodes remaining")
        except Exception as e:
            print(f"⚠️  Error clearing database: {e}")
            raise
    
    def create_constraints_and_indexes(self):
        """Create constraints and indexes for better performance."""
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:POC) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (w:WarRoom) REQUIRE w.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (a:ApplicationContact) REQUIRE a.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Document) REQUIRE d.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Area) REQUIRE a.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Sheet) REQUIRE s.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:CoverageSchedule) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:SupportSchedule) REQUIRE s.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:SupportRoster) REQUIRE s.id IS UNIQUE",
        ]
        for constraint in constraints:
            try:
                self._execute_query(constraint)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    print(f"⚠️  Constraint warning: {e}")
        
        # Create indexes for common search fields
        indexes = [
            "CREATE INDEX IF NOT EXISTS FOR (p:Person) ON (p.name)",
            "CREATE INDEX IF NOT EXISTS FOR (a:Area) ON (a.name)",
            "CREATE INDEX IF NOT EXISTS FOR (d:Document) ON (d.content)",
        ]
        for index in indexes:
            try:
                self._execute_query(index)
            except Exception:
                pass
        
        print("✅ Constraints and indexes created")
    
    def create_sheet_nodes(self):
        """Create nodes for source sheets from all Excel files."""
        sheets = [
            # From WS2026 POCs - War Rooms.xlsx
            {'name': 'WS2026 POCs', 'description': 'Welcome Season 2026 Points of Contact', 'source_file': 'WS2026 POCs - War Rooms.xlsx'},
            {'name': 'War Rooms', 'description': 'War Room teams and schedules', 'source_file': 'WS2026 POCs - War Rooms.xlsx'},
            {'name': 'PS Digital Product-Eng', 'description': 'Product and Engineering contacts', 'source_file': 'WS2026 POCs - War Rooms.xlsx'},
            {'name': 'CSX Resolution Center Delegates', 'description': 'CSX Resolution Center business area delegates', 'source_file': 'WS2026 POCs - War Rooms.xlsx'},
            # From CMKDigital-Emerson_WS2026 Coverage Schedule.xlsx
            {'name': 'Coverage Schedule', 'description': 'Welcome Season 2026 coverage schedules and speak times', 'source_file': 'CMKDigital-Emerson_WS2026 Coverage Schedule.xlsx'},
            # From 2026_WS_Support_Roster.xlsx
            {'name': 'Support Roster', 'description': 'Welcome Season 2026 support roster with availability', 'source_file': '2026_WS_Support_Roster.xlsx'},
            # Static knowledge
            {'name': 'Static Knowledge', 'description': 'FAQ and general information', 'source_file': 'System'},
        ]
        
        for sheet in sheets:
            self._execute_query("""
                MERGE (s:Sheet {name: $name})
                SET s.description = $description,
                    s.source_file = $source_file
            """, name=sheet['name'], description=sheet['description'], source_file=sheet.get('source_file', 'Unknown'))
        
        print(f"   ✅ Created {len(sheets)} Sheet nodes")
    
    def _normalize_name(self, name: str) -> str:
        """Normalize a person's name for consistent matching."""
        if not name:
            return ""
        
        # Remove phone numbers first
        name = re.sub(r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}', '', name)
        
        # Extract name before parentheses (person name is usually before the description)
        # e.g., "Diana Newell (RxClaim Adj, CCA/COS, ...)" -> "Diana Newell"
        paren_match = re.match(r'^([^(]+)\(', name)
        if paren_match:
            name = paren_match.group(1).strip()
        else:
            # If no parentheses, take first part before comma (if comma-separated list)
            name = re.sub(r'[,;].*$', '', name)
        
        name = name.strip().strip(',').strip()
        return name
    
    def _is_valid_person_name(self, name: str) -> bool:
        """Validate that a string looks like a person's name."""
        if not name or len(name) < 2:
            return False
        
        name_lower = name.lower().strip()
        
        # Reject common non-name patterns
        invalid_patterns = [
            r'^layer\)?$',  # "Layer" or "Layer)"
            r'^mypbm\)?$',  # "myPBM" or "myPBM)"
            r'^comm\s+layer\)?$',  # "Comm Layer" or "Comm Layer)"
            r'^[^a-zA-Z]*$',  # No letters at all
            r'^\d+$',  # Only numbers
            r'^[A-Z]{2,}$',  # Only uppercase acronyms (like "CDH", "COS")
            r'^[a-z]+$',  # Only lowercase (likely not a name)
            r'\)$',  # Ends with just closing paren (incomplete extraction)
            r'^[^a-zA-Z]+$',  # No letters
        ]
        
        for pattern in invalid_patterns:
            if re.match(pattern, name_lower):
                return False
        
        # Must contain at least one letter and look like a name
        # Names typically have: first letter uppercase, rest mixed case, or multiple words
        if not re.search(r'[a-zA-Z]', name):
            return False
        
        # Check if it looks like a name (has spaces or is a reasonable single name)
        # Single names should be at least 3 chars and have mixed case or be a known pattern
        words = name.split()
        if len(words) == 1:
            # Single word - should be at least 3 chars and not all caps (unless it's a short name)
            if len(name) < 3:
                return False
            # Reject if it's all caps and longer than 4 chars (likely acronym)
            if name.isupper() and len(name) > 4:
                return False
        
        # Reject if it's clearly a system/application name
        system_keywords = ['layer', 'mypbm', 'comm', 'outputs', 'adj', 'pde', 'dur', 
                          'eligibility', 'vendor', 'transition', 'network', 'prescriber',
                          'claim', 'rxclaim', 'cdh', 'cos', 'cca', 'sbo', 'r&r', 'eztest',
                          'colas', 'mft', 'b2b', 'myclaims']
        if any(keyword in name_lower for keyword in system_keywords):
            return False
        
        return True
    
    def _extract_phone(self, text: str) -> str:
        """Extract phone number from text."""
        if not text:
            return None
        match = re.search(r'(\d{3}[-.\s]?\d{3}[-.\s]?\d{4})', text)
        return match.group(1) if match else None
    
    def get_or_create_person(self, name: str, phone: str = None, role: str = None) -> str:
        """Get or create a Person node, return the node ID."""
        if not name or name.strip() == '' or name.lower() in ['nan', 'none', 'n/a']:
            return None
            
        normalized = self._normalize_name(name)
        if not normalized:
            return None
        
        # Validate that this looks like a person name
        if not self._is_valid_person_name(normalized):
            return None
            
        # Check cache
        cache_key = normalized.lower()
        
        # Track roles for this person
        if cache_key not in self.person_roles:
            self.person_roles[cache_key] = set()
        if role:
            self.person_roles[cache_key].add(role)
        
        # Get all roles and derive labels/expertise
        roles = list(self.person_roles[cache_key])
        labels = list(set(self.ROLE_LABELS.get(r, '') for r in roles if self.ROLE_LABELS.get(r)))
        expertise = list(set(self.ROLE_EXPERTISE.get(r, '') for r in roles if self.ROLE_EXPERTISE.get(r)))
        
        if cache_key in self.persons:
            # Update existing person with new role info
            if role:
                self._execute_query("""
                    MATCH (p:Person {name: $name})
                    SET p.roles = $roles,
                        p.labels = $labels,
                        p.expertise = $expertise
                """, name=normalized, roles=roles, labels=labels, expertise=expertise)
                
                # Add dynamic label based on role
                label = self.ROLE_LABELS.get(role)
                if label:
                    self._execute_query(f"""
                        MATCH (p:Person {{name: $name}})
                        SET p:{label}
                    """, name=normalized)
            return self.persons[cache_key]
        
        # Create unique ID
        person_id = f"person_{len(self.persons)}"
        
        # Create node with roles array and expertise
        result = self._execute_query("""
            MERGE (p:Person {name: $name})
            ON CREATE SET p.id = $id, p.phone = $phone, p.roles = $roles, p.labels = $labels, p.expertise = $expertise
            ON MATCH SET p.phone = COALESCE(p.phone, $phone),
                         p.roles = $roles,
                         p.labels = $labels,
                         p.expertise = $expertise
            RETURN p.id as id
        """, name=normalized, id=person_id, phone=phone, roles=roles, labels=labels, expertise=expertise)
        
        # Add dynamic label based on role
        if role:
            label = self.ROLE_LABELS.get(role)
            if label:
                self._execute_query(f"""
                    MATCH (p:Person {{name: $name}})
                    SET p:{label}
                """, name=normalized)
        
        actual_id = result[0]['row'][0] if result and result[0].get('row') else person_id
        self.persons[cache_key] = actual_id
        
        return actual_id
    
    def get_or_create_area(self, area_name: str) -> str:
        """Get or create an Area node, return the node ID."""
        if not area_name or area_name.strip() == '' or area_name.lower() in ['nan', 'none', 'n/a', 'area/team']:
            return None
            
        area_name = area_name.strip()
        cache_key = area_name.lower()
        
        if cache_key in self.areas:
            return self.areas[cache_key]
        
        area_id = f"area_{len(self.areas)}"
        
        self._execute_query("""
            MERGE (a:Area {name: $name})
            ON CREATE SET a.id = $id
            RETURN a.id
        """, name=area_name, id=area_id)
        
        self.areas[cache_key] = area_id
        return area_id
    
    def import_pocs(self, df: pd.DataFrame) -> int:
        """Import POCs with relationships."""
        count = 0
        for idx, row in df.iterrows():
            # Use named columns from spreadsheet
            area_team = row.get('Area/Team')
            if pd.isna(area_team) or str(area_team).strip() == '':
                # Try to get data from other columns even if area is empty
                has_data = any(pd.notna(row.get(col)) for col in ['Engineering and Product Leaders', 'WS Point of Contact', "POC's Manager(s)"])
                if not has_data:
                    continue
                area_team = None
            else:
                area_team = str(area_team).strip()
                
            props = {
                'id': f"poc_{idx}",
                'area_team': area_team,
                'engineering_product_leaders': str(row.get('Engineering and Product Leaders')).strip() if pd.notna(row.get('Engineering and Product Leaders')) else None,
                'ws_point_of_contact': str(row.get('WS Point of Contact')).strip() if pd.notna(row.get('WS Point of Contact')) else None,
                'poc_managers': str(row.get("POC's Manager(s)")).strip() if pd.notna(row.get("POC's Manager(s)")) else None,
                'product_application_service': str(row.get('Product, Application or Service')).strip() if pd.notna(row.get('Product, Application or Service')) else None,
                'source_sheet': 'WS2026 POCs',
                'row_number': int(idx),
            }
            props = {k: v for k, v in props.items() if v is not None}
            
            # Build content for search
            text_parts = [f"{k}: {v}" for k, v in props.items() if k not in ['id', 'source_sheet', 'row_number']]
            props['content'] = '\n'.join(text_parts)
            
            # Create POC node
            try:
                self._execute_query("""
                    CREATE (p:POC:Document $props)
                    RETURN p.id as id
                """, props=props)
            except Exception as e:
                print(f"⚠️  Error creating POC node {props.get('id')}: {e}")
                continue
            
            # Create relationships
            poc_id = props['id']
            
            # Link to Sheet
            self._execute_query("""
                MATCH (p:POC {id: $poc_id}), (s:Sheet {name: 'WS2026 POCs'})
                MERGE (p)-[:FROM_SHEET]->(s)
            """, poc_id=poc_id)
            
            # Link to Area
            if area_team:
                area_id = self.get_or_create_area(area_team)
                if area_id:
                    self._execute_query("""
                        MATCH (p:POC {id: $poc_id}), (a:Area {name: $area_name})
                        MERGE (p)-[:BELONGS_TO]->(a)
                    """, poc_id=poc_id, area_name=area_team.strip())
            
            # Link to WS Point of Contact (Person)
            ws_contact = props.get('ws_point_of_contact')
            if ws_contact:
                # For contacts with descriptions in parentheses, extract the name part only
                # e.g., "Diana Newell (RxClaim Adj, ...)" -> just process "Diana Newell"
                # Don't split by comma if there are parentheses (the comma is part of the description)
                if '(' in ws_contact and ')' in ws_contact:
                    # Extract name before parentheses
                    normalized_contact = self._normalize_name(ws_contact)
                    if normalized_contact and self._is_valid_person_name(normalized_contact):
                        phone = self._extract_phone(ws_contact)
                        person_id = self.get_or_create_person(normalized_contact, phone, 'WS Point of Contact')
                        if person_id:
                            self._execute_query("""
                                MATCH (p:POC {id: $poc_id}), (person:Person {name: $person_name})
                                MERGE (p)-[:HAS_CONTACT {role: 'WS Point of Contact'}]->(person)
                            """, poc_id=poc_id, person_name=normalized_contact)
                else:
                    # Handle multiple contacts separated by comma or semicolon (no parentheses)
                    contacts = re.split(r'[,;]', ws_contact)
                    for contact in contacts:
                        contact = contact.strip()
                        if contact:
                            normalized_contact = self._normalize_name(contact)
                            if normalized_contact and self._is_valid_person_name(normalized_contact):
                                phone = self._extract_phone(contact)
                                person_id = self.get_or_create_person(normalized_contact, phone, 'WS Point of Contact')
                                if person_id:
                                    self._execute_query("""
                                        MATCH (p:POC {id: $poc_id}), (person:Person {name: $person_name})
                                        MERGE (p)-[:HAS_CONTACT {role: 'WS Point of Contact'}]->(person)
                                    """, poc_id=poc_id, person_name=normalized_contact)
            
            # Link to POC Managers
            managers = props.get('poc_managers')
            if managers:
                # Handle names with descriptions in parentheses
                if '(' in managers and ')' in managers:
                    normalized_manager = self._normalize_name(managers)
                    if normalized_manager and self._is_valid_person_name(normalized_manager):
                        person_id = self.get_or_create_person(normalized_manager, None, 'POC Manager')
                        if person_id:
                            self._execute_query("""
                                MATCH (p:POC {id: $poc_id}), (person:Person {name: $person_name})
                                MERGE (p)-[:MANAGED_BY]->(person)
                            """, poc_id=poc_id, person_name=normalized_manager)
                else:
                    manager_list = re.split(r'[,;]', managers)
                    for manager in manager_list:
                        manager = manager.strip()
                        if manager:
                            normalized_manager = self._normalize_name(manager)
                            if normalized_manager and self._is_valid_person_name(normalized_manager):
                                person_id = self.get_or_create_person(normalized_manager, None, 'POC Manager')
                                if person_id:
                                    self._execute_query("""
                                        MATCH (p:POC {id: $poc_id}), (person:Person {name: $person_name})
                                        MERGE (p)-[:MANAGED_BY]->(person)
                                    """, poc_id=poc_id, person_name=normalized_manager)
            
            # Link engineering/product leaders
            leaders = props.get('engineering_product_leaders')
            if leaders:
                # Handle names with descriptions in parentheses
                if '(' in leaders and ')' in leaders:
                    normalized_leader = self._normalize_name(leaders)
                    if normalized_leader and self._is_valid_person_name(normalized_leader):
                        person_id = self.get_or_create_person(normalized_leader, None, 'Engineering/Product Leader')
                        if person_id:
                            self._execute_query("""
                                MATCH (p:POC {id: $poc_id}), (person:Person {name: $person_name})
                                MERGE (p)-[:HAS_LEADER]->(person)
                            """, poc_id=poc_id, person_name=normalized_leader)
                else:
                    leader_list = re.split(r'[,;]', leaders)
                    for leader in leader_list:
                        leader = leader.strip()
                        if leader:
                            normalized_leader = self._normalize_name(leader)
                            if normalized_leader and self._is_valid_person_name(normalized_leader):
                                person_id = self.get_or_create_person(normalized_leader, None, 'Engineering/Product Leader')
                                if person_id:
                                    self._execute_query("""
                                        MATCH (p:POC {id: $poc_id}), (person:Person {name: $person_name})
                                        MERGE (p)-[:HAS_LEADER]->(person)
                                    """, poc_id=poc_id, person_name=normalized_leader)
            
            count += 1
            
        return count
    
    def import_war_rooms(self, df: pd.DataFrame) -> int:
        """Import War Rooms with relationships."""
        count = 0
        for idx, row in df.iterrows():
            team_name = row.get('War Room Team')
            if pd.isna(team_name) or str(team_name).strip() == '':
                continue
            
            # Schedule date columns
            schedule_dates = [
                'Thur. 1/1', 'Fri. 1/2', 'Sat. J1/3', 'Sun. 1/4', 'Mon. 1/5',
                'Tues. 1/6', 'Wed. 1/7', 'Thurs. 1/8', 'Fri. 1/9', 'Sat. 1/10',
                'Sun. 1/11', 'Mon. 1/12', 'Tues. 1/13', 'Wed. 1/14', 'Thurs. 1/15',
                'Fri. 1/16', 'Sat. 1/17', 'Sun. 1/18', 'Mon. 1/19', 'Tues. 1/20'
            ]
            
            props = {
                'id': f"warroom_{idx}",
                'team_name': str(team_name).strip(),
                'meeting_link': str(row.get('Meeting Link', '')) if pd.notna(row.get('Meeting Link')) else None,
                'vp': str(row.get('VP', '')) if pd.notna(row.get('VP')) else None,
                'primary_lead': str(row.get('Primary Lead & Phone #`', '')) if pd.notna(row.get('Primary Lead & Phone #`')) else None,
                'secondary_lead': str(row.get('Secondary Lead & Phone #', '')) if pd.notna(row.get('Secondary Lead & Phone #')) else None,
                'location_type': str(row.get('Virtual, Onsite (physical location), Combined (physical location)', '')) if pd.notna(row.get('Virtual, Onsite (physical location), Combined (physical location)')) else None,
                'onsite_vendors': str(row.get('Onsite Vendors', '')) if pd.notna(row.get('Onsite Vendors')) else None,
                'source_sheet': 'War Rooms',
                'row_number': int(idx),
            }
            
            # Add all schedule dates
            schedule_info = {}
            for date_col in schedule_dates:
                if date_col in row.index and pd.notna(row.get(date_col)):
                    # Normalize column name to valid property key
                    key = date_col.lower().replace('.', '').replace(' ', '_').replace('/', '_')
                    schedule_info[key] = str(row.get(date_col))
            
            props['schedule'] = schedule_info if schedule_info else None
            props = {k: v for k, v in props.items() if v is not None}
            
            # Build content including schedule for search
            text_parts = [f"{k}: {v}" for k, v in props.items() if k not in ['id', 'source_sheet', 'row_number', 'schedule']]
            if schedule_info:
                text_parts.append(f"Schedule: {schedule_info}")
            props['content'] = '\n'.join(text_parts)
            
            # Create WarRoom node
            self._execute_query("CREATE (w:WarRoom:Document $props)", props=props)
            
            warroom_id = props['id']
            
            # Link to Sheet
            self._execute_query("""
                MATCH (w:WarRoom {id: $warroom_id}), (s:Sheet {name: 'War Rooms'})
                MERGE (w)-[:FROM_SHEET]->(s)
            """, warroom_id=warroom_id)
            
            # Link to Area (team name as area)
            area_id = self.get_or_create_area(props['team_name'])
            if area_id:
                self._execute_query("""
                    MATCH (w:WarRoom {id: $warroom_id}), (a:Area {name: $area_name})
                    MERGE (w)-[:BELONGS_TO]->(a)
                """, warroom_id=warroom_id, area_name=props['team_name'])
            
            # Link to VP
            vp = props.get('vp')
            if vp:
                person_id = self.get_or_create_person(vp, None, 'VP')
                if person_id:
                    self._execute_query("""
                        MATCH (w:WarRoom {id: $warroom_id}), (p:Person {name: $person_name})
                        MERGE (w)-[:HAS_VP]->(p)
                    """, warroom_id=warroom_id, person_name=self._normalize_name(vp))
            
            # Link to Primary Lead
            primary = props.get('primary_lead')
            if primary:
                phone = self._extract_phone(primary)
                person_id = self.get_or_create_person(primary, phone, 'Primary Lead')
                if person_id:
                    self._execute_query("""
                        MATCH (w:WarRoom {id: $warroom_id}), (p:Person {name: $person_name})
                        MERGE (w)-[:HAS_PRIMARY_LEAD]->(p)
                    """, warroom_id=warroom_id, person_name=self._normalize_name(primary))
            
            # Link to Secondary Lead
            secondary = props.get('secondary_lead')
            if secondary:
                phone = self._extract_phone(secondary)
                person_id = self.get_or_create_person(secondary, phone, 'Secondary Lead')
                if person_id:
                    self._execute_query("""
                        MATCH (w:WarRoom {id: $warroom_id}), (p:Person {name: $person_name})
                        MERGE (w)-[:HAS_SECONDARY_LEAD]->(p)
                    """, warroom_id=warroom_id, person_name=self._normalize_name(secondary))
            
            count += 1
            
        return count
    
    def import_application_contacts(self, df: pd.DataFrame) -> int:
        """Import Application Contacts with relationships."""
        count = 0
        for idx, row in df.iterrows():
            structure = row.get('nornic Ops Structure')
            if pd.isna(structure) or str(structure).strip() == '':
                continue
                
            props = {
                'id': f"appcontact_{idx}",
                'nornic_ops_structure': str(structure).strip(),
                'cmk_com': str(row.get('CMK.com', '')) if pd.notna(row.get('CMK.com')) else None,
                'cvsh_app': str(row.get('CVSH App', '')) if pd.notna(row.get('CVSH App')) else None,
                'cvsh_app_mapping': str(row.get('CVSH App Mapping', '')) if pd.notna(row.get('CVSH App Mapping')) else None,
                'product_lead': str(row.get('Product Lead', '')) if pd.notna(row.get('Product Lead')) else None,
                'product_contact': str(row.get('Product Contact', '')) if pd.notna(row.get('Product Contact')) else None,
                'engineering_lead': str(row.get('Engineering Lead', '')) if pd.notna(row.get('Engineering Lead')) else None,
                'engineering_contact': str(row.get('Engineering Contact', '')) if pd.notna(row.get('Engineering Contact')) else None,
                'super_app_product_lead': str(row.get('Super App Product Lead', '')) if pd.notna(row.get('Super App Product Lead')) else None,
                'super_app_product_contact': str(row.get('Super App Product Contact', '')) if pd.notna(row.get('Super App Product Contact')) else None,
                'super_app_engineering_lead': str(row.get('Super App Engineering Lead', '')) if pd.notna(row.get('Super App Engineering Lead')) else None,
                'super_app_engineering_contact': str(row.get('Super App Engineering Contact', '')) if pd.notna(row.get('Super App Engineering Contact')) else None,
                'source_sheet': 'PS Digital Product-Eng',
                'row_number': int(idx),
            }
            props = {k: v for k, v in props.items() if v is not None}
            
            text_parts = [f"{k}: {v}" for k, v in props.items() if k not in ['id', 'source_sheet', 'row_number']]
            props['content'] = '\n'.join(text_parts)
            
            self._execute_query("CREATE (a:ApplicationContact:Document $props)", props=props)
            
            app_id = props['id']
            
            # Link to Sheet
            self._execute_query("""
                MATCH (a:ApplicationContact {id: $app_id}), (s:Sheet {name: 'PS Digital Product-Eng'})
                MERGE (a)-[:FROM_SHEET]->(s)
            """, app_id=app_id)
            
            # Link to Area (ops structure as area)
            area_id = self.get_or_create_area(props['nornic_ops_structure'])
            if area_id:
                self._execute_query("""
                    MATCH (a:ApplicationContact {id: $app_id}), (area:Area {name: $area_name})
                    MERGE (a)-[:BELONGS_TO]->(area)
                """, app_id=app_id, area_name=props['nornic_ops_structure'])
            
            # Link Product Lead
            product_lead = props.get('product_lead')
            if product_lead:
                person_id = self.get_or_create_person(product_lead, None, 'Product Lead')
                if person_id:
                    self._execute_query("""
                        MATCH (a:ApplicationContact {id: $app_id}), (p:Person {name: $person_name})
                        MERGE (a)-[:HAS_PRODUCT_LEAD]->(p)
                    """, app_id=app_id, person_name=self._normalize_name(product_lead))
            
            # Link Product Contact
            product_contact = props.get('product_contact')
            if product_contact:
                person_id = self.get_or_create_person(product_contact, None, 'Product Contact')
                if person_id:
                    self._execute_query("""
                        MATCH (a:ApplicationContact {id: $app_id}), (p:Person {name: $person_name})
                        MERGE (a)-[:HAS_PRODUCT_CONTACT]->(p)
                    """, app_id=app_id, person_name=self._normalize_name(product_contact))
            
            # Link Engineering Lead
            eng_lead = props.get('engineering_lead')
            if eng_lead:
                person_id = self.get_or_create_person(eng_lead, None, 'Engineering Lead')
                if person_id:
                    self._execute_query("""
                        MATCH (a:ApplicationContact {id: $app_id}), (p:Person {name: $person_name})
                        MERGE (a)-[:HAS_ENGINEERING_LEAD]->(p)
                    """, app_id=app_id, person_name=self._normalize_name(eng_lead))
            
            # Link Engineering Contact
            eng_contact = props.get('engineering_contact')
            if eng_contact:
                person_id = self.get_or_create_person(eng_contact, None, 'Engineering Contact')
                if person_id:
                    self._execute_query("""
                        MATCH (a:ApplicationContact {id: $app_id}), (p:Person {name: $person_name})
                        MERGE (a)-[:HAS_ENGINEERING_CONTACT]->(p)
                    """, app_id=app_id, person_name=self._normalize_name(eng_contact))
            
            # Link Super App Product Lead
            super_product_lead = props.get('super_app_product_lead')
            if super_product_lead:
                person_id = self.get_or_create_person(super_product_lead, None, 'Product Lead')
                if person_id:
                    self._execute_query("""
                        MATCH (a:ApplicationContact {id: $app_id}), (p:Person {name: $person_name})
                        MERGE (a)-[:HAS_SUPER_APP_PRODUCT_LEAD]->(p)
                    """, app_id=app_id, person_name=self._normalize_name(super_product_lead))
            
            # Link Super App Product Contact
            super_product_contact = props.get('super_app_product_contact')
            if super_product_contact:
                person_id = self.get_or_create_person(super_product_contact, None, 'Product Contact')
                if person_id:
                    self._execute_query("""
                        MATCH (a:ApplicationContact {id: $app_id}), (p:Person {name: $person_name})
                        MERGE (a)-[:HAS_SUPER_APP_PRODUCT_CONTACT]->(p)
                    """, app_id=app_id, person_name=self._normalize_name(super_product_contact))
            
            # Link Super App Engineering Lead
            super_eng_lead = props.get('super_app_engineering_lead')
            if super_eng_lead:
                person_id = self.get_or_create_person(super_eng_lead, None, 'Engineering Lead')
                if person_id:
                    self._execute_query("""
                        MATCH (a:ApplicationContact {id: $app_id}), (p:Person {name: $person_name})
                        MERGE (a)-[:HAS_SUPER_APP_ENGINEERING_LEAD]->(p)
                    """, app_id=app_id, person_name=self._normalize_name(super_eng_lead))
            
            # Link Super App Engineering Contact
            super_eng_contact = props.get('super_app_engineering_contact')
            if super_eng_contact:
                person_id = self.get_or_create_person(super_eng_contact, None, 'Engineering Contact')
                if person_id:
                    self._execute_query("""
                        MATCH (a:ApplicationContact {id: $app_id}), (p:Person {name: $person_name})
                        MERGE (a)-[:HAS_SUPER_APP_ENGINEERING_CONTACT]->(p)
                    """, app_id=app_id, person_name=self._normalize_name(super_eng_contact))
            
            count += 1
            
        return count
    
    def import_csx_delegates(self, df: pd.DataFrame) -> int:
        """Import CSX Resolution Center Delegates with relationships."""
        count = 0
        for idx, row in df.iterrows():
            functional_area = row.get('PBM and Corporate Functional Area')
            if pd.isna(functional_area) or str(functional_area).strip() == '':
                continue
                
            props = {
                'id': f"csx_delegate_{idx}",
                'functional_area': str(functional_area).strip(),
                'business_group': str(row.get('Business Group', '')) if pd.notna(row.get('Business Group')) else None,
                'business_group_owner': str(row.get('Business Group Owner', '')) if pd.notna(row.get('Business Group Owner')) else None,
                'business_area': str(row.get('Business Area', '')) if pd.notna(row.get('Business Area')) else None,
                'business_area_owner': str(row.get('Business Area Owner', '')) if pd.notna(row.get('Business Area Owner')) else None,
                'delegates': str(row.get('Delegates', '')) if pd.notna(row.get('Delegates')) else None,
                'source_sheet': 'CSX Resolution Center Delegates',
                'row_number': int(idx),
            }
            props = {k: v for k, v in props.items() if v is not None}
            
            text_parts = [f"{k}: {v}" for k, v in props.items() if k not in ['id', 'source_sheet', 'row_number']]
            props['content'] = '\n'.join(text_parts)
            
            self._execute_query("CREATE (c:CSXDelegate:Document $props)", props=props)
            
            delegate_id = props['id']
            
            # Link to Sheet
            self._execute_query("""
                MATCH (c:CSXDelegate {id: $delegate_id}), (s:Sheet {name: 'CSX Resolution Center Delegates'})
                MERGE (c)-[:FROM_SHEET]->(s)
            """, delegate_id=delegate_id)
            
            # Link to Area (functional area)
            area_id = self.get_or_create_area(props['functional_area'])
            if area_id:
                self._execute_query("""
                    MATCH (c:CSXDelegate {id: $delegate_id}), (a:Area {name: $area_name})
                    MERGE (c)-[:BELONGS_TO]->(a)
                """, delegate_id=delegate_id, area_name=props['functional_area'])
            
            # Link Business Group Owner
            bg_owner = props.get('business_group_owner')
            if bg_owner:
                person_id = self.get_or_create_person(bg_owner, None, 'Business Group Owner')
                if person_id:
                    self._execute_query("""
                        MATCH (c:CSXDelegate {id: $delegate_id}), (p:Person {name: $person_name})
                        MERGE (c)-[:HAS_BUSINESS_GROUP_OWNER]->(p)
                    """, delegate_id=delegate_id, person_name=self._normalize_name(bg_owner))
            
            # Link Business Area Owner
            ba_owner = props.get('business_area_owner')
            if ba_owner:
                person_id = self.get_or_create_person(ba_owner, None, 'Business Area Owner')
                if person_id:
                    self._execute_query("""
                        MATCH (c:CSXDelegate {id: $delegate_id}), (p:Person {name: $person_name})
                        MERGE (c)-[:HAS_BUSINESS_AREA_OWNER]->(p)
                    """, delegate_id=delegate_id, person_name=self._normalize_name(ba_owner))
            
            # Link Delegates (may be multiple, separated by commas)
            delegates = props.get('delegates')
            if delegates:
                # Handle names with descriptions in parentheses
                if '(' in delegates and ')' in delegates:
                    # If it's a single entry with parentheses, extract the name
                    normalized_delegate = self._normalize_name(delegates)
                    if normalized_delegate and self._is_valid_person_name(normalized_delegate):
                        person_id = self.get_or_create_person(normalized_delegate, None, 'Delegate')
                        if person_id:
                            self._execute_query("""
                                MATCH (c:CSXDelegate {id: $delegate_id}), (p:Person {name: $person_name})
                                MERGE (c)-[:HAS_DELEGATE]->(p)
                            """, delegate_id=delegate_id, person_name=normalized_delegate)
                else:
                    # Multiple delegates separated by commas
                    delegate_list = re.split(r'[,;]', delegates)
                    for delegate in delegate_list:
                        delegate = delegate.strip()
                        if delegate:
                            normalized_delegate = self._normalize_name(delegate)
                            if normalized_delegate and self._is_valid_person_name(normalized_delegate):
                                person_id = self.get_or_create_person(normalized_delegate, None, 'Delegate')
                                if person_id:
                                    self._execute_query("""
                                        MATCH (c:CSXDelegate {id: $delegate_id}), (p:Person {name: $person_name})
                                        MERGE (c)-[:HAS_DELEGATE]->(p)
                                    """, delegate_id=delegate_id, person_name=normalized_delegate)
            
            count += 1
            
        return count
    
    def import_static_knowledge(self) -> int:
        """Import static knowledge documents."""
        static_docs = [
            {
                'id': 'static_schedule',
                'title': 'Welcome Season 2026 Schedule',
                'content': '''Welcome Season 2026 Schedule and Timeline
Welcome Season 2026 runs from January 1, 2026 through January 20, 2026.
Peak days are during the first three weeks of January.
Key dates:
- January 1, 2026: Welcome Season starts (New Year's Day)
- January 1-20, 2026: Peak period for War Rooms
- War Rooms operate daily during peak period
This is the annual period when new healthcare benefits take effect and member activity is highest.
Welcome Season is also known as WS2026 or Welcome Season '26.''',
                'type': 'schedule',
            },
            {
                'id': 'static_what_is_ws',
                'title': 'What is Welcome Season',
                'content': '''What is Welcome Season?
Welcome Season is the annual period at the beginning of the calendar year when:
- New health insurance benefits take effect for members
- Members call to understand their new coverage
- Prescription transfers and new enrollments are processed
- Call volumes are significantly higher than normal
- War Rooms are activated to handle increased demand
Welcome Season 2026 (WS2026) runs from January 1 through January 20, 2026.
This is a critical operational period for healthcare and pharmacy services.''',
                'type': 'general_info',
            },
            {
                'id': 'static_war_rooms',
                'title': 'What are War Rooms',
                'content': '''What are War Rooms?
War Rooms are dedicated operational command centers activated during Welcome Season.
Purpose:
- Coordinate response to high-volume periods
- Monitor system performance and issues
- Provide rapid escalation paths
- Ensure business continuity during peak demand
War Rooms are staffed by VPs, Primary Leads, and Secondary Leads.
They can be Virtual, Onsite (physical location), or Combined.
War Room schedules follow the Welcome Season peak period (January 1-20).''',
                'type': 'general_info',
            },
            {
                'id': 'static_find_pocs',
                'title': 'How to find POCs',
                'content': '''How to find Points of Contact (POCs)
To find the right contact for Welcome Season 2026:
1. Check the WS2026 POCs for team-specific contacts
2. Look up Area/Team name to find WS Point of Contact
3. POC Managers are listed for escalation
4. War Room contacts (VP, Primary Lead, Secondary Lead) handle operational issues
5. Application contacts have product and engineering contacts
Common search terms: POC, contact, manager, lead, who to call, escalation''',
                'type': 'help',
            },
            {
                'id': 'static_key_dates',
                'title': 'Welcome Season 2026 Key Dates Calendar',
                'content': '''Welcome Season 2026 Key Dates and Activity Levels

ACTIVITY LEVEL DEFINITIONS:
- PEAK: Highest volume days. All War Rooms fully staffed. Maximum readiness required. Expect highest call volumes and system load.
- HIGH: Elevated volume days. War Rooms active with enhanced staffing. Above-normal call volumes expected.
- BAU (Business As Usual): Normal operational levels. Standard staffing in effect. Regular monitoring continues.

COMPLETE DATE-BY-DATE SCHEDULE:

Week 1 (January 1-4, 2026):
- Thursday, January 1, 2026 (New Year's Day): PEAK
  * Welcome Season officially begins
  * New health insurance benefits take effect
  * Highest initial call volume as members discover coverage changes
  * All War Rooms activated

- Friday, January 2, 2026: PEAK
  * Continued high volume from benefit activation
  * First business day for many employers
  * Heavy prescription transfer activity

- Saturday, January 3, 2026: BAU
  * Weekend reduced staffing
  * Lower call volumes typical

- Sunday, January 4, 2026: BAU
  * Weekend operations continue
  * Preparation for Monday peak

Week 2 (January 5-11, 2026):
- Monday, January 5, 2026: PEAK
  * First full business week begins
  * Major spike in employer-related inquiries
  * System load at maximum

- Tuesday, January 6, 2026: HIGH
  * Sustained elevated volumes
  * Follow-up calls from Monday issues

- Wednesday, January 7, 2026: HIGH
  * Mid-week elevated activity
  * Processing backlog from peak days

- Thursday, January 8, 2026: PEAK
  * Another peak day as issues accumulate
  * Escalation volumes increase

- Friday, January 9, 2026: PEAK
  * End of first full week
  * Resolution push before weekend

- Saturday, January 10, 2026: BAU
  * Weekend operations
  * Catch-up processing

- Sunday, January 11, 2026: BAU
  * Weekend continues
  * System maintenance window

Week 3 (January 12-18, 2026):
- Monday, January 12, 2026: PEAK
  * Second week begins with elevated volumes
  * New issues from weekend discoveries

- Tuesday, January 13, 2026: HIGH
  * Sustained high activity
  * Continued member outreach

- Wednesday, January 14, 2026: HIGH
  * Mid-week processing
  * Trending toward normalization

- Thursday, January 15, 2026: BAU
  * Volumes beginning to normalize
  * Standard operations resuming

- Friday, January 16, 2026: BAU
  * Continued normalization
  * End of third week

- Saturday, January 17, 2026: BAU
  * Weekend operations
  * Reduced staffing

- Sunday, January 18, 2026: BAU
  * Weekend continues
  * Martin Luther King Jr. Day preparation

Week 4 (January 19-20, 2026):
- Monday, January 19, 2026 (MLK Day): HIGH
  * Federal holiday - some offices closed
  * Elevated calls from holiday coverage questions
  * Last HIGH volume day

- Tuesday, January 20, 2026: BAU
  * Welcome Season 2026 officially ends
  * Return to normal operations
  * War Rooms begin deactivation
  * Post-season analysis begins

SUMMARY BY ACTIVITY LEVEL:
- PEAK Days (7 total): Jan 1, 2, 5, 8, 9, 12
- HIGH Days (5 total): Jan 6, 7, 13, 14, 19
- BAU Days (8 total): Jan 3, 4, 10, 11, 15, 16, 17, 18, 20

KEY CONTACTS FOR EACH ACTIVITY LEVEL:
- PEAK Days: Contact War Room Primary Lead first, then VP for escalation
- HIGH Days: Contact WS Point of Contact, escalate to War Room if needed
- BAU Days: Normal support channels, POC Managers for escalation''',
                'type': 'calendar',
            },
            {
                'id': 'static_activity_levels',
                'title': 'Activity Level Definitions',
                'content': '''Welcome Season Activity Level Guide

PEAK - Maximum Readiness
Definition: Days with highest expected call volumes and system load
Staffing: All War Rooms fully activated, maximum personnel
Expected Volume: 150-200% of normal daily volume
Actions Required:
- All hands on deck
- Executive monitoring active
- Rapid escalation protocols in effect
- System performance monitoring every 15 minutes
- Customer wait time targets: under 5 minutes

HIGH - Elevated Operations  
Definition: Days with above-normal but manageable volumes
Staffing: War Rooms active with enhanced staffing levels
Expected Volume: 120-150% of normal daily volume
Actions Required:
- Enhanced monitoring
- Backup personnel on standby
- Escalation paths clearly communicated
- System checks every 30 minutes
- Customer wait time targets: under 10 minutes

BAU (Business As Usual) - Standard Operations
Definition: Normal operational levels during Welcome Season
Staffing: Standard staffing with Welcome Season awareness
Expected Volume: 100-120% of normal daily volume
Actions Required:
- Normal monitoring procedures
- Standard escalation paths
- Regular system checks
- Customer wait time targets: standard SLAs

ESCALATION PATH BY LEVEL:
PEAK: Team Member → Primary Lead → VP → War Room Command
HIGH: Team Member → WS Point of Contact → POC Manager → Primary Lead
BAU: Team Member → Supervisor → WS Point of Contact → POC Manager''',
                'type': 'reference',
            },
        ]
        
        count = 0
        for doc in static_docs:
            doc['source_sheet'] = 'Static Knowledge'
            self._execute_query("""
                CREATE (s:StaticKnowledge:Document $props)
            """, props=doc)
            
            # Link to Sheet
            self._execute_query("""
                MATCH (s:StaticKnowledge {id: $doc_id}), (sheet:Sheet {name: 'Static Knowledge'})
                MERGE (s)-[:FROM_SHEET]->(sheet)
            """, doc_id=doc['id'])
            
            count += 1
                
        return count
    
    def create_person_area_relationships(self):
        """Create WORKS_IN relationships between Person and Area nodes."""
        # Link persons to areas through their associated documents
        self._execute_query("""
            MATCH (p:Person)<-[:HAS_CONTACT|MANAGED_BY|HAS_LEADER]-(poc:POC)-[:BELONGS_TO]->(a:Area)
            MERGE (p)-[:WORKS_IN]->(a)
        """)
        
        self._execute_query("""
            MATCH (p:Person)<-[:HAS_VP|HAS_PRIMARY_LEAD|HAS_SECONDARY_LEAD]-(w:WarRoom)-[:BELONGS_TO]->(a:Area)
            MERGE (p)-[:WORKS_IN]->(a)
        """)
        
        self._execute_query("""
            MATCH (p:Person)<-[:HAS_PRODUCT_LEAD|HAS_PRODUCT_CONTACT|HAS_ENGINEERING_LEAD|HAS_ENGINEERING_CONTACT|HAS_SUPER_APP_PRODUCT_LEAD|HAS_SUPER_APP_PRODUCT_CONTACT|HAS_SUPER_APP_ENGINEERING_LEAD|HAS_SUPER_APP_ENGINEERING_CONTACT]-(app:ApplicationContact)-[:BELONGS_TO]->(a:Area)
            MERGE (p)-[:WORKS_IN]->(a)
        """)
        
        self._execute_query("""
            MATCH (p:Person)<-[:HAS_BUSINESS_GROUP_OWNER|HAS_BUSINESS_AREA_OWNER|HAS_DELEGATE]-(csx:CSXDelegate)-[:BELONGS_TO]->(a:Area)
            MERGE (p)-[:WORKS_IN]->(a)
        """)
        
        # Link persons from Coverage Schedules to Areas
        self._execute_query("""
            MATCH (p:Person)<-[:FOR_PERSON]-(c:CoverageSchedule)-[:BELONGS_TO]->(a:Area)
            MERGE (p)-[:WORKS_IN]->(a)
        """)
        
        # Link persons from Support Schedules to Areas
        self._execute_query("""
            MATCH (p:Person)<-[:FOR_PERSON]-(s:SupportSchedule)-[:BELONGS_TO]->(a:Area)
            MERGE (p)-[:WORKS_IN]->(a)
        """)
        
        # Link persons from Support Rosters to Areas
        self._execute_query("""
            MATCH (p:Person)<-[:FOR_PERSON]-(sr:SupportRoster)-[:BELONGS_TO]->(a:Area)
            MERGE (p)-[:WORKS_IN]->(a)
        """)
        
        print("   ✅ Created Person-Area relationships")
    
    def create_navigation_relationships(self):
        """Create additional relationships for easier data navigation."""
        
        # Link People who work together (same POC)
        self._execute_query("""
            MATCH (p1:Person)<-[]-(poc:POC)-[]->(p2:Person)
            WHERE p1 <> p2
            MERGE (p1)-[:WORKS_WITH]->(p2)
        """)
        
        # Link People who work together (same War Room)
        self._execute_query("""
            MATCH (p1:Person)<-[]-(w:WarRoom)-[]->(p2:Person)
            WHERE p1 <> p2
            MERGE (p1)-[:WORKS_WITH]->(p2)
        """)
        
        # Link Areas that share personnel
        self._execute_query("""
            MATCH (a1:Area)<-[:WORKS_IN]-(p:Person)-[:WORKS_IN]->(a2:Area)
            WHERE a1 <> a2
            MERGE (a1)-[:SHARES_PERSONNEL]->(a2)
        """)
        
        # Link War Rooms to POCs through shared Areas
        self._execute_query("""
            MATCH (w:WarRoom)-[:BELONGS_TO]->(a:Area)<-[:BELONGS_TO]-(poc:POC)
            MERGE (w)-[:RELATED_TO]->(poc)
        """)
        
        # Link managers to their direct reports via POC relationships
        self._execute_query("""
            MATCH (manager:Person)<-[:MANAGED_BY]-(poc:POC)-[:HAS_CONTACT]->(contact:Person)
            MERGE (manager)-[:MANAGES]->(contact)
        """)
        
        # Create REPORTS_TO relationships (inverse of MANAGES)
        self._execute_query("""
            MATCH (manager:Person)-[:MANAGES]->(report:Person)
            MERGE (report)-[:REPORTS_TO]->(manager)
        """)
        
        # Link VPs to their War Room leads
        self._execute_query("""
            MATCH (vp:Person)<-[:HAS_VP]-(w:WarRoom)-[:HAS_PRIMARY_LEAD|HAS_SECONDARY_LEAD]->(lead:Person)
            MERGE (vp)-[:OVERSEES]->(lead)
        """)
        
        # Create schedule-based relationships (Peak days)
        self._execute_query("""
            MATCH (w:WarRoom)
            WHERE w.schedule IS NOT NULL
            WITH w, [key IN keys(w.schedule) WHERE w.schedule[key] = 'Peak'] as peak_days
            WHERE size(peak_days) > 0
            SET w.peak_days = peak_days, w.peak_day_count = size(peak_days)
        """)
        
        # Link Product to their corresponding Applications
        self._execute_query("""
            MATCH (app:ApplicationContact)
            WHERE app.product_application_service IS NOT NULL OR app.nornic_ops_structure IS NOT NULL
            WITH app, COALESCE(app.product_application_service, app.nornic_ops_structure) as product_name
            MERGE (prod:Product {name: product_name})
            MERGE (app)-[:FOR_PRODUCT]->(prod)
        """)
        
        # Link Coverage Schedules to War Rooms through shared Areas
        self._execute_query("""
            MATCH (c:CoverageSchedule)-[:BELONGS_TO]->(a:Area)<-[:BELONGS_TO]-(w:WarRoom)
            MERGE (c)-[:RELATED_TO]->(w)
        """)
        
        # Link Support Schedules to War Rooms through shared Areas
        self._execute_query("""
            MATCH (s:SupportSchedule)-[:BELONGS_TO]->(a:Area)<-[:BELONGS_TO]-(w:WarRoom)
            MERGE (s)-[:RELATED_TO]->(w)
        """)
        
        # Link Support Rosters to POCs through shared Areas
        self._execute_query("""
            MATCH (sr:SupportRoster)-[:BELONGS_TO]->(a:Area)<-[:BELONGS_TO]-(poc:POC)
            MERGE (sr)-[:RELATED_TO]->(poc)
        """)
        
        # Link Coverage Schedules to POCs through shared Persons
        self._execute_query("""
            MATCH (c:CoverageSchedule)-[:FOR_PERSON]->(p:Person)<-[:HAS_CONTACT|MANAGED_BY|HAS_LEADER]-(poc:POC)
            MERGE (c)-[:RELATED_TO]->(poc)
        """)
        
        # Link Support Schedules to POCs through shared Persons
        self._execute_query("""
            MATCH (s:SupportSchedule)-[:FOR_PERSON]->(p:Person)<-[:HAS_CONTACT|MANAGED_BY|HAS_LEADER]-(poc:POC)
            MERGE (s)-[:RELATED_TO]->(poc)
        """)
        
        # Create date-based relationships for schedules
        self._execute_query("""
            MATCH (s:SupportSchedule)
            WHERE s.date IS NOT NULL
            WITH s, s.date as date_str
            WHERE date_str <> ''
            MERGE (d:Date {date: date_str})
            MERGE (s)-[:ON_DATE]->(d)
        """)
        
        print("   ✅ Created navigation relationships")
    
    def import_coverage_schedule(self, df: pd.DataFrame, sheet_name: str = "Example") -> int:
        """Import coverage schedule data with speak times and availability.
        
        Expected structure (from Example sheet):
        - Row 0: Headers (Contact Name, Cell Phone, days of week)
        - Row 1: Week label, day names
        - Row 2: Dates
        - Row 3+: Contact name, phone, schedule times per day
        """
        count = 0
        
        # Skip header rows (0-2) and process data rows
        for idx in range(3, len(df)):
            row = df.iloc[idx]
            
            # Skip empty rows
            if row.isna().all():
                continue
            
            # Get contact name (usually column 1, index 1)
            contact_name = None
            phone = None
            
            # Try to find contact name and phone in first few columns
            for col_idx in [1, 2]:
                if col_idx < len(row):
                    val = str(row.iloc[col_idx]).strip() if pd.notna(row.iloc[col_idx]) else ""
                    if val and val.lower() not in ['nan', 'none', 'n/a', 'contact name', 'cell phone']:
                        # Check if it looks like a phone number
                        if re.search(r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}', val):
                            phone = self._extract_phone(val) or val
                        else:
                            contact_name = val
            
            if not contact_name:
                continue
            
            # Extract schedule information from remaining columns (days of week)
            schedule_info = {}
            day_dates = {}  # Map day to date
            
            # First, try to get dates from row 2 (if available in context)
            # We'll need to read the full dataframe to get row 2
            if len(df) > 2:
                date_row = df.iloc[2]
                for col_idx in range(3, len(df.columns)):
                    date_val = date_row.iloc[col_idx] if col_idx < len(date_row) else None
                    if pd.notna(date_val):
                        day_dates[col_idx] = str(date_val).strip()
            
            # Extract schedule times for each day
            for col_idx in range(3, len(row)):
                if col_idx < len(row):
                    schedule_val = row.iloc[col_idx]
                    if pd.notna(schedule_val):
                        schedule_str = str(schedule_val).strip()
                        if schedule_str and schedule_str.upper() != 'OFF' and schedule_str.lower() not in ['nan', 'none']:
                            # Get column name for day identification
                            col_name = df.columns[col_idx] if col_idx < len(df.columns) else f"Day_{col_idx}"
                            schedule_info[col_name] = schedule_str
                            
                            # Add date if available
                            if col_idx in day_dates:
                                schedule_info[f"{col_name}_date"] = day_dates[col_idx]
            
            if not schedule_info:
                continue
            
            # Build properties
            props = {
                'id': f"coverage_{idx}",
                'contact_name': contact_name,
                'phone': phone,
                'schedule': schedule_info,
                'source_sheet': sheet_name,
                'source_file': 'CMKDigital-Emerson_WS2026 Coverage Schedule.xlsx',
                'row_number': int(idx),
            }
            
            # Build content for search
            text_parts = [f"Contact: {contact_name}"]
            if phone:
                text_parts.append(f"Phone: {phone}")
            text_parts.append(f"Schedule: {schedule_info}")
            props['content'] = '\n'.join(text_parts)
            
            # Create CoverageSchedule node
            self._execute_query("CREATE (c:CoverageSchedule:Document $props)", props=props)
            
            coverage_id = props['id']
            
            # Link to Sheet
            self._execute_query("""
                MATCH (c:CoverageSchedule {id: $coverage_id}), (s:Sheet {name: $sheet_name})
                MERGE (c)-[:FROM_SHEET]->(s)
            """, coverage_id=coverage_id, sheet_name='Coverage Schedule')
            
            # Link to Person
            person_id = self.get_or_create_person(contact_name, phone, 'Coverage Contact')
            if person_id:
                self._execute_query("""
                    MATCH (c:CoverageSchedule {id: $coverage_id}), (p:Person {name: $person_name})
                    MERGE (c)-[:FOR_PERSON]->(p)
                """, coverage_id=coverage_id, person_name=self._normalize_name(contact_name))
            
            count += 1
        
        return count
    
    def import_support_roster(self, df: pd.DataFrame, sheet_name: str) -> int:
        """Import support roster with availability and shift information.
        
        Handles multiple sheet formats:
        - DDAT Support Schedule: Tower/area in col 0, time slots in row 1, dates in row 0, names in subsequent rows
        - Account Engineering: On-call periods with primary/secondary contacts
        - PCW Engineering: Tower names with contact info
        """
        count = 0
        
        # Detect structure type based on sheet name and content
        sheet_lower = sheet_name.lower()
        
        if 'ddat' in sheet_lower or 'support schedule' in sheet_lower:
            # Structure: Row 0 has dates, Row 1 has time slots, Row 2+ has tower/area and person names
            return self._import_ddat_schedule(df, sheet_name)
        elif 'account' in sheet_lower:
            # Structure: On-call periods with primary/secondary contacts
            return self._import_account_engineering(df, sheet_name)
        elif 'pcw' in sheet_lower:
            # Structure: Tower names with contact names and phones
            return self._import_pcw_engineering(df, sheet_name)
        else:
            # Generic import
            return self._import_generic_roster(df, sheet_name)
    
    def _import_ddat_schedule(self, df: pd.DataFrame, sheet_name: str) -> int:
        """Import DDAT Support Schedule format."""
        count = 0
        
        # Row 0: Dates (starting from col 2)
        # Row 1: Time slots
        # Row 2+: Tower/Area in col 0 or 1, person names in date columns
        
        if len(df) < 2:
            return 0
        
        # Extract dates from row 0
        dates = {}
        date_row = df.iloc[0]
        for col_idx in range(2, len(date_row)):
            date_val = date_row.iloc[col_idx]
            if pd.notna(date_val):
                try:
                    # Try to parse as date
                    if isinstance(date_val, pd.Timestamp):
                        date_str = date_val.strftime('%Y-%m-%d')
                    else:
                        date_str = str(date_val).strip()
                    dates[col_idx] = date_str
                except:
                    pass
        
        # Extract time slots from row 1
        time_slots = {}
        time_row = df.iloc[1]
        for col_idx in range(2, len(time_row)):
            time_val = time_row.iloc[col_idx]
            if pd.notna(time_val):
                time_slots[col_idx] = str(time_val).strip()
        
        # Process data rows (starting from row 2)
        for row_idx in range(2, len(df)):
            row = df.iloc[row_idx]
            
            # Get tower/area name (usually col 0 or 1)
            tower_area = None
            for col_idx in [0, 1]:
                if col_idx < len(row):
                    val = row.iloc[col_idx]
                    if pd.notna(val):
                        val_str = str(val).strip()
                        if val_str and val_str.lower() not in ['nan', 'none', 'tower', 'unnamed']:
                            tower_area = val_str
                            break
            
            if not tower_area:
                continue
            
            # Extract person assignments for each date/time slot
            for col_idx in range(2, len(row)):
                person_name = row.iloc[col_idx] if col_idx < len(row) else None
                
                if pd.notna(person_name):
                    person_name = str(person_name).strip()
                    if person_name and person_name.lower() not in ['nan', 'none', '']:
                        # Get date and time slot for this assignment
                        date = dates.get(col_idx, '')
                        time_slot = time_slots.get(col_idx, '')
                        
                        # Create schedule entry
                        props = {
                            'id': f"support_schedule_{row_idx}_{col_idx}",
                            'tower_area': tower_area,
                            'person_name': person_name,
                            'date': date,
                            'time_slot': time_slot,
                            'source_sheet': sheet_name,
                            'source_file': '2026_WS_Support_Roster.xlsx',
                            'row_number': row_idx,
                            'col_number': col_idx,
                        }
                        
                        text_parts = [f"Tower/Area: {tower_area}", f"Person: {person_name}"]
                        if date:
                            text_parts.append(f"Date: {date}")
                        if time_slot:
                            text_parts.append(f"Time: {time_slot}")
                        props['content'] = '\n'.join(text_parts)
                        
                        # Create SupportSchedule node
                        self._execute_query("CREATE (s:SupportSchedule:Document $props)", props=props)
                        
                        schedule_id = props['id']
                        
                        # Link to Sheet
                        self._execute_query("""
                            MATCH (s:SupportSchedule {id: $schedule_id}), (sheet:Sheet {name: $sheet_name})
                            MERGE (s)-[:FROM_SHEET]->(sheet)
                        """, schedule_id=schedule_id, sheet_name='Support Roster')
                        
                        # Link to Person
                        person_id = self.get_or_create_person(person_name, None, 'Support Staff')
                        if person_id:
                            self._execute_query("""
                                MATCH (s:SupportSchedule {id: $schedule_id}), (p:Person {name: $person_name})
                                MERGE (s)-[:FOR_PERSON]->(p)
                            """, schedule_id=schedule_id, person_name=self._normalize_name(person_name))
                        
                        # Link to Area
                        area_id = self.get_or_create_area(tower_area)
                        if area_id:
                            self._execute_query("""
                                MATCH (s:SupportSchedule {id: $schedule_id}), (a:Area {name: $area_name})
                                MERGE (s)-[:BELONGS_TO]->(a)
                            """, schedule_id=schedule_id, area_name=tower_area)
                        
                        count += 1
        
        return count
    
    def _import_account_engineering(self, df: pd.DataFrame, sheet_name: str) -> int:
        """Import Account Engineering on-call schedule."""
        count = 0
        
        # Structure: On-call periods, primary/secondary contacts, names and phones
        for idx, row in df.iterrows():
            if row.isna().all():
                continue
            
            # Get on-call period (col 0)
            period = row.iloc[0] if len(row) > 0 and pd.notna(row.iloc[0]) else None
            if not period or str(period).strip().lower() in ['nan', 'none', 'on call period', 'core account (login)']:
                continue
            
            period = str(period).strip()
            
            # Extract contacts from various columns
            # Look for name/phone pairs
            contacts = []
            for col_idx in range(1, len(row)):
                val = row.iloc[col_idx] if col_idx < len(row) else None
                if pd.notna(val):
                    val_str = str(val).strip()
                    if val_str and val_str.lower() not in ['nan', 'none', 'primary', 'secondary', 'name', 'phone number']:
                        # Check if it's a phone number
                        phone = self._extract_phone(val_str)
                        if phone:
                            # Previous column might be name
                            if col_idx > 0:
                                name_val = row.iloc[col_idx - 1]
                                if pd.notna(name_val):
                                    name = str(name_val).strip()
                                    if name and name.lower() not in ['nan', 'none', 'name']:
                                        contacts.append({'name': name, 'phone': phone})
                        elif len(val_str) > 3 and not val_str.replace('-', '').replace(' ', '').isdigit():
                            # Might be a name
                            contacts.append({'name': val_str, 'phone': None})
            
            if not contacts:
                continue
            
            # Create roster entry for period
            for contact in contacts:
                props = {
                    'id': f"account_eng_{idx}_{contacts.index(contact)}",
                    'on_call_period': period,
                    'person_name': contact['name'],
                    'phone': contact['phone'],
                    'source_sheet': sheet_name,
                    'source_file': '2026_WS_Support_Roster.xlsx',
                    'row_number': int(idx),
                }
                props = {k: v for k, v in props.items() if v is not None}
                
                text_parts = [f"Period: {period}", f"Contact: {contact['name']}"]
                if contact['phone']:
                    text_parts.append(f"Phone: {contact['phone']}")
                props['content'] = '\n'.join(text_parts)
                
                self._execute_query("CREATE (s:SupportRoster:Document $props)", props=props)
                
                roster_id = props['id']
                
                # Link to Sheet
                self._execute_query("""
                    MATCH (s:SupportRoster {id: $roster_id}), (sheet:Sheet {name: $sheet_name})
                    MERGE (s)-[:FROM_SHEET]->(sheet)
                """, roster_id=roster_id, sheet_name='Support Roster')
                
                # Link to Person
                person_id = self.get_or_create_person(contact['name'], contact['phone'], 'Account Engineering')
                if person_id:
                    self._execute_query("""
                        MATCH (s:SupportRoster {id: $roster_id}), (p:Person {name: $person_name})
                        MERGE (s)-[:FOR_PERSON]->(p)
                    """, roster_id=roster_id, person_name=self._normalize_name(contact['name']))
                
                count += 1
        
        return count
    
    def _import_pcw_engineering(self, df: pd.DataFrame, sheet_name: str) -> int:
        """Import PCW Engineering contacts."""
        count = 0
        
        # Structure: Tower/application names with contact names and phones
        for idx, row in df.iterrows():
            if row.isna().all():
                continue
            
            # Get tower/application name (col 0)
            tower_app = row.iloc[0] if len(row) > 0 and pd.notna(row.iloc[0]) else None
            if not tower_app or str(tower_app).strip().lower() in ['nan', 'none', 'prescriptions & orders be']:
                continue
            
            tower_app = str(tower_app).strip()
            
            # Extract contacts (name and phone pairs in subsequent columns)
            for col_idx in range(1, len(row), 2):
                name = row.iloc[col_idx] if col_idx < len(row) and pd.notna(row.iloc[col_idx]) else None
                phone = row.iloc[col_idx + 1] if col_idx + 1 < len(row) and pd.notna(row.iloc[col_idx + 1]) else None
                
                if name:
                    name = str(name).strip()
                    if name and name.lower() not in ['nan', 'none']:
                        phone_str = str(phone).strip() if phone and pd.notna(phone) else None
                        phone_clean = self._extract_phone(phone_str) if phone_str else None
                        
                        props = {
                            'id': f"pcw_eng_{idx}_{col_idx}",
                            'tower_application': tower_app,
                            'person_name': name,
                            'phone': phone_clean or phone_str,
                            'source_sheet': sheet_name,
                            'source_file': '2026_WS_Support_Roster.xlsx',
                            'row_number': int(idx),
                        }
                        props = {k: v for k, v in props.items() if v is not None}
                        
                        text_parts = [f"Tower/Application: {tower_app}", f"Contact: {name}"]
                        if phone_clean or phone_str:
                            text_parts.append(f"Phone: {phone_clean or phone_str}")
                        props['content'] = '\n'.join(text_parts)
                        
                        self._execute_query("CREATE (s:SupportRoster:Document $props)", props=props)
                        
                        roster_id = props['id']
                        
                        # Link to Sheet
                        self._execute_query("""
                            MATCH (s:SupportRoster {id: $roster_id}), (sheet:Sheet {name: $sheet_name})
                            MERGE (s)-[:FROM_SHEET]->(sheet)
                        """, roster_id=roster_id, sheet_name='Support Roster')
                        
                        # Link to Person
                        person_id = self.get_or_create_person(name, phone_clean or phone_str, 'PCW Engineering')
                        if person_id:
                            self._execute_query("""
                                MATCH (s:SupportRoster {id: $roster_id}), (p:Person {name: $person_name})
                                MERGE (s)-[:FOR_PERSON]->(p)
                            """, roster_id=roster_id, person_name=self._normalize_name(name))
                        
                        # Link to Area
                        area_id = self.get_or_create_area(tower_app)
                        if area_id:
                            self._execute_query("""
                                MATCH (s:SupportRoster {id: $roster_id}), (a:Area {name: $area_name})
                                MERGE (s)-[:BELONGS_TO]->(a)
                            """, roster_id=roster_id, area_name=tower_app)
                        
                        count += 1
        
        return count
    
    def _import_generic_roster(self, df: pd.DataFrame, sheet_name: str) -> int:
        """Generic import for unrecognized roster formats."""
        count = 0
        # Basic implementation - can be enhanced
        return count
    
    def import_all(self):
        """Import all data from Excel into Neo4j."""
        print("="*60)
        print("NEO4J IMPORT - Welcome Season 2026 Data")
        print("="*60)
        
        if not self.excel_file.exists():
            print(f"❌ Excel file not found: {self.excel_file}")
            return
        
        # Clear existing data
        self.clear_database()
        
        # Create constraints and indexes
        self.create_constraints_and_indexes()
        
        # Create sheet nodes
        print("\n📥 Creating Sheet nodes...")
        self.create_sheet_nodes()
        
        # Read Excel file
        print(f"\n📄 Reading Excel file: {self.excel_file}")
        xl = pd.ExcelFile(self.excel_file)
        print(f"   Sheets found: {xl.sheet_names}")
        
        total_count = 0
        
        # Import POCs (header is on row 1, not row 0)
        print("\n📥 Importing WS2026 POCs...")
        df_pocs = pd.read_excel(self.excel_file, sheet_name='WS2026 POCs', header=1)
        poc_count = self.import_pocs(df_pocs)
        print(f"   ✅ Created {poc_count} POC nodes")
        total_count += poc_count
        
        # Import War Rooms
        print("\n📥 Importing War Rooms...")
        df_war_rooms = pd.read_excel(self.excel_file, sheet_name='War Rooms')
        war_room_count = self.import_war_rooms(df_war_rooms)
        print(f"   ✅ Created {war_room_count} War Room nodes")
        total_count += war_room_count
        
        # Import Application Contacts
        print("\n📥 Importing Application Contacts...")
        df_app = pd.read_excel(self.excel_file, sheet_name='PS Digital Product-Eng')
        app_count = self.import_application_contacts(df_app)
        print(f"   ✅ Created {app_count} Application Contact nodes")
        total_count += app_count
        
        # Import CSX Resolution Center Delegates
        print("\n📥 Importing CSX Resolution Center Delegates...")
        df_csx = pd.read_excel(self.excel_file, sheet_name='CSX Resolution Center Delegates')
        csx_count = self.import_csx_delegates(df_csx)
        print(f"   ✅ Created {csx_count} CSX Delegate nodes")
        total_count += csx_count
        
        # Import Static Knowledge
        print("\n📥 Importing Static Knowledge...")
        static_count = self.import_static_knowledge()
        print(f"   ✅ Created {static_count} Static Knowledge nodes")
        total_count += static_count
        
        # Import Coverage Schedule from CMKDigital-Emerson file
        if self.coverage_schedule_file.exists():
            print(f"\n📄 Reading Coverage Schedule file: {self.coverage_schedule_file}")
            try:
                xl_coverage = pd.ExcelFile(self.coverage_schedule_file)
                print(f"   Sheets found: {xl_coverage.sheet_names}")
                
                for sheet_name in xl_coverage.sheet_names:
                    if 'template' in sheet_name.lower() and 'example' not in sheet_name.lower():
                        continue  # Skip template sheets
                    
                    print(f"\n📥 Importing Coverage Schedule from sheet: {sheet_name}...")
                    try:
                        df_coverage = pd.read_excel(xl_coverage, sheet_name=sheet_name, header=1)
                        coverage_count = self.import_coverage_schedule(df_coverage, sheet_name)
                        print(f"   ✅ Created {coverage_count} Coverage Schedule nodes")
                        total_count += coverage_count
                    except Exception as e:
                        print(f"   ⚠️  Error importing sheet {sheet_name}: {e}")
            except Exception as e:
                print(f"   ⚠️  Error reading coverage schedule file: {e}")
        else:
            print(f"\n⚠️  Coverage Schedule file not found: {self.coverage_schedule_file}")
        
        # Import Support Roster from 2026_WS_Support_Roster file
        if self.support_roster_file.exists():
            print(f"\n📄 Reading Support Roster file: {self.support_roster_file}")
            try:
                xl_roster = pd.ExcelFile(self.support_roster_file)
                print(f"   Sheets found: {xl_roster.sheet_names}")
                
                for sheet_name in xl_roster.sheet_names:
                    if sheet_name.lower() == 'sheet1':
                        continue  # Skip generic Sheet1
                    
                    print(f"\n📥 Importing Support Roster from sheet: {sheet_name}...")
                    try:
                        # Try different header rows
                        df_roster = None
                        for header_row in [0, 1, 2, 3, 4]:
                            try:
                                df_roster = pd.read_excel(xl_roster, sheet_name=sheet_name, header=header_row)
                                if len(df_roster.columns) > 2:  # Valid header found
                                    break
                            except:
                                continue
                        
                        if df_roster is not None and len(df_roster.columns) > 2:
                            roster_count = self.import_support_roster(df_roster, sheet_name)
                            print(f"   ✅ Created {roster_count} Support Roster nodes")
                            total_count += roster_count
                        else:
                            print(f"   ⚠️  Could not read sheet {sheet_name}")
                    except Exception as e:
                        print(f"   ⚠️  Error importing sheet {sheet_name}: {e}")
            except Exception as e:
                print(f"   ⚠️  Error reading support roster file: {e}")
        else:
            print(f"\n⚠️  Support Roster file not found: {self.support_roster_file}")
        
        # Create cross-entity relationships
        print("\n📥 Creating cross-entity relationships...")
        self.create_person_area_relationships()
        
        # Create navigation relationships for easier data traversal
        print("\n📥 Creating navigation relationships...")
        self.create_navigation_relationships()
        
        # Summary
        print("\n" + "="*60)
        print("IMPORT COMPLETE")
        print("="*60)
        print(f"\nTotal document nodes created: {total_count}")
        print(f"Person nodes created: {len(self.persons)}")
        print(f"Area nodes created: {len(self.areas)}")
        
        # Show counts by label
        result = self._execute_query("""
            MATCH (n)
            RETURN labels(n) as labels, count(*) as count
            ORDER BY count DESC
        """)
        print("\nNodes by label:")
        for record in result:
            if record.get('row') and len(record['row']) >= 2:
                labels_list = record['row'][0] if isinstance(record['row'][0], list) else []
                labels = ':'.join(labels_list) if labels_list else 'Unknown'
                count = record['row'][1]
                print(f"   - {labels}: {count}")
        
        # Count relationships
        result = self._execute_query("""
            MATCH ()-[r]->()
            RETURN type(r) as type, count(*) as count
            ORDER BY count DESC
        """)
        print("\nRelationships by type:")
        for record in result:
            if record.get('row') and len(record['row']) >= 2:
                rel_type = record['row'][0]
                count = record['row'][1]
                print(f"   - {rel_type}: {count}")
    
    def verify_import(self):
        """Verify the import by running sample queries."""
        print("\n" + "="*60)
        print("VERIFICATION QUERIES")
        print("="*60)
        
        # Count all documents
        result = self._execute_query("MATCH (d:Document) RETURN count(d) as count")
        count = result[0]['row'][0] if result and result[0].get('row') and len(result[0]['row']) > 0 else 0
        print(f"\n📊 Total Document nodes: {count}")
        
        # Sample graph traversal - find all people in a war room
        print("\n🔍 Sample: People in War Rooms")
        result = self._execute_query("""
            MATCH (w:WarRoom)-[r]->(p:Person)
            RETURN w.team_name as war_room, type(r) as role, p.name as person
            LIMIT 5
        """)
        for record in result:
            if record.get('row'):
                print(f"   - {record['row'][0]} --[{record['row'][1]}]--> {record['row'][2]}")
        
        # Find people who work in multiple areas
        print("\n🔍 People working in multiple areas:")
        result = self._execute_query("""
            MATCH (p:Person)-[:WORKS_IN]->(a:Area)
            WITH p, count(a) as area_count, collect(a.name) as areas
            WHERE area_count > 1
            RETURN p.name, area_count, areas
            LIMIT 5
        """)
        for record in result:
            if record.get('row'):
                print(f"   - {record['row'][0]}: {record['row'][1]} areas")
        
        # Sample search path
        print("\n🔍 Graph path example (POC -> Person -> Area):")
        result = self._execute_query("""
            MATCH path = (poc:POC)-[:HAS_CONTACT]->(p:Person)-[:WORKS_IN]->(a:Area)
            RETURN poc.area_team as poc_area, p.name as contact, a.name as works_in
            LIMIT 3
        """)
        for record in result:
            if record.get('row'):
                print(f"   - POC[{record['row'][0]}] -> Person[{record['row'][1]}] -> Area[{record['row'][2]}]")
        
        # Show people with their roles and expertise tags
        print("\n🏷️  People by role labels:")
        result = self._execute_query("""
            MATCH (p:Person)
            WHERE p.roles IS NOT NULL AND size(p.roles) > 0
            RETURN p.name, p.roles, p.expertise, labels(p) as node_labels
            ORDER BY size(p.roles) DESC
            LIMIT 10
        """)
        for record in result:
            if record.get('row'):
                name = record['row'][0]
                roles = record['row'][1] or []
                expertise = record['row'][2] or []
                node_labels = record['row'][3] or []
                # Filter out 'Person' from labels to show only role labels
                role_labels = [l for l in node_labels if l != 'Person']
                print(f"   - {name}")
                print(f"     Labels: {role_labels}")
                print(f"     Roles: {roles}")
                print(f"     Expertise: {expertise}")
        
        # Count people by expertise
        print("\n📊 People by expertise area:")
        result = self._execute_query("""
            MATCH (p:Person)
            WHERE p.expertise IS NOT NULL
            UNWIND p.expertise as exp
            RETURN exp, count(DISTINCT p) as count
            ORDER BY count DESC
        """)
        for record in result:
            if record.get('row'):
                print(f"   - {record['row'][0]}: {record['row'][1]} people")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Import Welcome Season data into Neo4j via GraphQL')
    parser.add_argument('--uri', default='http://localhost:7474/graphql', help='GraphQL API URI')
    parser.add_argument('--user', default='admin', help='Neo4j username')
    parser.add_argument('--password', default='password', help='Neo4j password')
    parser.add_argument('--verify', action='store_true', help='Run verification queries after import')
    
    args = parser.parse_args()
    
    importer = Neo4jImporter(uri=args.uri, user=args.user, password=args.password)
    
    try:
        importer.import_all()
        
        if args.verify:
            importer.verify_import()
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise
    finally:
        importer.close()


if __name__ == "__main__":
    main()
