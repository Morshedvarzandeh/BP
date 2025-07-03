import pandas as pd
from neo4j import GraphDatabase, exceptions
import time

# --- Connection Details ---
# Use the URI copied from the AuraDB Driver screen
URI = "neo4j+s://d9211aeb.databases.neo44j.io"

# Your Neo4j username (always 'neo4j' for AuraDB)
USER = "neo4j" 

# IMPORTANT: Replace "YOUR_ACTUAL_AURA_PASSWORD" with the password you generated/reset from the AuraDB console!
PWD = "YOUR_ACTUAL_AURA_PASSWORD" 

# --- Configuration ---
BATCH_SIZE = 50
DELAY_BETWEEN_BATCHES = 0.2 # in seconds

# --- Data Loading ---
try:
    # Assuming you've corrected these URLs to the raw ones with tokens if your repo is private
    nodes = pd.read_csv("https://raw.githubusercontent.com/Morshedvarzandeh/BP/main/data/nodes.csv")
    rels = pd.read_csv("https://raw.githubusercontent.com/Morshedvarzandeh/BP/main/data/relations.csv")
    print("CSV files loaded successfully from GitHub.")
except Exception as e:
    print(f"Error reading CSV files from GitHub: {e}")
    exit()

# --- Data Cleaning ---
nodes.columns = nodes.columns.str.strip()
rels.columns = rels.columns.str.strip()
nodes.rename(columns={'id:ID': 'id', 'name:STRING': 'name', ':LABEL': 'label'}, inplace=True)
rels.rename(columns={':START_ID': 'start_id', ':END_ID': 'end_id', ':TYPE': 'type'}, inplace=True)


# --- Database Interaction ---
driver = None
try:
    driver = GraphDatabase.driver(URI, auth=(USER, PWD), keep_alive=True) # Use USER and PWD
    driver.verify_connectivity()
    print("Database connection verified.")

    with driver.session(database="neo4j") as session: # Database name is 'neo4j' for AuraDB
        print("Creating constraint...")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (p:Process) REQUIRE p.id IS UNIQUE")
        
        print("Loading nodes...")
        session.run("""
            UNWIND $rows AS row
            MERGE (p:Process {id: toInteger(row.id)})
            SET p.name = row.name
        """, rows=nodes.to_dict("records"))

        print("Loading relationships...")
        unique_rel_types = rels['type'].unique()

        for rel_type in unique_rel_types:
            print(f"  -> Processing relationships of type: [{rel_type}]")
            
            rels_of_this_type = rels[rels['type'] == rel_type]
            total_rels = len(rels_of_this_type)
            
            for i in range(0, total_rels, BATCH_SIZE):
                batch = rels_of_this_type.iloc[i:i + BATCH_SIZE]
                
                with session.begin_transaction() as tx:
                    query = f"""
                        UNWIND $rels AS rel
                        MATCH (a:Process {{id: toInteger(rel.start_id)}})
                        MATCH (b:Process {{id: toInteger(rel.end_id)}})
                        MERGE (a)-[:`{rel_type}`]->(b)
                    """
                    tx.run(query, rels=batch.to_dict("records"))

                print(f"    -> Committed batch {i//BATCH_SIZE + 1} ({i+len(batch)} of {total_rels})")
                time.sleep(DELAY_BETWEEN_BATCHES)

    print("\n✅ Data loaded successfully!")

except exceptions.ServiceUnavailable as e:
    print(f"\n❌ A 'ServiceUnavailable' error occurred: {e}")
    print("\nThis means the database is unreachable or not responding. Double-check your URI, password, and AuraDB instance status.")
    print("If your AuraDB instance just started, give it a minute or two to fully become available.")

except Exception as e:
    print(f"\n❌ An unexpected error occurred: {e}")

finally:
    if driver:
        driver.close()
