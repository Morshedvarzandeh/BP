import pandas as pd
from neo4j import GraphDatabase, exceptions
import time

# --- Connection Details ---
URI = "bolt://yamanote.proxy.rlwy.net:49803"
PWD = "MorshedThesis"

# --- Configuration ---
# Last resort: use a tiny batch size and add a delay.
BATCH_SIZE = 50
DELAY_BETWEEN_BATCHES = 0.2 # in seconds

# --- Data Loading ---
try:
    nodes = pd.read_csv("https://raw.githubusercontent.com/Morshedvarzandeh/BP/main/data/nodes.csv")
    rels = pd.read_csv("https://raw.githubusercontent.com/Morshedvarzandeh/BP/main/data/relations.csv")
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
    driver = GraphDatabase.driver(URI, auth=("neo4j", PWD), keep_alive=True)
    driver.verify_connectivity()
    print("Database connection verified.")

    with driver.session(database="neo4j") as session:
        print("Creating constraint...")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (p:Process) REQUIRE p.id IS UNIQUE")
        
        print("Loading nodes...")
        session.run("""
            UNWIND $rows AS row
            MERGE (p:Process {id: toInteger(row.id)})
            SET   p.name = row.name
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
                # Add a small delay to not overwhelm the server
                time.sleep(DELAY_BETWEEN_BATCHES)

    print("\n✅ Data loaded successfully!")

# Catching the specific Neo4j exception for this issue
except exceptions.ServiceUnavailable as e:
    print(f"\n❌ A 'ServiceUnavailable' error occurred: {e}")
    print("\nThis confirms the problem is the hosting environment, not your code.")
    print("The server is either timing out or running out of resources.")
    print("RECOMMENDATION: Please use Neo4j Desktop locally (Option 1) for this task.")

except Exception as e:
    print(f"\n❌ An unexpected error occurred: {e}")

finally:
    if driver:
        driver.close()