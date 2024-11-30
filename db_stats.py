import os
from typing import Dict, List, Tuple
from dotenv import load_dotenv
from neo4j import GraphDatabase
from tabulate import tabulate

load_dotenv()

class Neo4jStats:
    def __init__(self, uri: str, username: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))

    def close(self):
        self.driver.close()

    def get_node_stats(self) -> List[Dict]:
        with self.driver.session() as session:
            # Get node counts and labels using a simpler query
            result = session.run("""
                MATCH (n)
                UNWIND labels(n) as label
                WITH DISTINCT label, count(n) as count
                RETURN label, count
                ORDER BY label
            """)
            
            node_stats = []
            try:
                for record in result:
                    if record and record.get("label") is not None:
                        node_stats.append({
                            "Label": record["label"],
                            "Count": record["count"]
                        })
            except Exception as e:
                print(f"Warning: Error processing node labels: {str(e)}")

            # Get index information
            try:
                result = session.run("SHOW INDEXES")
                index_info = result.data() or []
            except Exception as e:
                print(f"Warning: Could not fetch index information: {str(e)}")
                index_info = []

            # Combine node counts with index information
            final_stats = []
            for node in node_stats:
                label = node["Label"]
                # Find all indexes for this label
                label_indexes = [idx for idx in index_info if label in (idx.get("labelsOrTypes") or [])]
                
                if label_indexes:
                    for idx in label_indexes:
                        final_stats.append({
                            "Node Label": label,
                            "Count": node["Count"],
                            "Index Name": idx.get("name", "N/A"),
                            "Index Type": idx.get("type", "N/A"),
                            "Indexed Property": ", ".join(idx.get("properties", []))
                        })
                else:
                    final_stats.append({
                        "Node Label": label,
                        "Count": node["Count"],
                        "Index Name": "No Index",
                        "Index Type": "N/A",
                        "Indexed Property": "N/A"
                    })
            
            return final_stats or []

    def get_relationship_stats(self) -> List[Dict]:
        with self.driver.session() as session:
            result = session.run("""
                MATCH (start)-[r]->(end)
                WITH type(r) as relType,
                     CASE WHEN size(labels(start)) > 0 THEN head(labels(start)) 
                          ELSE '(no label)' END as startLabel,
                     CASE WHEN size(labels(end)) > 0 THEN head(labels(end)) 
                          ELSE '(no label)' END as endLabel
                WITH relType, startLabel, endLabel, count(*) as count
                RETURN relType as relationshipType, startLabel, endLabel, count
                ORDER BY count DESC
            """)
            
            rel_stats = []
            try:
                for record in result:
                    if record and record.get("relationshipType") is not None:
                        rel_stats.append({
                            "Relationship": record["relationshipType"],
                            "Start Node": record["startLabel"],
                            "End Node": record["endLabel"],
                            "Count": record["count"]
                        })
            except Exception as e:
                print(f"Warning: Error processing relationships: {str(e)}")
            
            return rel_stats or []

def get_db_choice() -> Tuple[str, str, str]:
    print("\nAvailable databases:")
    print("1. Local Database")
    print("2. Remote Database")
    
    while True:
        choice = input("\nSelect database (1 or 2): ").strip()
        if choice == "1":
            return (
                os.getenv("LOCAL_NEO4J_URI"),
                os.getenv("LOCAL_NEO4J_USERNAME"),
                os.getenv("LOCAL_NEO4J_PASSWORD")
            )
        elif choice == "2":
            return (
                os.getenv("NEO4J_URI"),
                os.getenv("NEO4J_USERNAME"),
                os.getenv("NEO4J_PASSWORD")
            )
        else:
            print("Invalid choice. Please select 1 or 2.")

def main():
    uri, username, password = get_db_choice()
    stats = None
    
    try:
        stats = Neo4jStats(uri, username, password)
        
        # Get and display node statistics
        node_stats = stats.get_node_stats()
        print("\nNode Statistics:")
        print(tabulate(node_stats, headers="keys", tablefmt="grid"))
        
        # Get and display relationship statistics
        rel_stats = stats.get_relationship_stats()
        print("\nRelationship Statistics:")
        print(tabulate(rel_stats, headers="keys", tablefmt="grid"))
        
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        if stats is not None:
            stats.close()

if __name__ == "__main__":
    main()
