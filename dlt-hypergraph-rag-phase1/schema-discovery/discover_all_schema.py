
"""
Enhanced schema discovery using dlt with SQLAlchemy
This integrates dlt's capabilities with our schema discovery
"""

import dlt
from dlt.sources.sql_database import sql_database
from sqlalchemy import create_engine, inspect, text
import json
from datetime import datetime
from typing import Dict, Any
import config

# Import SchemaDiscovery from the main module
from schema_discovery import SchemaDiscovery

class DLTSchemaDiscovery:
    """Schema discovery using dlt's SQL database source"""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.engine = create_engine(database_url)
        
    def discover_with_dlt(self) -> Dict[str, Any]:
        """Use dlt's built-in discovery"""
        try:
            # Create dlt source
            source = sql_database(self.database_url)
            
            # Discover resources (tables)
            schema_info = {
                "dlt_resources": {},
                "discovered_at": datetime.now().isoformat()
            }
            
            # List all resources that dlt discovered
            for resource_name in source.resources.keys():
                resource = source.resources[resource_name]
                
                # Get resource schema info
                resource_info = {
                    "name": resource_name,
                    "write_disposition": getattr(resource, 'write_disposition', None)
                }
                
                # Try to get columns if available
                if hasattr(resource, 'columns'):
                    resource_info["columns"] = list(resource.columns.keys())
                
                # Try to get primary key if available
                if hasattr(resource, 'primary_key'):
                    resource_info["primary_key"] = resource.primary_key
                
                schema_info["dlt_resources"][resource_name] = resource_info
            
            return schema_info
            
        except Exception as e:
            print(f"Error with dlt discovery: {e}")
            return {}
    
    def get_foreign_key_graph(self) -> Dict[str, Any]:
        """Create a graph of foreign key relationships"""
        graph = {}
        
        with self.engine.connect() as conn:
            inspector = inspect(self.engine)
            
            # Initialize graph for all tables
            for table_name in inspector.get_table_names():
                graph[table_name] = {
                    "incoming": [],
                    "outgoing": []
                }
            
            # Build outgoing foreign keys
            for table_name in inspector.get_table_names():
                for fk in inspector.get_foreign_keys(table_name):
                    graph[table_name]["outgoing"].append({
                        "to_table": fk["referred_table"],
                        "columns": fk["constrained_columns"],
                        "references": fk["referred_columns"],
                        "constraint_name": fk.get("name", "unnamed")
                    })
            
            # Build incoming foreign keys
            for table_name in inspector.get_table_names():
                for other_table in inspector.get_table_names():
                    if other_table != table_name:
                        for fk in inspector.get_foreign_keys(other_table):
                            if fk["referred_table"] == table_name:
                                graph[table_name]["incoming"].append({
                                    "from_table": other_table,
                                    "columns": fk["constrained_columns"],
                                    "references": fk["referred_columns"],
                                    "constraint_name": fk.get("name", "unnamed")
                                })
        
        return graph
    
    def generate_complete_report(self) -> Dict[str, Any]:
        """Generate complete schema report combining dlt and SQLAlchemy"""
        
        # Use our existing SchemaDiscovery
        standard_discovery = SchemaDiscovery(self.database_url)
        
        if standard_discovery.connect():
            print("Running standard schema discovery...")
            standard_info = standard_discovery.discover_all()
            
            # Add dlt-specific info
            print("Running dlt discovery...")
            dlt_info = self.discover_with_dlt()
            
            # Add foreign key graph
            print("Building foreign key graph...")
            fk_graph = self.get_foreign_key_graph()
            
            # Combine all info
            complete_info = {
                **standard_info,
                "dlt_resources": dlt_info.get("dlt_resources", {}),
                "foreign_key_graph": fk_graph
            }
            
            # Save complete report
            output_file = config.OUTPUT_DIR / f"complete_schema_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(complete_info, f, indent=2, default=str)
            
            print(f"\n✅ Complete schema report saved to: {output_file}")
            
            # Print FK relationships summary
            print("\n🔗 Foreign Key Relationships:")
            for table, relations in fk_graph.items():
                if relations["outgoing"]:
                    print(f"   {table} -> {[rel['to_table'] for rel in relations['outgoing']]}")
                if relations["incoming"]:
                    print(f"   {table} <- {[rel['from_table'] for rel in relations['incoming']]}")
            
            # Print dlt resources summary
            if dlt_info.get("dlt_resources"):
                print(f"\n📦 dlt Resources: {len(dlt_info['dlt_resources'])}")
                for resource_name in dlt_info["dlt_resources"].keys():
                    print(f"   - {resource_name}")
            
            return complete_info
        else:
            print("❌ Failed to connect to database")
            return {}


if __name__ == "__main__":
    discovery = DLTSchemaDiscovery(config.DATABASE_URL)
    discovery.generate_complete_report()
